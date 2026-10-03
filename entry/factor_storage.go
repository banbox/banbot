package entry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/goods"
	"github.com/banbox/banbot/orm"
	runtimectx "github.com/banbox/banbot/runtime"
	"github.com/shopspring/decimal"
)

// Preflight is side-effect free: neither a database nor execution account is
// opened before the definition, range, budgets and PIT policy are accepted.
func preflightFactorStorageConfig(spec *config.RunSpec, c *runner.Config) error {
	u := spec.Config()
	pit, _ := u.Data["pit_policy"].(string)
	if pit != "static-approximation" && pit != "strict" {
		return errors.New("factor: ordinary historical storage requires explicit data.pit_policy: static-approximation; strict requires an immutable revision reader")
	}
	if pit == "strict" {
		return errors.New("factor: strict PIT cannot be proven by the ordinary latest-value storage adapter; use an immutable revision archive or an attested HistoricalInputFactory")
	}
	snapshot, err := spec.RuntimeSnapshot()
	if err != nil {
		return err
	}
	cfg := snapshot.View()
	if cfg.TimeRange == nil || cfg.TimeRange.StartMS <= 0 || cfg.TimeRange.EndMS < cfg.TimeRange.StartMS {
		return errors.New("factor: historical storage requires a valid time_range")
	}
	if c.MaxRecords <= 0 || c.MaxPending <= 0 || c.DecisionInterval <= 0 || c.LatencyMS <= 0 || c.ExpiryMS <= c.LatencyMS {
		return errors.New("factor: positive bounded historical replay configuration required")
	}
	if c.Manifest.Costs.FundingPolicy != "explicit-zero" && c.Manifest.Costs.FundingPolicy != "required-stream" {
		return errors.New("factor: execution.funding_policy must explicitly declare explicit-zero or required-stream")
	}
	if c.Manifest.Costs.FundingPolicy == "required-stream" && c.FundingSource == "" {
		return errors.New("factor: required funding source must be declared")
	}
	c.Snapshot.VisibilityPolicy = pit
	if c.Snapshot.AdjustmentVersion == "" {
		c.Snapshot.AdjustmentVersion = "raw"
	}
	if c.Prices.Source == "" {
		if c.Mode == runner.Events {
			c.Prices = runner.PriceStream{Source: orm.SeriesSourceKline, Frequency: "1m", Field: "close"}
		} else {
			c.Prices = runner.PriceStream{Source: c.Factor.Source, Frequency: c.Factor.Frequency, Field: c.Factor.Field}
		}
	}
	if c.Mode == runner.Events && c.Prices.Frequency != "event" && c.Prices.Frequency != "1m" {
		return errors.New("factor: events storage replay requires tick or 1m observable prices")
	}
	if c.Mode == runner.Events {
		leverage := cfg.Leverage
		if leverage <= 0 {
			leverage = 1
		}
		accountNAV := c.AccountInitialNAV
		if accountNAV == 0 {
			accountNAV = c.InitialNAV
		}
		if c.Execution.MarginRate.IsZero() {
			c.Execution.MarginRate = decimal.NewFromInt(1).Div(decimal.NewFromFloat(leverage))
		}
		if c.Execution.MaxAccountMargin.IsZero() {
			c.Execution.MaxAccountMargin = decimal.NewFromFloat(accountNAV)
		}
		if c.Execution.MaxVirtualGross.IsZero() {
			c.Execution.MaxVirtualGross = decimal.NewFromFloat(accountNAV * leverage)
		}
		if c.Execution.StrategyGrossLimit.IsZero() {
			c.Execution.StrategyGrossLimit = decimal.NewFromFloat(c.InitialNAV * leverage)
		}
	}
	_, _, compileErr := runner.CompileDefinition(*c)
	return compileErr
}

type factorStorageInput struct {
	runner.HistoricalInputFactory
	assembly      *factorStorageAssembly
	plan          *data.SubscriptionPlan
	bootstrapOnce sync.Once
	bootstrapErr  error
}

func (f *factorStorageInput) InputBudgetReport() any { return f.plan.BudgetReport() }

func (f *factorStorageInput) Open(ctx context.Context, c runner.Config, chunk runner.Chunk) (runner.HistoricalInput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := f.plan.Validate(); err != nil {
		return nil, err
	}
	f.bootstrapOnce.Do(func() {
		if f.assembly.bootstrap != nil {
			f.bootstrapErr = f.assembly.bootstrap(ctx, f.plan)
		}
	})
	if f.bootstrapErr != nil {
		return nil, f.bootstrapErr
	}
	if err := f.plan.Validate(); err != nil {
		return nil, err
	}
	if c.Snapshot.Schemas[orm.SeriesSourceKline] != "" {
		actual, err := f.assembly.klineSchemaHash(ctx, f.plan)
		if err != nil {
			return nil, err
		}
		if actual != c.Snapshot.Schemas[orm.SeriesSourceKline] {
			return nil, errors.New("factor: kline schema changed after subscription compilation")
		}
	}
	return f.HistoricalInputFactory.Open(ctx, c, chunk)
}

type factorStorageAssembly struct {
	spec      *config.RunSpec
	runtime   *runtimectx.Runtime
	requests  []data.SubscriptionRequest
	query     func(context.Context, orm.Subscription, int64, int64, int) ([]*orm.DataSeries, error)
	bootstrap func(context.Context, *data.SubscriptionPlan) error
}

// prepareFactorStorageInputs keeps the source session alive until all replay
// consumers have stopped and joined. It never exports a temporary archive.
func prepareFactorStorageInputs(ctx context.Context, args *config.CmdArgs, spec *config.RunSpec, configs []runner.Config) ([]runner.Config, func() error, error) {
	if ctx == nil {
		return nil, nil, errors.New("factor: historical storage context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	needed := false
	for i := range configs {
		if len(configs[i].Chunks) == 0 {
			needed = true
			if err := preflightFactorStorageConfig(spec, &configs[i]); err != nil {
				return nil, nil, err
			}
		}
	}
	if !needed {
		return configs, func() error { return nil }, nil
	}
	session, snapshot, openErr := openExplicitEntrySessionFromSpecContext(ctx, args, spec, "factor", "historical")
	if openErr != nil {
		return nil, nil, openErr
	}
	cleanup := func() error { session.close(); return session.process.CloseError() }
	if err := session.ensureExchange(snapshot, core.RunModeBackTest); err != nil {
		return nil, nil, errors.Join(err, cleanup())
	}
	task, err := session.newStorageRuntime(snapshot, core.RunModeBackTest, snapshot.View().TimeRange.StartMS)
	if err != nil {
		return nil, nil, errors.Join(err, cleanup())
	}
	pairs, pairErr := goods.RefreshPairListWithRuntimeDeps(&goods.RuntimeDeps{Core: task.Core, Clock: task.Clock, Config: snapshot.View(), DataDir: snapshot.DataDir, Symbols: task.Symbols, Storage: task.Storage, Exchange: task.Exchange}, task.Clock.TimeMS())
	if pairErr != nil {
		return nil, nil, errors.Join(pairErr, cleanup())
	}
	if len(pairs) == 0 {
		return nil, nil, errors.Join(errors.New("factor: historical storage universe has no ordinary pairs"), cleanup())
	}
	repo := orm.NewSeriesRepo(task.Storage)
	query := func(readCtx context.Context, sub orm.Subscription, start, end int64, limit int) ([]*orm.DataSeries, error) {
		return task.Catalog.ReadSubscriptionPageWithRuntimeDeps(readCtx, repo, task.DataDeps(), sub, start, end, limit)
	}
	prepared, assembleErr := assembleFactorStorageInputs(ctx, spec, task, configs, pairs, query)
	if assembleErr != nil {
		return nil, nil, errors.Join(assembleErr, cleanup())
	}
	for i := range prepared {
		c := &prepared[i]
		if c.Mode != runner.Events || len(c.Chunks) > 0 {
			continue
		}
		units := map[int32]execution.Instrument{}
		for sid, unit := range c.Execution.Instruments {
			units[sid] = unit
		}
		for _, sid := range append(slices.Clone(c.Snapshot.Universe.Tracked), c.Snapshot.Universe.Investable...) {
			if _, exists := units[sid]; exists {
				continue
			}
			symbol := task.Symbols.GetSymbolByID(sid)
			market, marketErr := task.Exchange.GetMarket(symbol.Symbol)
			if marketErr != nil {
				return nil, nil, errors.Join(marketErr, cleanup())
			}
			info := task.Exchange.Info()
			info.CurrByCodeLock.Lock()
			currency := info.CurrenciesByCode[market.Settle]
			info.CurrByCodeLock.Unlock()
			unit, unitErr := execution.InstrumentFromBanexgMarket(symbol.Symbol, market, currency)
			if unitErr != nil {
				return nil, nil, errors.Join(unitErr, cleanup())
			}
			if unit.SettlementCurrency != c.Manifest.Currency {
				return nil, nil, errors.Join(errors.New("factor: market settlement metadata differs from configured currency"), cleanup())
			}
			units[sid] = unit
		}
		c.Execution.Instruments = units
	}
	return prepared, cleanup, nil
}

func assembleFactorStorageInputs(ctx context.Context, spec *config.RunSpec, task *runtimectx.Runtime, configs []runner.Config, pairs []string, query func(context.Context, orm.Subscription, int64, int64, int) ([]*orm.DataSeries, error)) ([]runner.Config, error) {
	if task == nil || task.Catalog == nil || task.Symbols == nil || query == nil {
		return nil, errors.New("factor: storage runtime, source catalog, symbols and page reader required")
	}
	configs = slices.Clone(configs)
	u := spec.Config()
	policies := []*config.PolicyV2{}
	for _, policy := range u.RunPolicy {
		if policy.Engine == config.EngineFactor {
			policies = append(policies, policy)
		}
	}
	if len(policies) != len(configs) {
		return nil, errors.New("factor: storage strategies must match policy declarations")
	}
	assembly := &factorStorageAssembly{spec: spec, runtime: task, query: query}
	if task.Storage != nil {
		assembly.bootstrap = func(ctx context.Context, plan *data.SubscriptionPlan) error {
			return plan.Bootstrap(ctx, orm.NewSeriesRepo(task.Storage))
		}
	}
	for index := range configs {
		c := &configs[index]
		if len(c.Chunks) > 0 {
			continue
		}
		c.Snapshot = factor.CloneSnapshotSpec(c.Snapshot)
		selected := pairs
		if len(policies[index].Pairs) > 0 {
			selected = policies[index].Pairs
		}
		ids := []int32{}
		symbols := map[int32]string{}
		for _, pair := range selected {
			symbol, err := task.Symbols.GetExSymbolCur(pair)
			if err != nil {
				return nil, err
			}
			ids = append(ids, symbol.ID)
			symbols[symbol.ID] = symbol.Symbol
		}
		if len(ids) == 0 {
			return nil, errors.New("factor: storage policy has an empty universe")
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		ids = slices.Compact(ids)
		if c.Snapshot.Universe.Version == "" {
			raw, _ := json.Marshal(ids)
			digest := sha256.Sum256(raw)
			c.Snapshot.Universe = factor.Universe{Version: "storage-static:" + hex.EncodeToString(digest[:]), Investable: ids, Reference: ids, Tradable: ids, Evaluation: ids, Tracked: ids, Static: true}
		}
		if c.Snapshot.SIDMap == nil {
			c.Snapshot.SIDMap = map[int32]string{}
		}
		requiredIDs := slices.Clone(c.Snapshot.Universe.Tracked)
		requiredIDs = append(requiredIDs, c.Snapshot.Universe.Investable...)
		requiredIDs = append(requiredIDs, c.Snapshot.Universe.Reference...)
		requiredIDs = append(requiredIDs, c.Snapshot.Universe.Evaluation...)
		for _, sid := range requiredIDs {
			symbol := task.Symbols.GetSymbolByID(sid)
			if symbol == nil {
				return nil, fmt.Errorf("factor: storage SID %d missing symbol identity", sid)
			}
			if declared := c.Snapshot.SIDMap[sid]; declared != "" && declared != symbol.Symbol {
				return nil, fmt.Errorf("factor: storage SID %d identity mismatch", sid)
			}
			c.Snapshot.SIDMap[sid] = symbol.Symbol
		}
		plan, _, err := runner.CompileDefinition(*c)
		if err != nil {
			return nil, err
		}
		factorIDs := slices.Compact(append(slices.Clone(c.Snapshot.Universe.Reference), c.Snapshot.Universe.Investable...))
		add := func(source, freq string, fields []string, warm int, sids []int32, consumer string, maxAge int64) error {
			for _, sid := range sids {
				symbol := task.Symbols.GetSymbolByID(sid)
				if symbol == nil {
					return fmt.Errorf("factor: storage SID %d missing source identity", sid)
				}
				assembly.requests = append(assembly.requests, data.SubscriptionRequest{Subscription: orm.Subscription{Source: source, TimeFrame: freq, Fields: fields, WarmupNum: warm, ExSymbol: symbol}, Consumer: consumer, Required: true, MaxAgeMS: maxAge})
			}
			return nil
		}
		for _, input := range plan.Inputs() {
			if err := add(input.Source, input.Frequency, input.Fields, input.WarmupLength, factorIDs, "factor:"+c.StrategyID, input.MaxAge); err != nil {
				return nil, err
			}
		}
		executionIDs := append(slices.Clone(c.Snapshot.Universe.Investable), c.Snapshot.Universe.Tracked...)
		if err := add(c.Prices.Source, c.Prices.Frequency, []string{c.Prices.Field}, 0, executionIDs, "price:"+c.StrategyID, 0); err != nil {
			return nil, err
		}
		if c.Manifest.Costs.FundingPolicy == "required-stream" {
			if err := add(c.FundingSource, "event", []string{"rate"}, 0, executionIDs, "funding:"+c.StrategyID, 0); err != nil {
				return nil, err
			}
		}
	}
	return assembly.install(ctx, configs, nil)
}

func (a *factorStorageAssembly) install(ctx context.Context, configs []runner.Config, extra []*orm.Subscription) ([]runner.Config, error) {
	u := a.spec.Config()
	snapshot, snapshotErr := a.spec.RuntimeSnapshot()
	if snapshotErr != nil {
		return nil, snapshotErr
	}
	timeRange := snapshot.View().TimeRange
	requests := slices.Clone(a.requests)
	for _, sub := range extra {
		if sub != nil {
			requests = append(requests, data.SubscriptionRequest{Subscription: *sub, Consumer: "legacy-time-series", Required: true})
		}
	}
	namespace := a.runtime.ID
	if a.runtime.Storage != nil {
		namespace = a.runtime.Storage.Identity()
	}
	if configured, ok := u.Data["namespace"].(string); ok {
		namespace += "/namespace/" + configured
	}
	page, prefetch := 20000, 0
	pageBytes := int64(0)
	if value, ok := u.Data["page_rows"]; ok {
		if err := decodeFactorFields(map[string]any{"PageRows": value}, &struct{ PageRows *int }{&page}); err != nil {
			return nil, err
		}
	}
	if value, ok := u.Data["prefetch_rows"]; ok {
		if err := decodeFactorFields(map[string]any{"PrefetchRows": value}, &struct{ PrefetchRows *int }{&prefetch}); err != nil {
			return nil, err
		}
	} else {
		for _, c := range configs {
			if len(c.Chunks) == 0 && (prefetch == 0 || c.MaxRecords/2 < prefetch) {
				prefetch = c.MaxRecords / 2
			}
		}
	}
	if value, ok := u.Data["page_bytes"]; ok {
		if err := decodeFactorFields(map[string]any{"PageBytes": value}, &struct{ PageBytes *int64 }{&pageBytes}); err != nil {
			return nil, err
		}
	}
	plan, err := a.runtime.Catalog.CompileSubscriptionPlan(ctx, requests, data.SubscriptionPlanOptions{Namespace: namespace, AnchorMS: timeRange.StartMS, EndMS: timeRange.EndMS, PageRows: page, PrefetchRows: prefetch, PageBytes: pageBytes})
	if err != nil {
		return nil, err
	}
	streams := []runner.StorageStream{}
	meta := plan.SourceMetadata()
	if _, exists := meta[orm.SeriesSourceKline]; exists {
		schema, err := a.klineSchemaHash(ctx, plan)
		if err != nil {
			return nil, err
		}
		meta[orm.SeriesSourceKline] = data.SubscriptionSourceMetadata{Version: "storage-latest:" + schema, SchemaHash: schema}
	}
	for _, stream := range plan.Streams() {
		identity := meta[stream.Subscription.Source]
		if identity.Version == "" {
			identity.Version = "storage-latest:" + identity.SchemaHash
		}
		meta[stream.Subscription.Source] = identity
		streams = append(streams, runner.StorageStream{Subscription: stream.Subscription, WarmupStartMS: stream.WarmupStartMS, SourceVersion: identity.Version, SchemaHash: identity.SchemaHash})
	}
	// One source identity is shared across every frequency of that source.
	for i := range streams {
		identity := meta[streams[i].Subscription.Source]
		streams[i].SourceVersion = identity.Version
		streams[i].SchemaHash = identity.SchemaHash
	}
	options := plan.Options()
	for _, c := range configs {
		if len(c.Chunks) == 0 && c.MaxRecords < options.PrefetchRows+len(streams) {
			return nil, errors.New("factor: max_records must cover compiled prefetch plus stream snapshots")
		}
	}
	queryPage := func(ctx context.Context, sub orm.Subscription, start, end int64, limit int) ([]*orm.DataSeries, error) {
		ctx = orm.WithSeriesReadByteLimit(ctx, options.PageBytes)
		rows, err := a.query(ctx, sub, start, end, limit)
		if err == nil {
			err = orm.CheckDataSeriesBytes(ctx, rows)
		}
		if err != nil {
			return nil, err
		}
		return rows, nil
	}
	factory, err := runner.NewStorageInputFactory(runner.StorageInputOptions{Namespace: namespace, PITPolicy: "static-approximation", FromMS: timeRange.StartMS, ToMS: timeRange.EndMS, PageRows: options.PageRows, PrefetchRows: options.PrefetchRows, Streams: streams, QueryPage: queryPage})
	if err != nil {
		return nil, err
	}
	configs = slices.Clone(configs)
	input := &factorStorageInput{HistoricalInputFactory: factory, assembly: a, plan: plan}
	for i := range configs {
		c := &configs[i]
		if len(c.Chunks) > 0 {
			continue
		}
		c.Snapshot = factor.CloneSnapshotSpec(c.Snapshot)
		if c.Snapshot.Schemas == nil {
			c.Snapshot.Schemas = map[string]string{}
		}
		if c.Snapshot.SourceVersions == nil {
			c.Snapshot.SourceVersions = map[string]string{}
		}
		for source, identity := range meta {
			c.Snapshot.Schemas[source] = identity.SchemaHash
			c.Snapshot.SourceVersions[source] = identity.Version
		}
		c.HistoricalInput = input
		c.ComputationContext.DataNamespace = namespace
	}
	return configs, nil
}

func (a *factorStorageAssembly) klineSchemaHash(ctx context.Context, plan *data.SubscriptionPlan) (string, error) {
	fields := map[string][]string{}
	for _, stream := range plan.Streams() {
		if stream.Subscription.Source == orm.SeriesSourceKline {
			tf := stream.Subscription.TimeFrame
			fields[tf] = orm.MergeSeriesFields(fields[tf], stream.Subscription.Fields, stream.Subscription.SeriesFields)
		}
	}
	timeframes := make([]string, 0, len(fields))
	for tf := range fields {
		timeframes = append(timeframes, tf)
		sort.Strings(fields[tf])
	}
	sort.Strings(timeframes)
	schemas := make([]orm.KlineProjectionSchema, 0, len(fields))
	prefix := "projection-metadata-only:"
	if a.runtime.Storage != nil && len(fields) > 0 {
		session, conn, err := a.runtime.Storage.Conn(ctx)
		if err != nil {
			return "", err
		}
		defer conn.Release()
		for _, tf := range timeframes {
			schema, err := session.ReadKlineProjectionSchema(ctx, tf, fields[tf])
			if err != nil {
				return "", err
			}
			schemas = append(schemas, schema)
		}
		prefix = ""
	}
	raw, err := json.Marshal(struct {
		Namespace, Source string
		Projection        map[string][]string
		Schemas           []orm.KlineProjectionSchema
	}{plan.Options().Namespace, orm.SeriesSourceKline, fields, schemas})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return prefix + hex.EncodeToString(digest[:]), nil
}

// Called after actual mixed TS jobs exist, before opening any replay reader.
// This rebuilds the immutable union plan without repeating strategy startup.
func extendFactorStorageInputs(ctx context.Context, configs []runner.Config, subs []*orm.Subscription) ([]runner.Config, error) {
	for _, c := range configs {
		if input, ok := c.HistoricalInput.(*factorStorageInput); ok {
			return input.assembly.install(ctx, configs, subs)
		}
	}
	return configs, nil
}
