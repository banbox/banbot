package entry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sort"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/opt"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	runtimepkg "github.com/banbox/banbot/runtime"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/shopspring/decimal"
)

type replayAccount struct {
	key            execution.AccountKey
	opts           execution.SharedExecutionOptions
	base           runner.Config
	seededStrategy string
	paperSink      *runner.AccountSink
}

func mixedTimeFrameScores(rt *runtimepkg.Runtime, pairs []string) map[string]map[string]float64 {
	scores := make(map[string]map[string]float64, len(pairs))
	for _, pair := range pairs {
		scores[pair] = map[string]float64{}
	}
	for _, policy := range rt.Config.View().RunPolicy {
		for _, tf := range rt.Strategies.NewStrategy(policy).RunTimeFrames {
			for _, pair := range pairs {
				scores[pair][tf] = 1
			}
		}
	}
	return scores
}

// Mixed replay keeps the ordinary TS jobs and their callbacks. The historical
// input advances both engines at one visibility boundary and one account owner.
func runMixedFactorConfigs(ctx context.Context, spec *config.RunSpec, configs []runner.Config, writer io.Writer) (results []runner.Result, resultErr error) {
	snapshot, err := spec.RuntimeSnapshot()
	if err != nil {
		return nil, err
	}
	exchange, err := exg.NewForRuntime(snapshot, true)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := exchange.Close(); closeErr != nil {
			resultErr = errors.Join(resultErr, closeErr)
		}
	}()
	if err := initializeExplicitExchange(exchange, snapshot, true, core.RunModeBackTest, nil); err != nil {
		return nil, err
	}
	return replayMixedEngines(ctx, spec, snapshot, exchange, configs, writer)
}

func replayMixedEngines(ctx context.Context, spec *config.RunSpec, snapshot *config.Snapshot, exchange banexg.BanExchange, configs []runner.Config, writer io.Writer) (results []runner.Result, resultErr error) {
	if len(configs) == 0 || configs[0].Mode != runner.Events {
		return nil, errors.New("mixed backtest requires execution.mode: events")
	}
	u := spec.Config()
	p := runtimepkg.NewProcess()
	defer func() { p.Close(); resultErr = errors.Join(resultErr, p.CloseError()) }()
	accounts := map[string]replayAccount{}
	sinks, cleanup, err := runner.NewPaperSinksWithAccounts(ctx, configs, func(key execution.AccountKey, opts execution.SharedExecutionOptions) (*execution.SharedAccountBorrow, error) {
		accounts[key.Account] = replayAccount{key: key, opts: opts}
		return p.BorrowAccount(key, opts)
	})
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, cleanup()) }()
	// TS-only accounts borrow the same process account core, without creating a
	// factor Session or synthetic strategy job for those accounts.
	for _, policy := range u.RunPolicy {
		account := policyAccount(u, policy)
		if policy.Engine != config.EngineTimeSeries {
			continue
		}
		if _, exists := accounts[account]; exists {
			continue
		}
		base := configs[0]
		base.AccountID = account
		base.StrategyID = policy.ID
		if base.StrategyID == "" {
			base.StrategyID = policy.RunPolicyConfig.ID()
		}
		capital := base.AccountInitialNAV
		if capital == 0 {
			capital = base.InitialNAV
		}
		base.AccountInitialNAV, base.InitialNAV = capital, capital*policyCapitalWeight(u, policy, account)
		base.Execution.StorePath, base.Execution.SenderLeaseDir, base.Execution.HistoryPath = "", "", ""
		base.Execution.HistoryPath, err = accountHistoryPath(spec, account)
		if err != nil {
			return nil, err
		}
		fields := u.AccountExecution[account]
		settings := map[string]any{}
		for key, value := range fields {
			switch key {
			case "margin_rate", "max_account_margin", "max_virtual_gross", "strategy_gross_limit", "instruments":
				settings[key] = value
			}
		}
		if err := decodeFactorFields(settings, &base.Execution); err != nil {
			return nil, err
		}
		paperSink, close, err := runner.NewPaperSinkWithAccount(ctx, base, func(key execution.AccountKey, opts execution.SharedExecutionOptions) (*execution.SharedAccountBorrow, error) {
			accounts[account] = replayAccount{key: key, opts: opts, base: base, seededStrategy: base.StrategyID}
			return p.BorrowAccount(key, opts)
		})
		if err != nil {
			return nil, err
		}
		owner := accounts[account]
		owner.paperSink = paperSink
		accounts[account] = owner
		defer func() { resultErr = errors.Join(resultErr, close()) }()
	}
	type consumer struct {
		task    *runtimepkg.Runtime
		trader  biz.Trader
		quotes  map[string]execution.VisibleQuote
		base    runner.Config
		account string
	}
	var consumers []consumer
	var subscriptions []*orm.Subscription
	tsUnits := map[int32]execution.Instrument{}
	tsSymbols := map[int32]*orm.ExSymbol{}
	accountNames := make([]string, 0, len(accounts))
	for account := range accounts {
		accountNames = append(accountNames, account)
	}
	sort.Strings(accountNames)
	for _, account := range accountNames {
		owner := accounts[account]
		cfg := snapshot.View().Clone()
		cfg.RunPolicy = nil
		cfg.Accounts = map[string]*config.AccountConfig{account: {}}
		if original := snapshot.View().Accounts[account]; original != nil {
			cfg.Accounts = config.CloneAccountConfigsForRuntime(map[string]*config.AccountConfig{account: original})
		}
		base := owner.base
		for _, c := range configs {
			if c.AccountID == account {
				base = c
				break
			}
		}
		owner.base = base
		accounts[account] = owner
		quotes := map[string]execution.VisibleQuote{}
		risk := execution.PortfolioRisk{MarginRate: base.Execution.MarginRate, MaxAccountMargin: base.Execution.MaxAccountMargin, MaxVirtualGross: base.Execution.MaxVirtualGross, StrategyGrossLimits: map[execution.StrategyID]decimal.Decimal{}}
		bridge := &biz.SharedOrderBridgeConfig{Version: "v1", Instruments: map[string]execution.Instrument{}, Strategies: map[string]biz.SharedStrategyBinding{}, Risk: risk, IntentTTLMS: base.ExpiryMS,
			Quote: func(id string, _ int64) (execution.VisibleQuote, error) {
				q, ok := quotes[id]
				if !ok {
					return q, fmt.Errorf("mixed replay: no visible quote for %s", id)
				}
				return q, nil
			}}
		for _, instrument := range base.Execution.Instruments {
			bridge.Instruments[instrument.ID] = instrument
		}
		for _, c := range configs {
			if c.AccountID == account {
				for _, instrument := range c.Execution.Instruments {
					bridge.Instruments[instrument.ID] = instrument
				}
			}
		}
		for _, policy := range u.RunPolicy {
			if policy.Engine != config.EngineTimeSeries || policyAccount(u, policy) != account {
				continue
			}
			if _, registered := strat.GetStrategyFactory(policy.Name); !registered {
				return nil, fmt.Errorf("mixed replay: TS strategy %s is not registered", policy.Name)
			}
			cfg.RunPolicy = append(cfg.RunPolicy, policy.RunPolicyConfig.Clone())
			id := policy.ID
			if id == "" {
				id = policy.RunPolicyConfig.ID()
			}
			capital := base.AccountInitialNAV
			if capital == 0 {
				capital = base.InitialNAV
			}
			nav := decimal.NewFromFloat(capital * policyCapitalWeight(u, policy, account))
			if !nav.IsPositive() {
				return nil, errors.New("mixed replay: TS capital allocation must be positive")
			}
			stake := decimal.NewFromFloat(cfg.StakePct / 100)
			if stake.IsZero() && cfg.StakeAmount > 0 {
				stake = decimal.NewFromFloat(cfg.StakeAmount).Div(nav)
			}
			bridge.Strategies[policy.RunPolicyConfig.ID()] = biz.SharedStrategyBinding{ID: execution.StrategyID(id), StakeNAVFraction: stake, MaxNotional: base.Execution.StrategyGrossLimit}
			bridge.Risk.StrategyGrossLimits[execution.StrategyID(id)] = base.Execution.StrategyGrossLimit
			borrow, err := p.BorrowAccount(owner.key, owner.opts)
			if err != nil {
				return nil, err
			}
			if id != owner.seededStrategy {
				err = borrow.CashEvent(execution.CashEvent{ID: "paper-ts-allocation:" + id, Kind: execution.CapitalTransfer, Postings: []execution.CashPosting{{Amount: nav.Neg()}, {Strategy: execution.StrategyID(id), Amount: nav}}})
			}
			borrow.Release()
			if err != nil {
				return nil, err
			}
		}
		if len(cfg.RunPolicy) == 0 {
			continue
		}
		// Runtime construction freezes the bridge. Admit the configured TS pair
		// candidates before construction; the actual jobs determine replay needs.
		pairs := append([]string(nil), cfg.Pairs...)
		for _, policy := range cfg.RunPolicy {
			pairs = append(pairs, policy.Pairs...)
		}
		if len(pairs) == 0 {
			for symbol := range bridge.Instruments {
				pairs = append(pairs, symbol)
			}
		}
		sort.Strings(pairs)
		pairs = slices.Compact(pairs)
		for _, pair := range pairs {
			if _, exists := bridge.Instruments[pair]; exists {
				continue
			}
			unit, err := mixedReplayInstrument(exchange, pair, base.Manifest.Currency)
			if err != nil {
				return nil, err
			}
			bridge.Instruments[pair] = unit
		}
		info := exchange.Info()
		start := configs[0].Chunks
		if configs[0].HistoricalInput != nil {
			start = configs[0].HistoricalInput.Ranges()
		}
		rt, err := p.NewRuntime(runtimepkg.Options{Context: ctx, Config: cfg, DataDir: snapshot.DataDir, StrategyDir: snapshot.StrategyDir, Mode: core.RunModeBackTest, Env: core.RunEnvDryRun, StartAt: start[0].From, Exchange: exchange, ExchangeName: info.ID, Market: info.MarketType, NetDisable: true, AccountOwnerKey: &owner.key, SharedExecution: &owner.opts, SharedOrderBridge: bridge})
		if err != nil {
			return nil, err
		}
		defer func() { rt.Close(); rt.Join() }()
		// The storage catalog owns ordinary pair SIDs, including symbols outside
		// the factor universe. Archives provide identities through SIDMap below.
		for _, c := range configs {
			if input, ok := c.HistoricalInput.(*factorStorageInput); ok {
				for _, symbol := range input.assembly.runtime.Symbols.GetExSymbols(info.ID, info.MarketType) {
					if err := rt.Symbols.CacheExSymbolChecked(symbol); err != nil {
						return nil, err
					}
				}
			}
		}
		for _, c := range append(append([]runner.Config(nil), configs...), base) {
			for sid, symbol := range c.Snapshot.SIDMap {
				if err := rt.Symbols.CacheExSymbolChecked(&orm.ExSymbol{ID: sid, Exchange: info.ID, Market: info.MarketType, Symbol: symbol}); err != nil {
					return nil, err
				}
			}
		}
		scores := mixedTimeFrameScores(rt, pairs)
		biz.InitFakeWalletsWithRuntimeDeps(rt.BizDeps())
		biz.InitLocalOrderMgrWithRuntimeDeps(rt.BizDeps(), nil, false)
		if _, _, err := strat.LoadStratJobsWithState(rt.Strategies, rt.Core, rt.Symbols, pairs, scores, rt.Orders); err != nil {
			return nil, err
		}
		manager, ok := biz.GetOdMgrWithState(rt.Trading, account).(*biz.SharedOrderMgr)
		if !ok {
			return nil, errors.New("mixed replay: shared TS order manager is required")
		}
		if err := manager.BindJobs(rt.Strategies.CollectJobs()); err != nil {
			return nil, err
		}
		for _, job := range rt.Strategies.CollectJobs() {
			unit := bridge.Instruments[job.Symbol.Symbol]
			if previous, exists := tsUnits[job.Symbol.ID]; exists && !sameFactorInstrument(previous, unit) {
				return nil, fmt.Errorf("mixed replay: conflicting TS execution unit for SID %d", job.Symbol.ID)
			}
			tsUnits[job.Symbol.ID], tsSymbols[job.Symbol.ID] = unit, job.Symbol
			subscriptions = append(subscriptions, &orm.Subscription{Source: orm.SeriesSourceKline, ExSymbol: job.Symbol, TimeFrame: job.TimeFrame, WarmupNum: job.Strat.WarmupNum})
			for _, sub := range strat.CollectDataSubsWithSymbolState(rt.Symbols, job) {
				copySub := *sub
				subscriptions = append(subscriptions, &copySub)
			}
		}
		trader, traderErr := biz.NewTraderWithRuntimeDeps(rt.BizDeps())
		if traderErr != nil {
			return nil, traderErr
		}
		consumers = append(consumers, consumer{task: rt, trader: trader, quotes: quotes, base: base, account: account})
	}
	if len(consumers) == 0 {
		return nil, errors.New("mixed replay: no TS strategies were assembled")
	}
	// Every factor reader consumes the immutable union stream. Its execution
	// sink must recognize TS quotes/funding, while its DAG universe stays intact.
	for i := range configs {
		units, err := mergeMixedReplayUnits(configs[i].Execution.Instruments, tsUnits)
		if err != nil {
			return nil, err
		}
		configs[i].Execution.Instruments = units
		sink := sinks[i].(*runner.AccountSink)
		sink.Instruments = units
		if err := sink.RegisterExecution(); err != nil {
			return nil, err
		}
		for _, symbol := range tsSymbols {
			subscriptions = append(subscriptions, mixedReplayExecutionSubscriptions(configs[i], symbol)...)
		}
	}
	for _, consumer := range consumers {
		owner := accounts[consumer.account]
		if owner.paperSink == nil {
			continue
		}
		units, err := mergeMixedReplayUnits(owner.paperSink.Instruments, tsUnits)
		if err != nil {
			return nil, err
		}
		owner.paperSink.Instruments = units
		if err := owner.paperSink.RegisterExecution(); err != nil {
			return nil, err
		}
		for _, symbol := range tsSymbols {
			subscriptions = append(subscriptions, mixedReplayExecutionSubscriptions(consumer.base, symbol)...)
		}
	}
	configs, err = extendFactorStorageInputs(ctx, configs, subscriptions)
	if err != nil {
		return nil, err
	}
	observeConsumers := func(selected []consumer) func(context.Context, runner.HistoricalBatch) error {
		return func(ctx context.Context, batch runner.HistoricalBatch) error {
			for _, consumer := range selected {
				consumer.task.Clock.SetTimeMS(batch.AtMS)
				for _, row := range batch.Records {
					if row.Series.Source != consumer.base.Prices.Source || row.Series.TimeFrame != consumer.base.Prices.TimeFrame {
						continue
					}
					symbol := consumer.task.Symbols.GetSymbolByID(row.Series.Sid)
					if symbol == nil {
						continue
					}
					price := factor.Number(row.Series.Values, consumer.base.Prices.Field)
					if price.Validity == factor.Valid && price.Value > 0 {
						bid, ask := price.Value, price.Value
						b, a := factor.Number(row.Series.Values, "bid"), factor.Number(row.Series.Values, "ask")
						if b.Validity == factor.Valid && a.Validity == factor.Valid && b.Value > 0 && a.Value >= b.Value {
							bid, ask = b.Value, a.Value
						}
						consumer.quotes[symbol.Symbol] = execution.VisibleQuote{Bid: decimal.NewFromFloat(bid), Ask: decimal.NewFromFloat(ask), AtMS: row.EventTime, ReceivedMS: batch.AtMS, ValidUntilMS: batch.AtMS + consumer.base.DecisionInterval + consumer.base.ExpiryMS, Bar: row.EventTime / consumer.base.DecisionInterval}
						if sink := accounts[consumer.account].paperSink; sink != nil {
							if err := sink.ObserveQuote(ctx, row.Series.Sid, backtest.Quote{Price: price.Value, Bid: bid, Ask: ask, AtMS: row.EventTime, AvailableAt: batch.AtMS}, batch.AtMS); err != nil {
								return err
							}
						}
					}
				}
				if sink := accounts[consumer.account].paperSink; sink != nil && consumer.base.Manifest.Costs.FundingPolicy == "required-stream" {
					for _, row := range batch.Records {
						if row.Series.IsWarmUp || row.Series.Source != consumer.base.FundingSource {
							continue
						}
						rate := factor.Number(row.Series.Values, "rate")
						if rate.Validity != factor.Valid {
							return errors.New("mixed replay: invalid TS funding rate")
						}
						settlement := backtest.Funding{ID: fmt.Sprintf("%s:%d:%d", row.Series.Source, row.Series.Sid, row.EventTime), SID: row.Series.Sid, AtMS: row.EventTime, AvailableAt: row.AvailableAt, Rate: rate.Value}
						if err := sink.ObserveFunding(ctx, settlement, batch.AtMS); err != nil {
							return err
						}
					}
				}
				for _, row := range batch.Records {
					if err := ctx.Err(); err != nil {
						return err
					}
					if consumer.task.Symbols.GetSymbolByID(row.Series.Sid) == nil {
						continue
					}
					series := row.Series
					if !series.IsWarmUp && series.Source == consumer.base.Prices.Source && series.TimeFrame == consumer.base.Prices.TimeFrame {
						if err := biz.GetOdMgrWithState(consumer.task.Trading, consumer.trader.RuntimeDependencies().DefaultAccount).UpdateByDataSeries(nil, &series); err != nil {
							return err
						}
					}
					if err := consumer.trader.FeedDataSeries(&series); err != nil {
						return err
					}
				}
				biz.TryFireBatchesWithRuntimeDeps(consumer.trader.RuntimeDependencies(), consumer.task.Batch, batch.AtMS+1, false)
			}
			return nil
		}
	}
	// Each account's first CS consumer advances only that account's TS jobs.
	// A CS runner cannot advance another account's clock before its decision.
	observedAccounts := map[string]bool{}
	for i := range configs {
		account := configs[i].AccountID
		if observedAccounts[account] {
			continue
		}
		observedAccounts[account] = true
		var selected []consumer
		for _, consumer := range consumers {
			if consumer.account == account || i == 0 && accounts[consumer.account].seededStrategy != "" {
				selected = append(selected, consumer)
			}
		}
		if len(selected) == 0 {
			continue
		}
		observer, previous := observeConsumers(selected), configs[i].ObserveBatch
		configs[i].ObserveBatch = func(ctx context.Context, batch runner.HistoricalBatch) error {
			if previous != nil {
				if err := previous(ctx, batch); err != nil {
					return err
				}
			}
			return observer(ctx, batch)
		}
	}
	task, err := p.NewRuntime(runtimepkg.Options{Context: ctx, Mode: core.RunModeBackTest, NetDisable: true})
	if err != nil {
		return nil, err
	}
	defer func() { task.Close(); task.Join() }()
	outputs := make([]runner.Output, len(configs))
	writer = &synchronizedWriter{writer: writer}
	for i := range configs {
		outputs[i] = &runner.JSONOutput{Writer: writer, SIDs: configs[i].Snapshot.Universe.Evaluation}
		if configs[i].ComputationContext.DataNamespace == "" {
			configs[i].ComputationContext.DataNamespace = "mixed-replay"
		}
		configs[i].ComputationContext.ClockDomain = task.ID
		configs[i].ComputationContext.SamplingIdentity = "publication-replay"
	}
	if err := task.InstallFactorReplay(configs, sinks, outputs); err != nil {
		return nil, err
	}
	results, resultErr = task.FactorState.Run(ctx)
	if resultErr != nil {
		return results, resultErr
	}
	if err := archiveFactorAccounts(ctx, configs, sinks); err != nil {
		return results, err
	}
	for _, consumer := range consumers {
		if consumer.base.ArtifactPath != "" {
			path := filepath.Join(filepath.Dir(consumer.base.ArtifactPath), "ts-"+url.PathEscape(consumer.account))
			if err := saveMixedOrders(consumer.task, consumer.account, path); err != nil {
				return results, err
			}
		}
	}
	for _, account := range accountNames {
		owner := accounts[account]
		if owner.seededStrategy == "" {
			continue
		}
		borrow, err := p.BorrowAccount(owner.key, owner.opts)
		if err != nil {
			return results, err
		}
		snapshot, err := borrow.Snapshot(ctx)
		if err == nil && owner.base.ArtifactPath != "" {
			path, pathErr := filepath.Abs(filepath.Join(filepath.Dir(owner.base.ArtifactPath), "account-"+url.PathEscape(account)))
			if pathErr != nil {
				err = pathErr
			} else {
				_, err = borrow.ArchiveCommittedEvents(ctx, path, 0, 512)
			}
		}
		borrow.Release()
		if err != nil {
			return results, err
		}
		// This account result records simulation assumptions, without claiming
		// a factor definition, research labels or factor snapshot lineage.
		manifest := research.ManifestSpec{Currency: owner.base.Manifest.Currency, Costs: owner.base.Manifest.Costs, CodeRevision: core.Version, ExecutionMode: string(runner.Events),
			LatencyAssumption: fmt.Sprintf("shared paper execution; next visible quote after intent; price=%s/%s/%s; quote TTL=%dms; intent TTL=%dms", owner.base.Prices.Source, owner.base.Prices.TimeFrame, owner.base.Prices.Field, owner.base.DecisionInterval+owner.base.ExpiryMS, owner.base.ExpiryMS)}
		results = append(results, runner.Result{Engine: "time_series", AccountID: account, Account: &snapshot, Manifest: manifest})
	}
	return results, nil
}

func mixedReplayInstrument(exchange banexg.BanExchange, symbol, currency string) (execution.Instrument, error) {
	market, err := exchange.GetMarket(symbol)
	if err != nil {
		return execution.Instrument{}, err
	}
	info := exchange.Info()
	info.CurrByCodeLock.Lock()
	settlement := info.CurrenciesByCode[market.Settle]
	info.CurrByCodeLock.Unlock()
	unit, unitErr := execution.InstrumentFromBanexgMarket(symbol, market, settlement)
	if unitErr != nil {
		return unit, unitErr
	}
	if unit.SettlementCurrency != currency {
		return unit, errors.New("mixed replay: TS settlement metadata differs from configured currency")
	}
	return unit, nil
}

func sameFactorInstrument(a, b execution.Instrument) bool {
	return a.ID == b.ID && a.Version == b.Version && a.Valuation == b.Valuation && a.SettlementCurrency == b.SettlementCurrency && a.MoneyScale == b.MoneyScale && a.MinSteps == b.MinSteps && a.QuantityStep.Equal(b.QuantityStep) && a.ContractSize.Equal(b.ContractSize) && a.PriceTick.Equal(b.PriceTick) && a.MinNotional.Equal(b.MinNotional)
}

func mergeMixedReplayUnits(base, extra map[int32]execution.Instrument) (map[int32]execution.Instrument, error) {
	units := make(map[int32]execution.Instrument, len(base)+len(extra))
	for sid, unit := range base {
		units[sid] = unit
	}
	for sid, unit := range extra {
		if previous, exists := units[sid]; exists && !sameFactorInstrument(previous, unit) {
			return nil, fmt.Errorf("mixed replay: conflicting execution unit for SID %d", sid)
		}
		units[sid] = unit
	}
	return units, nil
}

func mixedReplayExecutionSubscriptions(c runner.Config, symbol *orm.ExSymbol) []*orm.Subscription {
	subs := []*orm.Subscription{{Source: c.Prices.Source, TimeFrame: c.Prices.TimeFrame, Fields: []string{c.Prices.Field}, ExSymbol: symbol}}
	if c.Manifest.Costs.FundingPolicy == "required-stream" {
		subs = append(subs, &orm.Subscription{Source: c.FundingSource, TimeFrame: "event", Fields: []string{"rate"}, ExSymbol: symbol})
	}
	return subs
}

func saveMixedOrders(task *runtimepkg.Runtime, account, path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	staging, err := os.MkdirTemp(filepath.Dir(path), ".ts-report-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(staging)
	var copies []*ormo.InOutOrder
	seen := map[int64]bool{}
	add := func(order *ormo.InOutOrder) error {
		if order == nil || seen[order.ID] {
			return nil
		}
		seen[order.ID] = true
		copies = append(copies, order.Clone())
		return nil
	}
	if shared := task.SharedExecution(); shared != nil && shared.Service().Store().HasMemoryHistory() {
		if manager, ok := biz.GetOdMgrWithState(task.Trading, account).(*biz.SharedOrderMgr); ok {
			if err := manager.VisitOrderViews(task.Context(), add); err != nil {
				return err
			}
		}
	}
	for _, order := range task.Orders.HistoricalOrders() {
		if err := add(order); err != nil {
			return err
		}
	}
	open, lock := task.Orders.GetOpenODs(account)
	lock.Lock()
	for _, order := range open {
		_ = add(order)
	}
	lock.Unlock()
	if err := opt.DumpOrdersCSVWithRuntimeDeps(copies, filepath.Join(staging, "orders.csv"), task.BizDeps()); err != nil {
		return err
	}
	if err := ormo.DumpOrdersGobItems(filepath.Join(staging, "orders.gob"), copies); err != nil {
		return err
	}
	for _, name := range []string{"orders.csv", "orders.gob"} {
		file, err := os.OpenFile(filepath.Join(staging, name), os.O_RDWR, 0)
		if err != nil {
			return err
		}
		if err := errors.Join(file.Sync(), file.Close()); err != nil {
			return err
		}
	}
	return os.Rename(staging, path)
}
