package entry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/runner"
	runtimectx "github.com/banbox/banbot/runtime"
	"github.com/banbox/banexg/utils"
	"github.com/go-viper/mapstructure/v2"
	"github.com/shopspring/decimal"
)

func decodeFactorFields(fields map[string]any, target any) error {
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Result: target, ErrorUnused: true,
		MatchName: func(key, field string) bool { return strings.EqualFold(strings.ReplaceAll(key, "_", ""), field) },
		DecodeHook: func(from, to reflect.Type, value any) (any, error) {
			if to == reflect.TypeOf(decimal.Decimal{}) {
				switch n := value.(type) {
				case string:
					return decimal.NewFromString(n)
				case float64:
					return decimal.NewFromFloat(n), nil
				case int:
					return decimal.NewFromInt(int64(n)), nil
				}
			}
			return value, nil
		},
	})
	if err != nil {
		return err
	}
	return decoder.Decode(fields)
}

// buildFactorConfigs derives ordinary defaults once from the single policy
// list. Each runner receives a private config and account budget.
func buildFactorConfigs(spec *config.RunSpec, mode runner.Mode) ([]runner.Config, error) {
	u := spec.Config()
	var result []runner.Config
	identities := map[string]bool{}
	for index, policy := range u.RunPolicy {
		if policy.Engine != config.EngineFactor {
			continue
		}
		c := runner.Config{Mode: mode, MaxRecords: 100000, MaxPending: 64, LatencyMS: 1, ExpiryMS: 60000, InitialNAV: 10000, Definition: policy.Name, Factor: research.DefaultMomentumVolConfig()}
		if c.Definition == "MomentumVol" {
			c.Definition = "momentum-vol"
		}
		c.Manifest = research.ManifestSpec{CodeRevision: core.Version, Currency: "USD", Portfolio: research.PortfolioDefinition{K: 10, LongNotional: .5, ShortNotional: .5, Mode: factor.Full}}
		if len(u.Root.StakeCurrency) > 0 {
			c.Manifest.Currency = u.Root.StakeCurrency[0]
		}
		if nav := u.Root.WalletAmounts[c.Manifest.Currency]; nav > 0 {
			c.InitialNAV = nav
		}
		timeframe := "1h"
		frames := policy.RunTimeframes
		if len(frames) == 0 {
			frames = u.Root.RunTimeframes
		}
		if len(frames) > 1 {
			return nil, fmt.Errorf("factor %s: one decision timeframe is required", policy.Name)
		}
		if len(frames) == 1 {
			timeframe = frames[0]
		}
		seconds, err := utils.TFToSecSafe(timeframe)
		if err != nil || seconds <= 0 {
			return nil, fmt.Errorf("factor %s: invalid decision timeframe", policy.Name)
		}
		c.DecisionInterval, c.Factor.TimeFrame = int64(seconds)*1000, timeframe
		c.Manifest.Labels = []research.LabelSpec{{Name: timeframe, Kind: research.ExecutableReturn, Horizon: c.DecisionInterval, PeriodsPerYear: 365.25 * 86400000 / float64(c.DecisionInterval)}}
		c.StrategyID, c.AccountID = policy.ID, policyAccount(u, policy)
		if c.StrategyID == "" {
			c.StrategyID = policy.Name
		}
		if c.AccountID == "" {
			c.AccountID = "default"
		}
		for key, value := range policy.Params {
			switch key {
			case "window":
				c.Factor.Window = int(value)
			case "k":
				c.Manifest.Portfolio.K = int(value)
			default:
			}
		}
		fields := make(map[string]any)
		for key, value := range policy.Factor {
			switch key {
			case "archive", "portfolio", "decision", "research":
			default:
				fields[key] = value
			}
		}
		if err := decodeFactorFields(fields, &c); err != nil {
			return nil, fmt.Errorf("factor %s: %w", policy.Name, err)
		}
		if portfolio, ok := policy.Factor["portfolio"].(map[string]any); ok {
			if err := decodeFactorFields(portfolio, &c.Manifest.Portfolio); err != nil {
				return nil, err
			}
		}
		if decision, ok := policy.Factor["decision"].(map[string]any); ok {
			for key, value := range decision {
				mapped := map[string]string{"interval_ms": "DecisionInterval", "delay_ms": "DecisionDelayMS", "latency_ms": "LatencyMS", "expiry_ms": "ExpiryMS", "max_pending": "MaxPending"}[key]
				if mapped == "" {
					return nil, fmt.Errorf("unknown factor decision key %s", key)
				}
				fields = map[string]any{mapped: value}
				if err := decodeFactorFields(fields, &c); err != nil {
					return nil, err
				}
			}
		}
		if researchFields, ok := policy.Factor["research"].(map[string]any); ok {
			for key, value := range researchFields {
				switch key {
				case "labels":
					if err := decodeFactorFields(map[string]any{"Labels": value}, &c.Manifest); err != nil {
						return nil, err
					}
				case "label_wait_ms":
					if err := decodeFactorFields(map[string]any{"LabelWaitMS": value}, &c); err != nil {
						return nil, err
					}
				default:
					return nil, fmt.Errorf("unknown factor research key %s", key)
				}
			}
		}
		if path, ok := policy.Factor["archive"].(string); ok && path != "" {
			c.Chunks = []runner.Chunk{{Path: path}}
		}
		c.Mode = mode
		if c.Expressions != nil {
			// policy.name identifies a strategy; only factor.definition selects a Go builder.
			explicitDefinition := false
			for key := range policy.Factor {
				explicitDefinition = explicitDefinition || strings.EqualFold(key, "definition")
			}
			if !explicitDefinition {
				c.Definition = ""
			}
			if c.Expressions.TimeFrame == "" {
				c.Expressions.TimeFrame = timeframe
			}
			if c.Expressions.TimeFrame != timeframe {
				return nil, fmt.Errorf("run_policy[%d].factor.expressions.timeframe must match run_timeframes", index)
			}
			if _, _, err := runner.CompileDefinition(c); err != nil {
				return nil, fmt.Errorf("run_policy[%d].factor: %w", index, err)
			}
		}
		if c.Manifest.Parameters == nil {
			c.Manifest.Parameters = make(map[string]float64)
		}
		for key, value := range policy.Params {
			c.Manifest.Parameters[key] = value
		}
		if policy.ID != "" {
			c.StrategyID = policy.ID
		}
		if policy.Account != "" {
			c.AccountID = policy.Account
		}
		identity := c.AccountID + "/" + c.StrategyID
		if identities[identity] {
			return nil, fmt.Errorf("factor %s: repeated definitions require distinct id values", policy.Name)
		}
		identities[identity] = true
		if policy.CapitalWeight != nil {
			c.AccountInitialNAV = c.InitialNAV
			c.InitialNAV *= policyCapitalWeight(u, policy, c.AccountID)
		}
		for chunkIndex := range c.Chunks {
			field := fmt.Sprintf("run_policy[%d].chunks[%d].path", index, chunkIndex)
			if _, ok := policy.Factor["archive"]; ok {
				field = fmt.Sprintf("run_policy[%d].archive", index)
			}
			if !filepath.IsAbs(c.Chunks[chunkIndex].Path) {
				path, err := spec.ResolvePath(field)
				if err != nil {
					return nil, err
				}
				c.Chunks[chunkIndex].Path = path
			}
		}
		if store, ok := u.Execution["store"].(string); ok && store != "" {
			path, err := spec.ResolvePath("execution.store")
			if err != nil {
				return nil, err
			}
			c.Execution.StorePath = path
		}
		executionFields := make(map[string]any, len(u.Execution))
		for key, value := range u.Execution {
			executionFields[key] = value
		}
		accountOverrides := u.AccountExecution[c.AccountID]
		for key, value := range accountOverrides {
			executionFields[key] = value
		}
		if funding, ok := executionFields["funding_policy"].(string); ok {
			c.Manifest.Costs.FundingPolicy = funding
		}
		settings := make(map[string]any)
		for key, value := range executionFields {
			switch key {
			case "store", "history", "sender_lease_dir", "live_provider", "funding_policy", "accounts", "mode":
			default:
				settings[key] = value
			}
		}
		if err := decodeFactorFields(settings, &c.Execution); err != nil {
			return nil, err
		}
		for key, destination := range map[string]*string{"store": &c.Execution.StorePath, "history": &c.Execution.HistoryPath, "sender_lease_dir": &c.Execution.SenderLeaseDir} {
			if _, exists := executionFields[key]; !exists {
				continue
			}
			field := "execution." + key
			if _, overridden := accountOverrides[key]; overridden {
				field = "accounts." + c.AccountID + "." + key
			}
			path, err := spec.ResolvePath(field)
			if err != nil {
				return nil, err
			}
			*destination = path
		}
		if maxRecords, ok := u.Data["max_records"]; ok {
			if err := decodeFactorFields(map[string]any{"MaxRecords": maxRecords}, &c); err != nil {
				return nil, err
			}
		}
		if archive, ok := u.Data["archive"].(string); ok && len(c.Chunks) == 0 {
			path, err := spec.ResolvePath("data.archive")
			if err != nil {
				return nil, err
			}
			c.Chunks = []runner.Chunk{{Path: path}}
			_ = archive
		}
		for key := range u.Data {
			if len(c.Chunks) > 0 && key != "max_records" && key != "archive" && key != "pit_policy" {
				return nil, fmt.Errorf("data.%s: archive driver cannot apply a storage subscription override", key)
			}
		}
		if pit, ok := u.Data["pit_policy"].(string); ok && len(c.Chunks) > 0 && pit != "available-at" && pit != "strict" {
			return nil, fmt.Errorf("data.pit_policy: unsupported archive visibility policy %q", pit)
		}
		if mode != runner.Trade && len(c.Chunks) > 0 {
			if err := deriveArchiveIdentity(&c); err != nil {
				return nil, err
			}
		} else if mode != runner.Trade {
			if err := preflightFactorStorageConfig(spec, &c); err != nil {
				return nil, err
			}
		}
		if _, _, err := runner.CompileDefinition(c); err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	if len(result) == 0 {
		return nil, errors.New("factor: run_policy contains no factor strategy")
	}
	return result, nil
}

func policyAccount(u *config.UnifiedConfig, policy *config.PolicyV2) string {
	if policy.Account != "" {
		return policy.Account
	}
	if u.Root.Accounts["default"] != nil {
		return "default"
	}
	var names []string
	for name, account := range u.Root.Accounts {
		if account != nil && !account.NoTrade {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	if len(names) > 0 {
		return names[0]
	}
	return "default"
}

// Shared tasks validate explicit allocations before assembly. An exclusive
// strategy retains the account's full capital and its existing sizing.
func policyCapitalWeight(u *config.UnifiedConfig, policy *config.PolicyV2, account string) float64 {
	if policy.CapitalWeight != nil {
		return *policy.CapitalWeight
	}
	return 1
}

func deriveArchiveIdentity(c *runner.Config) error {
	if len(c.Chunks) == 0 {
		return errors.New("factor: archive/chunks or a configured historical input is required")
	}
	if c.MaxRecords <= 0 {
		return errors.New("factor: max_records must be positive")
	}
	if c.Snapshot.SourceVersions != nil && c.Snapshot.Schemas != nil && c.Snapshot.SIDMap != nil {
		for index := range c.Chunks {
			if c.Chunks[index].From == 0 || c.Chunks[index].To == 0 {
				if err := inspectArchiveChunk(c, index, nil, nil); err != nil {
					return err
				}
			}
		}
		return deriveArchivePrice(c)
	}
	sids := map[int32]bool{}
	schemas := map[string]map[string]string{}
	if c.Snapshot.SIDMap == nil {
		c.Snapshot.SIDMap = map[int32]string{}
	}
	if c.Snapshot.SourceVersions == nil {
		c.Snapshot.SourceVersions = map[string]string{}
	}
	if c.Snapshot.Schemas == nil {
		c.Snapshot.Schemas = map[string]string{}
	}
	for index := range c.Chunks {
		if err := inspectArchiveChunk(c, index, sids, schemas); err != nil {
			return err
		}
	}
	var ids []int32
	for sid := range sids {
		ids = append(ids, sid)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	var tracked []int32
	for sid := range c.Snapshot.SIDMap {
		tracked = append(tracked, sid)
	}
	sort.Slice(tracked, func(i, j int) bool { return tracked[i] < tracked[j] })
	if c.Snapshot.Universe.Version == "" {
		c.Snapshot.Universe = factor.Universe{Version: "archive-static", Investable: ids, Reference: ids, Tradable: ids, Evaluation: ids, Tracked: tracked, Static: true}
	}
	for source, fields := range schemas {
		if c.Snapshot.Schemas[source] == "" {
			raw, _ := json.Marshal(fields)
			digest := sha256.Sum256(raw)
			c.Snapshot.Schemas[source] = hex.EncodeToString(digest[:])
		}
	}
	if c.Snapshot.VisibilityPolicy == "" {
		c.Snapshot.VisibilityPolicy = "available-at"
	}
	if c.Snapshot.AdjustmentVersion == "" {
		c.Snapshot.AdjustmentVersion = "raw"
	}
	return deriveArchivePrice(c)
}

func deriveArchivePrice(c *runner.Config) error {
	if c.Prices.Source == "" {
		if _, ok := c.Snapshot.Schemas["tick"]; ok {
			c.Prices = runner.PriceStream{Source: "tick", TimeFrame: "event", Field: "price"}
		} else if c.Expressions != nil {
			if _, ok := c.Snapshot.Schemas["kline"]; !ok {
				return errors.New("factor: expressions require explicit prices when archive has no tick/kline price stream")
			}
			c.Prices = runner.PriceStream{Source: "kline", TimeFrame: c.Expressions.TimeFrame, Field: "close"}
		} else {
			c.Prices = runner.PriceStream{Source: c.Factor.Source, TimeFrame: c.Factor.TimeFrame, Field: c.Factor.Field}
		}
	}
	if c.Manifest.Costs.FundingPolicy == "" {
		if _, ok := c.Snapshot.Schemas["funding"]; ok {
			c.FundingSource = "funding"
			c.Manifest.Costs.FundingPolicy = "required-stream"
		} else {
			return errors.New("factor: execution.funding_policy must explicitly declare explicit-zero when no funding source is present")
		}
	}
	return nil
}

func inspectArchiveChunk(c *runner.Config, index int, sids map[int32]bool, schemas map[string]map[string]string) error {
	var inputs []factor.InputSpec
	if sids != nil {
		plan, _, err := runner.CompileDefinition(*c)
		if err != nil {
			return err
		}
		inputs = plan.Inputs()
	}
	store, err := factor.OpenVersionStore(c.Chunks[index].Path, c.MaxRecords)
	if err != nil {
		return err
	}
	rows, err := store.Records()
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return errors.New("factor: empty archive")
	}
	from, to := rows[0].EventTime, rows[0].EventTime
	for _, row := range rows {
		from = min(from, row.EventTime)
		to = max(to, row.EventTime)
		if sids != nil {
			for _, input := range inputs {
				if input.Source == row.Series.Source && input.TimeFrame == row.Series.TimeFrame {
					sids[row.Series.Sid] = true
					break
				}
			}
			if c.Snapshot.SIDMap[row.Series.Sid] == "" {
				c.Snapshot.SIDMap[row.Series.Sid] = fmt.Sprintf("sid:%d", row.Series.Sid)
			}
			if version := c.Snapshot.SourceVersions[row.Series.Source]; version != "" && version != row.SourceVersion {
				return fmt.Errorf("factor: source version changes require explicit PIT schema: %s", row.Series.Source)
			}
			c.Snapshot.SourceVersions[row.Series.Source] = row.SourceVersion
			if schemas[row.Series.Source] == nil {
				schemas[row.Series.Source] = map[string]string{}
			}
			for field, value := range row.Series.Values {
				if value != nil {
					typeName := fmt.Sprintf("%T", value)
					if previous := schemas[row.Series.Source][field]; previous != "" && previous != typeName {
						return fmt.Errorf("factor: archive field type changes require an explicit schema version: %s.%s", row.Series.Source, field)
					}
					schemas[row.Series.Source][field] = typeName
				}
			}
		}
	}
	if c.Chunks[index].From == 0 {
		c.Chunks[index].From = from
	}
	if c.Chunks[index].To == 0 {
		c.Chunks[index].To = to
	}
	return nil
}

type synchronizedWriter struct {
	mu     sync.Mutex
	writer io.Writer
}

func (w *synchronizedWriter) Write(body []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writer.Write(body)
}

func runFactorConfigs(ctx context.Context, configs []runner.Config, factory FactorSinkFactory, writer io.Writer) (results []runner.Result, resultErr error) {
	process := runtimectx.NewProcess()
	start := int64(0)
	if len(configs) > 0 && len(configs[0].Chunks) > 0 {
		start = configs[0].Chunks[0].From
	}
	task, err := process.NewRuntime(runtimectx.Options{Context: ctx, Mode: core.RunModeBackTest, StartAt: start, NetDisable: true})
	if err != nil {
		process.Close()
		return nil, err
	}
	defer func() {
		task.Close()
		task.Join()
		process.Close()
		resultErr = errors.Join(resultErr, process.CloseError())
	}()
	ctx = task.Context()
	sinks := make([]runner.Sink, len(configs))
	outputs := make([]runner.Output, len(configs))
	var cleanups []func() error
	defer func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			resultErr = errors.Join(resultErr, cleanups[i]())
		}
	}()
	writer = &synchronizedWriter{writer: writer}
	if factory == nil && len(configs) > 0 && configs[0].Mode == runner.Events {
		var cleanup func() error
		var err error
		sinks, cleanup, err = runner.NewPaperSinksWithAccounts(ctx, configs, process.BorrowAccount)
		if err != nil {
			return nil, err
		}
		cleanups = append(cleanups, cleanup)
	}
	for i, c := range configs {
		if c.Mode == runner.Events || c.Mode == runner.Trade {
			if factory == nil {
				if sinks[i] == nil {
					return nil, errors.New("factor: account execution factory unavailable")
				}
			} else {
				sink, cleanup, err := factory(ctx, c, true)
				if err != nil {
					return nil, err
				}
				sinks[i] = sink
				if cleanup != nil {
					cleanups = append(cleanups, cleanup)
				}
			}
		}
		outputs[i] = &runner.JSONOutput{Writer: writer, SIDs: c.Snapshot.Universe.Evaluation}
	}
	for index := range configs {
		if configs[index].ComputationContext.DataNamespace == "" {
			configs[index].ComputationContext.DataNamespace = "archive"
		}
		configs[index].ComputationContext.ClockDomain = task.ID
		configs[index].ComputationContext.SamplingIdentity = "publication-replay"
	}
	if err := task.InstallFactorReplay(configs, sinks, outputs); err != nil {
		return nil, err
	}
	results, resultErr = task.FactorState.Run(ctx)
	if resultErr == nil {
		resultErr = archiveFactorAccounts(ctx, configs, sinks)
	}
	return results, resultErr
}

func archiveFactorAccounts(ctx context.Context, configs []runner.Config, sinks []runner.Sink) error {
	seen := map[*execution.SharedAccount]bool{}
	for i, sink := range sinks {
		account, ok := sink.(*runner.AccountSink)
		if !ok || configs[i].ArtifactPath == "" || seen[account.Account.Service()] {
			continue
		}
		seen[account.Account.Service()] = true
		path, err := filepath.Abs(filepath.Join(filepath.Dir(configs[i].ArtifactPath), "account-"+url.PathEscape(account.AccountID)))
		if err != nil {
			return err
		}
		if _, err := account.Account.ArchiveCommittedEvents(ctx, path, 0, 512); err != nil {
			return err
		}
	}
	return nil
}
