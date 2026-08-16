package runtimeplan

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strings"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	utils2 "github.com/banbox/banexg/utils"
	ta "github.com/banbox/banta"
	"gopkg.in/yaml.v3"
)

func DecodeRequest(data []byte) (*RequestV1, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var req RequestV1
	if err := dec.Decode(&req); err != nil {
		return nil, fmt.Errorf("decode runtime data plan request: %w", err)
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("decode runtime data plan request: trailing JSON value")
		}
		return nil, fmt.Errorf("decode runtime data plan request: %w", err)
	}
	return &req, nil
}

func Inspect(req *RequestV1, dataDir string) (*OutputV1, error) {
	requestHash, cfg, err := validateRequest(req)
	if err != nil {
		return nil, err
	}
	if !filepath.IsAbs(dataDir) {
		return nil, fmt.Errorf("runtime data plan data directory must be absolute")
	}
	restoreConfig, err := installRuntimeConfig(req, cfg)
	if err != nil {
		return nil, err
	}
	defer restoreConfig()
	config.DataDir = dataDir

	symbols := make([]*orm.ExSymbol, 0, len(req.MarketUniverse))
	for _, item := range req.MarketUniverse {
		symbols = append(symbols, &orm.ExSymbol{
			ID: item.SID, Exchange: item.Exchange, ExgReal: item.ExgReal, Market: item.Market,
			Symbol: item.Symbol, Combined: item.Combined, ListMs: item.ListMS, DelistMs: item.DelistMS,
		})
	}
	restoreSymbols, err := orm.InstallFrozenExSymbols(symbols)
	if err != nil {
		return nil, fmt.Errorf("install frozen market universe: %w", err)
	}
	defer restoreSymbols()

	semantic := collectSemanticPlan(req, cfg)
	output := &OutputV1{
		Version: Version, RequestSHA256: requestHash, SelectionMode: semantic.SelectionMode,
		InitialSymbols: semantic.InitialSymbols, Policies: semantic.Policies,
		Requirements: semantic.Requirements, Unsupported: semantic.Unsupported,
	}
	output.SemanticPlanSHA256, err = SemanticPlanSHA256(semantic)
	if err != nil {
		return nil, err
	}
	if len(output.Unsupported) > 0 {
		return output, fmt.Errorf("runtime data plan contains %d unsupported item(s)", len(output.Unsupported))
	}
	return output, nil
}

func MarshalOutput(output *OutputV1) ([]byte, error) {
	data, err := json.Marshal(output)
	if err != nil {
		return nil, fmt.Errorf("marshal runtime data plan output: %w", err)
	}
	return data, nil
}

// RequestSHA256 returns the canonical versioned digest of a runtime plan request.
func RequestSHA256(request RequestV1) (string, error) {
	return hashJSON(requestHashDomain, request)
}

// SemanticPlanSHA256 returns the canonical versioned digest of a semantic runtime plan.
func SemanticPlanSHA256(plan SemanticPlanV1) (string, error) {
	return hashJSON(semanticHashDomain, plan)
}

// MarketUniverseSHA256 returns the canonical versioned digest of a market universe.
func MarketUniverseSHA256(markets []MarketSymbolV1) (string, error) {
	return hashJSON(universeHashDomain, markets)
}

// InputPairsSHA256 returns the canonical versioned digest of the ordered input symbols.
func InputPairsSHA256(symbols []string) (string, error) {
	return hashJSON(pairsHashDomain, symbols)
}

func validateRequest(req *RequestV1) (string, *config.Config, error) {
	if req == nil || req.Version != Version {
		return "", nil, fmt.Errorf("runtime data plan request version must be %d", Version)
	}
	requiredText := map[string]string{
		"compile_key": req.CompileKey, "compile_base_name": req.CompileBaseName,
		"compile_version": req.CompileVersion, "banbot_commit": req.BanbotCommit,
	}
	for name, value := range requiredText {
		if strings.TrimSpace(value) == "" {
			return "", nil, fmt.Errorf("%s is required", name)
		}
	}
	hashes := map[string]string{
		"strategy_input_sha256":         req.StrategyInputSHA256,
		"strategy_source_sha256":        req.StrategySourceSHA256,
		"compile_manifest_sha256":       req.CompileManifestSHA256,
		"compiled_binary_sha256":        req.CompiledBinarySHA256,
		"banbot_source_manifest_sha256": req.BanbotSourceManifestSHA256,
		"config_sha256":                 req.ConfigSHA256,
		"config_semantic_sha256":        req.ConfigSemanticSHA256,
		"market_universe_sha256":        req.MarketUniverseSHA256,
		"input_pairs_sha256":            req.InputPairsSHA256,
	}
	for name, value := range hashes {
		if !validSHA256(value) {
			return "", nil, fmt.Errorf("%s must be a lowercase SHA-256", name)
		}
	}
	if (strings.TrimSpace(req.MarketSnapshotIdentity) == "") !=
		(strings.TrimSpace(req.MarketSnapshotSHA256) == "") {
		return "", nil, fmt.Errorf("market snapshot identity and SHA-256 must be provided together")
	}
	if req.MarketSnapshotIdentity != "" && !validSHA256(req.MarketSnapshotSHA256) {
		return "", nil, fmt.Errorf("market_snapshot_sha256 must be a lowercase SHA-256")
	}
	if req.ConfigYAML == "" || rawHash([]byte(req.ConfigYAML)) != req.ConfigSHA256 {
		return "", nil, fmt.Errorf("config_sha256 does not match exact config_yaml bytes")
	}
	if req.TimeStartMS < 100_000_000_000 || req.TimeEndMS <= req.TimeStartMS {
		return "", nil, fmt.Errorf("invalid runtime data plan time range")
	}
	if req.MarketUniverse == nil || !canonicalUniverse(req.MarketUniverse) {
		return "", nil, fmt.Errorf("market_universe must be sorted, unique, and valid")
	}
	universeHash, err := MarketUniverseSHA256(req.MarketUniverse)
	if err != nil {
		return "", nil, fmt.Errorf("hash market_universe: %w", err)
	}
	if universeHash != req.MarketUniverseSHA256 {
		return "", nil, fmt.Errorf("market_universe_sha256 mismatch")
	}
	if req.InitialSymbols == nil || !orderedUniqueStrings(req.InitialSymbols) {
		return "", nil, fmt.Errorf("initial_symbols must be ordered, unique, and valid")
	}
	pairsHash, err := InputPairsSHA256(req.InitialSymbols)
	if err != nil {
		return "", nil, fmt.Errorf("hash initial_symbols: %w", err)
	}
	if pairsHash != req.InputPairsSHA256 {
		return "", nil, fmt.Errorf("input_pairs_sha256 mismatch")
	}
	var cfg config.Config
	if err := yaml.Unmarshal([]byte(req.ConfigYAML), &cfg); err != nil {
		return "", nil, fmt.Errorf("decode config_yaml: %w", err)
	}
	if cfg.Exchange == nil || cfg.Exchange.Name == "" || cfg.MarketType == "" {
		return "", nil, fmt.Errorf("config_yaml exchange.name and market_type are required")
	}
	if len(cfg.RunPolicy) == 0 {
		return "", nil, fmt.Errorf("config_yaml run_policy is required")
	}
	requestHash, err := RequestSHA256(*req)
	if err != nil {
		return "", nil, fmt.Errorf("hash runtime data plan request: %w", err)
	}
	return requestHash, &cfg, nil
}

func installRuntimeConfig(req *RequestV1, cfg *config.Config) (func(), error) {
	oldData, oldExchange, oldPairMgr := config.Data, config.Exchange, config.PairMgr
	oldPolicies, oldTFs, oldStake := config.RunPolicy, config.RunTimeframes, config.StakeCurrency
	oldPairs, oldRange, oldDataDir := config.Pairs, config.TimeRange, config.DataDir
	oldExg, oldMarket := core.ExgName, core.Market
	oldBacktest, oldLive, oldNet := core.BackTestMode, core.LiveMode, core.NetDisable
	oldTime := btime.CurTimeMS
	restore := func() {
		config.Data, config.Exchange, config.PairMgr = oldData, oldExchange, oldPairMgr
		config.RunPolicy, config.RunTimeframes, config.StakeCurrency = oldPolicies, oldTFs, oldStake
		config.Pairs, config.TimeRange, config.DataDir = oldPairs, oldRange, oldDataDir
		core.ExgName, core.Market = oldExg, oldMarket
		core.BackTestMode, core.LiveMode, core.NetDisable = oldBacktest, oldLive, oldNet
		btime.CurTimeMS = oldTime
	}

	runTimeframes := canonicalizedStrings(cfg.RunTimeframes)
	if len(runTimeframes) == 0 {
		runTimeframes = canonicalizedStrings(config.SplitTimeFrames(cfg.TimeFrames))
	}
	pairMgr := cfg.PairMgr
	if pairMgr == nil {
		pairMgr = &config.PairMgrConfig{}
	}
	runtimeRange := &config.TimeTuple{StartMS: req.TimeStartMS, EndMS: req.TimeEndMS}
	cfg.RunTimeframes = runTimeframes
	cfg.Pairs = slices.Clone(req.InitialSymbols)
	cfg.PairMgr = pairMgr
	cfg.TimeRange = runtimeRange
	config.Data = *cfg
	config.Exchange = cfg.Exchange
	config.PairMgr = pairMgr
	config.StakeCurrency = slices.Clone(cfg.StakeCurrency)
	config.RunTimeframes = runTimeframes
	config.Pairs = slices.Clone(req.InitialSymbols)
	config.TimeRange = runtimeRange
	core.ExgName, core.Market = cfg.Exchange.Name, cfg.MarketType
	core.BackTestMode, core.LiveMode, core.NetDisable = true, false, true
	btime.CurTimeMS = req.TimeStartMS
	config.ClearRefineMap()
	if err := config.SetRunPolicy(true, cfg.RunPolicy...); err != nil {
		restore()
		return nil, fmt.Errorf("initialize runtime data plan policies: %s", err.Short())
	}
	return restore, nil
}

func collectSemanticPlan(req *RequestV1, cfg *config.Config) SemanticPlanV1 {
	plan := SemanticPlanV1{
		Version: Version, SelectionMode: SelectionMode, InitialSymbols: slices.Clone(req.InitialSymbols),
		Policies: make([]PolicyV1, 0, len(config.RunPolicy)), Requirements: []RequirementV1{}, Unsupported: []UnsupportedV1{},
	}
	universe := make(map[string]*orm.ExSymbol, len(req.MarketUniverse))
	envs := make(map[string]*ta.BarEnv)
	for _, item := range req.MarketUniverse {
		exs := orm.GetSymbolByID(item.SID)
		universe[symbolKey(item.Exchange, item.Market, item.Symbol)] = exs
	}
	for _, symbol := range req.InitialSymbols {
		if universe[symbolKey(cfg.Exchange.Name, cfg.MarketType, symbol)] == nil {
			plan.Unsupported = append(plan.Unsupported, unsupported("symbol_outside_snapshot", "", symbol, "", "initial symbol is outside the configured frozen market"))
		}
	}
	frozenStaticPairs := len(req.InitialSymbols) > 0 && len(cfg.PairFilters) == 0 &&
		(cfg.PairMgr == nil || !cfg.PairMgr.ForceFilters) && cfg.BTStrict && cfg.BTNoKlineDownload
	frameworkPairListSymbols := slices.Clone(req.InitialSymbols)
	if frozenStaticPairs {
		// Strict replay already binds the static universe and forbids downloads.
		// The live pair-list/score discovery path must not add requirements that
		// the replay runtime will never read.
		frameworkPairListSymbols = nil
	}
	for _, policy := range config.RunPolicy {
		frameworkPairListSymbols = append(frameworkPairListSymbols, policy.Pairs...)
	}
	frameworkPairListSymbols = stableUniqueStrings(frameworkPairListSymbols)
	frameworkPairScoreSymbols := slices.Clone(frameworkPairListSymbols)

	for _, policy := range config.RunPolicy {
		policyID := policy.ID()
		base, panicText := makeStrategy(policy)
		if panicText != "" {
			plan.Unsupported = append(plan.Unsupported, unsupported("constructor_panic", policyID, "", "", panicText))
			plan.Policies = append(plan.Policies, PolicyV1{PolicyID: policyID, SelectedSymbols: []string{}, CoverageSymbols: []string{}, AllowedRunTimeframes: []string{}})
			continue
		}
		baseAllowed := allowedTimeframes(base)
		if len(policy.Filters) > 0 {
			plan.Unsupported = append(plan.Unsupported, unsupported("policy_filters_require_runtime_data", policyID, "", "", "run_policy filters cannot be evaluated without runtime market data"))
			plan.Policies = append(plan.Policies, PolicyV1{PolicyID: policyID, SelectedSymbols: []string{}, CoverageSymbols: []string{}, AllowedRunTimeframes: baseAllowed})
			continue
		}
		selected := req.InitialSymbols
		if len(policy.Pairs) > 0 {
			selected = policy.Pairs
		}
		selected = slices.Clone(selected)
		if base.OnSymbols != nil {
			var callbackPanic string
			selected, callbackPanic = callOnSymbols(base, selected)
			if callbackPanic != "" {
				plan.Unsupported = append(plan.Unsupported, unsupported("callback_panic", policyID, "", "", "OnSymbols: "+callbackPanic))
				selected = nil
			}
		}
		selected = stableUniqueStrings(selected)
		validSelected := selected[:0]
		for _, symbol := range selected {
			if symbol == "" || strings.TrimSpace(symbol) != symbol {
				plan.Unsupported = append(plan.Unsupported, unsupported("invalid_symbol", policyID, symbol, "", "selected symbol must be a non-empty canonical string"))
				continue
			}
			if universe[symbolKey(cfg.Exchange.Name, cfg.MarketType, symbol)] == nil {
				plan.Unsupported = append(plan.Unsupported, unsupported("symbol_outside_snapshot", policyID, symbol, "", "selected symbol is outside the configured frozen market"))
				continue
			}
			validSelected = append(validSelected, symbol)
		}
		selected = validSelected
		if !frozenStaticPairs {
			frameworkPairScoreSymbols = append(frameworkPairScoreSymbols, selected...)
		}
		maxPair, maxPairErr := effectivePolicyMaxPair(policy, cfg)
		if maxPairErr != "" {
			plan.Unsupported = append(plan.Unsupported, unsupported("invalid_max_pair", policyID, "", "", maxPairErr))
			selected = selected[:0]
		} else if maxPair > 0 && len(selected) > maxPair {
			selected = selected[:maxPair]
		}
		coverage := canonicalizedStrings(selected)
		if base.PickTimeFrame != nil {
			plan.Unsupported = append(plan.Unsupported, unsupported("pick_timeframe_requires_scores", policyID, "", "", "PickTimeFrame requires runtime timeframe scores that are absent from request v1"))
			plan.Policies = append(plan.Policies, PolicyV1{PolicyID: policyID, SelectedSymbols: selected, CoverageSymbols: coverage, AllowedRunTimeframes: baseAllowed})
			continue
		}
		allowedSet := make(map[string]bool)
		for _, tf := range baseAllowed {
			if _, tfErr := utils2.TFToSecSafe(tf); tfErr != nil {
				plan.Unsupported = append(plan.Unsupported, unsupported("invalid_timeframe", policyID, "", tf, tfErr.Error()))
				continue
			}
			allowedSet[tf] = true
		}
		for _, symbol := range selected {
			jobSymbol := universe[symbolKey(cfg.Exchange.Name, cfg.MarketType, symbol)]
			if jobSymbol == nil {
				plan.Unsupported = append(plan.Unsupported, unsupported("symbol_outside_snapshot", policyID, symbol, "", "selected symbol is outside the configured frozen market"))
				continue
			}
			jobPolicy, pairSpecific := policy.PairDup(symbol)
			jobStrategy := base
			if pairSpecific {
				jobStrategy, panicText = makeStrategy(jobPolicy)
				if panicText != "" {
					plan.Unsupported = append(plan.Unsupported, unsupported("constructor_panic", policyID, symbol, "", panicText))
					continue
				}
				if jobStrategy.PickTimeFrame != nil {
					plan.Unsupported = append(plan.Unsupported, unsupported("pick_timeframe_requires_scores", policyID, symbol, "", "pair-specific PickTimeFrame requires runtime timeframe scores that are absent from request v1"))
					continue
				}
			}
			allowed := allowedTimeframes(jobStrategy)
			for _, tf := range allowed {
				if _, tfErr := utils2.TFToSecSafe(tf); tfErr != nil {
					plan.Unsupported = append(plan.Unsupported, unsupported("invalid_timeframe", policyID, symbol, tf, tfErr.Error()))
					continue
				}
				allowedSet[tf] = true
				collectJob(&plan, universe, envs, policyID, jobStrategy, jobSymbol, tf)
			}
		}
		allowed := make([]string, 0, len(allowedSet))
		for tf := range allowedSet {
			allowed = append(allowed, tf)
		}
		slices.Sort(allowed)
		if len(allowed) == 0 {
			plan.Unsupported = append(plan.Unsupported, unsupported("invalid_timeframe", policyID, "", "", "strategy has no allowed run timeframe"))
		}
		plan.Policies = append(plan.Policies, PolicyV1{PolicyID: policyID, SelectedSymbols: selected, CoverageSymbols: coverage, AllowedRunTimeframes: allowed})
	}
	collectFrameworkRequirements(&plan, cfg, universe, frameworkPairListSymbols,
		stableUniqueStrings(frameworkPairScoreSymbols))
	canonicalizePlan(&plan)
	return plan
}

func collectFrameworkRequirements(plan *SemanticPlanV1, cfg *config.Config, universe map[string]*orm.ExSymbol,
	pairListSymbols, pairScoreSymbols []string,
) {
	const (
		frameworkPolicyID = "__framework__"
		staticPairTF      = "1h" // goods.RefreshPairList reads one bar for explicit pairs.
		pairScoreBars     = 600  // strat.CalcPairTfScores scores every allowed timeframe.
	)
	if cfg.PairMgr != nil && cfg.PairMgr.ForceFilters && len(cfg.PairFilters) > 1 {
		plan.Unsupported = append(plan.Unsupported, unsupported("forced_pair_filters_require_runtime_data",
			frameworkPolicyID, "", "", "forced pair filters cannot be inspected without runtime market data"))
		return
	}
	scoreTimeframes := slices.Clone(cfg.RunTimeframes)
	for _, policy := range plan.Policies {
		scoreTimeframes = append(scoreTimeframes, policy.AllowedRunTimeframes...)
	}
	scoreTimeframes = canonicalizedStrings(scoreTimeframes)
	for _, symbol := range pairListSymbols {
		target := universe[symbolKey(cfg.Exchange.Name, cfg.MarketType, symbol)]
		if target == nil {
			plan.Unsupported = append(plan.Unsupported, unsupported("symbol_outside_snapshot", frameworkPolicyID,
				symbol, "", "framework pair-list symbol is outside the configured frozen market"))
			continue
		}
		addRequirement(plan, RequirementV1{
			PolicyID: frameworkPolicyID, JobExchange: target.Exchange, JobMarket: target.Market,
			JobSymbol: symbol, JobTimeframe: staticPairTF, Source: orm.SeriesSourceKline,
			TargetExchange: target.Exchange, TargetMarket: target.Market, TargetSymbol: symbol,
			Timeframe: staticPairTF, WarmupBars: 1, Fields: orm.DefaultKlineFields(),
			SeriesFields: []string{}, Reason: "pair_list",
		})
	}
	for _, timeframe := range scoreTimeframes {
		seconds, err := utils2.TFToSecSafe(timeframe)
		if err == nil && seconds < 60 {
			return
		}
	}
	for _, symbol := range pairScoreSymbols {
		target := universe[symbolKey(cfg.Exchange.Name, cfg.MarketType, symbol)]
		if target == nil {
			plan.Unsupported = append(plan.Unsupported, unsupported("symbol_outside_snapshot", frameworkPolicyID,
				symbol, "", "framework pair-score symbol is outside the configured frozen market"))
			continue
		}
		for _, timeframe := range scoreTimeframes {
			addRequirement(plan, RequirementV1{
				PolicyID: frameworkPolicyID, JobExchange: target.Exchange, JobMarket: target.Market,
				JobSymbol: symbol, JobTimeframe: timeframe, Source: orm.SeriesSourceKline,
				TargetExchange: target.Exchange, TargetMarket: target.Market, TargetSymbol: symbol,
				Timeframe: timeframe, WarmupBars: pairScoreBars, Fields: orm.DefaultKlineFields(),
				SeriesFields: []string{}, Reason: "pair_score",
			})
		}
	}
}

func collectJob(plan *SemanticPlanV1, universe map[string]*orm.ExSymbol, envs map[string]*ta.BarEnv,
	policyID string, strategy *strat.TradeStrat, symbol *orm.ExSymbol, tf string,
) {
	if _, err := utils2.TFToSecSafe(tf); err != nil {
		plan.Unsupported = append(plan.Unsupported, unsupported("invalid_timeframe", policyID, symbol.Symbol, tf, err.Error()))
		return
	}
	env, err := inspectionBarEnv(envs, symbol, tf)
	if err != nil {
		plan.Unsupported = append(plan.Unsupported, unsupported("invalid_timeframe", policyID, symbol.Symbol, tf, err.Error()))
		return
	}
	var effects []string
	job := strat.NewInspectionJob(strategy, env, symbol, tf, config.DefAcc,
		func(name string) {
			effects = append(effects, name)
		})
	addRequirement(plan, RequirementV1{
		PolicyID: policyID, JobExchange: symbol.Exchange, JobMarket: symbol.Market, JobSymbol: symbol.Symbol, JobTimeframe: tf,
		Source: orm.SeriesSourceKline, TargetExchange: symbol.Exchange, TargetMarket: symbol.Market, TargetSymbol: symbol.Symbol,
		Timeframe: tf, WarmupBars: max(0, strategy.WarmupNum), Fields: orm.DefaultKlineFields(), SeriesFields: []string{}, Reason: "primary",
	})
	if strategy.Policy.RefineTF == nil && strategy.RefineTF != nil {
		strategy.Policy.RefineTF = strategy.RefineTF
	}
	matchTF, panicText := refineTimeframe(strategy.Name, tf)
	if panicText != "" {
		plan.Unsupported = append(plan.Unsupported, unsupported("invalid_timeframe", policyID, symbol.Symbol, tf, "refine timeframe: "+panicText))
		return
	}
	if matchTF != tf {
		addRequirement(plan, RequirementV1{
			PolicyID: policyID, JobExchange: symbol.Exchange, JobMarket: symbol.Market, JobSymbol: symbol.Symbol, JobTimeframe: tf,
			Source: orm.SeriesSourceKline, TargetExchange: symbol.Exchange, TargetMarket: symbol.Market, TargetSymbol: symbol.Symbol,
			Timeframe: matchTF, WarmupBars: 0, Fields: orm.DefaultKlineFields(), SeriesFields: []string{}, Reason: "primary",
		})
	}
	if strategy.OnStartUp != nil {
		panicText := callJobCallback(strategy.OnStartUp, job)
		if effect := inspectionOrderEffect(job, effects, "OnStartUp"); effect != "" {
			plan.Unsupported = append(plan.Unsupported, unsupported("startup_order_effect", policyID, symbol.Symbol, tf, effect))
			return
		}
		if panicText != "" {
			plan.Unsupported = append(plan.Unsupported, unsupported("callback_panic", policyID, symbol.Symbol, tf, "OnStartUp: "+panicText))
			return
		}
	}
	if strategy.OnPairInfos != nil {
		items, panicText := callPairInfos(strategy, job)
		if effect := inspectionOrderEffect(job, effects, "OnPairInfos"); effect != "" {
			plan.Unsupported = append(plan.Unsupported, unsupported("startup_order_effect", policyID, symbol.Symbol, tf, effect))
			return
		} else if panicText != "" {
			plan.Unsupported = append(plan.Unsupported, unsupported("callback_panic", policyID, symbol.Symbol, tf, "OnPairInfos: "+panicText))
		} else {
			for _, sub := range items {
				if sub == nil {
					continue
				}
				targetSymbol := symbol.Symbol
				if sub.Pair != "" && sub.Pair != "_cur_" {
					targetSymbol = sub.Pair
				}
				target := universe[symbolKey(symbol.Exchange, symbol.Market, targetSymbol)]
				collectSubscription(plan, policyID, job, target, orm.SeriesSourceKline, sub.TimeFrame, sub.WarmupNum,
					orm.DefaultKlineFields(), nil, "pair_info", targetSymbol)
			}
		}
	}
	if strategy.OnDataSubs != nil {
		items, panicText := callDataSubs(strategy, job)
		if effect := inspectionOrderEffect(job, effects, "OnDataSubs"); effect != "" {
			plan.Unsupported = append(plan.Unsupported, unsupported("startup_order_effect", policyID, symbol.Symbol, tf, effect))
			return
		} else if panicText != "" {
			plan.Unsupported = append(plan.Unsupported, unsupported("callback_panic", policyID, symbol.Symbol, tf, "OnDataSubs: "+panicText))
		} else {
			for _, sub := range items {
				if sub == nil {
					continue
				}
				target := sub.ExSymbol
				if target == nil {
					target = symbol
				}
				targetSymbol := target.Symbol
				frozen := universe[symbolKey(target.Exchange, target.Market, target.Symbol)]
				if target == symbol {
					frozen = symbol
				} else if frozen != nil && (target.ID > 0 && target.ID != frozen.ID || target.ExgReal != "" && target.ExgReal != frozen.ExgReal) {
					frozen = nil
				}
				source := orm.NormalizeSeriesSource(sub.Source)
				seriesFields := orm.MergeSeriesFields(sub.SeriesFields)
				fields := orm.NormalizeSeriesFields(source, sub.Fields)
				if len(seriesFields) > 0 {
					fields = orm.MergeSeriesFields(fields, seriesFields)
				}
				collectSubscription(plan, policyID, job, frozen, source, sub.TimeFrame, sub.WarmupNum,
					fields, seriesFields, "data_sub", targetSymbol)
			}
		}
	}
}

func inspectionBarEnv(envs map[string]*ta.BarEnv, symbol *orm.ExSymbol, tf string) (*ta.BarEnv, error) {
	key := strings.Join([]string{symbol.Symbol, tf}, "_")
	if env := envs[key]; env != nil {
		return env, nil
	}
	env, err := ta.NewBarEnv(core.ExgName, core.Market, symbol.Symbol, tf)
	if err != nil {
		return nil, err
	}
	env.MaxCache = core.NumTaCache
	env.Data.Store("sid", int64(symbol.ID))
	envs[key] = env
	return env, nil
}

func inspectionOrderEffect(job *strat.StratJob, effects []string, callback string) string {
	if len(effects) > 0 {
		return callback + " invoked forbidden order API: " + strings.Join(stableUniqueStrings(effects), ",")
	}
	if len(job.Entrys) > 0 || len(job.Exits) > 0 || len(job.LongOrders) > 0 || len(job.ShortOrders) > 0 || job.OrderNum != 0 || job.EnteredNum != 0 {
		return callback + " directly changed order lifecycle state"
	}
	return ""
}

func collectSubscription(plan *SemanticPlanV1, policyID string, job *strat.StratJob, target *orm.ExSymbol,
	source, tf string, warmup int, fields, seriesFields []string, reason, targetSymbol string,
) {
	if target == nil {
		plan.Unsupported = append(plan.Unsupported, unsupported("symbol_outside_snapshot", policyID, job.Symbol.Symbol, job.TimeFrame,
			"subscription target is outside the frozen market: "+targetSymbol))
		return
	}
	if source != orm.SeriesSourceKline {
		plan.Unsupported = append(plan.Unsupported, unsupported("unsupported_source", policyID, job.Symbol.Symbol, job.TimeFrame,
			"unsupported data source: "+source))
		return
	}
	if _, err := utils2.TFToSecSafe(tf); err != nil {
		plan.Unsupported = append(plan.Unsupported, unsupported("invalid_subscription", policyID, job.Symbol.Symbol, job.TimeFrame,
			"invalid subscription timeframe: "+tf))
		return
	}
	allowed := orm.DefaultKlineFields()
	for _, field := range append(slices.Clone(fields), seriesFields...) {
		if !slices.Contains(allowed, field) {
			plan.Unsupported = append(plan.Unsupported, unsupported("unsupported_field", policyID, job.Symbol.Symbol, job.TimeFrame,
				"unsupported K-line field: "+field))
			return
		}
	}
	addRequirement(plan, RequirementV1{
		PolicyID: policyID, JobExchange: job.Symbol.Exchange, JobMarket: job.Symbol.Market,
		JobSymbol: job.Symbol.Symbol, JobTimeframe: job.TimeFrame, Source: source,
		TargetExchange: target.Exchange, TargetMarket: target.Market, TargetSymbol: target.Symbol,
		Timeframe: tf, WarmupBars: max(0, warmup), Fields: canonicalizedStrings(fields),
		SeriesFields: canonicalizedStrings(seriesFields), Reason: reason,
	})
}

func allowedTimeframes(strategy *strat.TradeStrat) []string {
	items := strategy.RunTimeFrames
	if len(items) == 0 {
		items = config.RunTimeframes
	}
	return canonicalizedStrings(items)
}

func makeStrategy(policy *config.RunPolicyConfig) (result *strat.TradeStrat, panicText string) {
	defer func() {
		if value := recover(); value != nil {
			panicText = fmt.Sprint(value)
		}
	}()
	result = strat.New(policy)
	if result == nil {
		panicText = "strategy constructor returned nil"
	}
	return
}

func callOnSymbols(strategy *strat.TradeStrat, symbols []string) (result []string, panicText string) {
	defer func() {
		if value := recover(); value != nil {
			panicText = fmt.Sprint(value)
		}
	}()
	return strategy.OnSymbols(slices.Clone(symbols)), ""
}

func callJobCallback(callback func(*strat.StratJob), job *strat.StratJob) (panicText string) {
	defer func() {
		if value := recover(); value != nil {
			panicText = fmt.Sprint(value)
		}
	}()
	callback(job)
	return ""
}

func callPairInfos(strategy *strat.TradeStrat, job *strat.StratJob) (result []*strat.PairSub, panicText string) {
	defer func() {
		if value := recover(); value != nil {
			panicText = fmt.Sprint(value)
		}
	}()
	return strategy.OnPairInfos(job), ""
}

func callDataSubs(strategy *strat.TradeStrat, job *strat.StratJob) (result []*strat.DataSub, panicText string) {
	defer func() {
		if value := recover(); value != nil {
			panicText = fmt.Sprint(value)
		}
	}()
	return strategy.OnDataSubs(job), ""
}

func refineTimeframe(strategyName, tf string) (result, panicText string) {
	defer func() {
		if value := recover(); value != nil {
			panicText = fmt.Sprint(value)
		}
	}()
	return config.EnsureStratRefineTF(strategyName, tf), ""
}

func addRequirement(plan *SemanticPlanV1, item RequirementV1) {
	item.Fields = canonicalizedStrings(item.Fields)
	item.SeriesFields = canonicalizedStrings(item.SeriesFields)
	plan.Requirements = append(plan.Requirements, item)
}

func canonicalizePlan(plan *SemanticPlanV1) {
	for i := range plan.Policies {
		plan.Policies[i].SelectedSymbols = stableUniqueStrings(plan.Policies[i].SelectedSymbols)
		plan.Policies[i].CoverageSymbols = canonicalizedStrings(plan.Policies[i].CoverageSymbols)
		plan.Policies[i].AllowedRunTimeframes = canonicalizedStrings(plan.Policies[i].AllowedRunTimeframes)
	}
	slices.SortFunc(plan.Policies, func(a, b PolicyV1) int { return cmp.Compare(a.PolicyID, b.PolicyID) })
	plan.Policies = slices.CompactFunc(plan.Policies, func(a, b PolicyV1) bool { return a.PolicyID == b.PolicyID })
	slices.SortFunc(plan.Requirements, compareRequirements)
	plan.Requirements = mergeRequirements(plan.Requirements)
	slices.SortFunc(plan.Unsupported, compareUnsupported)
	plan.Unsupported = slices.CompactFunc(plan.Unsupported, func(a, b UnsupportedV1) bool { return a == b })
}

func mergeRequirements(items []RequirementV1) []RequirementV1 {
	if len(items) == 0 {
		return []RequirementV1{}
	}
	out := items[:0]
	for _, item := range items {
		if len(out) == 0 || requirementIdentity(out[len(out)-1]) != requirementIdentity(item) {
			out = append(out, item)
			continue
		}
		last := &out[len(out)-1]
		last.WarmupBars = max(last.WarmupBars, item.WarmupBars)
		last.Fields = canonicalizedStrings(append(last.Fields, item.Fields...))
		last.SeriesFields = canonicalizedStrings(append(last.SeriesFields, item.SeriesFields...))
	}
	return out
}

func compareRequirements(a, b RequirementV1) int {
	if order := cmp.Compare(requirementIdentity(a), requirementIdentity(b)); order != 0 {
		return order
	}
	return cmp.Compare(a.WarmupBars, b.WarmupBars)
}

func requirementIdentity(item RequirementV1) string {
	return strings.Join([]string{item.PolicyID, item.JobExchange, item.JobMarket, item.JobSymbol, item.JobTimeframe,
		item.Source, item.TargetExchange, item.TargetMarket, item.TargetSymbol, item.Timeframe, item.Reason}, "\x00")
}

func compareUnsupported(a, b UnsupportedV1) int {
	return cmp.Compare(strings.Join([]string{a.Code, a.PolicyID, a.JobSymbol, a.JobTimeframe, a.Message}, "\x00"),
		strings.Join([]string{b.Code, b.PolicyID, b.JobSymbol, b.JobTimeframe, b.Message}, "\x00"))
}

func unsupported(code, policyID, symbol, tf, message string) UnsupportedV1 {
	return UnsupportedV1{Code: code, PolicyID: policyID, JobSymbol: symbol, JobTimeframe: tf, Message: message}
}

func canonicalUniverse(items []MarketSymbolV1) bool {
	seenSID := make(map[int32]bool, len(items))
	seenKey := make(map[string]bool, len(items))
	for i, item := range items {
		if item.SID <= 0 || item.Exchange == "" || item.Market == "" || item.Symbol == "" || item.ListMS < 0 || item.DelistMS < 0 || seenSID[item.SID] {
			return false
		}
		key := symbolKey(item.Exchange, item.Market, item.Symbol)
		if seenKey[key] || i > 0 && compareMarketSymbols(items[i-1], item) >= 0 {
			return false
		}
		seenSID[item.SID], seenKey[key] = true, true
	}
	return true
}

func compareMarketSymbols(a, b MarketSymbolV1) int {
	if order := cmp.Compare(a.Exchange, b.Exchange); order != 0 {
		return order
	}
	if order := cmp.Compare(a.Market, b.Market); order != 0 {
		return order
	}
	if order := cmp.Compare(a.Symbol, b.Symbol); order != 0 {
		return order
	}
	if order := cmp.Compare(a.ExgReal, b.ExgReal); order != 0 {
		return order
	}
	return cmp.Compare(a.SID, b.SID)
}

func orderedUniqueStrings(items []string) bool {
	seen := make(map[string]bool, len(items))
	for _, item := range items {
		if item == "" || strings.TrimSpace(item) != item || seen[item] {
			return false
		}
		seen[item] = true
	}
	return true
}

func canonicalizedStrings(items []string) []string {
	out := slices.Clone(items)
	slices.Sort(out)
	out = slices.Compact(out)
	if out == nil {
		return []string{}
	}
	return out
}

func stableUniqueStrings(items []string) []string {
	seen := make(map[string]bool, len(items))
	out := make([]string, 0, len(items))
	for _, item := range items {
		if !seen[item] {
			seen[item] = true
			out = append(out, item)
		}
	}
	return out
}

func effectivePolicyMaxPair(policy *config.RunPolicyConfig, cfg *config.Config) (int, string) {
	if policy.MaxPair < 0 {
		return 0, "run_policy max_pair must not be negative"
	}
	if policy.MaxPair != 0 {
		return policy.MaxPair, ""
	}
	names := make([]string, 0, len(cfg.Accounts))
	for name, account := range cfg.Accounts {
		if account != nil && !account.NoTrade {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	if slices.Contains(names, config.DefAcc) {
		return checkedAccountMaxPair(cfg.Accounts[config.DefAcc].MaxPair)
	}
	if len(names) > 0 {
		return checkedAccountMaxPair(cfg.Accounts[names[0]].MaxPair)
	}
	return 999, ""
}

func checkedAccountMaxPair(limit int) (int, string) {
	if limit < 0 {
		return 0, "backtest account max_pair must not be negative"
	}
	if limit == 0 {
		return 999, ""
	}
	return limit, ""
}

func symbolKey(exchange, market, symbol string) string {
	return exchange + "\x00" + market + "\x00" + symbol
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func rawHash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func domainHash(domain string, data []byte) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte(domain))
	_, _ = hash.Write(data)
	return hex.EncodeToString(hash.Sum(nil))
}

func hashJSON(domain string, value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return domainHash(domain, data), nil
}
