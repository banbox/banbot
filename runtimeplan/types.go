package runtimeplan

const (
	Version       = 1
	SelectionMode = "all_allowed_timeframes_no_kline"

	requestHashDomain  = "banbot-runtime-data-plan-request-v1\x00"
	semanticHashDomain = "banbot-runtime-data-plan-v1\x00"
	universeHashDomain = "banbot-runtime-market-universe-v1\x00"
	pairsHashDomain    = "banbot-runtime-input-pairs-v1\x00"
)

type RequestV1 struct {
	Version                    int              `json:"version"`
	CompileKey                 string           `json:"compile_key"`
	CompileBaseName            string           `json:"compile_base_name"`
	CompileVersion             string           `json:"compile_version"`
	StrategyInputSHA256        string           `json:"strategy_input_sha256"`
	StrategySourceSHA256       string           `json:"strategy_source_sha256"`
	CompileManifestSHA256      string           `json:"compile_manifest_sha256"`
	CompiledBinarySHA256       string           `json:"compiled_binary_sha256"`
	BanbotCommit               string           `json:"banbot_commit"`
	BanbotSourceManifestSHA256 string           `json:"banbot_source_manifest_sha256"`
	ConfigYAML                 string           `json:"config_yaml"`
	ConfigSHA256               string           `json:"config_sha256"`
	ConfigSemanticSHA256       string           `json:"config_semantic_sha256"`
	MarketSnapshotIdentity     string           `json:"market_snapshot_identity"`
	MarketSnapshotSHA256       string           `json:"market_snapshot_sha256"`
	MarketUniverseSHA256       string           `json:"market_universe_sha256"`
	MarketUniverse             []MarketSymbolV1 `json:"market_universe"`
	InitialSymbols             []string         `json:"initial_symbols"`
	InputPairsSHA256           string           `json:"input_pairs_sha256"`
	TimeStartMS                int64            `json:"time_start_ms"`
	TimeEndMS                  int64            `json:"time_end_ms"`
}

type MarketSymbolV1 struct {
	SID      int32  `json:"sid"`
	Exchange string `json:"exchange"`
	ExgReal  string `json:"exg_real"`
	Market   string `json:"market"`
	Symbol   string `json:"symbol"`
	Combined bool   `json:"combined"`
	ListMS   int64  `json:"list_ms"`
	DelistMS int64  `json:"delist_ms"`
}

type SemanticPlanV1 struct {
	Version        int             `json:"version"`
	SelectionMode  string          `json:"selection_mode"`
	InitialSymbols []string        `json:"initial_symbols"`
	Policies       []PolicyV1      `json:"policies"`
	Requirements   []RequirementV1 `json:"requirements"`
	Unsupported    []UnsupportedV1 `json:"unsupported"`
}

type PolicyV1 struct {
	PolicyID             string   `json:"policy_id"`
	SelectedSymbols      []string `json:"selected_symbols"`
	AllowedRunTimeframes []string `json:"allowed_run_timeframes"`
}

type RequirementV1 struct {
	PolicyID       string   `json:"policy_id"`
	JobExchange    string   `json:"job_exchange"`
	JobMarket      string   `json:"job_market"`
	JobSymbol      string   `json:"job_symbol"`
	JobTimeframe   string   `json:"job_timeframe"`
	Source         string   `json:"source"`
	TargetExchange string   `json:"target_exchange"`
	TargetMarket   string   `json:"target_market"`
	TargetSymbol   string   `json:"target_symbol"`
	Timeframe      string   `json:"timeframe"`
	WarmupBars     int      `json:"warmup_bars"`
	Fields         []string `json:"fields"`
	SeriesFields   []string `json:"series_fields"`
	Reason         string   `json:"reason"`
}

type UnsupportedV1 struct {
	Code         string `json:"code"`
	PolicyID     string `json:"policy_id"`
	JobSymbol    string `json:"job_symbol"`
	JobTimeframe string `json:"job_timeframe"`
	Message      string `json:"message"`
}

type OutputV1 struct {
	Version            int             `json:"version"`
	RequestSHA256      string          `json:"request_sha256"`
	SelectionMode      string          `json:"selection_mode"`
	InitialSymbols     []string        `json:"initial_symbols"`
	Policies           []PolicyV1      `json:"policies"`
	Requirements       []RequirementV1 `json:"requirements"`
	Unsupported        []UnsupportedV1 `json:"unsupported"`
	SemanticPlanSHA256 string          `json:"semantic_plan_sha256"`
}
