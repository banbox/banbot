package config

import (
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/llm"
)

var (
	Data        Config
	Args        *CmdArgs
	Accounts    map[string]*AccountConfig // Exchange tradable account 交易所可交易账户
	BakAccounts map[string]*AccountConfig // Exchange account, not tradable 交易所账户，不可交易
	DefAcc      = "default"               // For non-real trading, the default key of the account (backtesting, simulated trading) 非实盘交易时，账户默认的key（回测、模拟交易）

	Name             string
	Loaded           bool
	Leverage         float64
	LimitVolSecs     int // How long the limit order is expected to wait for execution, in seconds 限价单预期等待多长时间成交，单位秒
	PutLimitSecs     int // Only limit orders executed within this expected time will be submitted to the exchange. 在此预期时间内成交的限价单，才提交到交易所
	AccountPullSecs  int
	OdBookTtl        int64
	StopEnterBars    int // The entry limit order will be canceled if it is not filled after the number of candles. 入场限价单超过多少个蜡烛未成交则取消
	OrderType        string
	PreFire          float64
	MarginAddRate    float64 // When trading contracts, if a loss occurs and the loss reaches this value of the initial margin ratio, additional margin will be required to avoid forced liquidation. 交易合约时，如出现亏损，亏损达到初始保证金比率的此值时，进行追加保证金，避免强平
	ChargeOnBomb     bool
	TakeOverStrat    string
	CloseOnStuck     int
	StakeAmount      float64 // The amount of a single order, the priority is lower than StakePct 单笔开单金额，优先级低于StakePct
	StakePct         float64 // Percentage of single bill amount 单笔开单金额百分比
	MaxStakeAmt      float64 // Maximum bill amount for a single transaction 单笔最大开单金额
	OpenVolRate      float64 // When opening an order without specifying a quantity, the multiple of the maximum allowed order quantity/average candle trading volume, defaults to 1 未指定数量开单时，最大允许开单数量/平均蜡烛成交量的倍数，默认1
	MinOpenRate      float64 // When the wallet balance is less than the single amount, orders are allowed to be issued when it reaches this ratio of the single amount. 钱包余额不足单笔金额时，达到单笔金额的此比例则允许开单
	LowCostAction    string  // Actions taken when stake amount less than the minimum amount 花费不足最小金额时的动作：ignore, keep
	BTNetCost        float64 // Order placement delay during backtesting, simulated slippage, unit seconds 回测时下单延迟，模拟滑点，单位秒
	RelaySimUnFinish bool    // 交易新品种时(回测/实盘)，是否从开始时间未平仓订单接力开始交易
	NTPLangCode      string  // NTP真实时间同步所用langCode，默认none不启用
	ShowLangCode     string
	BTInLive         *BtInLiveConfig
	OrderBarMax      int // 查找开始时间未平仓订单向前模拟最大bar数量
	MaxOpenOrders    int
	MaxSimulOpen     int
	WalletAmounts    map[string]float64
	DrawBalanceOver  float64
	StakeCurrency    []string
	StakeCurrencyMap map[string]bool
	FatalStop        map[int]float64
	FatalStopHours   int
	TimeRange        *TimeTuple
	RunTimeframes    []string
	KlineSource      string
	WatchJobs        map[string][]string
	RunPolicy        []*RunPolicyConfig
	StratPerf        *StratPerfConfig
	Pairs            []string
	PairMgr          *PairMgrConfig
	PairFilters      []*CommonPairFilter
	Exchange         *ExchangeConfig
	DataDir          string
	stratDir         string
	Database         *DatabaseConfig
	SpiderAddr       string
	APIServer        *APIServerConfig
	RPCChannels      map[string]map[string]interface{}
	Mail             *MailConfig
	Webhook          map[string]map[string]string
)

const (
	MinPairCronGapMS = 1800000 // 交易对刷新最小间隔半小时
)

var (
	noExtends = map[string]bool{
		"run_policy":     true,
		"wallet_amounts": true,
		"fatal_stop":     true,
		"watch_jobs":     true,
	}
)

// Config Is the root configuration structure 是根配置结构体
type Config struct {
	Name             string                            `yaml:"name,omitempty" mapstructure:"name"`
	Env              string                            `yaml:"env,omitempty" mapstructure:"env"`
	Leverage         float64                           `yaml:"leverage,omitempty" mapstructure:"leverage"`
	LimitVolSecs     int                               `yaml:"limit_vol_secs,omitempty" mapstructure:"limit_vol_secs"`
	PutLimitSecs     int                               `yaml:"put_limit_secs,omitempty" mapstructure:"put_limit_secs"`
	AccountPullSecs  int                               `yaml:"account_pull_secs,omitempty" mapstructure:"account_pull_secs"`
	MarketType       string                            `yaml:"market_type,omitempty" mapstructure:"market_type"`
	ContractType     string                            `yaml:"contract_type,omitempty" mapstructure:"contract_type"`
	OdBookTtl        int64                             `yaml:"odbook_ttl,omitempty" mapstructure:"odbook_ttl"`
	StopEnterBars    int                               `yaml:"stop_enter_bars,omitempty" mapstructure:"stop_enter_bars"`
	ConcurNum        int                               `yaml:"concur_num,omitempty" mapstructure:"concur_num"`
	OrderType        string                            `yaml:"order_type,omitempty" mapstructure:"order_type"`
	PreFire          float64                           `yaml:"prefire,omitempty" mapstructure:"prefire"`
	MarginAddRate    float64                           `yaml:"margin_add_rate,omitempty" mapstructure:"margin_add_rate"`
	ChargeOnBomb     bool                              `yaml:"charge_on_bomb,omitempty" mapstructure:"charge_on_bomb"`
	TakeOverStrat    string                            `yaml:"take_over_strat,omitempty" mapstructure:"take_over_strat"`
	CloseOnStuck     int                               `yaml:"close_on_stuck,omitempty" mapstructure:"close_on_stuck"`
	StakeAmount      float64                           `yaml:"stake_amount,omitempty" mapstructure:"stake_amount"`
	StakePct         float64                           `yaml:"stake_pct,omitempty" mapstructure:"stake_pct"`
	MaxStakeAmt      float64                           `yaml:"max_stake_amt,omitempty" mapstructure:"max_stake_amt"`
	OpenVolRate      float64                           `yaml:"open_vol_rate,omitempty" mapstructure:"open_vol_rate"`
	MinOpenRate      float64                           `yaml:"min_open_rate,omitempty" mapstructure:"min_open_rate"`
	LowCostAction    string                            `yaml:"low_cost_action,omitempty" mapstructure:"low_cost_action"`
	BTNetCost        float64                           `yaml:"bt_net_cost,omitempty" mapstructure:"bt_net_cost"`
	RelaySimUnFinish bool                              `yaml:"relay_sim_unfinish,omitempty" mapstructure:"relay_sim_unfinish"`
	NTPLangCode      string                            `yaml:"ntp_lang_code,omitempty" mapstructure:"ntp_lang_code"`
	ShowLangCode     string                            `yaml:"show_lang_code,omitempty" mapstructure:"show_lang_code"`
	BTInLive         *BtInLiveConfig                   `yaml:"bt_in_live,omitempty" mapstructure:"bt_in_live"`
	OrderBarMax      int                               `yaml:"order_bar_max,omitempty" mapstructure:"order_bar_max"`
	MaxOpenOrders    int                               `yaml:"max_open_orders,omitempty" mapstructure:"max_open_orders"`
	MaxSimulOpen     int                               `yaml:"max_simul_open,omitempty" mapstructure:"max_simul_open"`
	WalletAmounts    map[string]float64                `yaml:"wallet_amounts,omitempty" mapstructure:"wallet_amounts"`
	DrawBalanceOver  float64                           `yaml:"draw_balance_over,omitempty" mapstructure:"draw_balance_over"`
	StakeCurrency    []string                          `yaml:"stake_currency,omitempty,flow" mapstructure:"stake_currency"`
	FatalStop        map[string]float64                `yaml:"fatal_stop,omitempty" mapstructure:"fatal_stop"`
	FatalStopHours   int                               `yaml:"fatal_stop_hours,omitempty" mapstructure:"fatal_stop_hours"`
	TimeRangeRaw     string                            `yaml:"timerange,omitempty" mapstructure:"timerange"`
	TimeStart        string                            `yaml:"time_start,omitempty" mapstructure:"time_start"`
	TimeEnd          string                            `yaml:"time_end,omitempty" mapstructure:"time_end"`
	TimeRange        *TimeTuple                        `yaml:"-" json:"-" mapstructure:"-"`
	TimeFrames       string                            `yaml:"timeframes" mapstructure:"timeframes"`
	RunTimeframes    []string                          `yaml:"run_timeframes,omitempty,flow" mapstructure:"run_timeframes"`
	KlineSource      string                            `yaml:"kline_source,omitempty" mapstructure:"kline_source"`
	WatchJobs        map[string][]string               `yaml:"watch_jobs,omitempty" mapstructure:"watch_jobs"`
	RunPolicy        []*RunPolicyConfig                `yaml:"run_policy,omitempty" mapstructure:"run_policy"`
	StratPerf        *StratPerfConfig                  `yaml:"strat_perf,omitempty" mapstructure:"strat_perf"`
	Pairs            []string                          `yaml:"pairs,omitempty,flow" mapstructure:"pairs"`
	PairMgr          *PairMgrConfig                    `yaml:"pairmgr,omitempty" mapstructure:"pairmgr"`
	PairFilters      []*CommonPairFilter               `yaml:"pairlists,omitempty" mapstructure:"pairlists"`
	Accounts         map[string]*AccountConfig         `yaml:"accounts,omitempty" mapstructure:"accounts,omitempty"`
	Exchange         *ExchangeConfig                   `yaml:"exchange,omitempty" mapstructure:"exchange"`
	Database         *DatabaseConfig                   `yaml:"database,omitempty" mapstructure:"database"`
	SpiderAddr       string                            `yaml:"spider_addr,omitempty" mapstructure:"spider_addr"`
	APIServer        *APIServerConfig                  `yaml:"api_server,omitempty" mapstructure:"api_server"`
	RPCChannels      map[string]map[string]interface{} `yaml:"rpc_channels,omitempty" mapstructure:"rpc_channels"`
	Mail             *MailConfig                       `yaml:"mail,omitempty" mapstructure:"mail"`
	Webhook          map[string]map[string]string      `yaml:"webhook,omitempty" mapstructure:"webhook"`
	LLMModels        map[string]*llm.LLMModelConfig    `yaml:"llm_models" mapstructure:"llm_models"`
}

// The strategy to run, multiple strategies can be run at the same time 运行的策略，可以多个策略同时运行
type RunPolicyConfig struct {
	Name          string                        `yaml:"name" mapstructure:"name"`
	Filters       []*CommonPairFilter           `yaml:"filters,omitempty" mapstructure:"filters"`
	TimeFrames    string                        `yaml:"timeframes" mapstructure:"timeframes"`
	RunTimeframes []string                      `yaml:"run_timeframes,omitempty,flow" mapstructure:"run_timeframes"`
	RefineTF      interface{}                   `yaml:"refine_tf" mapstructure:"refine_tf"`
	MaxPair       int                           `yaml:"max_pair,omitempty" mapstructure:"max_pair"`
	MaxOpen       int                           `yaml:"max_open,omitempty" mapstructure:"max_open"`
	MaxSimulOpen  int                           `yaml:"max_simul_open,omitempty" mapstructure:"max_simul_open"`
	OrderBarMax   int                           `yaml:"order_bar_max,omitempty" mapstructure:"order_bar_max"`
	StakeRate     float64                       `yaml:"stake_rate,omitempty" mapstructure:"stake_rate"`
	Dirt          string                        `yaml:"dirt,omitempty" mapstructure:"dirt"`
	StopLoss      interface{}                   `yaml:"stop_loss,omitempty" mapstructure:"stop_loss"`
	StratPerf     *StratPerfConfig              `yaml:"strat_perf,omitempty" mapstructure:"strat_perf"`
	Pairs         []string                      `yaml:"pairs,omitempty,flow" mapstructure:"pairs"`
	Params        map[string]float64            `yaml:"params,omitempty" mapstructure:"params"`
	PairParams    map[string]map[string]float64 `yaml:"pair_params,omitempty" mapstructure:"pair_params"`
	More          map[string]interface{}        `yaml:",inline" mapstructure:",remain"`
	defs          map[string]*core.Param
	Score         float64
	Index         int // index in run_policy array
}

type BtInLiveConfig struct {
	Cron   string   `yaml:"cron" mapstructure:"cron"`
	Acount string   `yaml:"account" mapstructure:"account"`
	MailTo []string `yaml:"mail_to" mapstructure:"mail_to"`
}

type StratPerfConfig struct {
	Enable    bool    `yaml:"enable" mapstructure:"enable"`
	MinOdNum  int     `yaml:"min_od_num,omitempty" mapstructure:"min_od_num"`
	MaxOdNum  int     `yaml:"max_od_num,omitempty" mapstructure:"max_od_num"`
	MinJobNum int     `yaml:"min_job_num,omitempty" mapstructure:"min_job_num"`
	MidWeight float64 `yaml:"mid_weight,omitempty" mapstructure:"mid_weight"`
	BadWeight float64 `yaml:"bad_weight,omitempty" mapstructure:"bad_weight"`
}

type DatabaseConfig struct {
	Url         string  `yaml:"url,omitempty" mapstructure:"url"`
	Retention   string  `yaml:"retention,omitempty" mapstructure:"retention"`
	MaxPoolSize int     `yaml:"max_pool_size,omitempty" mapstructure:"max_pool_size"`
	AutoCreate  bool    `yaml:"auto_create" mapstructure:"auto_create"`
	QdbMemPct   float64 `yaml:"qdb_mem_pct,omitempty" mapstructure:"qdb_mem_pct"`     // QuestDB memory usage ratio (0~1), default 0.3
	QdbMaxMemMB int     `yaml:"qdb_max_mem_mb,omitempty" mapstructure:"qdb_max_mem_mb"` // QuestDB max memory in MB, default 16384
}

type APIServerConfig struct {
	Enable       bool          `yaml:"enable" mapstructure:"enable"`                           // Whether to enable 是否启用
	BindIPAddr   string        `yaml:"bind_ip" mapstructure:"bind_ip"`                         // Binding address, 0.0.0.0 means exposed to the public network 绑定地址，0.0.0.0表示暴露到公网
	Port         int           `yaml:"port" mapstructure:"port"`                               // LOCAL LISTENING PORT 本地监听端口
	Verbosity    string        `yaml:"verbosity" mapstructure:"verbosity"`                     // Detail level 详细程度
	JWTSecretKey string        `yaml:"jwt_secret_key,omitempty" mapstructure:"jwt_secret_key"` // Key used for password encryption 用于密码加密的密钥
	CORSOrigins  []string      `yaml:"CORS_origins,flow" mapstructure:"CORS_origins"`          // When accessing banweb, you need to add the address of banweb here to allow access. banweb访问时，要这里添加banweb的地址放行
	Users        []*UserConfig `yaml:"users" mapstructure:"users"`                             // Login user 登录用户
}

type UserConfig struct {
	Username    string            `yaml:"user,omitempty" mapstructure:"user"`           // 用户名
	Password    string            `yaml:"pwd,omitempty" mapstructure:"pwd"`             // 密码
	AllowIPs    []string          `yaml:"allow_ips" mapstructure:"allow_ips"`           // Allow access from specific IP addresses 允许从特定IP地址访问
	AccRoles    map[string]string `yaml:"acc_roles,omitempty" mapstructure:"acc_roles"` // Role permissions for different accounts 对不同账户的角色权限
	ExpireHours float64           `yaml:"exp_hours" mapstructure:"exp_hours"`           // Token expiration time, default 168 hours token过期时间，默认168小时
}

/** ********************************** RPC Channel Configuration 渠道配置 ******************************** */

type MailConfig struct {
	Enable   bool   `yaml:"enable" mapstructure:"enable"`
	Host     string `yaml:"host" mapstructure:"host"`
	Port     int    `yaml:"port" mapstructure:"port"`
	Username string `yaml:"username" mapstructure:"username"`
	Password string `yaml:"password" mapstructure:"password"`
}

/** ********************************** Symbol FILTER标的筛选器  ******************************** */

type PairMgrConfig struct {
	Cron string `yaml:"cron" mapstructure:"cron"`
	// Offset limited quantity selection. 偏移限定数量选择。
	Offset int `yaml:"offset" mapstructure:"offset,omitempty"`
	// Limit the number of currencies 限制币种数量
	Limit int `yaml:"limit" mapstructure:"limit,omitempty"`
	// apply filters to static pairs force
	ForceFilters bool `yaml:"force_filters" mapstructure:"force_filters,omitempty"`
	// hold/close 品种切换时保留还是退出仓位
	PosOnRotation string `yaml:"pos_on_rotation" mapstructure:"pos_on_rotation"`
	UseLatest     bool   `yaml:"use_latest" mapstructure:"use_latest"`
}

// UNIVERSAL FILTER 通用的过滤器
type CommonPairFilter struct {
	Name  string                 `yaml:"name" mapstructure:"name"`
	Items map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

/** ********************************** Exchange part configuration 交易所部分配置 ******************************** */

// ExchangeConfig Represents the configuration information of the exchange 表示交易所的配置信息
type ExchangeConfig struct {
	Name  string                            `yaml:"name" mapstructure:"name"`
	Items map[string]map[string]interface{} `yaml:",inline" mapstructure:",remain"`
}

// AccountConfig Configuration to store API keys and secrets 存储 API 密钥和秘密的配置
type AccountConfig struct {
	NoTrade       bool                      `yaml:"no_trade,omitempty" mapstructure:"no_trade"`
	StakeRate     float64                   `yaml:"stake_rate,omitempty" mapstructure:"stake_rate"`       // Multiple of billing amount relative to benchmark  相对基准的开单金额倍数
	StakePctAmt   float64                   `yaml:"-"`                                                    // The amount currently allowed when billing by percentage按百分比开单时，当前允许的金额
	MaxStakeAmt   float64                   `yaml:"max_stake_amt,omitempty" mapstructure:"max_stake_amt"` // Maximum amount allowed for a single transaction 允许的单笔最大金额
	Leverage      float64                   `yaml:"leverage,omitempty" mapstructure:"leverage"`
	MaxPair       int                       `yaml:"max_pair,omitempty" mapstructure:"max_pair"`
	MaxOpenOrders int                       `yaml:"max_open_orders,omitempty" mapstructure:"max_open_orders"`
	RPCChannels   []map[string]interface{}  `yaml:"rpc_channels,omitempty" mapstructure:"rpc_channels"`
	APIServer     *AccPwdRole               `yaml:"api_server,omitempty" mapstructure:"api_server"`
	Exchanges     map[string]*ExgApiSecrets `yaml:",inline" mapstructure:",remain"`
}

type ExgApiSecrets struct {
	Prod *ApiSecretConfig `yaml:"prod,omitempty" mapstructure:"prod"`
	Test *ApiSecretConfig `yaml:"test,omitempty" mapstructure:"test"`
}

type ApiSecretConfig struct {
	APIKey    string `yaml:"api_key,omitempty" mapstructure:"api_key"`
	APISecret string `yaml:"api_secret,omitempty" mapstructure:"api_secret"`
	Password  string `yaml:"password,omitempty" mapstructure:"password"`
}

type AccPwdRole struct {
	Pwd  string `yaml:"pwd,omitempty" mapstructure:"pwd"`
	Role string `yaml:"role,omitempty" mapstructure:"role"`
}

type TimeTuple struct {
	StartMS int64
	EndMS   int64
}

type MarketRange struct {
	Exchange  string `yaml:"exchange"`
	ExgReal   string `yaml:"exg_real"`
	Market    string `yaml:"market"`
	TimeRange string `yaml:"time_range"`
}

type MarketSymbolsRange struct {
	*MarketRange `yaml:",inline"`
	Symbols      []string `yaml:"symbols,flow"`
}

type MarketTFSymbolsRange struct {
	*MarketSymbolsRange `yaml:",inline"`
	TimeFrames          []string `yaml:"timeframes,flow"`
}

// ExportConfig represents the export configuration
type ExportConfig struct {
	Klines     []*MarketTFSymbolsRange `yaml:"klines"`
	AdjFactors []*MarketSymbolsRange   `yaml:"adj_factors"`
	Calendars  []*MarketRange          `yaml:"calendars"`
}
