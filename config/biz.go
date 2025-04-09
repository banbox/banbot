package config

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/core"
	utils2 "github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/banbox/banexg/utils"
	"github.com/go-viper/mapstructure/v2"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

func GetDataDir() string {
	if DataDir == "" {
		DataDir = getEnvPath("BanDataDir")
		if DataDir == "" {
			panic("env `BanDataDir` or args `-datadir` is required")
		}
	}
	return DataDir
}

func GetDataDirSafe() string {
	if DataDir == "" {
		DataDir = getEnvPath("BanDataDir")
	}
	return DataDir
}

func GetLogsDir() string {
	logDir := filepath.Join(GetDataDir(), "logs")
	err := utils2.EnsureDir(logDir, 0755)
	if err != nil {
		panic(err)
	}
	return logDir
}

func GetStratDir() string {
	if stratDir == "" {
		stratDir = getEnvPath("BanStratDir")
	}
	return stratDir
}

func getEnvPath(key string) string {
	absPath, err := filepath.Abs(strings.TrimSpace(os.Getenv(key)))
	if err != nil {
		panic(err)
	}
	return strings.TrimSpace(absPath)
}

func LoadConfig(args *CmdArgs) *errs.Error {
	if Loaded {
		return nil
	}
	cfg, err := GetConfig(args, true)
	if err != nil {
		return err
	}
	return ApplyConfig(args, cfg)
}

/*
GetConfig get config from args

args: NoDefault, Configs, TimeRange, MaxPoolSize, StakeAmount, StakePct, TimeFrames, Pairs
*/
func GetConfig(args *CmdArgs, showLog bool) (*Config, *errs.Error) {
	args.Init()
	var paths []string
	var res Config
	if !args.NoDefault {
		dataDir := GetDataDir()
		if dataDir == "" {
			return nil, errs.NewMsg(errs.CodeParamRequired, "-datadir or env `BanDataDir` is required")
		}
		tryNames := []string{"config.yml", "config.local.yml"}
		for _, name := range tryNames {
			path := filepath.Join(dataDir, name)
			if _, err := os.Stat(path); err == nil {
				paths = append(paths, path)
			}
		}
	}
	if args.Configs != nil {
		paths = append(paths, args.Configs...)
	}
	var merged = make(map[string]interface{})
	for _, path := range paths {
		if showLog {
			log.Info("Using " + path)
		}
		fileData, err := os.ReadFile(ParsePath(path))
		if err != nil {
			return nil, errs.NewFull(core.ErrIOReadFail, err, "Read %s Fail", path)
		}
		var unpak map[string]interface{}
		err = yaml.Unmarshal(fileData, &unpak)
		if err != nil {
			return nil, errs.NewFull(errs.CodeUnmarshalFail, err, "Unmarshal %s Fail", path)
		}
		for key := range noExtends {
			if _, ok := unpak[key]; ok {
				delete(merged, key)
			}
		}
		utils2.DeepCopyMap(merged, unpak)
	}
	err := mapstructure.Decode(merged, &res)
	if err != nil {
		return nil, errs.NewFull(errs.CodeUnmarshalFail, err, "decode Config Fail")
	}
	err = res.Apply(args)
	if err != nil {
		return nil, errs.New(errs.CodeRunTime, err)
	}
	return &res, nil
}

func ParseConfig(path string) (*Config, *errs.Error) {
	var res Config
	fileData, err := os.ReadFile(ParsePath(path))
	if err != nil {
		return nil, errs.NewFull(core.ErrIOReadFail, err, "Read %s Fail", path)
	}
	var unpak map[string]interface{}
	err = yaml.Unmarshal(fileData, &unpak)
	if err != nil {
		return nil, errs.NewFull(errs.CodeUnmarshalFail, err, "Unmarshal %s Fail", path)
	}
	err = mapstructure.Decode(unpak, &res)
	if err != nil {
		return nil, errs.NewFull(errs.CodeUnmarshalFail, err, "decode Config Fail")
	}
	return &res, nil
}

func (c *Config) Apply(args *CmdArgs) error {
	if args.TimeRange != "" {
		c.TimeRangeRaw = args.TimeRange
	}
	if args.TimeStart != "" {
		c.TimeStart = args.TimeStart
		c.TimeEnd = args.TimeEnd
	}
	if args.MaxPoolSize > 0 {
		c.Database.MaxPoolSize = args.MaxPoolSize
	}
	var start, stop = int64(0), int64(0)
	var err error
	if c.TimeStart != "" {
		start, err = btime.ParseTimeMS(c.TimeStart)
		if err != nil {
			return err
		}
		if c.TimeEnd != "" {
			stop, err = btime.ParseTimeMS(c.TimeEnd)
			if err != nil {
				return err
			}
		} else {
			stop = btime.UTCStamp()
		}
	} else {
		start, stop, err = ParseTimeRange(c.TimeRangeRaw)
		if err != nil {
			return err
		}
	}
	c.TimeRange = &TimeTuple{start, stop}
	if args.StakeAmount > 0 {
		c.StakeAmount = args.StakeAmount
	}
	if args.StakePct > 0 {
		c.StakePct = args.StakePct
	}
	if len(args.TimeFrames) > 0 {
		c.RunTimeframes = args.TimeFrames
	}
	if len(args.Pairs) > 0 {
		c.Pairs = args.Pairs
	}
	return nil
}

func ApplyConfig(args *CmdArgs, c *Config) *errs.Error {
	Loaded = true
	Name = c.Name
	Args = args
	Data = *c
	if args.DataDir != "" {
		DataDir = args.DataDir
	}
	core.SetRunEnv(c.Env)
	Leverage = c.Leverage
	LimitVolSecs = c.LimitVolSecs
	if LimitVolSecs == 0 {
		LimitVolSecs = 10
	}
	PutLimitSecs = c.PutLimitSecs
	if PutLimitSecs == 0 {
		PutLimitSecs = 120
	}
	AccountPullSecs = c.AccountPullSecs
	if AccountPullSecs == 0 {
		AccountPullSecs = 60
	}
	core.ExgName = c.Exchange.Name
	core.Market = c.MarketType
	if core.Market == banexg.MarketSpot || core.Market == banexg.MarketMargin {
		c.ContractType = ""
	} else if c.ContractType == "" {
		c.ContractType = banexg.MarketSwap
	}
	core.ContractType = c.ContractType
	OdBookTtl = c.OdBookTtl
	if OdBookTtl == 0 {
		OdBookTtl = 500
	}
	StopEnterBars = c.StopEnterBars
	core.ConcurNum = c.ConcurNum
	if core.ConcurNum == 0 {
		core.ConcurNum = 2
	}
	OrderType = c.OrderType
	PreFire = c.PreFire
	MarginAddRate = c.MarginAddRate
	if MarginAddRate == 0 {
		MarginAddRate = 0.66
	}
	ChargeOnBomb = c.ChargeOnBomb
	TakeOverStrat = c.TakeOverStrat
	StakeAmount = c.StakeAmount
	StakePct = c.StakePct
	MaxStakeAmt = c.MaxStakeAmt
	OpenVolRate = c.OpenVolRate
	if OpenVolRate == 0 {
		OpenVolRate = 1
	}
	MinOpenRate = c.MinOpenRate
	if MinOpenRate == 0 {
		MinOpenRate = 0.5
	}
	if c.LowCostAction != "" {
		if _, ok := core.LowCostVals[c.LowCostAction]; !ok {
			return errs.NewMsg(core.ErrBadConfig, "invalid low_cost_action: %s", c.LowCostAction)
		}
	}
	LowCostAction = c.LowCostAction
	BTNetCost = c.BTNetCost
	if BTNetCost == 0 {
		BTNetCost = 15
	}
	RelaySimUnFinish = c.RelaySimUnFinish
	NTPLangCode = c.NTPLangCode
	if NTPLangCode == "" {
		NTPLangCode = "none"
	}
	OrderBarMax = c.OrderBarMax
	if OrderBarMax == 0 {
		OrderBarMax = 500
	}
	MaxOpenOrders = c.MaxOpenOrders
	MaxSimulOpen = c.MaxSimulOpen
	WalletAmounts = c.WalletAmounts
	DrawBalanceOver = c.DrawBalanceOver
	StakeCurrency = c.StakeCurrency
	if len(StakeCurrency) == 0 {
		panic("config `stake_currency` cannot be empty")
	}
	StakeCurrencyMap = make(map[string]bool)
	for _, curr := range c.StakeCurrency {
		StakeCurrencyMap[curr] = true
	}
	FatalStop = make(map[int]float64)
	if len(c.FatalStop) > 0 {
		for text, rate := range c.FatalStop {
			mins, err_ := strconv.Atoi(text)
			if err_ != nil || mins < 1 {
				panic("config fatal_stop." + text + " invalid, must be int >= 1")
			}
			FatalStop[mins] = rate
		}
	}
	if c.FatalStopHours == 0 {
		c.FatalStopHours = 8
	}
	FatalStopHours = c.FatalStopHours
	TimeRange = c.TimeRange
	RunTimeframes = c.RunTimeframes
	WatchJobs = c.WatchJobs
	if c.StratPerf == nil {
		c.StratPerf = &StratPerfConfig{
			MinOdNum:  5,
			MaxOdNum:  30,
			MinJobNum: 10,
			MidWeight: 0.3,
			BadWeight: 0.15,
		}
	} else {
		c.StratPerf.Validate()
	}
	StratPerf = c.StratPerf
	ApplyPairPolicy(c.Pairs, c.RunPolicy)
	if c.PairMgr == nil {
		c.PairMgr = &PairMgrConfig{}
	}
	PairMgr = c.PairMgr
	PairFilters = c.PairFilters
	Accounts = c.Accounts
	Exchange = c.Exchange
	if Exchange == nil {
		Exchange = &ExchangeConfig{
			Name:  "binance",
			Items: make(map[string]map[string]interface{}),
		}
	}
	err := initExgAccs(args)
	if err != nil {
		return err
	}
	Database = c.Database
	SpiderAddr = strings.ReplaceAll(c.SpiderAddr, "host.docker.internal", "127.0.0.1")
	if SpiderAddr == "" {
		SpiderAddr = "127.0.0.1:6789"
	}
	APIServer = c.APIServer
	RPCChannels = c.RPCChannels
	Webhook = c.Webhook
	return nil
}

func ApplyPairPolicy(pairs []string, policies []*RunPolicyConfig) {
	staticPairs, fixPairs := initPolicies(policies)
	if len(pairs) > 0 {
		staticPairs = true
		fixPairs = append(fixPairs, pairs...)
	}
	if staticPairs && len(fixPairs) > 0 {
		pairs, _ = utils2.UniqueItems(fixPairs)
	}
	Pairs, _ = utils2.UniqueItems(pairs)
}

func initPolicies(policyList []*RunPolicyConfig) (bool, []string) {
	if policyList == nil {
		policyList = make([]*RunPolicyConfig, 0)
	}
	var polPairs []string
	staticPairs := true
	for _, pol := range policyList {
		if pol.Params == nil {
			pol.Params = make(map[string]float64)
		}
		if pol.PairParams == nil {
			pol.PairParams = make(map[string]map[string]float64)
		}
		pol.defs = make(map[string]*core.Param)
		if len(pol.Pairs) > 0 {
			polPairs = append(polPairs, pol.Pairs...)
		} else {
			staticPairs = false
		}
	}
	RunPolicy = policyList
	return staticPairs, polPairs
}

func GetTakeOverTF(pair, defTF string) string {
	if pairMap, ok := core.StgPairTfs[TakeOverStrat]; ok {
		if tf, ok := pairMap[pair]; ok {
			return tf
		}
	}
	return defTF
}

func GetAccLeverage(account string) float64 {
	acc, ok := Accounts[account]
	if ok && acc.Leverage > 0 {
		return acc.Leverage
	} else {
		return Leverage
	}
}

func initExgAccs(args *CmdArgs) *errs.Error {
	loc, err := args.parseTimeZone()
	if err != nil {
		return err
	}
	if loc != nil {
		btime.LocShow = loc
	} else if Exchange.Name == "china" {
		btime.LocShow, _ = time.LoadLocation("Asia/Shanghai")
	} else {
		btime.LocShow = btime.UTCLocale
	}
	var accs = Accounts
	Accounts = make(map[string]*AccountConfig)
	BakAccounts = make(map[string]*AccountConfig)
	if core.EnvReal {
		DefAcc = ""
	}
	for name, val := range accs {
		if val.NoTrade {
			BakAccounts[name] = val
		} else if !core.EnvReal {
			// Non-production environment, only enable one account
			// 非生产环境，只启用一个账号
			Accounts[DefAcc] = val
		} else {
			Accounts[name] = val
			if DefAcc == "" {
				// Production environment, the first record is the default account
				// 生产环境，第一个记录为默认账户
				DefAcc = name
			}
		}
	}
	if len(Accounts) == 0 {
		if core.EnvReal {
			return errs.NewMsg(core.ErrBadConfig, "no valid accounts for %s", Exchange.Name)
		}
		log.Warn("no account configured, use default", zap.String("exg", Exchange.Name))
		Accounts[DefAcc] = &AccountConfig{}
	}
	return nil
}

func (p *StratPerfConfig) Validate() {
	if p.MinOdNum < 5 {
		p.MinOdNum = 5
	}
	if p.MaxOdNum < 8 {
		p.MaxOdNum = 8
	}
	if p.MinJobNum < 7 {
		p.MinJobNum = 7
	}
	if p.MidWeight == 0 {
		p.MidWeight = 0.4
	}
	if p.BadWeight == 0 {
		p.BadWeight = 0.15
	}
}

func ParsePath(path string) string {
	if strings.HasPrefix(path, "$") {
		path = strings.TrimLeft(path, "$\\/")
		return filepath.Join(GetDataDir(), path)
	} else if strings.HasPrefix(path, "@") {
		path = strings.TrimLeft(path, "@\\/")
		return filepath.Join(GetDataDir(), path)
	}
	return path
}

func GetStakeAmount(accName string) float64 {
	var amount float64
	acc, ok := Accounts[accName]
	// Prioritize using percentage to open orders
	// 优先使用百分比开单
	if ok && acc.StakePctAmt > 0 {
		amount = acc.StakePctAmt
	} else {
		amount = StakeAmount
	}
	// Multiply by the account multiplier
	// 乘以账户倍率
	if ok && acc.StakeRate > 0 {
		amount *= acc.StakeRate
	}
	// Check if the maximum amount is exceeded
	// 检查是否超出最大金额
	if ok && acc.MaxStakeAmt > 0 && acc.MaxStakeAmt < amount {
		amount = acc.MaxStakeAmt
	} else if MaxStakeAmt > 0 && MaxStakeAmt < amount {
		amount = MaxStakeAmt
	}
	return amount
}

func (c *Config) DumpYaml() ([]byte, *errs.Error) {
	data, err_ := core.MarshalYaml(c)
	if err_ != nil {
		return nil, errs.New(errs.CodeMarshalFail, err_)
	}
	return data, nil
}

func (c *Config) HashCode() (string, *errs.Error) {
	cfgData, err2 := c.DumpYaml()
	if err2 != nil {
		return "", err2
	}
	return utils.MD5(cfgData)[:10], nil
}

func (c *Config) Strats() []string {
	resMap := make(map[string]bool)
	var result = make([]string, 0, 4)
	for _, p := range c.RunPolicy {
		if _, ok := resMap[p.Name]; !ok {
			resMap[p.Name] = true
			result = append(result, p.Name)
		}
	}
	return result
}

func (c *Config) TimeFrames() []string {
	resMap := make(map[string]bool)
	var result = make([]string, 0, 4)
	var requireRoot = false
	for _, p := range c.RunPolicy {
		if len(p.RunTimeframes) == 0 {
			requireRoot = true
			continue
		}
		for _, tf := range p.RunTimeframes {
			if _, ok := resMap[tf]; !ok {
				resMap[tf] = true
				result = append(result, tf)
			}
		}
	}
	if requireRoot {
		for _, tf := range c.RunTimeframes {
			if _, ok := resMap[tf]; !ok {
				resMap[tf] = true
				result = append(result, tf)
			}
		}
	}
	return result
}

func (c *Config) ShowPairs() string {
	var showPairs string
	if len(c.Pairs) > 0 {
		if len(c.Pairs) == 1 {
			showPairs = c.Pairs[0]
		} else {
			showPairs = fmt.Sprintf("num_%d", len(c.Pairs))
		}
	} else if len(c.PairFilters) > 0 {
		for _, f := range c.PairFilters {
			if f.Name == "OffsetFilter" {
				limit := utils.GetMapVal(f.Items, "limit", 0)
				if limit > 0 {
					showPairs = fmt.Sprintf("top_%d", limit)
				} else {
					rate := utils.GetMapVal(f.Items, "rate", float64(0))
					showPairs = fmt.Sprintf("top_%.0f", rate*100)
				}
			}
		}
	}
	return showPairs
}

func (c *Config) Clone() *Config {
	return &Config{
		Name:             c.Name,
		Env:              c.Env,
		Leverage:         c.Leverage,
		LimitVolSecs:     c.LimitVolSecs,
		PutLimitSecs:     c.PutLimitSecs,
		AccountPullSecs:  c.AccountPullSecs,
		MarketType:       c.MarketType,
		ContractType:     c.ContractType,
		OdBookTtl:        c.OdBookTtl,
		StopEnterBars:    c.StopEnterBars,
		ConcurNum:        c.ConcurNum,
		OrderType:        c.OrderType,
		PreFire:          c.PreFire,
		MarginAddRate:    c.MarginAddRate,
		ChargeOnBomb:     c.ChargeOnBomb,
		TakeOverStrat:    c.TakeOverStrat,
		StakeAmount:      c.StakeAmount,
		StakePct:         c.StakePct,
		MaxStakeAmt:      c.MaxStakeAmt,
		OpenVolRate:      c.OpenVolRate,
		MinOpenRate:      c.MinOpenRate,
		LowCostAction:    c.LowCostAction,
		BTNetCost:        c.BTNetCost,
		RelaySimUnFinish: c.RelaySimUnFinish,
		OrderBarMax:      c.OrderBarMax,
		MaxOpenOrders:    c.MaxOpenOrders,
		MaxSimulOpen:     c.MaxSimulOpen,
		WalletAmounts:    c.WalletAmounts,
		DrawBalanceOver:  c.DrawBalanceOver,
		StakeCurrency:    c.StakeCurrency,
		FatalStop:        c.FatalStop,
		FatalStopHours:   c.FatalStopHours,
		TimeRangeRaw:     c.TimeRangeRaw,
		TimeStart:        c.TimeStart,
		TimeEnd:          c.TimeEnd,
		TimeRange:        c.TimeRange,
		RunTimeframes:    c.RunTimeframes,
		KlineSource:      c.KlineSource,
		WatchJobs:        c.WatchJobs,
		RunPolicy:        c.RunPolicy,
		StratPerf:        c.StratPerf,
		Pairs:            c.Pairs,
		PairMgr:          c.PairMgr,
		PairFilters:      c.PairFilters,
		SpiderAddr:       c.SpiderAddr,
		Webhook:          c.Webhook,
		Accounts:         c.Accounts,
		Exchange:         c.Exchange,
	}
}

/*
Desensitize
屏蔽配置对象中的敏感信息
database.url
exchange.account_*.*.(api_key|api_secret)
rpc_channels.*.*secret
api_server.jwt_secret_key
api_server.users[*].pwd
*/
func (c *Config) Desensitize() *Config {
	var res = c.Clone()

	if res.Accounts != nil {
		for _, acc := range res.Accounts {
			acc.Exchanges = nil
			acc.APIServer = nil
		}
	}

	// 处理数据库配置
	if c.Database != nil {
		res.Database = &DatabaseConfig{
			Url:         "",
			Retention:   c.Database.Retention,
			MaxPoolSize: c.Database.MaxPoolSize,
			AutoCreate:  c.Database.AutoCreate,
		}
	}

	// 处理RPC通道配置
	if c.RPCChannels != nil {
		res.RPCChannels = make(map[string]map[string]interface{})
		for channelName, channelConfig := range c.RPCChannels {
			chlType := utils.GetMapVal(channelConfig, "type", "")
			resChannel := make(map[string]interface{})
			for k, v := range channelConfig {
				if !strings.Contains(strings.ToLower(k), "secret") {
					resChannel[k] = v
				}
			}
			if chlType == "wework" {
				delete(resChannel, "corp_id")
			}
			res.RPCChannels[channelName] = resChannel
		}
	}

	// 处理API服务器配置
	if c.APIServer != nil {
		res.APIServer = &APIServerConfig{
			Enable:      c.APIServer.Enable,
			BindIPAddr:  c.APIServer.BindIPAddr,
			Port:        c.APIServer.Port,
			Verbosity:   c.APIServer.Verbosity,
			CORSOrigins: c.APIServer.CORSOrigins,
		}
		if c.APIServer.Users != nil {
			res.APIServer.Users = make([]*UserConfig, len(c.APIServer.Users))
			for i, user := range c.APIServer.Users {
				res.APIServer.Users[i] = &UserConfig{
					Username:    user.Username,
					AccRoles:    user.AccRoles,
					ExpireHours: user.ExpireHours,
				}
			}
		}
	}

	return res
}

func DumpYaml(desensitize bool) ([]byte, *errs.Error) {
	c := &Data
	if desensitize {
		c = c.Desensitize()
	}
	data, err := c.DumpYaml()
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (c *RunPolicyConfig) ID() string {
	if c.Dirt == "" {
		return c.Name
	}
	if c.Dirt == "long" {
		return c.Name + ":l"
	} else if c.Dirt == "short" {
		return c.Name + ":s"
	} else {
		panic(fmt.Sprintf("unknown run_policy dirt: %v", c.Dirt))
	}
}

func (c *RunPolicyConfig) Key() string {
	tfStr := strings.Join(c.RunTimeframes, "|")
	pairStr := strings.Join(c.Pairs, "|")
	return fmt.Sprintf("%s/%s/%s", c.ID(), tfStr, pairStr)
}

func (c *RunPolicyConfig) OdDirt() int {
	dirt := core.OdDirtBoth
	if c.Dirt == "long" {
		dirt = core.OdDirtLong
	} else if c.Dirt == "short" {
		dirt = core.OdDirtShort
	} else if c.Dirt != "" {
		panic(fmt.Sprintf("unknown run_policy dirt: %v", c.Dirt))
	}
	return dirt
}

func (c *RunPolicyConfig) Param(k string, dv float64) float64 {
	if v, ok := c.Params[k]; ok {
		return v
	}
	return dv
}

func (c *RunPolicyConfig) Def(k string, dv float64, p *core.Param) float64 {
	val := c.Param(k, dv)
	if p.Mean == 0 {
		p.Mean = dv
	}
	if p.VType == core.VTypeNorm && p.Rate == 0 {
		p.Rate = 1
	}
	if p.Name == "" {
		p.Name = k
	}
	if c.defs == nil {
		c.defs = make(map[string]*core.Param)
	}
	c.defs[k] = p
	return val
}

func (c *RunPolicyConfig) DefInt(k string, dv int, p *core.Param) int {
	val := c.Param(k, float64(dv))
	if p.Mean == 0 {
		p.Mean = float64(dv)
	}
	// 确保每个整数命中概率基本一致
	p.Min = math.Round(p.Min) - 0.49
	p.Max = math.Round(p.Max) + 0.49
	p.IsInt = true
	if p.VType == core.VTypeNorm && p.Rate == 0 {
		p.Rate = 1
	}
	if p.Name == "" {
		p.Name = k
	}
	if c.defs == nil {
		c.defs = make(map[string]*core.Param)
	}
	c.defs[k] = p
	return int(math.Round(val))
}

func (c *RunPolicyConfig) IsInt(k string) bool {
	if p, ok := c.defs[k]; ok {
		return p.IsInt
	}
	return false
}

func (c *RunPolicyConfig) HyperParams() []*core.Param {
	res := make([]*core.Param, 0, len(c.defs))
	for _, p := range c.defs {
		res = append(res, p)
	}
	return res
}

/*
KeepHyperOnly Only keep the given hyperparameters for optimization and remove other hyperparameters
*/
func (c *RunPolicyConfig) KeepHyperOnly(keys ...string) {
	var res = make(map[string]*core.Param)
	for _, k := range keys {
		if v, ok := c.defs[k]; ok {
			res[k] = v
		}
	}
	c.defs = res
}

func (c *RunPolicyConfig) ToYaml() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("  - name: %s\n", c.Name))
	if c.Dirt != "" {
		b.WriteString(fmt.Sprintf("    dirt: %s\n", c.Dirt))
	}
	if len(c.RunTimeframes) > 0 {
		b.WriteString(fmt.Sprintf("    run_timeframes: [ %s ]\n", strings.Join(c.RunTimeframes, ", ")))
	}
	if len(c.Pairs) > 0 {
		b.WriteString(fmt.Sprintf("    pairs: [ %s ]\n", strings.Join(c.Pairs, ", ")))
	}
	argText := utils2.MapToStr(c.Params, true, 2)
	if len(c.Pairs) == 1 {
		b.WriteString("    pair_params:\n")
		b.WriteString(fmt.Sprintf("      %s: {%s}\n", c.Pairs[0], argText))
	} else {
		b.WriteString(fmt.Sprintf("    params: {%s}\n", argText))
	}
	return b.String()
}

func (c *RunPolicyConfig) Clone() *RunPolicyConfig {
	res := &RunPolicyConfig{
		Name:          c.Name,
		Filters:       c.Filters,
		RunTimeframes: c.RunTimeframes,
		MaxPair:       c.MaxPair,
		MaxOpen:       c.MaxOpen,
		MaxSimulOpen:  c.MaxSimulOpen,
		Dirt:          c.Dirt,
		StratPerf:     c.StratPerf,
		Pairs:         c.Pairs,
		Params:        make(map[string]float64),
		PairParams:    make(map[string]map[string]float64),
		defs:          make(map[string]*core.Param),
	}
	if len(c.Params) > 0 {
		for k, v := range c.Params {
			res.Params[k] = v
		}
	}
	if len(c.PairParams) > 0 {
		for k, mp := range c.PairParams {
			pairPms := make(map[string]float64)
			for k2, v := range mp {
				pairPms[k2] = v
			}
			res.PairParams[k] = pairPms
		}
	}
	if len(c.defs) > 0 {
		for k, v := range c.defs {
			res.defs[k] = v
		}
	}
	return res
}

func (c *RunPolicyConfig) PairDup(pair string) (*RunPolicyConfig, bool) {
	params, _ := c.PairParams[pair]
	isDiff := true
	if len(params) == 0 {
		params = c.Params
		isDiff = false
	}
	res := c.Clone()
	res.Params = params
	return res, isDiff
}

func (a *AccountConfig) GetApiSecret() *ApiSecretConfig {
	if a == nil || len(a.Exchanges) == 0 {
		return &ApiSecretConfig{}
	}
	cfg, _ := a.Exchanges[Exchange.Name]
	if cfg != nil {
		if core.RunEnv != core.RunEnvTest && cfg.Prod != nil {
			return cfg.Prod
		} else if core.RunEnv == core.RunEnvTest && cfg.Test != nil {
			return cfg.Test
		}
	}
	return &ApiSecretConfig{}
}

func LoadPerfs(inDir string) {
	if StratPerf == nil || !StratPerf.Enable {
		return
	}
	inPath := fmt.Sprintf("%s/strat_perfs.yml", inDir)
	_, err_ := os.Stat(inPath)
	if err_ != nil {
		return
	}
	data, err_ := os.ReadFile(inPath)
	if err_ != nil {
		log.Error("read strat_perfs.yml fail", zap.Error(err_))
		return
	}
	var unpak map[string]map[string]interface{}
	err_ = yaml.Unmarshal(data, &unpak)
	if err_ != nil {
		log.Error("unmarshal strat_perfs fail", zap.Error(err_))
		return
	}
	for strat, cfg := range unpak {
		sta := &core.PerfSta{}
		err_ = mapstructure.Decode(cfg, &sta)
		if err_ != nil {
			log.Error(fmt.Sprintf("decode %s fail", strat), zap.Error(err_))
			continue
		}
		core.StratPerfSta[strat] = sta
		perfVal, ok := cfg["perf"]
		if ok && perfVal != nil {
			var perf = map[string]string{}
			err_ = mapstructure.Decode(perfVal, &perf)
			if err_ != nil {
				log.Error(fmt.Sprintf("decode %s.perf fail", strat), zap.Error(err_))
				continue
			}
			for pairTf, arrStr := range perf {
				arr := strings.Split(arrStr, "|")
				num, _ := strconv.Atoi(arr[0])
				profit, _ := strconv.ParseFloat(arr[1], 64)
				score, _ := strconv.ParseFloat(arr[2], 64)
				core.JobPerfs[fmt.Sprintf("%s_%s", strat, pairTf)] = &core.JobPerf{
					Num:       num,
					TotProfit: profit,
					Score:     score,
				}
			}
		}
	}
	log.Info("load strat_perfs ok", zap.String("path", inPath))
}

// ParseTimeRange parses time range string in format "YYYYMMDD-YYYYMMDD"
func ParseTimeRange(timeRange string) (int64, int64, error) {
	parts := strings.Split(timeRange, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid time range format: %s", timeRange)
	}
	startMS, err := btime.ParseTimeMS(parts[0])
	if err != nil {
		return 0, 0, err
	}
	stopMS, err := btime.ParseTimeMS(parts[1])
	return startMS, stopMS, err
}

func GetExportConfig(path string) (*ExportConfig, *errs.Error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, errs.New(errs.CodeIOReadFail, err)
	}

	var cfg ExportConfig
	if err = yaml.Unmarshal(data, &cfg); err != nil {
		return nil, errs.New(core.ErrBadConfig, err)
	}
	return &cfg, nil
}

func (tr *TimeTuple) Clone() *TimeTuple {
	return &TimeTuple{
		StartMS: tr.StartMS,
		EndMS:   tr.EndMS,
	}
}

func GetApiUsers() []*UserConfig {
	res := make([]*UserConfig, 0)
	for name, acc := range Accounts {
		if acc.APIServer == nil {
			continue
		}
		res = append(res, &UserConfig{
			Username: name,
			Password: acc.APIServer.Pwd,
			AccRoles: map[string]string{
				name: acc.APIServer.Role,
			},
		})
	}
	return append(res, APIServer.Users...)
}
