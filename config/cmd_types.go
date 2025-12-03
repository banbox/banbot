package config

type ArrString []string

func (i *ArrString) String() string {
	return "my string representation"
}

func (i *ArrString) Set(value string) error {
	*i = append(*i, value)
	return nil
}

type CmdArgs struct {
	Configs       ArrString
	ConfigData    string
	Logfile       string
	DataDir       string
	NoCompress    bool
	NoDefault     bool
	LogLevel      string
	TimeRange     string
	TimeStart     string
	TimeEnd       string
	RawTimeFrames string
	TimeFrames    []string
	StakeAmount   float64
	StakePct      float64
	RawPairs      string
	Pairs         []string
	RawTables     string
	Tables        []string
	Force         bool
	WithSpider    bool
	Medium        string
	MaxPoolSize   int
	InPath        string
	PrgOut        string
	OutPath       string
	OutType       string // output data type
	AdjType       string // adjustment type: 复权类型: pre,post,none
	TimeZone      string
	ExgReal       string
	OptRounds     int     // Hyperparameter optimization single task execution round 超参数优化单任务执行轮次
	Concur        int     // Hyperparameter optimization of multi-process concurrency 超参数优化多进程并发数量
	Sampler       string  // Hyperparameter optimization methods 超参数优化的方法: tpe/bayes/random/cmaes/ipop-cmaes/bipop-cmaes
	EachPairs     bool    // Execute target by target 逐个标的执行
	ReviewPeriod  string  // During continuous parameter adjustment and backtesting, the period of parameter adjustment review 持续调参回测时，调参回顾的周期
	RunPeriod     string  // During continuous parameter adjustment and backtesting, the effective running period after parameter adjustment 持续调参回测时，调参后有效运行周期
	Picker        string  // Method for selecting targets from multiple hyperparameter optimization results 从多个超参数优化结果中挑选目标的方法
	Alpha         float64 // the smoothing factor of calculate EMA 计算EMA的平滑因子
	PairPicker    string  // pairs picker for hyper opt
	InType        string  // Input file data type 输入文件的数据类型
	RunEveryTF    string  // run once every n timeframe
	BatchSize     int
	Separate      bool // Used for backtesting. When true, the strategy combination is tested separately. 用于回测，true时策略组合单独测试
	Inited        bool
	DeadLock      bool
}
