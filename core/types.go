package core

type DownRange struct {
	Start   int64
	End     int64
	Reverse bool // Indicates whether downloading should be done from back to front 指示是否应该从后往前下载
}

type TfScore struct {
	TF    string
	Score float64
}

type JobPerf struct {
	Num       int
	TotProfit float64 // TOTAL PROFIT 总利润
	Score     float64 // Order opening ratio, less than 1 means reducing the order opening 开单倍率，小于1表示减少开单
}

/*
PerfSta Statistics of a certain strategy for all targets 某个策略针对所有标的的统计信息
*/
type PerfSta struct {
	OdNum    int         `yaml:"od_num" mapstructure:"od_num"`
	LastGpAt int         `yaml:"last_gp_at" mapstructure:"last_gp_at"` // The number of orders for the last time clustering was performed 上次执行聚类的订单数量
	Splits   *[4]float64 `yaml:"splits" mapstructure:"splits"`
	Delta    float64     `yaml:"delta" mapstructure:"delta"` // Multiplier before logarithmizing TotProfit 对TotProfit进行对数处理前的乘数
}

type StrAny struct {
	Str string `json:"str"`
	Val any    `json:"val"`
}

type StrVal[T comparable] struct {
	Str string `json:"str"`
	Val T      `json:"val"`
}

type StrInt64 struct {
	Str string `json:"str"`
	Int int64  `json:"int"`
}

type StrFloat struct {
	Str string
	Val float64
}

type Param struct {
	Name  string
	VType int // VTypeNorm / VTypeUniform
	Min   float64
	Max   float64
	Mean  float64
	IsInt bool
	Rate  float64 // Valid for normal distribution, defaults to 1. The larger the value, the more the random values tend to be Mean. 正态分布时有效，默认1，值越大，随机值越趋向于Mean
	edgeY float64 // Calculate cache of normal distribution edge y 计算正态分布边缘y的缓存
}

type ApiClient struct {
	IP        string
	UserAgent string
	User      string
	AccRoles  map[string]string
	Token     string
}
