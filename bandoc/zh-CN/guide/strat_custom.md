您可通过我们的[示例策略项目](https://github.com/banbox/banstrats)快速熟悉和使用，当然您可能有自己的交易策略需要测试，此页面帮助您理解如何实现自定义策略。

## 概述
要开始一个新策略，只需两步。

首先实现一个`func Demo1(pol *config.RunPolicyConfig) *strat.TradeStrat`的策略函数。

其次，您只需在当前go package的`init`函数中注册此策略即可。

下面我们看看在策略函数中应该如何实现我们的自定义逻辑。

::: tip
强烈推荐您使用我们的在线AI助手辅助开发：[链接](https://www.banbot.site/zh-CN/backtest/new){target="_self"}。  
AI助手虽然很方便，但依然会犯错，您必须检查并理解AI输出的策略代码，确保符合您的逻辑，否则后续回测和实盘没有意义。
:::

## 策略命名
推荐的命名格式是`文件夹:策略名称`。您可能会有很多不同类别的交易策略，为方便长期维护，建议您将类似的策略放在同一个文件夹中。深度不限。

比如：
```text
root
 |-org
 |  |-ma
 |  |  |-demo.go
 |  |  |-trend.go
 | grid
 |  |-inv.go
 |-main.go
```
上面有`ma`和`grid`两个有效的策略组，您也可以在`ma`中继续创建子文件夹管理策略。然后您可在`ma`的任意go文件中注册策略：
```go
func init() {
	// 注册策略到banbot中，后续在配置文件中使用ma:demo即可引用此策略
	// `init`函数是go中的特殊函数，会在当前包被导入时立刻执行
	strat.AddStratGroup("ma", map[string]strat.FuncMakeStrat{
		"demo": Demo,
	})
}
```
注意，所有策略组都需要在根目录的`main.go`中注册：
```go
import (
	"github.com/banbox/banbot/entry"
	_ "github.com/banbox/banstrats/org/ma"
	_ "github.com/banbox/banstrats/grid"
)
```

## RunPolicyConfig参数 

策略函数的参数`pol`是yaml配置文件中`run_policy`列表中的一项。  
您可通过`atrLen := pol.Param("atrLen", 9)`获取从配置文件传入的参数，以便针对不同品种使用不同的参数。  
您也可使用`atrLen := pol.Def("atrLen", 9, core.PNorm(7, 20))`定义一个超参数，回测时和`pol.Param`作用完全一样，但当切换到超参数调优模式时，`atrLen`的值将使用默认值为均值，上下限为`(7, 20)`的正太分布随机生成。  
您也可以将`core.PNorm`替换为`core.PNormF`，指定另外的均值和倍率。
同样，您也可以使用`core.PUniform`指定一个均匀线性分布的超参数。  

下面是几种完整的代码示例：
```go
func Demo(pol *config.RunPolicyConfig) *strat.TradeStrat {
    atrLen := pol.Param("atrLen", 9)
    atrLen1 := pol.Def("atrLen1", 9, core.PNorm(3, 20))
    atrLen2 := pol.Def("atrLen2", 9, core.PNormF(3, 20, 12, 1))
    atrLen3 := pol.Def("atrLen3", 9, core.PUniform(3, 20))
    other1 := pol.More["other1"].(string)
    other2 := pol.More["other2"].(int)
    other3 := pol.More["other3"].(float64)
    return &strat.TradeStrat{
		// 更多内容
    }
}
```
您可在yml中设置对应的策略参数，会覆盖上面的默认值：
```yaml
run_policy:
  - name: demo:demo
    params:
      atrLen: 9
      atrLen1: 10
      atrLen2: 11
      atrLen3: 12
    other1: value1
    other2: 123
    other3: 1.23
```
::: warning 提示
yaml配置`run_policy`中的每一项对应一次对策略函数的调用，生成一个特定参数的策略。  
同一个策略可以在多个`run_poolicy`项中出现，即同一个策略函数可能会执行多次。  
策略函数返回的`*strat.TradeStrat`会应用到多个品种上，对应多个`*strat.StratJob`。
所以任何单个品种相关的变量都不要保存在策略函数内，而应当通过`*strat.StratJob.More`保存。  
策略函数中的变量应保持不被修改。否则会导致意外的状态。
:::

## strat.TradeStrat 策略对象

`TradeStrat`提供了丰富的属性配置和回调函数。最常用的是`OnBar`。

下面是`TradeStrat`的完整定义：
```go
type TradeStrat struct {
	Name          string // 策略名称，无需设置，会自动设置为注册时的名称
	Version       int // 策略版本号
	WarmupNum     int // K线预热的长度
	MinTfScore    float64 // 最小时间周期质量，默认0.75
    WsSubs        map[string]string    // websocket订阅: core.WsSubKLine, core.WsSubTrade, core.WsSubDepth
	DrawDownExit  bool // 是否启用回撤止损（即跟踪止损）
    HedgeOff      bool    // 关闭合约双向持仓
	BatchInOut    bool    // 是否批量执行入场/出场
	BatchInfo     bool    // 是否对OnInfoBar后执行批量处理
	StakeRate     float64 // 相对基础金额开单倍率
    StopLoss      float64 // 此策略打开所有订单的默认止损比率（不带杠杆）
	StopEnterBars int // 限价单如果超过给定K线仍未入场则取消
	EachMaxLong   int      // 单个品种最大同时开多数量，默认0不限制
	EachMaxShort  int      // 单个品种最大同时开空数量，默认0不限制
	AllowTFs      []string // 允许运行的时间周期，不提供时使用全局配置
	Outputs       []string // 策略输出的文本文件内容，每个字符串是一行
	Policy        *config.RunPolicyConfig // 运行时配置

	OnPairInfos         func(s *StratJob) []*PairSub // 策略额外需要的品种或其他周期的数据
	OnStartUp           func(s *StratJob) // 启动时回调。初次执行前调用
	OnBar               func(s *StratJob) // 每个K线的回调函数
	OnInfoBar           func(s *StratJob, e *ta.BarEnv, pair, tf string)   // 其他依赖的bar数据
	OnWsTrades          func(s *StratJob, pair string, trades []*banexg.Trade) // 逐笔交易数据
    OnWsDepth           func(s *StratJob, dep *banexg.OrderBook)               // 推送深度信息
    OnWsKline           func(s *StratJob, pair string, k *banexg.Kline)        // Websocket推送的实时K线(可能未完成)
	OnBatchJobs         func(jobs []*StratJob)                             // 当前时间所有标的job，用于批量开单/平仓
	OnBatchInfos        func(tf string, jobs map[string]*JobEnv)            // 当前时间所有info标的job，用于批量处理
	OnCheckExit         func(s *StratJob, od *ormo.InOutOrder) *ExitReq     // 自定义订单退出逻辑
	OnOrderChange       func(s *StratJob, od *ormo.InOutOrder, chgType int) // 订单更新回调
	GetDrawDownExitRate CalcDDExitRate                                     // 计算跟踪止盈回撤退出的比率
	PickTimeFrame       PickTimeFrameFunc                                  // 为指定币选择适合的交易周期
	OnShutDown          func(s *StratJob)                                  // 每个策略任务停止时回调
	OnStratExit         func()                                             // 策略配置退出时回调，在所有OnShutDown之后执行
}
```
:::tip tip
`MinTfScore`是针对每个品种在指定周期k线通过价格变动、bar实体占比等计算的分数，用于淘汰变动太小，波动不足的交易对；或计算交易对的最佳周期。阈值默认取0.75，小于此阈值的交易所将会被过滤，如需禁用，可设为非零较小值0.0001
:::

## 简单策略示例
```go
package ma

import (
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/strat"
	ta "github.com/banbox/banta"
)

func init() {
	// 注册策略到banbot中，后续在配置文件中使用ma:demo即可引用此策略
	// `init`函数是go中的特殊函数，会在当前包被导入时立刻执行
	strat.AddStratGroup("ma", map[string]strat.FuncMakeStrat{
		"demo": Demo,
	})
}

func Demo(pol *config.RunPolicyConfig) *strat.TradeStrat {
	smlLen := int(pol.Def("smlLen", 5, core.PNorm(3, 10)))
	bigLen := int(pol.Def("bigLen", 20, core.PNorm(10, 40)))
	return &strat.TradeStrat{
		WarmupNum: 100,
		OnBar: func(s *strat.StratJob) {
			e := s.Env
			ma5 := ta.SMA(e.Close, smlLen)
			ma20 := ta.SMA(e.Close, bigLen)
			maCrx := ta.Cross(ma5, ma20)

			if maCrx == 1 {
				s.OpenOrder(&strat.EnterReq{Tag: "open"})
			} else if maCrx == -1 {
				s.CloseOrders(&strat.ExitReq{Tag: "exit"})
			}
		},
	}
}
```
> 您可在任意回调函数中使用指标，但注意，**所有指标都必须先在OnBar中注册调用一遍**；否则指标计算会出现错误。

## 技术指标库banta
banbot使用高性能指标库[banta](https://github.com/banbox/banta)，它会对每个bar的指标计算状态进行缓存，这是banbot高性能的关键，您可访问[DeepWiki](https://deepwiki.com/banbox/banta)了解关于banta的更多信息。

`banta.BarEnv` 和 `banta.Series`是banta中的两个关键结构体。

`banta.BarEnv`是某个技术指标的运行环境，其中存储了当前交易所、市场、品种、时间周期等信息。
一个策略任务至少会需要一个`banta.BarEnv`，如果通过`OnPairInfos`订阅了其他品种或时间周期，则需要多个运行环境。
但需要注意的是，每个交易所、每个市场、每个品种、每个时间周期，只会创建一个运行环境`BarEnv`。

`banta.BarEnv`中内置了几个原始的`Series`，即：`Open`,`High`,`Low`,`Close`,`Volume`，分别保存了开盘价、最高价、最低价、收盘价、成交量的序列信息。

`Series`是`banta`中用于存储序列化数据的特殊结构体。`banta`中的所有技术指标函数，都是接收一个或多个`Series`，经过运算后，返回一个`Series`。

您可通过`Series.Get(0)`获取某个序列的最新值，比如`e.Close.Get(0)`获取最新收盘价；
也可通过`Series.Range(0,5)`获取某个序列最近的5个值，返回长度为5的`[]float64`数组。

当在同一个bar时间执行多次重复的计算时（如实盘多个账号运行同一批策略），`Series`会自动使用缓存的结果，避免重复计算。

对于布林带指标`banta.BBANDS`这样返回`upper/mid/lower`三个部分的指标，您需要通过以下方式提取为3个序列对象：
```go
    upper, mid, lower := ta.BBANDS(e.Close, 10, 2, 2)
```
然后您可对`upper/mid/lower`分别执行`Get(i)`来获取值。
::: tip 指标运行提示
`banta`是事件驱动的技术指标库。`OnBar`函数会对每一个K线执行一次，也就是说每次调用技术指标只会更新一个最新的值到`Series`中。
而非`ta-lib`那样向量化计算，一次性计算历史所有序列。

`Series`保存了每次指标计算的最新状态，收到新的K线时，会从上次计算状态快速计算，无需重复计算序列的所有数据。

比如`banta.EMA`计算时需要上一个均线值，`ta-lib`向量指标库需要给定整列的历史数据从头计算，而`banta`会从上次缓存的值只计算一次即可。
:::
## 自定义指标

您可以简单快速地实现您的自定义指标逻辑，以`Highest`为例：
```go:line-numbers {1}
func Highest(obj *Series, period int) *Series {
	res := obj.To("_hh", period)
	if res.Cached() {
		return res
	}
	if obj.Len() < period {
		return res.Append(math.NaN())
	}
	resVal := slices.Max(obj.Range(0, period))
	return res.Append(resVal)
}
```
如上是一个简单的指定周期内最高值的技术指标，所有技术指标的前4行几乎都类似，从`obj`派生得到一个`Series`对象用于保存结果。
其中的`_hh`和`period`参数这里是用作哈希。

实际上`Series`有一个成员属性：
```go
Subs  map[string]map[int]*Series
```
它保存了从该`Series`派生的所有`Series`，本质上策略中所有的`Series`都是由最初的`Open/High/Low/Close/Volume`派生出来的。

由于指标函数每次调用只更新一个值，所以在6-8行长度不足时，直接返回nan。第9行是此指标的关键，计算最近n个序列的最大值返回。

## 更复杂的自定义指标

您的自定义指标可能有时候需要缓存更多的中间状态信息，这时候您可以自定义结构体，然后将其保存在`Series.More`中。
```go:line-numbers {1}
type sumState struct {
	sumVal float64
	addLen int
}

func Sum(obj *Series, period int) *Series {
	res := obj.To("_sum", period)
	if res.Cached() {
		return res
	}
	sta, _ := res.More.(*sumState)
	if sta == nil {
		sta = &sumState{}
		res.More = sta
	}
	curVal := obj.Get(0)
	if math.IsNaN(curVal) {
		// 输入值无效，重置，重新开始累计
		curVal = 0
		sta.sumVal = 0
		sta.addLen = 0
	} else {
		if sta.addLen < period {
			sta.sumVal += curVal
			sta.addLen += 1
		} else {
			oldVal := obj.Get(period)
			if math.IsNaN(oldVal) {
				sta.sumVal = 0
				sta.addLen = 0
			} else {
				sta.sumVal += curVal - oldVal
			}
		}
	}
	if sta.addLen < period {
		return res.Append(math.NaN())
	}
	return res.Append(sta.sumVal)
}
```
上面是一个计算最近n个周期的序列和的指标。其中的11~39行其实可以用下面的一行替换，但上面缓存上一次计算结果的做法，比下面的两次循环效率明显更高。
```go
    // gonum.org/v1/gonum/floats
    return res.Append(floats.Sum(obj.Range(0, period)))
```
## 策略预热  

大多数指标都有一个不稳定的启动期，在此期间它们要么不可用（NaN），要么计算不正确。这可能会导致不一致，因为 banbot 不知道这个不稳定期应该有多长。
为了解决这个问题，可以为策略设置`WarmupNum`属性。这应该设置为策略计算稳定指标所需的最大K线数量。

对于`OnPairInfos`额外订阅的其他品种或周期，您可在其返回值中指定：
```go
    OnPairInfos: func(s *strat.StratJob) []*strat.PairSub {
        return []*strat.PairSub{
            {"_cur_", "1h", 30},
        }
    },
```
上面即额外订阅了当前品种的`1h`周期的K线，预热数量是30.

## 使用其他周期/品种K线 {#info_bar}

您的策略可能不仅仅需要单个品种单个周期，有时候可能需要其他周期K线，或其他品种的K线，您可通过`OnPairInfos`和`OnInfoBar`订阅并使用。
```go
package ma

import (
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/strat"
	ta "github.com/banbox/banta"
)

type Demo2Sta struct {
	bigDirt int
}

func Demo2(pol *config.RunPolicyConfig) *strat.TradeStrat {
	smlLen := int(pol.Def("smlLen", 5, core.PNorm(3, 10)))
	bigLen := int(pol.Def("bigLen", 20, core.PNorm(10, 40)))
	return &strat.TradeStrat{
		WarmupNum: 100,
		OnPairInfos: func(s *strat.StratJob) []*strat.PairSub {
			return []*strat.PairSub{
				{"_cur_", "1h", 30},
			}
		},
		OnStartUp: func(s *strat.StratJob) {
			s.More = &Demo2Sta{}
		},
		OnBar: func(s *strat.StratJob) {
			e := s.Env
			m, _ := s.More.(*Demo2Sta)
			ma5 := ta.SMA(e.Close, smlLen)
			ma20 := ta.SMA(e.Close, bigLen)
			maCrx := ta.Cross(ma5, ma20)

			if maCrx == 1 && m.bigDirt > 0 {
				s.OpenOrder(&strat.EnterReq{Tag: "open"})
			} else if maCrx == -1 {
				s.CloseOrders(&strat.ExitReq{Tag: "exit"})
			}
		},
		OnInfoBar: func(s *strat.StratJob, e *ta.BarEnv, pair, tf string) {
			m, _ := s.More.(*Demo2Sta)
			ma5 := ta.SMA(e.Close, smlLen)
			ma20 := ta.SMA(e.Close, bigLen)
			m.bigDirt = ta.Cross(ma5, ma20)
		},
	}
}
```
如上是一个大小周期结合的均线交叉策略，当大周期短均线位于长均线上方，且小周期短均线上穿长均线时，才入场。

此策略可在yml配置中指定运行在`5m`小周期，同时策略内部指定了订阅`1h`大周期的数据。`OnPairInfos`中`_cur_`表示当前品种，如需要其他品种，可硬编码替换此处的值。

::: tip 注意
`OnInfoBar`仅用于额外K线消费，请勿在此函数中执行开平仓或更新止损等逻辑。
:::

## 保存临时变量

有时候您可能需要保存一些中间状态，用于下次计算；或者需要在不同的回调函数之间同步一些信息。

如[上一节](#info_bar)在`OnBar`中如果要使用`OnInfoBar`的信息，需要经`StratJob.More`中转。

首先在`OnStartUp`中为`StratJob.More`初始化。
然后在`OnBar`和`OnInfoBar`中进行类型转换，并赋值到`m`变量即可：
```go
m, _ := s.More.(*Demo2Sta)
```

## 发出入场信号

发出入场信号只需要调用`StratJob.OpenOrder`方法，并传递一个`*strat.EnterReq`参数即可。

最简单的情况是只对`EnterReq`的`Tag`属性赋值，其他全部使用默认参数。即按默认开单金额和杠杆倍数，打开一个做多订单。没有止盈止损。

下面是`EnterReq`的全部参数：
```go
type EnterReq struct {
	Tag             string  // 入场信号
	StgyName        string  // 策略名称
	Short           bool    // 是否做空
	OrderType       int     // 订单类型, core.OrderType*
	Limit           float64 // 限价单入场价格，指定时订单将作为限价单提交
    Stop            float64 // 止损(触发价格)，做多订单时价格上涨到触发价格才入场（做空相反）
	CostRate        float64 // 开仓倍率、默认按配置1倍。用于计算LegalCost
	LegalCost       float64 // 花费法币金额。指定时忽略CostRate
	Leverage        float64 // 杠杆倍数
	Amount          float64 // 入场标的数量，由LegalCost和price计算
	StopLossVal     float64 // 入场价格到止损价格的距离，用于计算StopLoss
	StopLoss        float64 // 止损触发价格，不为空时在交易所提交一个止损单
	StopLossLimit   float64 // 止损限制价格，不提供使用StopLoss
	StopLossRate    float64 // 止损退出比例，0表示全部退出，需介于(0,1]之间
	StopLossTag     string  // 止损原因
	TakeProfitVal   float64 // 入场价格到止盈价格的距离，用于计算TakeProfit
	TakeProfit      float64 // 止盈触发价格，不为空时在交易所提交一个止盈单。
	TakeProfitLimit float64 // 止盈限制价格，不提供使用TakeProfit
	TakeProfitRate  float64 // 止盈退出比率，0表示全部退出，需介于(0,1]之间
	TakeProfitTag   string  // 止盈原因
	StopBars        int     // 入场限价单超过多少个bar未成交则取消
    ClientID        string  // 用于设置提交到交易所的ClientOrderID尾部部分。
    Infos           map[string]string // 设置保存到订单的额外信息
	Log             bool // 是否自动记录错误日志
}
```

如果需要触发价格入场，可以设置`Limit`或`Stop`参数。

`Limit`是限价订单，做多订单可设置更低的价格挂单等待成交，做空订单可设置更高的价格挂单等待成交。

`Stop`是触发订单（止损），做多订单可设置更高的价格等待突破后入场，做空订单可设置更低的价格挂单等待跌破后入场。

比如当前价格100，希望在价格上涨到120时限价122入场做多，可设置`{Stop: 120, Limit: 122}`

当`OpenOrder`返回错误时，表示开单失败，您可`log.Error`记录错误信息，也可在`EnterReq`中设置`Log`为true，让系统自动记录错误日志。

:::tip tip
并非策略的任意位置调用OpenOrder均能开单，默认只有在`OnBar`, `OnOrderChange`, `OnBatchJobs`, `OnPostApi`这几个回调函数中调用开平仓会即刻执行。

如果您需要在其他回调中也即刻入场，可自行调用`_, _, err := biz.GetOdMgr(s.Account).ProcessOrders(nil, s)`提交处理

注意`OnOrderChange`默认只处理当前品种的开平仓信号，如需触发其他品种的开平仓，需自行对其他job调用`ProcessOrders(nil, job)`。
:::

## 发出离场信号

发出离场信号只需要调用`StratJob.CloseOrders`方法，并传递一个`*strat.ExitReq`参数即可。

最简单的情况是只对`ExitReq`的`Tag`属性赋值，其他全部使用默认参数。即以市价单关闭全部订单，包含做多订单和做空订单。

下面是`ExitReq`的全部参数：
```go
type ExitReq struct {
	Tag        string  // 退出信号
	StgyName   string  // 策略名称
	EnterTag   string  // 只退出入场信号为EnterTag的订单
	Dirt       int     // core.OdDirt* long/short/both
	OrderType  int     // 订单类型, core.OrderType*
	Limit      float64 // 限价单退出价格，指定时订单将作为限价单提交
	ExitRate   float64 // 退出比率，默认100%即所有订单全部退出
	Amount     float64 // 要退出的标的数量。指定时ExitRate无效
	OrderID    int64   // 只退出指定订单
	UnOpenOnly bool    // True时只退出尚未入场的订单
	FilledOnly bool    // True时只退出已入场的订单
	Force      bool    // 是否强制退出
	Log        bool    // 是否自动记录错误日志
}
```

当`CloseOrders`返回错误时，表示平仓失败，您可`log.Error`记录错误信息，也可在`ExitReq`中设置`Log`为true，让系统自动记录错误日志。

对于`ExitReq.Tag`，您可传入固定的字符串区分不同的退出原因，`core`包内置了以下常用标签：

```go
const (
    ExitTagUnknown     = "unknown"      // 未知原因
    ExitTagCancel      = "cancel"       // 用户取消
    ExitTagBotStop     = "bot_stop"     // 机器人停止
    ExitTagForceExit   = "force_exit"   // 强制退出
    ExitTagNoMatch     = "no_match"     // 无匹配订单
    ExitTagUserExit    = "user_exit"    // 用户主动退出
    ExitTagThird       = "third"        // 第三方触发
    ExitTagCli         = "cli"          // 命令行触发
    ExitTagFatalErr    = "fatal_err"    // 致命错误
    ExitTagPairDel     = "pair_del"     // 品种删除
    ExitTagStopLoss    = "stop_loss"    // 止损触发且亏损
    ExitTagSLTake      = "sl_take"      // 止损触发且盈利
    ExitTagTakeProfit  = "take_profit"  // 止盈触发
    ExitTagDrawDown    = "draw_down"    // 回撤止损
    ExitTagDataStuck   = "data_stuck"   // 长时间未收到K线数据自动平仓
    ExitTagLiquidation = "liquidation"  // 强制平仓
    ExitTagEnvEnd      = "env_end"      // 除权、主力合约切换等原因
    ExitTagEntExp      = "ent_expire"   // 入场限价单过期
    ExitTagExitDelay   = "exit_delay"   // 实盘重启时平仓应该更早退出的订单
)
```

## 止损和止盈

除了在入场时设置订单的止损止盈，您也可以在任意时间，直接设置某个订单的止损止盈：
```go
if len(s.LongOrders) > 0 {
    od := s.LongOrders[0]
    ma5Val := ma5.Get(0)
    od.SetStopLoss(&ormo.ExitTrigger{Price: ma5Val * 0.97})
    od.SetTakeProfit(&ormo.ExitTrigger{Price: ma5Val * 1.03})
}
```
您也可以设置全部订单的止损止盈：
```go
ma5Val := ma5.Get(0)
s.SetAllStopLoss(core.OdDirtLong, &ormo.ExitTrigger{
    Price: ma5Val * 0.97,
    Limit: ma5Val * 0.975,
    Rate:  0.5,
    Tag:   "stop half",
})
```
上面针对所有打开的做多订单，设置50%仓位的止损，价格触发5周期均线的0.97倍时，以0.975倍的限价单止损平仓一半。

## 批量任务处理
有时候您可能需要针对当前策略的所有品种一起进行某些计算（比如相关系数），得到一些中间状态保存，或者一起进行开单或平仓。
这时候您可以使用`OnBatchJobs`或`OnBatchInfos`回调函数。
> 注意OnBatchJobs的jobs参数是从map得到，不保证顺序
```go
func calcCorrs(jobs []*strat.StratJob, isBig bool) {
	// 计算各个品种与其他品种的平均相关系数，并保存到More中
	dataArr := make([][]float64, 0, len(jobs))
	for _, j := range jobs {
		dataArr = append(dataArr, j.Env.Close.Range(0, 70))
	}
	_, arr, err := utils.CalcCorrMat(dataArr, true)
	if err != nil {
		log.Error("calc corr mat fail", zap.Error(err))
		return
	}
	for i, j := range jobs {
		m, _ := j.More.(*BatchSta)
		if isBig {
			m.bigCorr = arr[i]
		} else {
			m.smlCorr = arr[i]
		}
	}
}

type BatchSta struct {
	smlCorr float64
	bigCorr float64
}

func BatchDemo(pol *config.RunPolicyConfig) *strat.TradeStrat {
	return &strat.TradeStrat{
		WarmupNum:  100,
		BatchInOut: true,
		BatchInfo:  true,
		OnPairInfos: func(s *strat.StratJob) []*strat.PairSub {
			return []*strat.PairSub{
				{"_cur_", "1h", 100},
			}
		},
		OnStartUp: func(s *strat.StratJob) {
			s.More = &BatchSta{}
		},
		OnBar: func(s *strat.StratJob) {
			m, _ := s.More.(*BatchSta)
			if m.bigCorr < 0.5 && m.smlCorr < 0.5 {
				// 当大小周期的相关度均低于50%时开单。
				s.OpenOrder(&strat.EnterReq{Tag: "open"})
			} else if m.smlCorr > 0.9 {
				// 当前品种小周期相关度高于90%，平仓
				s.CloseOrders(&strat.ExitReq{Tag: "close"})
			}
		},
		OnBatchJobs: func(jobs []*strat.StratJob) {
			if jobs[0].IsWarmUp {
				return
			}
			calcCorrs(jobs, false)
		},
		OnBatchInfos: func(tf string, jobs map[string]*JobEnv) {
			jobList := utils.ValsOfMap(jobs)
			if jobList[0].IsWarmUp {
				return
			}
			calcCorrs(jobList, true)
		},
	}
}
```
> 注意所有在OnBatchJobs或OnBatchInfos中使用的指标，都必须也在OnBar中调用一次；否则指标计算不正确。

## 自定义退出逻辑
您可以在每个K线，为每个打开的订单，执行自定义的退出逻辑检查：
```go
func CustomExitDemo(pol *config.RunPolicyConfig) *strat.TradeStrat {
	return &strat.TradeStrat{
		OnBar: func(s *strat.StratJob) {
			if len(s.LongOrders) == 0 {
				s.OpenOrder(&strat.EnterReq{Tag: "long"})
			} else if rand.Float64() < 0.1 {
				s.CloseOrders(&strat.ExitReq{Tag: "close"})
			}
		},
		OnCheckExit: func(s *strat.StratJob, od *ormo.InOutOrder) *strat.ExitReq {
			if od.ProfitRate > 0.1 {
				// 盈利超过10%退出
				return &strat.ExitReq{Tag: "profit"}
			}
			return nil
		},
	}
}
```
## 回撤止损（跟踪止损）
您可通过`DrawDownExit`和`GetDrawDownExitRate`方便地实现回撤止损。
比如当某个订单的历史最佳盈利超过10%后，回撤超过50%即退出：

```go
func DrawDown(pol *config.RunPolicyConfig) *strat.TradeStrat {
	return &strat.TradeStrat{
		DrawDownExit: true,
		OnBar: func(s *strat.StratJob) {
			if len(s.LongOrders) == 0 {
				s.OpenOrder(&strat.EnterReq{Tag: "long"})
			}
		},
		GetDrawDownExitRate: func(s *strat.StratJob, od *ormo.InOutOrder, maxChg float64) float64 {
			if maxChg > 0.1 {
				// 订单最佳盈利超过10%后，回撤50%退出
				return 0.5
			}
			return 0
		},
	}
}
```
## 订阅品种动态修改
您在yml中可直接指定`pairs`品种列表或通过`pairlist`动态筛选。但您也可在策略中通过`OnSymbols`回调函数监听品种列表并修改返回：
```go
func editPairs(p *config.RunPolicyConfig) *strat.TradeStrat {
	return &strat.TradeStrat{
		OnSymbols: func(items []string) []string {
			hasBTC := false
			for _, it := range items {
				if it == "BTC" {
					hasBTC = true
				}
			}
			if !hasBTC {
				return append([]string{"BTC"}, items...)
			}
			return items
		},
	}
}
```
上面是检查订阅品种中是否有BTC，没有就加上。
:::tip tip
您需返回修改后的品种列表，这些品种不一定全部交易，会受到`run_policy.max_pair`和`TradeStrat.MinTfScore`的限制。所以品种的前后顺序很重要。
:::

## 动态更新交易品种
您可在策略运行过程中通过`UpdatePairs`方法动态添加或移除交易品种，无需重启机器人。

```go
// 移除/移除品种并平仓
s.Strat.UpdatePairs(strat.PairUpdateReq{
	Add:    []string{"BTC", "ETH"},
	Remove:        []string{s.Symbol.Symbol},
	CloseOnRemove: true,
})
```

**参数说明：**
- `Add`: 要添加的品种列表
- `Remove`: 要移除的品种列表
- `CloseOnRemove`: 移除时是否关闭订单
- `ForceAdd`: 是否强制添加（跳过pairlist过滤）
- `Reason`: 更新原因（可选）

## 策略退出回调
当机器人停止时（回测结束或实盘停止），系统会依次调用策略的退出回调函数，用于清理资源或保存状态。

### OnShutDown
`OnShutDown`在每个策略任务（`StratJob`）停止时调用，参数为当前任务对象。由于一个策略可能运行在多个账号、多个品种、多个周期上，此函数会被调用多次。

适用场景：
- 保存单个品种的运行状态
- 清理品种相关的资源
- 记录品种级别的统计信息

```go
func Demo(pol *config.RunPolicyConfig) *strat.TradeStrat {
	return &strat.TradeStrat{
		OnBar: func(s *strat.StratJob) {
			// 策略逻辑
		},
		OnShutDown: func(s *strat.StratJob) {
			log.Info("job shutdown", 
				zap.String("pair", s.Symbol.Symbol),
				zap.String("tf", s.TimeFrame),
				zap.String("account", s.Account))
		},
	}
}
```

### OnStratExit
`OnStratExit`在策略配置退出时调用，无参数。每个策略配置只调用一次，在所有`OnShutDown`执行完毕后执行。

适用场景：
- 关闭策略级别的全局资源（如数据库连接、文件句柄）
- 保存策略整体的统计数据
- 清理策略共享的状态

```go
func Demo(pol *config.RunPolicyConfig) *strat.TradeStrat {
	// 策略级别的资源
	var logFile *os.File
	var err error
	
	return &strat.TradeStrat{
		OnBar: func(s *strat.StratJob) {
			if logFile == nil {
				logFile, err = os.Create("strategy.log")
				if err != nil {
					log.Error("create log file fail", zap.Error(err))
				}
			}
			// 策略逻辑
		},
		OnStratExit: func() {
			if logFile != nil {
				logFile.Close()
				log.Info("strategy log file closed")
			}
		},
	}
}
```

:::warning 注意
- `OnShutDown`接收`*StratJob`参数，可访问品种、账号等信息
- `OnStratExit`无参数，用于清理策略级别的全局资源
- 执行顺序：先执行所有`OnShutDown`，再执行`OnStratExit`
- 同一策略函数可能因不同参数被调用多次，但每个配置的`OnStratExit`只执行一次
:::

## Websocket高频数据订阅
您可在策略中订阅交易所通过Websocket推送的高频数据，包括：未完成K线，逐笔交易、订单簿深度。支持实时交易和回测。
注意从spider发送到机器人进程目前约有1ms延迟，另外交易所到您本地视网络情况也有一定延迟。

下面是一个示例：
```go
import (
	"fmt"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/log"
)

func ws(p *config.RunPolicyConfig) *strat.TradeStrat {
	return &strat.TradeStrat{
		WsSubs: map[string]string{
			core.WsSubDepth: "",
			core.WsSubTrade: "",
			core.WsSubKLine: "",
		},
		OnBar: func(s *strat.StratJob) {
			e := s.Env
			log.Info(fmt.Sprintf("OnBar %v: %v", e.TimeStop, e.Close.Get(0)))
		},
		OnWsKline: func(s *strat.StratJob, pair string, k *banexg.Kline) {
			log.Info(fmt.Sprintf("OnWsKline %v: %v", k.Time, k.Close))
            s.OpenOrder(&strat.EnterReq{Tag: "long"})
            _, _, err := biz.GetOdMgr(s.Account).ProcessOrders(nil, s)
            if err != nil {
                log.Error("process order fail", zap.Error(err))
            }
		},
		OnWsTrades: func(s *strat.StratJob, pair string, trades []*banexg.Trade) {
			last := trades[len(trades)-1]
			log.Info(fmt.Sprintf("OnWsTrades %v %v, %v", last.Timestamp, last.Price, last.Amount))
		},
		OnWsDepth: func(s *strat.StratJob, dep *banexg.OrderBook) {
			bp1, bm1 := dep.Bids.Price[0], dep.Bids.Size[0]
			ap1, am1 := dep.Asks.Price[0], dep.Asks.Size[0]
			log.Info(fmt.Sprintf("OnWsDepth %v %v, %v,, %v, %v", dep.TimeStamp, bp1, bm1, ap1, am1))
		},
	}
}
```
如上，您可通过`WsSubs`订阅所需的数据，值可默认为空或`_cur_`表示当前品种，也可使用其他品种，或同时订阅多个品种：`BTC,_cur_`，多个品种应当以逗号隔开。

这三个高频回调中默认未支持开平仓，如果需要您可仿照上面示例代码，自行调用`ProcessOrders`提交处理订单。

## HTTP Post接口回调
您可从外部通过http post请求对您的策略发起回调，仅支持实盘交易&模拟实盘，当您启用`api_server`时才可生效。

您可在TradingView或python等其他平台中出现信号时调用此接口触发策略回调。

要支持收到post请求时执行一些逻辑，您需要先在策略中配置`OnPostApi`回调函数：
```go
func PostApi(p *config.RunPolicyConfig) *strat.TradeStrat {
    return &strat.TradeStrat{
        OnBar: func(s *strat.StratJob) {
            // do nothing
        },
        OnPostApi: func(client *core.ApiClient, msg map[string]interface{}, jobs map[string]map[string]*strat.StratJob) error {
			// 您可自行解析收到的请求，下面仅做示例参考
            action := utils.PopMapVal(msg, "action", "")
            for acc, pairMap := range jobs {
                for pairTF, job := range pairMap {
                    if action == "openLong" {
                        log.Info("open long from api", zap.String("acc", acc), zap.String("pairTF", pairTF))
                        job.OpenOrder(&strat.EnterReq{
                            Tag: "long",
                        })
                    } else {
                        log.Warn("unknown action", zap.String("action", action))
                    }
                }
            }
            return nil
        },
    }
}
```

然后您可参考[实时交易](live_trading.md)配置并启动机器人。注意需要在yml中配置登录密码并启用：
```yaml
api_server:  # 供外部通过api控制机器人
  enable: true
  bind_ip: 0.0.0.0
  port: 8001
  users:
    - user: api
      pwd: very_strong_password
      allow_ips: []
      acc_roles: 
        user1: admin
```

然后Post api http://127.0.0.1:8001/api/strat_call
```json
{
    "token": "very_strong_password", // 这是api_server中的密码
    "strategy": "ma:postApi", // 这是请求策略的名称
    // 前两个是固定必填的，下面字段均可选，可在策略中自行解析
    "action": "openLong",
    "data1": 123, // 其他任意需要发送到策略的数据
    "data2": "hello"
}
```

## StratJob的所有成员函数
**CanOpen(short bool) bool**  
判断当前是否允许打开订单（传true判断是否允许开空，false判断是否允许开多）

**OpenOrder(req \*EnterReq) \*errs.Error**  
发出一个打开订单请求，如果失败，返回错误信息。

**CloseOrders(req \*ExitReq) \*errs.Error**  
发出一个关闭订单请求，可关闭一个或多个订单。如果失败，返回错误信息。

**GetOrderNum(dirt float64) int**  
返回当前策略任务的订单数量，参数dirt值可为`core.OdDirtLong/core.OdDirtShort/core.OdDirtBoth`。

**GetOrders(dirt float64) []\*ormo.InOutOrder**  
返回当前策略任务指定方向的所有订单，参数dirt的值可为`core.OdDirtLong/core.OdDirtShort/core.OdDirtBoth`。

**Position(dirt float64, enterTag string) float64**  
获取当前策略任务的仓位大小，返回基于基准金额的倍数。比如默认开单金额30U，返回2表示已花费60U开单。

**SetAllStopLoss(dirt float64, args \*ormo.ExitTrigger)**  
对当前策略任务的所有指定方向订单设置止损，参数dirt的值可为`core.OdDirtLong/core.OdDirtShort/core.OdDirtBoth`。

**SetAllTakeProfit(dirt float64, args \*ormo.ExitTrigger)**  
对当前策略任务的所有指定方向订单设置止盈，参数dirt的值可为`core.OdDirtLong/core.OdDirtShort/core.OdDirtBoth`。

