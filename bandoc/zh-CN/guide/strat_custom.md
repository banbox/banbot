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

::: tip 统计套利与截面策略
时序策略也可复用引擎无关的原语：`github.com/banbox/banbot/factor/statistics` 中的 `NewRollingSpread`/`NewBandTrader`（配对价差 z-score 与入场/离场/止损滞回）。全市场截面和配对策略的最短入口见[多因子与截面策略](./factor.md)。
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
    return &strat.TradeStrat{
        WarmupNum: max(int(atrLen), int(atrLen1), int(atrLen2), int(atrLen3)),
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
同一个策略可以在多个`run_policy`项中出现，即同一个策略函数可能会执行多次。
策略函数返回的`*strat.TradeStrat`会应用到多个品种上，对应多个`*strat.StratJob`。
所以任何单个品种相关的变量都不要保存在策略函数内，而应当通过`*strat.StratJob.More`保存。  
`TradeStrat`及其闭包由该策略的所有品种、账户共享，在闭包中保存的计数、标志位会被不同品种互相覆盖。  
策略函数中的变量应保持不被修改。否则会导致意外的状态。
:::

### 参数校验 {#require}

构造函数中可用`pol.Require(cond, msg)`或`pol.Requiref(cond, format, args...)`校验参数。条件不满足时该策略构造失败，并作为配置错误（BadConfig）报告，而不是被当作运行时崩溃；真正的运行时错误（如空指针）不会被改写为配置错误。
```go
func Demo(pol *config.RunPolicyConfig) *strat.TradeStrat {
    fast := pol.DefInt("fast", 10, core.PNorm(5, 30))
    slow := pol.DefInt("slow", 30, core.PNorm(20, 100))
    pol.Require(fast < slow, "require fast < slow")
    return &strat.TradeStrat{WarmupNum: slow * 3}
}
```

## strat.TradeStrat 策略对象

`TradeStrat`提供了丰富的属性配置和回调函数。新策略推荐使用 `OnData`；`OnBar` 仍是可用的 K 线兼容回调。

下面是 `TradeStrat` 常用的公开字段（省略其他配置字段）：
```go
type TradeStrat struct {
	Name          string // 策略名称，无需设置，会自动设置为注册时的名称
	Version       int // 策略版本号
	WarmupNum     int // K线预热的长度
	OdBarMax      int // 回测查找未完成持仓最多向前模拟的bar数，默认500；run_policy.order_bar_max优先
	MinTfScore    float64 // 最小时间周期质量，默认0.75
    WsSubs        map[string]string    // websocket订阅: core.WsSubKLine, core.WsSubTrade, core.WsSubDepth
	DrawDownExit  bool // 是否启用回撤止损（即跟踪止损）
    HedgeOff      bool    // 关闭合约双向持仓
	BatchInOut    bool    // 是否在主周期OnData(RoleMain)/OnBar后批量执行入场/出场
	BatchInfo     bool    // 是否在辅助周期OnData(RoleInfo)/OnInfoBar后执行批量处理
	StakeRate     float64 // 相对基础金额开单倍率
    StopLoss      float64 // 此策略打开所有订单的默认止损比率（不带杠杆）
	StopEnterBars int // 限价单如果超过给定K线仍未入场则取消
	OrderOnRotation string // 品种被轮换移出后的旧任务：open表示有订单时仍允许继续开单；平仓与否见pairmgr.pos_on_rotation
	EachMaxLong   int      // 单个品种最大同时开多数量，默认0不限制
	EachMaxShort  int      // 单个品种最大同时开空数量，默认0不限制
	RunTimeFrames []string // 允许运行的时间周期，不提供时使用当前 Runtime 配置快照中的默认周期
	RefineTF      interface{} // 策略默认的订单撮合周期，如"5m"、12、"3-6"；run_policy.refine_tf显式配置时优先
	Outputs       []string // 策略输出的文本文件内容，每个字符串是一行
	Policy        *config.RunPolicyConfig // 运行时配置

	OnDataSubs          func(s *StratJob) []*DataSub
	OnData              func(s *StratJob, data DataEvent)
	OnWsData            func(s *StratJob, evt *orm.DataSeries)
	OnSymbols           func(items []string) []string
	OnPostApi           FnOnPostApi
	OnPairInfos         func(s *StratJob) []*PairSub // 策略额外需要的品种或其他周期的数据
	OnStartUp           func(s *StratJob) // 启动时回调。初次执行前调用
	OnBar               func(s *StratJob) // 每个K线的回调函数
	OnInfoBar           func(s *StratJob, e *ta.BarEnv, pair, tf string)   // 其他依赖的bar数据
	OnWsTrades          func(s *StratJob, pair string, trades []*banexg.Trade) // 逐笔交易数据
    OnWsDepth           func(s *StratJob, dep *banexg.OrderBook)               // 推送深度信息
    OnWsKline           func(s *StratJob, pair string, k *banexg.Kline)        // Websocket推送的实时K线(可能未完成)
	OnBatchJobs         func(jobs []*StratJob)                             // 当前时间所有标的job，用于批量开单/平仓
	OnBatchInfos        func(tf string, jobs map[string]*JobEnv)            // 当前时间所有info标的job，用于批量处理（其中的开平仓不会自动提交）
	OnCheckExit         func(s *StratJob, od *ormo.InOutOrder) *ExitReq     // 自定义订单退出逻辑
	OnOrderChange       func(s *StratJob, od *ormo.InOutOrder, chgType int) // 订单更新回调
	GetDrawDownExitRate CalcDDExitRate                                     // 计算跟踪止盈回撤退出的比率
	PickTimeFrame       PickTimeFrameFunc                                  // 为指定币选择适合的交易周期
	OnShutDown          func(s *StratJob)                                  // 每个策略任务停止时回调
	OnStratExit         func()                                             // 策略配置退出时回调，在所有OnShutDown之后执行
}
```
:::tip tip
`MinTfScore`是针对每个品种在指定周期k线通过价格变动、bar实体占比等计算的分数，用于淘汰变动太小，波动不足的交易对；或计算交易对的最佳周期。阈值默认取0.75，小于此阈值的品种将会被过滤，如需禁用，可设为非零较小值0.0001
:::

## 简单策略示例

新策略使用统一回调，下面是可直接注册为 `data:demo` 的完整示例。`OnData` 不能与 `OnBar` 或 `OnInfoBar` 同时设置，否则构造时 panic；本页后续 OnBar 示例仍可单独使用。辅助或自定义事件请用 RouteData 分流。

```go
package dataexample

import (
    "github.com/banbox/banbot/config"
    "github.com/banbox/banbot/strat"
    ta "github.com/banbox/banta"
)

func init() { strat.RegisterStrategy("data:demo", Demo) }

func Demo(pol *config.RunPolicyConfig) *strat.TradeStrat {
    return &strat.TradeStrat{
        WarmupNum: 100,
        OnData: strat.RouteData(strat.DataHandlers{
            Main: func(s *strat.StratJob, data strat.DataEvent) {
                fast, slow := ta.SMA(s.Env.Close, 5), ta.SMA(s.Env.Close, 20)
                cross := ta.Cross(fast, slow)
                if s.IsWarmUpState() { return }
                if cross == 1 {
                    _ = s.OpenOrder(&strat.EnterReq{Tag: "cross_up", Log: true})
                } else if cross == -1 {
                    _ = s.CloseOrders(&strat.ExitReq{Tag: "cross_down", Log: true})
                }
            },
        }),
    }
}
```


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
> 您可在任意回调函数中使用指标，但注意，**所有指标都必须先在主周期 `OnData` 处理器中无条件调用一遍**；否则指标计算会出现错误。

## 技术指标库banta
banbot使用高性能指标库[banta](https://github.com/banbox/banta)，它会对每个bar的指标计算状态进行缓存，这是banbot高性能的关键，指标与调用签名请查阅 [banta 上游仓库](https://github.com/banbox/banta)及 [Go API](https://pkg.go.dev/github.com/banbox/banta)。

`banta.BarEnv` 和 `banta.Series`是banta中的两个关键结构体。

`banta.BarEnv`是某个技术指标的运行环境，其中存储了当前交易所、市场、品种、时间周期等信息。
一个策略任务至少会需要一个`banta.BarEnv`，如果通过`OnPairInfos`订阅了其他品种或时间周期，则需要多个运行环境。
在同一个 Runtime 中，相同交易所、市场、品种和周期可共享 BarEnv；不同 Runtime 的环境相互独立。

`banta.BarEnv`中内置了几个原始的`Series`，即：`Open`,`High`,`Low`,`Close`,`Volume`，分别保存了开盘价、最高价、最低价、收盘价、成交量的序列信息。

`Series`是`banta`中用于存储序列化数据的特殊结构体。`banta`中的所有技术指标函数，都是接收一个或多个`Series`，经过运算后，返回一个`Series`。

当前可用指标还包括：`VWMA/VWAP`、`DEMA/T3/TEMA/MAMA/SSF/SWMA/TRIMA/VIDYA/ZLMA`、`DMI/ADX/AROONOSC/DX/SAR/PSAR/Ichimoku/KST`、`StochF/STOCHF/ULTOSC/Fisher/ROCR/TRIX/TSI/DPO/MOM/WilliamsPercent`、`Squeeze/NATR/Supertrend/Donchian/KeltnerChannel/PMAX`、`CMF/ADL/ADOSC/EFI/OBV/VPCI/MFI`、`LinReg/Slope/Correlation/LINEARREG_ANGLE`、`PivotHigh/PivotLow` 和 `CDL*` K线形态。多返回值函数按当前依赖版本的 Go API 顺序接收。

您可通过`Series.Get(0)`获取某个序列的最新值，比如`e.Close.Get(0)`获取最新收盘价；
也可通过`Series.Range(0,5)`获取某个序列最近的5个值，返回至多 5 个值的 `[]float64` 切片；历史不足时更短。

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
func Highest(obj *ta.Series, period int) *ta.Series {
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
Subs  map[string]map[int]*ta.Series
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

func Sum(obj *ta.Series, period int) *ta.Series {
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

此策略可在yml配置中指定运行在`5m`小周期，同时策略内部指定了订阅`1h`大周期的数据。`OnPairInfos`中`_cur_`表示当前品种，如需要其他品种，可硬编码替换此处的值。在当前交易所和市场中找不到的品种，该订阅会被静默跳过。

::: tip 注意
`OnInfoBar`仅用于额外K线消费，请勿在此函数中执行开平仓或更新止损等逻辑。
:::

## 参考行情：其他品种或周期 {#ref_kline}

使用`OnData`的策略可通过`RefKlineSub`声明只读的K线参考订阅，不会为该品种创建交易任务。

| 方法 | 说明 |
| --- | --- |
| `s.RefKlineSub(symbol, timeframe string, warmupNum ...int) *DataSub` | `symbol`为空或`_cur_`表示当前品种的其他周期；也可写`BTC`或`BTC/USDT:USDT`，自动归一到当前交易所和市场。省略`warmupNum`时取`TradeStrat.WarmupNum` |
| `s.RefEnv(symbol, timeframe string) *ta.BarEnv` | 返回该订阅同步更新的K线环境；首根K线到达前为`nil`，非`nil`后`Open/High/Low/Close/Volume/Quote/BuyVolume/TradeNum`均可用 |
| `s.Data(sub *DataSub) *DataFields` | 返回订阅对应的数据视图；`sub.ExSymbol`为空时取当前品种 |

参考行情的每根K线以辅助事件（`Info`）送达，指标应在对应事件中计算，主回调只读取结果：

```go
type refState struct{ btcUp bool }

func RefDemo(pol *config.RunPolicyConfig) *strat.TradeStrat {
    return &strat.TradeStrat{
        WarmupNum: 100,
        OnStartUp: func(s *strat.StratJob) { s.More = &refState{} },
        OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
            return []*strat.DataSub{s.RefKlineSub("BTC/USDT:USDT", "1h", 100)}
        },
        OnData: strat.RouteData(strat.DataHandlers{
            Info: func(s *strat.StratJob, d strat.DataEvent) {
                env := s.RefEnv("BTC/USDT:USDT", "1h")
                if env == nil { return }
                s.More.(*refState).btcUp = env.Close.Get(0) > ta.SMA(env.Close, 20).Get(0)
            },
            Main: func(s *strat.StratJob, d strat.DataEvent) {
                if s.IsWarmUpState() || !s.More.(*refState).btcUp { return }
                _ = s.OpenOrder(&strat.EnterReq{Tag: "btc_trend", Log: true})
            },
        }),
    }
}
```
K线订阅的`DataFields.BarEnv()`返回完整的`*ta.BarEnv`（同样仅在首根K线到达后非`nil`；非K线数据源始终为`nil`），因此`ta.MFI(env, 14)`、`ta.WillR(env, 14)`、`ta.HeikinAshi(env)`这类需要完整OHLCV的指标可直接用于参考行情。

### 内置衍生品数据

资金费率、持仓量、标记/指数/溢价K线等内置数据源通过`OnDataSubs`按名称订阅，以`Custom`事件送达，需先下载数据：
```go
OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
    return []*strat.DataSub{
        {Source: orm.SeriesSourceFundingRate, TimeFrame: "8h", WarmupNum: 3},
        {Source: orm.SeriesSourcePremiumKline, TimeFrame: "1h", WarmupNum: 3},
    }
},
OnData: strat.RouteData(strat.DataHandlers{
    Custom: func(s *strat.StratJob, d strat.DataEvent) {
        if d.Source == orm.SeriesSourceFundingRate { s.More.(*derivState).funding = d.Float64("rate") }
    },
    Main: func(s *strat.StratJob, d strat.DataEvent) { /* ... */ },
}),
```
各数据源的字段、默认周期、下载与回测语义见[自定义时序数据](./custom_data)，完整策略见 [banstrats/examples/tsfeatures](https://github.com/banbox/banstrats/tree/main/examples/tsfeatures) 的`derivatives.go`。

## 日内时段工具 {#intraday}

`StratJob`内置了一组无未来函数的日内/时段统计，状态按任务（品种）独立保存。锚点参数均为UTC当日分钟数（`0`~`1439`，自动取模），`btime`包提供常用锚点，如`btime.NewYorkOpenMinutes`（13:30 UTC）、`btime.LondonCashOpenMinutes`（08:00 UTC）、`btime.AsiaCloseMinutes`（08:00 UTC）。

| 方法 | 说明 |
| --- | --- |
| `OpeningRange(anchorMinutesUTC, rangeMinutes int) OpeningRangeSnapshot` | 开盘区间：`[锚点, 锚点+rangeMinutes)`内的`Open/High/Low/Close/Mid/Width/Volume` |
| `OpeningRangeWithOpts(anchorMinutesUTC, rangeMinutes int, opts OpeningRangeOptions) OpeningRangeSnapshot` | 同上；`AllowSingleBarFallback`允许周期粗于区间时退化为单根K线（`DegradedSingleBar=true`） |
| `SessionRange(startMinUTC, endMinUTC int) SessionRangeSnapshot` | 循环时段区间（可跨午夜，如22:00-02:00），字段含`Completed/JustCompleted/InSession/InPostSession` |
| `PrevDayOHLC(anchorMinutesUTC int) (PrevOHLC, bool)` | 前一个完整交易日的`Open/High/Low/Close/Pivot/Range/Volume` |
| `PrevNDaysStats(anchorMinutesUTC, nDays int) (PrevNDaysSummary, bool)` | 最近`nDays`（1~366）个完整交易日的`HH/HC/LC/LL/AvgRange/DualThrustRange/History`等统计 |
| `DualThrustBands(anchorMinutesUTC, nDays int, k1, k2 float64) (upper, lower, dayOpen float64, ok bool)` | Dual Thrust上下轨：`dayOpen ± k × Range` |
| `RBreakerLevels(anchorMinutesUTC int) (RBreakerSnap, bool)` / `RBreakerLevelsBy(anchorMinutesUTC int, f1, f2, f3 float64)` | R-Breaker六个价位及当日已出现的开盘/最高/最低价 |

使用要点：
- 区间快照在`Ready`（`SessionRange`为`Completed`）之前，`High/Low`等值为0，不会提前泄露未完成区间；`Applicable=false`表示当前周期不适用（周期为1d及以上、区间与周期未对齐，或周期粗于区间且未开启单根退化），此时不会有结果。
- `PrevDayOHLC`等方法在锚点不是0点时，开头不完整的交易日不会被当作前一日；预热长度应覆盖至少一个完整交易日，且`ok`为`false`时不要交易。
- 在主回调里每根K线都调用，状态才会随K线推进；对同一根K线重复调用结果一致。
- 无环境（`s.Env==nil`）时返回零值和`false`。

```go
Main: func(s *strat.StratJob, _ strat.DataEvent) {
    orb := s.OpeningRange(btime.NewYorkOpenMinutes, 30) // 每根K线都调用
    if !orb.Ready || !orb.InPostRange || s.GetOrderNum(0) > 0 { return }
    if c := s.Env.Close.Get(0); c > orb.High {
        // 止损放在区间中点，按风险比例定仓
        _ = s.OpenOrder(&strat.EnterReq{Tag: "orb_long", StopLoss: orb.Mid, TakeProfitVal: orb.Width * 2, RiskRate: 0.005})
    }
},
```
运行周期须不大于区间长度（如`run_timeframes: [15m]`配30分钟区间）。完整示例见 [banstrats/examples/tsfeatures](https://github.com/banbox/banstrats/tree/main/examples/tsfeatures) 的`orb.go`。

## 保存临时变量

有时候您可能需要保存一些中间状态，用于下次计算；或者需要在不同的回调函数之间同步一些信息。

如[上一节](#info_bar)在`OnBar`中如果要使用`OnInfoBar`的信息，需要经`StratJob.More`中转。

首先在`OnStartUp`中为`StratJob.More`初始化。
然后在`OnBar`和`OnInfoBar`中进行类型转换，并赋值到`m`变量即可：
```go
m, _ := s.More.(*Demo2Sta)
```

## 发出入场信号 {#send_entry}

发出入场信号只需要调用`StratJob.OpenOrder`方法，并传递一个`*strat.EnterReq`参数即可。

最简单的情况是只对`EnterReq`的`Tag`属性赋值，其他全部使用默认参数。即按默认开单金额和杠杆倍数请求做多；策略默认 StopLoss 仍可能生成止损。

下面是 `EnterReq` 的常用策略参数（完整字段见 strat API）：
```go
type EnterReq struct {
    CommandID string // Stable request identity
	Tag             string  // 入场信号
	StratName        string  // 策略名称
	Short           bool    // 是否做空
	OrderType       int     // 订单类型, core.OrderType*
	Limit           float64 // 限价入场价格，指定时订单将作为限价单提交；与Stop同时设置时为Stop-Limit入场
    Stop            float64 // 入场触发价（不是止损）：做多时价格上涨到触发价才入场（做空相反）
	OCOGroup        string  // OCO挂单组：组内任一挂单入场成交后，自动撤销其余未成交挂单
	CostRate        float64 // 开仓倍率、默认按配置1倍。用于计算LegalCost
	RiskRate        float64 // 单笔止损亏损占账户权益的比例，[0,1]；未指定Amount/LegalCost且有止损时按止损距离计算仓位
	LegalCost       float64 // 花费法币金额。指定时忽略CostRate
	Leverage        float64 // 杠杆倍数
	Amount          float64 // 入场标的数量，由LegalCost和price计算
	StopLossVal     float64 // 入场价格到止损价格的距离（价格差，不是比例），用于计算StopLoss
	StopLoss        float64 // 止损触发价格；默认在交易所提交止损条件单，StratJob.ExgStopLoss=false时仅本地监控
	StopLossLimit   float64 // 止损触发后的限价（0为市价）；StopLoss为0时同时作为触发价
	StopLossRate    float64 // 止损退出比例，0表示全部退出，需介于(0,1]之间
	StopLossTag     string  // 止损原因
	ActivationPrice float64 // 追踪止损激活价格，0表示立即生效；仅CallbackPct>0时有效
	CallbackPct     float64 // 追踪止损回撤百分比（单位%，1.5即1.5%），0关闭，有效范围[0.05, 50]
	TakeProfitVal   float64 // 入场价格到止盈价格的距离，用于计算TakeProfit
	TakeProfit      float64 // 止盈触发价格；默认在交易所提交止盈单，StratJob.ExgTakeProfit=false时仅本地监控
	TakeProfitLimit float64 // 止盈触发后的限价（0为市价）；TakeProfit为0时同时作为触发价
	TakeProfitRate  float64 // 止盈退出比率，0表示全部退出，需介于(0,1]之间
	TakeProfitTag   string  // 止盈原因
	TakeProfits     []strat.TakeProfitTarget // 多级阶梯止盈，不能与TakeProfit/TakeProfitVal/TakeProfitLimit同时使用
	StopBars        int     // 入场限价单超过多少个bar未成交则取消
    ClientID        string  // 用于设置提交到交易所的ClientOrderID尾部部分。
    Infos           map[string]string // 设置保存到订单的额外信息
	Log             bool // 是否自动记录错误日志
}
```

如果需要触发价格入场，可以设置`Limit`或`Stop`参数。

`Limit`是限价订单，做多订单可设置更低的价格挂单等待成交，做空订单可设置更高的价格挂单等待成交。

`Stop`是入场触发价（不是止损，止损请用`StopLoss`），做多订单可设置更高的价格等待突破后入场，做空订单可设置更低的价格挂单等待跌破后入场。

在默认的prod/dry-run共享账户（Hub）路径上，订单价格（`Limit`、`StopLossLimit`、`TakeProfitLimit`、阶梯止盈各级的`Limit`，以及平仓请求和改单的`Limit`）提交前会按品种价格最小变动单位（tick）就近取整；触发价（`Stop`、`StopLoss`、`TakeProfit`、阶梯各级`Price`）不取整。

在该路径上，未成交的限价入场单也可以通过`SetEnterLimit`、`SetStopLoss`、`SetTakeProfit`、`SetTrailingStop`修改，修改会提交给账户执行服务；改单不会再触发`OnOrderChange`的“订单创建”回调。订单已平仓、账户执行服务拒绝或暂时无法确认的改单只记录告警并被忽略（至多重试数次），不会停止机器人；仅触发价为`Stop`的入场保持触发后市价，不会被`order_type: limit`改成限价。

未成交即终止的入场单在回测与Hub路径上以同样方式通知：一次`OnOrderChange`，类型为`OdChgExitFill`，订单`Status`为`ormo.InOutStatusDelete`，`ExitTag`为原因（详情见`od.Info["status_msg"]`）。可在此清除“订单待成交”标记。原因：限价入场超时（`core.ExitTagEntExp`，`ent_expire`）、OCO兄弟单被撤销（`oco_cancel`）、策略用`ExitReq`自行撤销（该请求的`Tag`）、请求被系统拒绝或丢弃（资金不足、数量为零或低于最小值、不支持的止盈阶梯/OCO组合、账户执行方的风控或价格tick拒绝）：`core.ExitTagForceExit`（`force_exit`）或`core.ExitTagFatalErr`（`fatal_err`，数量或不支持的请求），Hub路径下原因码在`od.Info["reject_code"]`。部分成交后撤销剩余部分不属于此情形。仅被准入限制（`max_open_orders`、`max_simul_open`、禁止入场开关、亏损停止暂停）拒绝的请求在两种模式下均不产生订单也不触发回调。Hub路径下该回调可能在`OpenOrder`/`ProcessOrders`内部执行；回调中新排队的请求随后提交，重启不会重复通知。

同时设置`Stop`和`Limit`即为Stop-Limit入场：价格触及`Stop`后才以`Limit`挂限价单；若`Stop`已被当前价越过，则直接按`Limit`挂限价单。`OrderType`为`core.OrderTypeMarket`时不能设置`Limit`。

例如当前价格为 100，等待突破 120 做多可设置 `&strat.EnterReq{Tag: "breakout", Stop: 120}`；突破 120 后最多愿意以 121 买入，并带止损：
```go
_ = s.OpenOrder(&strat.EnterReq{Tag: "breakout", Stop: 120, Limit: 121, StopLoss: 110, StopBars: 3})
```

当`OpenOrder`返回错误时，表示开单失败，您可`log.Error`记录错误信息，也可在`EnterReq`中设置`Log`为true，让系统自动记录错误日志。

### 按风险比例定仓 {#risk_rate}

设置`RiskRate`后，仓位由止损距离反推：当未指定`Amount`和`LegalCost`、`RiskRate>0`，且能得到止损价（`StopLoss`/`StopLossVal`、`StratJob.LongSLPrice/ShortSLPrice`或`TradeStrat.StopLoss`）时：

```text
LegalCost = AccountEquity() × RiskRate ÷ (|入场价 - 止损价| ÷ 入场价)
```
结果受杠杆（现货按1倍）和账户`max_stake_amt`封顶；没有止损时退回默认开单金额×`CostRate`。之后仍受`open_vol_rate`和最小开单金额限制。`RiskRate`须在`[0, 1]`内，指定`Amount`或`LegalCost`时被忽略。

`StratJob`提供三个账户状态方法，可用于仓位和风控判断：

| 方法 | 含义 |
| --- | --- |
| `AccountEquity() float64` | 账户权益（余额+未平仓浮动盈亏，计价币）；预热或钱包未就绪时回退为初始资金/默认开单金额，不会返回0或NaN |
| `AvailableBalance() float64` | 可用余额/可用保证金；不可用时为0 |
| `AccountDrawdown() float64` | 相对运行以来权益峰值的回撤比例，`[0,1]`；预热期为0。同一账户下所有品种共享 |

Hub 路径的机器人读取这些账户指标时，会共享一次最长 1 秒的缓存：在 `OnTick` 等亚秒级回调中，`AccountEquity()` 等返回值最多可能滞后 1 秒；1 分钟及以上周期的 K 线回调不受影响。

```go
Main: func(s *strat.StratJob, _ strat.DataEvent) {
    e := s.Env
    cross := ta.EMA(e.Close, 10).Cross(ta.EMA(e.Close, 30))
    atr := ta.ATR(e.High, e.Low, e.Close, 14).Get(0)
    if cross != 1 || math.IsNaN(atr) || atr <= 0 { return }
    if s.AccountDrawdown() >= 0.10 || s.AvailableBalance() < s.AccountEquity()*0.2 { return }
    // 每笔最多亏损权益的1%：止损距离为2倍ATR
    _ = s.OpenOrder(&strat.EnterReq{Tag: "ema_long", StopLossVal: 2 * atr, RiskRate: 0.01})
},
```
完整示例见 [banstrats/examples/tsfeatures](https://github.com/banbox/banstrats/tree/main/examples/tsfeatures)。

:::tip
`OpenOrder`/`CloseOrders` 只校验并排队，不保证成交。主周期 `OnData`/`OnBar`、`OnBatchJobs`、订单回调和成功返回的 `OnPostApi` 有框架处理路径；辅助和高频回调不要假设自动提交。

需要显式提交时调用当前 job 的 `_, _, err, bound := s.ProcessRuntimeOrders()`，检查 err 和 bound；bound=false 表示未绑定显式 Runtime，不能用全局同名账户替代。其他品种的请求要通过其所属 job 处理。预热期间 OpenOrder 可能返回 nil，但不会入队。
:::

## 发出离场信号

发出离场信号只需要调用`StratJob.CloseOrders`方法，并传递一个`*strat.ExitReq`参数即可。

最简单的情况是只对`ExitReq`的`Tag`属性赋值，其他全部使用默认参数。即以市价单关闭全部订单，包含做多订单和做空订单。

下面是 `ExitReq` 的常用策略参数：
```go
type ExitReq struct {
    CommandID string // Stable request identity
	Tag        string  // 退出信号
	StratName   string  // 策略名称
	EnterTag   string  // 只退出入场信号为EnterTag的订单
	Dirt       int     // core.OdDirt* long/short/both，零值即both
	OrderType  int     // 订单类型, core.OrderType*
	Limit      float64 // 限价单退出价格，指定时订单将作为限价单提交
	ExitRate   float64 // 退出比率，默认100%即所有订单全部退出
	Amount     float64 // 要退出的标的数量。指定时ExitRate无效
	OrderID    int64   // 只退出指定订单
	UnFillOnly bool    // True时只退出尚未入场的订单
	FilledOnly bool    // True时只退出已入场的订单
	Force      bool    // 是否强制退出
	Log        bool    // 是否自动记录错误日志
}
```

当`CloseOrders`返回错误时，表示平仓失败，您可`log.Error`记录错误信息，也可在`ExitReq`中设置`Log`为true，让系统自动记录错误日志。

`Dirt`零值等同于`core.OdDirtBoth`；同时持有多单和空单且设置了`Limit`时必须显式指定`Dirt`，否则返回错误。

对于`ExitReq.Tag`，您可传入固定的字符串区分不同的退出原因，`core`包内置了以下常用标签：

```go
const (
    ExitTagUnknown     = "unknown"      // 未知原因
    ExitTagCancel      = "cancel"       // 用户取消
    ExitTagHedgeOff    = "hedge_off"    // 关闭双向持仓时对冲平仓
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
    ExitTagTrailingStop = "trailing_stop" // 追踪止损触发
    ExitTagTakeProfit  = "take_profit"  // 止盈触发
    ExitTagDrawDown    = "draw_down"    // 回撤止损
    ExitTagDataStuck   = "data_stuck"   // 长时间未收到K线数据自动平仓
    ExitTagLiquidation = "liquidation"  // 强制平仓
    ExitTagEnvEnd      = "env_end"      // 除权、主力合约切换等原因
    ExitTagEntExp      = "ent_expire"   // 入场限价单过期
    ExitTagExitDelay   = "exit_delay"   // 实盘重启时平仓应该更早退出的订单
)
```
回测中还会出现`data_gap`（K线数据空洞导致强平）和`delist`（数据已长期中断的品种在回测结束时强平）两种退出标签，仅回测生效，见[回测指南](./backtest.md)。

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
    Limit: ma5Val * 0.965,
    Rate:  0.5,
    Tag:   "stop half",
})
```
上面针对所有打开的做多订单，设置50%仓位的止损，价格触发5周期均线的0.97倍时，以0.965倍的限价单止损平仓一半。

`args`传`nil`表示取消该方向全部止损（止盈）。`dirt`传`core.OdDirtBoth`（0）时，如果同时存在多单和空单会直接panic，此时须分别用`core.OdDirtLong`和`core.OdDirtShort`调用。

### 交易所条件单与本地监控

`StratJob.ExgStopLoss`、`StratJob.ExgTakeProfit`默认为true，止损/止盈会作为条件单提交到交易所。设为false后不再预挂条件单，改由本地按价格监控触发（回测与实盘均生效）；`CallbackPct`追踪止损在`ExgStopLoss=false`时同样本地监控。

### 追踪止损

`CallbackPct`（单位%，范围`[0.05, 50]`）从最优价回撤到该比例时平仓；`ActivationPrice`大于0时，价格先到达激活价才开始跟踪。实盘优先使用交易所原生追踪单，交易所不支持或拒绝时降级为本地价格监控，不会使开单失败。
```go
_ = s.OpenOrder(&strat.EnterReq{Tag: "trail", StopLossVal: atr * 2, ActivationPrice: price + atr, CallbackPct: 1.5})
```

### 阶梯止盈 {#take_profits}

`EnterReq.TakeProfits`可设置多级止盈，每级是一个`strat.TakeProfitTarget`：

| 字段 | 说明 |
| --- | --- |
| `Price` / `Val` / `Rate` | 三选一（必须大于0）：绝对触发价；距离入场成交均价的价差；相对入场成交均价的比例（0.02即2%）。同时给出时`Price`优先，其次`Val`。`Price`须位于盈利方向 |
| `Limit` | 触发后的限价，0为市价 |
| `ExitRate` | 本级平掉初始仓位的比例，`(0,1]`；仅一级且为0时按1处理；各级之和不得超过1 |
| `MoveSLToBreakeven` | 本级成交后，把剩余仓位的止损收紧到不差于入场价（不会放宽已更紧的止损） |
| `Tag` | 本级退出原因 |

各级按距离入场价由近到远依次触发，且不能与`TakeProfit`、`TakeProfitVal`、`TakeProfitLimit`同时使用；设置`TakeProfits`后`LongTPPrice/ShortTPPrice`不再生效。

在默认的prod/dry-run共享账户（Hub）路径上，阶梯由机器人驱动：第一级是仓位的止盈，上一级被报告为已执行后才安装下一级（在机器人下一次轮询时看到，所以会略有延迟）。非末级若小于一个数量步长，会并入下一级。`Val`/`Rate`级先按参考价（`Limit`，否则`Stop`，否则中间价）估算，观察到入场成交后再按真实成交均价重算。某级解析出非正价格时入场会被拒绝；对带阶梯的订单做单级`TakeProfit`改单也会被拒绝。每次换级都会让追踪止损重新起算，由阶梯或止损触发的出场在订单视图中没有分级出场标签。
```go
_ = s.OpenOrder(&strat.EnterReq{
    Tag: "ladder", StopLossVal: atr * 2,
    TakeProfits: []strat.TakeProfitTarget{
        {Val: atr * 1.5, ExitRate: 0.4, MoveSLToBreakeven: true, Tag: "tp1"},
        {Val: atr * 3, ExitRate: 0.3, Tag: "tp2"},
    },
    ActivationPrice: price + atr*1.5, CallbackPct: 1.5, // 剩余30%用追踪止损
})
```

### 默认止损止盈价

`StratJob.LongSLPrice/ShortSLPrice`和`LongTPPrice/ShortTPPrice`是开仓时的默认止损/止盈价：未在`EnterReq`中指定对应止损（止盈）时使用，止盈默认价在设置了`TakeProfits`时忽略。价格需满足做多止损低于入场价、止盈高于入场价（做空相反），否则`OpenOrder`返回错误。

### OCO挂单组

给多个入场挂单设置相同的`OCOGroup`，组内任一挂单入场成交后，其余未成交的挂单自动撤销，常用于区间两侧的突破单：
```go
_ = s.OpenOrder(&strat.EnterReq{Tag: "brk_long", Stop: hi, Limit: hi + slip, OCOGroup: "donchian", StopBars: 2, StopLossVal: atr * 2})
_ = s.OpenOrder(&strat.EnterReq{Tag: "brk_short", Short: true, Stop: lo, Limit: lo - slip, OCOGroup: "donchian", StopBars: 2, StopLossVal: atr * 2})
```
在默认的prod/dry-run共享账户（Hub）路径上，账户执行服务自己执行OCO：同一tick内有多个成员同时可执行时，只保留创建最早的一个，其余撤销；组内已有成员成交后，其余仍开放的未成交成员也被撤销。已有成交、在途交易所订单、占用预留或已有出场的成员不会被撤销；若多个成员都已成交则全部保留并记告警。机器人另会在观察到成交后补发撤销，被它撤销的成员以出场标签`oco_cancel`报告。

分组按（品种、策略、`OCOGroup`）区分，用于挂单（设置了`Stop`或`Limit`）；回测中建议配合`refine_tf`，以便在同一根K线内更准确地判断两侧触发的先后。挂单占用开单名额，`max_simul_open`须足够。完整示例见 [banstrats/examples/tsfeatures](https://github.com/banbox/banstrats/tree/main/examples/tsfeatures) 的`bracket.go`。

## 批量任务处理
有时候您可能需要针对当前策略的所有品种一起进行某些计算（比如相关系数），得到一些中间状态保存，或者一起进行开单或平仓。
这时候您可以使用`OnBatchJobs`或`OnBatchInfos`回调函数。
其中，`OnBatchJobs`仅在主周期`OnData(RoleMain)`/`OnBar`后触发，`OnBatchInfos`仅在辅助周期`OnData(RoleInfo)`/`OnInfoBar`后触发；分别由`BatchInOut`和`BatchInfo`开启。
> 回测中同一根K线内，主批次的品种按`strat.BarPriority(barMS, symbol, seed)`的中性哈希顺序交给`OnBatchJobs`（不是字母序），避免字母靠前的品种系统性优先占用资金；种子为配置`bt_bar_order_seed`（默认0），修改它会改变多品种回测结果。实盘批次顺序不作保证。若同时入场会竞争资金，请在回调内自行按策略得分排序。下面保存的相关性供后续主周期使用，不假设同一时间戳主辅回调的先后。
>
> `OnBatchJobs`中的开平仓请求在回调后由框架自动提交（预热期除外）；`OnBatchInfos`中的开平仓请求**不会**自动提交，只会记录WARN日志，不要在其中下单。
```go
// Imports: slices, config, strat, utils, banexg/log, banta as ta, zap.
type BatchSta struct {
    smlCorr, bigCorr float64
    smlReady, bigReady bool
}

func calcCorrs(jobs []*strat.StratJob, envs []*ta.BarEnv, isBig bool) {
    if len(jobs) < 2 { return }
    dataArr := make([][]float64, 0, len(envs))
    for _, env := range envs {
        values := env.Close.Range(0, 70)
        if len(values) < 70 { return }
        dataArr = append(dataArr, values)
    }
    _, values, err := utils.CalcCorrMat(70, dataArr, true)
    if err != nil {
        log.Error("calc corr mat fail", zap.Error(err))
        return
    }
    for i, job := range jobs {
        state := job.More.(*BatchSta)
        if isBig { state.bigCorr, state.bigReady = values[i], true
        } else { state.smlCorr, state.smlReady = values[i], true }
    }
}

func BatchDemo(pol *config.RunPolicyConfig) *strat.TradeStrat {
    return &strat.TradeStrat{
        WarmupNum: 100, BatchInOut: true, BatchInfo: true,
        OnPairInfos: func(s *strat.StratJob) []*strat.PairSub {
            return []*strat.PairSub{{Pair: "_cur_", TimeFrame: "1h", WarmupNum: 100}}
        },
        OnStartUp: func(s *strat.StratJob) { s.More = &BatchSta{} },
        OnBar: func(s *strat.StratJob) {
            state := s.More.(*BatchSta)
            if !state.smlReady || !state.bigReady { return }
            if state.bigCorr < 0.5 && state.smlCorr < 0.5 {
                _ = s.OpenOrder(&strat.EnterReq{Tag: "open", Log: true})
            } else if state.smlCorr > 0.9 {
                _ = s.CloseOrders(&strat.ExitReq{Tag: "close", Log: true})
            }
        },
        OnBatchJobs: func(jobs []*strat.StratJob) {
            envs := make([]*ta.BarEnv, 0, len(jobs))
            for _, job := range jobs { envs = append(envs, job.Env) }
            calcCorrs(jobs, envs, false)
        },
        OnBatchInfos: func(tf string, items map[string]*strat.JobEnv) {
            keys := utils.KeysOfMap(items)
            slices.Sort(keys)
            jobs := make([]*strat.StratJob, 0, len(keys))
            envs := make([]*ta.BarEnv, 0, len(keys))
            for _, key := range keys {
                item := items[key]
                jobs = append(jobs, item.Job)
                envs = append(envs, item.Env)
            }
            calcCorrs(jobs, envs, true)
        },
    }
}
```
> 有状态指标必须在对应数据每次更新时计算：主周期使用 OnData(Main)/OnBar，辅助周期使用 OnData(Info)/OnInfoBar。批量回调读取已更新结果，不能仅靠批量调用推进指标。

### 按目标权重调仓

跨品种的组合调仓可在`OnBatchJobs`中使用`strat.RebalanceBatchWeights`：传入每个品种的目标权重（相对`AccountEquity()`的带符号名义占比，正为多、负为空，缺省或0为空仓），它会先平仓/减仓腾出保证金，再按所需金额从大到小开仓/加仓，低于最小开单金额的订单会被静默跳过。

```go
func RebalanceBatchWeights(jobs []*StratJob, targetWeights map[string]float64, opts BatchRebalanceOpts) BatchRebalanceResult
func InverseVolWeights(jobs []*StratJob, signals map[string]float64, atrPeriod int, targetGross float64) map[string]float64
```

| `BatchRebalanceOpts`字段 | 说明 |
| --- | --- |
| `Tag` | 开平仓信号标签，默认`rebalance` |
| `DriftThreshold` | 同向持仓权重偏离不超过该值时不调仓，默认0.02，负数表示关闭 |
| `MaxGrossExposure` | 目标权重绝对值之和的上限，超出时按比例缩放 |
| `StopLossRate` / `TakeProfitRate` | 为新开仓/加仓单附带的止损/止盈距离（入场价的比例） |
| `CallbackPct` | 为新开仓单附带的追踪止损回撤百分比，范围`[0.05, 50]` |
| `Leverage` | 大于0时覆盖新开仓单的杠杆 |

返回的`BatchRebalanceResult`包含调仓前后的总敞口、各类动作计数（`ClosedCount/ReducedCount/OpenedCount/ScaledInCount/ErrorCount`等）和逐笔`Actions`。`InverseVolWeights`按`1/NATR`把方向信号（如+1/-1/0）转为风险平价权重，并归一到`targetGross`；它须在每根K线都被调用（`atrPeriod<=0`取14），以保持内部指标缓存同步。

```go
OnBatchJobs: func(jobs []*strat.StratJob) {
    weights := map[string]float64{} // symbol -> 目标权重，按你的排名/信号生成
    // ...
    strat.RebalanceBatchWeights(jobs, weights, strat.BatchRebalanceOpts{
        Tag: "mom_rebalance", DriftThreshold: 0.03, MaxGrossExposure: 1.0, StopLossRate: 0.15,
    })
},
```
需要开启`BatchInOut`，且`EachMaxLong/EachMaxShort`设为0以便加仓。完整示例见 [banstrats/examples/tsfeatures](https://github.com/banbox/banstrats/tree/main/examples/tsfeatures) 的`rebalance.go`。

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
您在yml中可直接指定`pairs`品种列表或通过`pairlists`动态筛选。但您也可在策略中通过`OnSymbols`回调函数监听品种列表并修改返回：
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
// botruntime aliases github.com/banbox/banbot/runtime; errs is banexg/errs.
// rt must be the Runtime that owns s, supplied by your embedding application.
func updatePairs(rt *botruntime.Runtime, s *strat.StratJob) (*strat.PairUpdateResult, *errs.Error) {
    return s.Strat.UpdatePairs(strat.PairUpdateReq{
        StrategyState: rt.Strategies,
        Core: rt.Core,
        Exchange: rt.Exchange,
        Add: []string{"BTC", "ETH"},
        Remove: []string{s.Symbol.Symbol},
        CloseOnRemove: true,
    })
}
```

普通策略回调没有公开的 Runtime 状态 getter；未由宿主注入 rt 时，优先使用配置轮换或 OnSymbols。动态更新要求加载器安装 SubWarmPairs，CloseOnRemove 还要求 ExitOrders。检查返回的错误、Added/Removed/Skipped 和 Warnings；有未决订单的移除任务会保留至订单终态。ForceAdd 跳过常规品种准入限制，不保证行情、评分或下单成功。


**参数说明：**
- `Add`: 要添加的品种列表
- `Remove`: 要移除的品种列表
- `CloseOnRemove`: 移除时是否关闭订单
- `ForceAdd`: 跳过常规品种准入限制；不保证数据、评分或交易所校验通过
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
您可在策略中订阅交易所通过Websocket推送的高频数据，包括：未完成K线，逐笔交易、订单簿深度。实盘订阅取决于交易所及数据源能力；普通 K 线回测不包含逐笔和深度历史，不能据此复现这些回调。
实际延迟取决于数据源、网络和运行负载，没有固定毫秒级保证。

下面是一个示例：
```go
import (
	"fmt"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/log"
    "go.uber.org/zap"
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
            _, _, err, bound := s.ProcessRuntimeOrders()
            if err != nil {
                log.Error("process order fail", zap.Error(err))
            } else if !bound {
                log.Warn("runtime order processor is not bound")
            }
		},
		OnWsTrades: func(s *strat.StratJob, pair string, trades []*banexg.Trade) {
			if len(trades) == 0 { return }
			last := trades[len(trades)-1]
			log.Info(fmt.Sprintf("OnWsTrades %v %v, %v", last.Timestamp, last.Price, last.Amount))
		},
		OnWsDepth: func(s *strat.StratJob, dep *banexg.OrderBook) {
			if dep == nil || dep.Bids == nil || dep.Asks == nil || len(dep.Bids.Price) == 0 || len(dep.Asks.Price) == 0 { return }
			bp1, bm1 := dep.Bids.Price[0], dep.Bids.Size[0]
			ap1, am1 := dep.Asks.Price[0], dep.Asks.Size[0]
			log.Info(fmt.Sprintf("OnWsDepth %v %v, %v,, %v, %v", dep.TimeStamp, bp1, bm1, ap1, am1))
		},
	}
}
```
如上，您可通过`WsSubs`订阅所需的数据，值可默认为空或`_cur_`表示当前品种，也可使用其他品种，或同时订阅多个品种：`BTC,_cur_`，多个品种应当以逗号隔开。

这三个高频回调中默认未支持开平仓，如果需要您可仿照上面示例代码，调用当前 job 的 `ProcessRuntimeOrders`，检查返回错误和 bound。

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
            action, _ := msg["action"].(string)
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
    "token": "very_strong_password",
    "strategy": "ma:postApi",
    "action": "openLong",
    "data1": 123,
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

**SetAllStopLoss(dirt float64, args \*ormo.ExitTrigger) \*errs.Error**
对当前策略任务的所有指定方向订单设置止损，参数dirt的值可为`core.OdDirtLong/core.OdDirtShort/core.OdDirtBoth`。

**SetAllTakeProfit(dirt float64, args \*ormo.ExitTrigger) \*errs.Error**
对当前策略任务的所有指定方向订单设置止盈，参数dirt的值可为`core.OdDirtLong/core.OdDirtShort/core.OdDirtBoth`。

**PositionAvgPrice(dirt float64, enterTag string) float64**  
返回当前策略任务指定方向（及入场标签）持仓的平均入场价。

**AccountEquity() float64 / AvailableBalance() float64 / AccountDrawdown() float64**  
账户权益、可用余额、回撤比例，见[按风险比例定仓](#risk_rate)。

**RefKlineSub(symbol, timeframe string, warmupNum ...int) \*DataSub / RefEnv(symbol, timeframe string) \*ta.BarEnv / Data(sub \*DataSub) \*DataFields**  
参考行情订阅、环境与数据视图，见[参考行情](#ref_kline)。

**OpeningRange / SessionRange / PrevDayOHLC / PrevNDaysStats / DualThrustBands / RBreakerLevels**  
日内时段统计，见[日内时段工具](#intraday)。

**ProcessRuntimeOrders() (entered, exited []\*ormo.InOutOrder, err \*errs.Error, bound bool)**  
显式提交当前任务排队的开平仓请求，见[发出入场信号](#send_entry)。

**ExecutionSnapshot() ExecutionSnapshot**  
返回当前任务订单与计数的快照，订单指针为借用，不要跨事件长期持有。

**IsWarmUpState() bool**  
当前是否处于预热状态；预热期间可更新指标，但开平仓请求不会入队。

## 多因子与截面引擎

本页 TradeStrat/OnBar 属于 time_series。因子用 runner.RegisterDefinition 产生原生 Plan/ComboSpec，或 expressions 声明多输出，不再为每个品种注册 OnBar。portfolio builder 只消费冻结 Frame/Universe，未来标签不进入当期推理。混合同账户策略明确 id/account/capital_weight，状态和 Stop/Join 借用仍独立。

参见[多因子与截面指南](./factor.md)和[因子 API](../api/factor.md)。

## 运行实例与回调状态

普通命令由 entry 创建并关闭任务级 Runtime。配置快照只读；账户、时钟、策略作业、价格及批量队列属于本次运行。嵌入运行或同进程多任务时，使用显式 Runtime 依赖，避免在回调内读写包级 config/core/btime 兼容状态。`StratJob` 的运行上下文和数据事件应来自当前任务；实盘时间与回测推进时间不能混用。

`BatchInOut`/`BatchInfo` 使用 trader 实例的 `BatchState`；所有回测固定就绪批次和作业顺序，实盘不承诺 map 顺序。因子截面计算则冻结整个 Universe 后生成目标组合；时序批量回调不等于因子引擎。参见 [Runtime API](../api/runtime.md) 和[因子指南](./factor.md)。
