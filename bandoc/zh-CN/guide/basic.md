本页面介绍banbot的一些基本原理和概念。

## 相关术语
* 策略(strategy)：您的交易策略，包括收到K线时如何计算指标和开平仓等。
* 订单(Order)：策略执行的一次完整入场和出场对应一个订单。
* 交易对/品种(symbol)：交易所的某个市场可交易的品种代码。
* 时间周期(timeframe)：交易的K线时间间隔（例如`5m`，`1h`）
* 指标(indicator)：技术指标（如SMA、EMA、RSI等）
* 杠杆倍率(leverage)：期货合约中使用，保证金=名义价值/杠杆倍率
* 限价止盈(TakeProfit)：为当前订单提交一个限价离场单到交易所（做多时10元入场，当前价格11元，可提交>11元的限价止盈等待成交）
* 止损(StopLoss)：为当前订单在交易所端设置止损，分为市价止损和限价止损（做多时10元入场，当前价格11元，可设置<11元止损触发价格）

## 手续费
banbot的所有利润计算均包含手续费。对于回测、超参数优化、实时模拟模式，将使用交易所默认手续费；对于实盘交易，将使用交易所收取的实际手续费（扣除返佣等）。

## 品种命名
数字货币通常用Base/Quote格式（如现货BTC/USDT，U本位合约BTC/USDT:USDT，币本位合约BTC/USDT:BTC）**推荐在`pairs`中直接写Base部分**，banbot将根据`stake_currency`第一项和`market_type`自动补全交易对。

```yaml
stake_currency: [USDT]
market_type: spot
pairs: [BTC, ETH, DASH]
```
上面配置和下面配置完全相同：
```yaml
stake_currency: [USDT]
market_type: spot
pairs: [BTC/USDT, ETH/USDT, DASH/USDT]
```

加密货币交易中经常涉及两个概念：**基础货币**和**报价货币**。前者是当前想交易的对象，后者是希望用哪种货币定价并兑换。不同交易所之间交易对命名可能有差异，我们按照统一的命名规范进行维护，并自动处理不同交易所之间的差异。

#### 现货交易对命名
```text
基础货币 ↓
       BTC/USDT
       ETH/BTC
      DASH/ETH
            ↑ 报价货币
```
现货交易中交易对是"/"隔开的两部分，前面是基础货币，后面是报价货币。

#### 合约交易对命名
```text
// 基础货币
// ↓
// ↓  报价货币
// ↓  ↓
// ↓  ↓    结算货币
// ↓  ↓    ↓
// ↓  ↓    ↓
'BTC/USDT:BTC'  // BTC/USDT 以BTC结算的反向永续合约
'BTC/USDT:USDT' // BTC/USDT 以USDT结算的线性永续合约
'ETH/USDT:ETH'  // ETH/USDT 以ETH结算的反向永续合约
'ETH/USDT:USDT' // ETH/USDT 以USDT结算的线性永续合约
```
合约交易中交易对是":"隔开的三部分，分别表示基础货币/报价货币:结算货币。其中结算货币是作为保证金和盈亏计算的货币。

合约按结算货币分两种：线性合约（U本位合约）和反向合约（币本位合约）。前者是以USDT/USDC等稳定币作为结算货币，后者是以BTC/ETH等数字货币作为结算货币。在yml中通过`market_type`配置为`linear`和`inverse`表示启用线性合约或反向合约。

合约按是否有交割日期分为永续合约和交割合约：
```text
// 基础货币
// ↓
// ↓  报价货币
// ↓  ↓
// ↓  ↓    结算货币
// ↓  ↓    ↓
// ↓  ↓    ↓     标识符 (交割日期)
// ↓  ↓    ↓     ↓
// ↓  ↓    ↓     ↓
'BTC/USDT:BTC-211225'  // BTC/USDT 以BTC结算的反向合约，交割日期 2021-12-25
'BTC/USDT:USDT-210625' // BTC/USDT 以USDT结算的线性合约，交割日期 2021-06-25
```
在yml中通过`contract_type`配置为`swap`和`future`表示启用永续合约或交割合约。

期权：
```text
// 基础货币
// ↓
// ↓  报价货币
// ↓  ↓
// ↓  ↓    结算货币
// ↓  ↓    ↓
// ↓  ↓    ↓       标识符 (交割日期)
// ↓  ↓    ↓       ↓
// ↓  ↓    ↓       ↓   行权价
// ↓  ↓    ↓       ↓   ↓
// ↓  ↓    ↓       ↓   ↓   类型, 卖put (P) or 买call (C)
// ↓  ↓    ↓       ↓   ↓   ↓
'BTC/USDT:BTC-211225-60000-P'  // BTC/USDT 以BTC结算，可在2021-12-25以60000卖出的反向合约
'ETH/USDT:USDT-211225-40000-C' // BTC/USDT 以USDT结算，可在2021-12-25以40000买入的线性合约
'ETH/USDT:ETH-210625-5000-P'   // ETH/USDT 以ETH结算，可在2021-06-25以5000卖出的反向合约
'ETH/USDT:USDT-210625-5000-C'  // ETH/USDT 以USDT结算，可在2021-06-25以5000买入的线性合约
```

## 主要组件

| 组件 | 用户需要了解的行为 |
| --- | --- |
| Hub（`bot hub`，别名 `spider`） | 采集交易所公共行情、写库并通过 TCP 分发；还可通过本机 Unix socket 承载账户执行服务 |
| 数据提供器 | 历史数据按时间回放，实时数据通过订阅送达；第三方序列使用注册的数据源 |
| 策略 | `time_series` 按 Job 接收事件；`factor` 对多标的快照计算因子并生成目标持仓 |
| 订单管理器 | 时序策略的开平仓请求先入队；回测本地撮合，实盘提交并跟踪成交 |
| 共享账户执行 | 多个策略的逻辑仓位按归属与预算管理，合并为物理账户执行计划 |
| 钱包 | 回测模拟可用余额、冻结和盈亏；实盘读取账户状态，并受执行与对账约束 |
| 品种管理器 | 使用显式 `pairs` 或过滤器流水线生成和更新品种池 |
| 通知与界面 | 企业微信、Telegram、邮件；开发 WebUI 和实盘 Dashboard 是两个不同入口 |

## 两种策略引擎

`run_policy.engine` 省略时为 `time_series`。同一个 YAML 可以包含时序和因子策略；混合运行使用 events 执行路径。

时序策略通过 `strat.AddStratGroup` 注册，再由 `run_policy.name` 引用。每个账户下的策略、品种与所选周期形成 `StratJob`。`run_timeframes` 是候选周期列表，由质量评分或 `PickTimeFrame` 选出周期，不表示自动对列表内每个周期都创建 Job；需要辅助周期时应声明订阅。

```yaml
run_policy:
  - name: ma:demo
    engine: time_series
    run_timeframes: [15m]
    dirt: long
    pairs: [BTC/USDT, ETH/USDT]
```

`ma:demo` 必须在策略项目中注册。`dirt` 可为 `long`、`short`、`any`，省略时允许双向，但现货不允许开空。策略同时运行的订单数还受钱包、账户与策略限制。

因子引擎按决策轮次冻结 Universe 和当前可见数据，区分 reference、investable、tradable 等集合，再计算因子和目标持仓；它不为每个标的调用时序 `OnBar`。配对统计套利使用因子/组合策略能力。详见[多因子与截面](./factor.md)。

## 时序数据与回调

`orm.DataSeries.Values` 同时传递标准 K 线字段和任意扩展字段。自定义字段保留类型、缺失与显式 NULL；指标使用的数值视图不代替原始数据。

新策略可使用 `OnData(s, event)`，按 `event.IsMain()` 或角色区分主周期、辅助周期和自定义序列。兼容回调 `OnBar`/`OnInfoBar` 仍保留，但不能和 `OnData` 同时设置。预热阶段可计算指标，不提交交易。详见[自定义策略](./strat_custom.md)和[自定义数据](./custom_data.md)。

## 从数据到订单

1. 加载并合并配置，选择引擎、品种和周期，为本次任务创建独立 Runtime。
2. 订阅数据并预热；回测按历史时间推进，实盘等待实时事件。
3. 主周期事件更新指标环境与数据视图，调用策略，再检查自定义退出并处理订单请求；辅助和自定义事件按订阅路由。
4. `OpenOrder`/`CloseOrders` 产生意图，订单管理器负责撮合或提交；`OnOrderChange` 用于跟踪订单变化。共享账户路径还会合并策略目标并保存执行账本。
5. 更新钱包、报告、通知和监控；停止时按当前任务释放订阅及资源。

批量策略通过 `BatchInOut`/`BatchInfo` 和批回调处理同轮多个任务。回测与实盘批次就绪条件不同，不应假设任意事件到达时就已收齐全部标的数据。

## 选择运行方式

| 目的 | 入口与含义 |
| --- | --- |
| 历史验证 | `bot backtest`；因子支持 weights/events，混合要求 events |
| 优化时序策略参数 | `bot optimize` 或 `bot bt-opt` |
| 时序实时模拟 | `env: dry_run` 配合 `bot trade`，消费实时行情、本地撮合 |
| 因子/混合历史 events 模拟 | `bot trade --dry-run`，不是实时模拟盘 |
| 真实交易 | `bot trade`；配置账户、数据与执行依赖，按引擎完成启动校验 |

默认实时入口通过 Hub 执行，当前只接受 `prod`/`dry_run` 线性永续账户。详细约束和恢复步骤见[共享账户与 Hub](./execution.md)。参见[命令行](./bot_usage.md)、[回测](./backtest.md)和[实时交易](./live_trading.md)。
