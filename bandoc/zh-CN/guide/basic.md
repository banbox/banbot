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
回测和实盘交易的执行涉及方方面面，为使项目结构始终灵活清晰，banbot中主要涉及下面一些组件：

* 爬虫进程(spider)：banbot使用单独的爬虫进程监听交易所的公共数据，写入数据库，并通过TCP实时推送给多个监听的机器人，这允许banbot支持多个账户同时运行一组策略。
* 数据提供器(DataProvider)：整合回测/实盘涉及的所有品种(每个品种对应一个Feeder，一个Feeder支持多个时间周期)，将收到的K线通过回调函数执行。
* 订单管理器(OrderMgr)：回测时对订单请求根据K线撮合成交；实盘时提交到交易所并监听成交进度实时更新。
* 钱包(Wallets)：回测时根据订单成交模拟钱包的可用余额、冻结金额、未实现盈亏等；实盘时监听交易所账户钱包状态实时更新。
* 交易所(Exchange)：基于banexg支持币安、欧易、bybit交易所的rest/websocket，用于下载K线、查询市场信息、订单处理、监听ws等。
* 品种管理器(PairFilters)：支持基于所有品种，按给定的过滤器筛选并排序。也可使用硬编码的品种列表。
* 消息通知(Notify)：在机器人开单、出现信号时发送通知，目前仅支持微信。
* Rest Api：实盘交易时可启用rest api，然后通过web ui查看和管理机器人。

## 关键约定
**策略任务(StratJob)**   

某个品种+某个策略+某个多空(both/long/short)+任意时间周期，只允许存在一个策略任务。  

以一个简单均线策略`ma:demo`为例，可设置支持从`yml`配置传入长短周期参数：
```yaml
run_policy:
  - name: ma:demo
    run_timeframes: [5m]
    dirt: long
    params: {smlLen: 5, bigLen: 20}
    pairs: [BTC/USDT, ETH/USDT]
  - name: ma:demo
    run_timeframes: [5m, 15m]
    dirt: short
    params: {smlLen: 7, bigLen: 30}
    pairs: [BTC/USDT, ETH/USDT]
  - name: ma:demo
    run_timeframes: [15m]
    dirt: long
    params: {smlLen: 4, bigLen: 15}
    pairs: [BCH/USDT, ETC/USDT, BTC/USDT]
  - name: ma:demo
    run_timeframes: [5m]
    dirt: long
    params: {smlLen: 5, bigLen: 20}
    pairs: [BCH/USDT, ETC/USDT]
```
如上配置了四组策略任务，全部使用`ma:demo`策略，每组都只能生成2个有效的策略任务(StratJob)，共计8个策略任务。

第二组提供了`[5m 15m]`两个时间周期，则会计算每个时间周期的K线质量分数，选择符合分数的最小周期。

第三组配置的`BTC/USDT`和第一组中重合了，会被忽略。  

注意：dirt参数忽略时，默认允许多空双开

## 回测时执行逻辑
* 加载配置，初始化：数据库会话、交易所、数据提供器、订单管理器、钱包、标的管理器等
* 根据标的管理器筛选此次要交易的品种列表
* 加载策略列表，结合品种列表、时间周期生成策略任务列表
* 从数据提供器订阅相关的品种及其时间周期K线
* 调用数据提供器的LoopMain按时间循环K线，对每个K线重复执行下面逻辑：
  * 检查是否需要触发批量入场回调(K线时间戳增加时触发)
  * 更新指标计算环境
  * 订单管理器根据K线更新订单（触发入场、止盈止损、计算利润等）
  * 触发所有策略任务的`OnBar`,`OnCheckExit`,`GetDrawDownExitRate`,`OnInfoBar`方法，并收集入场/离场请求
  * 提交订单入场/离场请求到订单管理器执行
  * 检查是否需要刷新品种列表(回测时可设置每隔一段时间刷新，模拟实时交易)

## 实时交易执行逻辑
* 加载配置，初始化：数据库会话、交易所、数据提供器、订单管理器、钱包、标的管理器、消息通知、Rest Api等
* 根据标的管理器筛选此次要交易的品种列表
* 加载策略列表，结合品种列表、时间周期生成策略任务列表
* 数据提供器连接爬虫进程，订阅相关的品种及其时间周期K线
* 启动后台任务：监听余额、监听个人订单、定时刷新交易对、定时更新市场信息、定时检查全局止损等
* 调用数据提供器的LoopMain循环等待爬虫推送的K线，对每个K线重复执行下面逻辑：
  * 更新指标计算环境
  * 订单管理器根据K线更新订单利润
  * 触发所有策略任务的`OnBar`,`OnCheckExit`,`GetDrawDownExitRate`,`OnInfoBar`方法，并收集入场/离场请求
  * 提交订单入场/离场请求到订单管理器执行
  * 推迟3s执行批量入场/离场（如3s内收到新品种K线，则取消执行再推迟3s）

