# com 包

com 提供 Runtime-owned 市场状态和 scheduler；兼容包级价格/cron 函数不是多个任务的共享业务状态。

## 市场状态

MarketState 组合 PriceState 和 PairCopiedState。价格缓存、bar 价格与历史复制进度跟随所属任务，支持构造时绑定 symbol parser。默认分隔符解析只处理通用格式；交易所特殊 symbol 语义由 banexg capability 绑定，不在 banbot 按交易所名添加分支。

独立 Runtime 使用自己的 Market.Prices；兼容价格 facade 保留历史调用，不能用于从另一个任务获取实时业务状态。

## Scheduler

Scheduler 接口为 AddFunc(spec, callback)、Start() 和 Stop() context.Context。NewSchedulerWithConfig(location, lang) 在构造时固定时区/NTP 配置，不读写进程级设置。

NewScheduler() 与 Cron() 保留旧调用形式；显式任务通过 Runtime.Cron 使用实例 scheduler。owned scheduler 由唯一 owner 停止并等待；borrowed scheduler 由外部 owner 关闭，某 Runtime 的关闭不能停掉其他任务。

见[runtime](runtime.md)与[实时交易](../guide/live_trading.md)。
