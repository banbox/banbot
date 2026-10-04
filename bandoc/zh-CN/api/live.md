# live 包

live 负责时序策略实时控制器；Web 请求/响应类型在 web/live，账户领域在 execution，截面驱动在 factor/runner。

## CryptoTrader 构造

NewCryptoTraderWithRuntimeDeps(deps biz.RuntimeDeps, startup CryptoTraderStartupFunc) (*CryptoTrader, *errs.Error) 是显式入口。startup 签名为 func(context.Context, *CryptoTrader) error。构造要求 deps.Callbacks 同时提供该任务的 RuntimeLifecycle，symbols、时钟、交易所和 callback tracker 来自同一 owner；缺字段返回错误，不回退到 globals。

NewCryptoTrader() 和 NewCryptoTraderWith(startup) 保留旧嵌入形式，新并行任务使用显式构造。Trader 使用 Runtime.Accounts 与同一锁，不修改 Config Snapshot。

## 启动与实时循环

entry 创建独立 Process/Runtime 后绑定交易器，再启动账户检查、通知、实例 Web API、数据 provider 和策略 jobs。历史预热和当前数据在任务自己的 clock/状态下执行；第三方数据的 Symbol/SID 必须属于该任务，错误身份明确拒绝。

Cron 任务在交易器的运行时方法中绑定配置、clock、symbols、订单/钱包与 Runtime.Cron；不存在可作为通用入口调用的包级 CronRefreshPairs/CronLoadMarkets 等公开 API。刷新交易对、损失检查、延迟监控和订单检查不应读取别的任务的状态。

## 停机合同

RuntimeLifecycle 暴露 Context、OnClose、OnCloseWait。取消先禁止新 batch/provider/socket 工作；等待已接纳回调和网络监听后，交易器 Run 返回，入口再 Close/Join Runtime 并释放共享依赖。自建 goroutine 需注册真实 stop/join，Runtime 不自动拥有所有外部工作。

## 因子与混合策略

因子使用 runner.NewLive、Observe/Flush 和订阅代际；混合策略可进入共享账户，但保留目标、预算和 TS 投影。真实执行需要 verified transport、当前数据 revision/publication/funding 证据及启动对账；缺能力失败，不自动降级 paper。

见[runtime](runtime.md)、[biz](biz.md)、[Web API](web.md)及[因子指南](../guide/factor.md)。
