# exg 包

exg 在任务构造边界创建 banexg session、绑定通用 capability 与执行包装。交易所差异归 banexg，不在 banbot 根据名称实现专属业务。

## 显式会话

NewForRuntime(snapshot *config.Snapshot, netDisable bool) (banexg.BanExchange, *errs.Error) 从输入快照创建会话，不修改 exg.Default。账户选择、环境、网络禁用和市场参数来自快照。会话关闭由创建它的 entry/外部 owner 负责，Runtime.Exchange 是借用依赖。

Setup()、GetWith(name, market, contractType) 和 GetLeverage/GetOdBook/GetTickers24Hr 等无显式 session 的工具保留兼容配置/默认会话路径；不能把它们当作多个 Runtime 的隔离入口。原 GetTickers 文档名称已改为真实 GetTickers24Hr。

## 精度与能力

PrecCost(exchange, symbol, cost)、PrecPrice(exchange, symbol, price)、PrecAmount(exchange, symbol, amount) 返回数值和 *errs.Error，显式传入所属 session。实际数量步长、合约单位、价格精度和最小金额由标准 instrument metadata 验证。

GetAlignOffForExchangeChecked(exchange, symbol, tfSecs) 返回 offset/error，使用当前 session market metadata。GetAlignOff(exchangeName, tfSecs) 仅兼容旧调用，不证明符号级市场对齐。symbol parser、order events、client-order IDs、funding 和 account download 分别探测统一 capability；缺能力明确失败。

BotExchange 包装底层 session 并转发统一接口；订单 callback、context-aware 请求及 timeout/Unknown 恢复保持账户合同。选择 live_provider 的名字不能制造 transport 或账户证明。

见[runtime](runtime.md)、[实时交易](../guide/live_trading.md)及[因子 API](factor.md)。
