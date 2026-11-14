# exg 包

exg 在任务构造边界创建 banexg session、绑定通用 capability 与执行包装。交易所差异归 banexg，不在 banbot 根据名称实现专属业务。

## 显式会话

NewForRuntime(snapshot *config.Snapshot, netDisable bool) (banexg.BanExchange, *errs.Error) 从输入快照创建会话，不修改 exg.Default。账户选择、环境、网络禁用和市场参数来自快照。会话关闭由创建它的 entry/外部 owner 负责，Runtime.Exchange 是借用依赖。

Setup()、GetWith(name, market, contractType) 和 GetLeverage/GetOdBook/GetTickers24Hr 等无显式 session 的工具保留兼容配置/默认会话路径；不能把它们当作多个 Runtime 的隔离入口。原 GetTickers 文档名称已改为真实 GetTickers24Hr。

## 精度与能力

PrecCost(exchange, symbol, cost)、PrecPrice(exchange, symbol, price)、PrecAmount(exchange, symbol, amount) 返回数值和 *errs.Error，显式传入所属 session。实际数量步长、合约单位、价格精度和最小金额由标准 instrument metadata 验证。

GetAlignOffForExchangeChecked(exchange, symbol, tfSecs) 返回 offset/error，使用当前 session market metadata。GetAlignOff(exchangeName, tfSecs) 仅兼容旧调用，不证明符号级市场对齐。符号解析、订单事件、client order id 和账户导出有各自能力接口；缺失能力的处理各不相同。

BotExchange 包装底层 session 并转发统一接口。不要绕过策略订单管理器直接下单，否则策略状态和真实账户可能不一致。

见[runtime](runtime.md)、[实时交易](../guide/live_trading.md)及[因子 API](factor.md)。

## 能力与错误处理

`ResolveRuntimePriceSymbol(exchange, pair)` 返回 `([4]string, *errs.Error)` (base/quote/settle/identifier)；特殊符号由适配器解析，通用符号可回退分隔符解析。`BuildClientOrderIDChecked` 为严格版本；旧 BuildClientOrderID 失败时回退旧格式，不能用于证明严格校验通过。

`SetOrderCallback(exchange, func(*PutOrderRes) *errs.Error) bool` 安装实例级回调，返回是否支持。BotExchange 对下单成功和失败均调用回调；回调错误可覆盖原错误，即使返回 error 仍可能有非 nil Order，需同时检查。显式设置 nil 会禁用旧全局 AfterCreateOrder 回退。

`GetAlignOffForExchangeChecked` 的 offset 单位为秒，基于品种交易时段，仅日线及以上需要非零偏移。无能力的账户导出或归档 URL 返回不支持错误；存在封装函数并不代表当前交易所实现了它。精度函数只做舍入/合约数量单位处理，不保证余额、最小下单额或成交。
