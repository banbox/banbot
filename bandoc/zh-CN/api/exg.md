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

`ResolveRuntimePriceSymbol(exchange, pair)` 返回 `([4]string, *errs.Error)` (base/quote/settle/identifier)；特殊符号由适配器解析，通用符号可回退分隔符解析。`BuildClientOrderID` 优先使用适配器能力，失败时回退旧格式，不能用于证明严格校验通过。

下单回调（`SetOrderCallback`、全局 `AfterCreateOrder`）已随旧实盘栈删除，`BotExchange` 不再拦截 `CreateOrder`；`PutOrderRes` 类型仅为旧 dump 的解码保留。

`GetAlignOffForMarket(market *banexg.Market, tfSecs int) int` 是纯函数版本，直接按品种元数据计算偏移（秒）。`GetAlignOffForExchangeChecked` 的 offset 单位为秒，基于品种交易时段，仅日线及以上需要非零偏移。无能力的账户导出（`StartAccountDownload`/`GetAccountDownload`）或归档 URL（`BuildArchiveURL`）返回不支持错误，目前没有适配器实现这些能力；存在封装函数并不代表当前交易所实现了它。精度函数只做舍入/合约数量单位处理，不保证余额、最小下单额或成交。

## 归档与衍生品数据

适配器可选实现以下能力，`BotExchange` 提供同名方法，包级函数以 `exchange banexg.BanExchange` 为首参数。第二个返回值 `bool` 表示适配器是否支持：`false` 且错误为 nil 表示不支持，调用方应回退到普通 REST 拉取。

- `FetchOHLCVArchive(ctx, symbol, timeframe string, startMS, endMS int64) ([]*banexg.Kline, bool, *errs.Error)`
- `FetchPriceOHLCVArchive(ctx, priceType, symbol, timeframe string, startMS, endMS int64) ([]*banexg.Kline, bool, *errs.Error)`：`priceType` 为 `mark`/`index`/`premiumIndex`
- `FetchFundingRateArchive(ctx, symbol string, startMS, endMS int64) ([]*banexg.FundingRate, bool, *errs.Error)`
- `FetchDerivativesMetricsArchive(ctx, symbol, timeframe string, startMS, endMS int64) ([]*banexg.DerivativesMetric, bool, *errs.Error)`
- `FetchDerivativesMetrics(ctx, symbol, timeframe string, startMS, endMS int64, limit int, params map[string]interface{}) ([]*banexg.DerivativesMetric, bool, *errs.Error)`

```go
kl, ok, err := exg.FetchPriceOHLCVArchive(ctx, exchange, "mark", "BTC/USDT:USDT", "1h", startMS, endMS)
if err != nil { return err }
if !ok { /* 适配器不支持，回退到普通拉取 */ }
```

## 附加市场与代理

根配置 `exchange` 之外，`extra_markets: [spot]` 可在同一交易所额外准入其他市场类型（`spot/linear/inverse/margin/option`，与 `market_type` 相同的项被忽略，非法值报配置错误），用于同时订阅或交易如线性永续与现货标的；见[配置指南](../guide/configuration.md)。交易所代理读取 `exchange.<name>.proxy`：URL、`no` 或留空；留空时按系统代理探测（进程内只探测一次），都没有则为 `no`，创建会话时始终显式传入。

