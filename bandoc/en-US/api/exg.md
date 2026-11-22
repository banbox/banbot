# exg Package

exg creates banexg sessions and binds generic capabilities/execution wrappers at task construction. Exchange-specific behavior belongs in banexg, not exchange-name branches in banbot.

## Explicit sessions

NewForRuntime(snapshot *config.Snapshot, netDisable bool) (banexg.BanExchange, *errs.Error) creates a session from the snapshot without changing exg.Default. Account selection, environment, network policy and market options follow that snapshot. The entry/external creator closes the session; Runtime.Exchange is a borrowed dependency.

Setup(), GetWith(name, market, contractType), GetLeverage/GetOdBook/GetTickers24Hr retain compatibility configuration/default-session paths. They are not isolated multi-Runtime constructors. The former GetTickers documentation name is corrected to GetTickers24Hr.

## Precision and capabilities

PrecCost(exchange, symbol, cost), PrecPrice(exchange, symbol, price) and PrecAmount(exchange, symbol, amount) return a value and *errs.Error using the explicit task session. Normalized instrument metadata validates quantity steps, contract units, price precision and venue minima.

GetAlignOffForExchangeChecked(exchange, symbol, tfSecs) returns offset/error from current market metadata. GetAlignOff(exchangeName, tfSecs) is legacy compatibility and does not establish symbol-specific alignment. Symbol parsing, order events, client-order IDs and account downloads have separate capabilities with different unsupported behavior.

BotExchange wraps and forwards the session. Direct orders bypassing the strategy order manager can leave strategy state inconsistent with the real account.

See [runtime](runtime.md), [live trading](../guide/live_trading.md) and [factor API](factor.md).

## Capabilities and errors

`ResolveRuntimePriceSymbol(exchange, pair)` returns `([4]string, *errs.Error)` (base/quote/settle/identifier). Adapters parse special symbols; generic symbols can fall back to delimiter parsing. `BuildClientOrderIDChecked` is strict; legacy BuildClientOrderID falls back to an old format on failure and does not prove strict validation succeeded.

`SetOrderCallback(exchange, func(*PutOrderRes) *errs.Error) bool` installs an instance callback and reports support. BotExchange calls it for both successful and failed order submissions. A callback error can replace the original error even when Order is non-nil; inspect both. Explicitly setting nil disables the legacy global AfterCreateOrder fallback.

`GetAlignOffForMarket(market *banexg.Market, tfSecs int) int` is the pure-function form that computes the offset (seconds) straight from instrument metadata. `GetAlignOffForExchangeChecked` returns an offset in seconds from instrument trading sessions; nonzero offsets apply to daily and longer periods. Unsupported account export (`StartAccountDownload`/`GetAccountDownload`) and archive URL (`BuildArchiveURL`) capabilities return errors, and no adapter currently implements them; a wrapper function does not imply exchange support. Precision helpers round values/adjust contract quantity units; they do not guarantee funds, minimum order size or a fill.

## Archive and derivatives data

Adapters may implement the following optional capabilities. `BotExchange` exposes methods of the same names, and the package-level functions take `exchange banexg.BanExchange` as the first argument. The `bool` result says whether the adapter supports the capability: `false` with a nil error means unsupported, and the caller should fall back to a normal REST fetch.

- `FetchOHLCVArchive(ctx, symbol, timeframe string, startMS, endMS int64) ([]*banexg.Kline, bool, *errs.Error)`
- `FetchPriceOHLCVArchive(ctx, priceType, symbol, timeframe string, startMS, endMS int64) ([]*banexg.Kline, bool, *errs.Error)`: `priceType` is `mark`/`index`/`premiumIndex`
- `FetchFundingRateArchive(ctx, symbol string, startMS, endMS int64) ([]*banexg.FundingRate, bool, *errs.Error)`
- `FetchDerivativesMetricsArchive(ctx, symbol, timeframe string, startMS, endMS int64) ([]*banexg.DerivativesMetric, bool, *errs.Error)`
- `FetchDerivativesMetrics(ctx, symbol, timeframe string, startMS, endMS int64, limit int, params map[string]interface{}) ([]*banexg.DerivativesMetric, bool, *errs.Error)`

```go
kl, ok, err := exg.FetchPriceOHLCVArchive(ctx, exchange, "mark", "BTC/USDT:USDT", "1h", startMS, endMS)
if err != nil { return err }
if !ok { /* adapter does not support it: fall back to a normal fetch */ }
```

## Extra markets and proxy

Besides the `exchange` root settings, `extra_markets: [spot]` admits additional market types on the same exchange (`spot/linear/inverse/margin/option`; entries equal to `market_type` are ignored and invalid values are a configuration error), for example to subscribe to or trade a spot symbol next to a linear perpetual; see the [configuration guide](../guide/configuration.md). The exchange proxy is read from `exchange.<name>.proxy`: a URL, `no`, or empty; when empty the system proxy is probed (once per process) and otherwise `no` is used, and the value is always passed explicitly when the session is created.

