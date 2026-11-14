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

`GetAlignOffForExchangeChecked` returns an offset in seconds from instrument trading sessions; nonzero offsets apply to daily and longer periods. Unsupported account export and archive URL capabilities return errors; a wrapper function does not imply exchange support. Precision helpers round values/adjust contract quantity units; they do not guarantee funds, minimum order size or a fill.
