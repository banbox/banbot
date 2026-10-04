# exg Package

exg creates banexg sessions and binds generic capabilities/execution wrappers at task construction. Exchange-specific behavior belongs in banexg, not exchange-name branches in banbot.

## Explicit sessions

NewForRuntime(snapshot *config.Snapshot, netDisable bool) (banexg.BanExchange, *errs.Error) creates a session from the snapshot without changing exg.Default. Account selection, environment, network policy and market options follow that snapshot. The entry/external creator closes the session; Runtime.Exchange is a borrowed dependency.

Setup(), GetWith(name, market, contractType), GetLeverage/GetOdBook/GetTickers24Hr retain compatibility configuration/default-session paths. They are not isolated multi-Runtime constructors. The former GetTickers documentation name is corrected to GetTickers24Hr.

## Precision and capabilities

PrecCost(exchange, symbol, cost), PrecPrice(exchange, symbol, price) and PrecAmount(exchange, symbol, amount) return a value and *errs.Error using the explicit task session. Normalized instrument metadata validates quantity steps, contract units, price precision and venue minima.

GetAlignOffForExchangeChecked(exchange, symbol, tfSecs) returns offset/error from current market metadata. GetAlignOff(exchangeName, tfSecs) is legacy compatibility and does not establish symbol-specific alignment. Symbol parsing, order events, client-order IDs, funding and account download probe separate generic capabilities; missing capabilities fail explicitly.

BotExchange wraps/forwards the underlying session. Order callbacks, context-aware requests and timeout/Unknown recovery retain account contracts. A live_provider name supplies no transport/account evidence.

See [runtime](runtime.md), [live trading](../guide/live_trading.md) and [factor API](factor.md).
