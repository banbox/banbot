# com Package

com provides Runtime-owned market state and scheduling. Package-level price/cron helpers are compatibility facades, not shared mutable state for multiple tasks.

## Market state

MarketState combines PriceState and PairCopiedState. Prices, bar prices and historical copy progress follow the task owner, with a symbol parser bound at construction. Default parsing handles generic delimiters; venue-specific semantics come from banexg capabilities, not exchange-name branches in banbot.

Independent tasks use Runtime.Market.Prices. Legacy price helpers must not retrieve another task's current business state.

## Price access

In an embedding host, let prices := rt.Market.Prices and nowMS := rt.Clock.TimeMS(), then use:

| Call | Result |
| --- | --- |
| `prices.GetPriceSafeExpAt(nowMS, symbol, side, expMS)` | `float64` |
| `prices.GetLastBarPriceAt(symbol)` | `float64` |
| `prices.GetLastBarTimeAt(symbol)` | `int64` |
| `prices.SetPriceAt(nowMS, symbol, ask, bid)` | — |
| `prices.SetBarPriceAt(nowMS, symbol, price)` | — |
| `prices.RefreshLatestPriceAt(nowMS, rt.Exchange, symbol)` | `*errs.Error` |

Use buy/sell for side; missing or expired prices can return 0, so check before use. Latest-price refresh accesses the network and must not replace replay prices. Strategies can read the current s.Env or DataEvent in their K-line callback.

## Scheduler

Scheduler exposes AddFunc(spec, callback), Start() and Stop() context.Context. NewSchedulerWithConfig(location, lang) fixes timezone/NTP choices at construction without reading or changing process settings.

NewScheduler() and Cron() retain legacy call forms. Explicit tasks use Runtime.Cron. An owned scheduler has one owner that stops/waits; a borrowed scheduler is closed by its external owner, preserving other tasks.

See [runtime](runtime.md) and [live trading](../guide/live_trading.md).

`AddFunc(spec string, callback func()) (cron.EntryID, error)` returns registration status; check the error. Cron expressions include seconds (six fields, e.g. `0 */5 * * * *`); callbacks run after Start. An external owner stops with ctx := scheduler.Stop() and waits for <-ctx.Done(); do not wait for the same callback from inside itself.
