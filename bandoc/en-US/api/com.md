# com Package

com provides Runtime-owned market state and scheduling. Package-level price/cron helpers are compatibility facades, not shared mutable state for multiple tasks.

## Market state

MarketState combines PriceState and PairCopiedState. Prices, bar prices and historical copy progress follow the task owner, with a symbol parser bound at construction. Default parsing handles generic delimiters; venue-specific semantics come from banexg capabilities, not exchange-name branches in banbot.

Independent tasks use Runtime.Market.Prices. Legacy price helpers must not retrieve another task's current business state.

## Scheduler

Scheduler exposes AddFunc(spec, callback), Start() and Stop() context.Context. NewSchedulerWithConfig(location, lang) fixes timezone/NTP choices at construction without reading or changing process settings.

NewScheduler() and Cron() retain legacy call forms. Explicit tasks use Runtime.Cron. An owned scheduler has one owner that stops/waits; a borrowed scheduler is closed by its external owner, preserving other tasks.

See [runtime](runtime.md) and [live trading](../guide/live_trading.md).
