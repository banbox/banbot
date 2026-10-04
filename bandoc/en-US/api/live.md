# live Package

live owns the time-series live controller. HTTP request/response types belong to web/live, account execution to execution and cross-sectional drivers to factor/runner.

## CryptoTrader construction

NewCryptoTraderWithRuntimeDeps(deps biz.RuntimeDeps, startup CryptoTraderStartupFunc) (*CryptoTrader, *errs.Error) is the explicit entry. startup is func(context.Context, *CryptoTrader) error. deps.Callbacks must provide the same task's RuntimeLifecycle; symbols, clock, exchange and callback tracking must share that owner. Missing dependencies fail rather than falling back to globals.

NewCryptoTrader() and NewCryptoTraderWith(startup) retain older embedding forms. New concurrent tasks use explicit construction. Trader uses Runtime.Accounts and its lock without mutating Config Snapshot.

## Startup and current records

entry creates Process/Runtime, binds the trader, then starts account checks, notifications, instance Web API, providers and jobs. Warmup/current input use the task clock/state. Third-party Symbol/SID identities must belong to that task; foreign identities fail explicitly.

Cron jobs are runtime-bound controller methods using configuration, clock, symbols, orders/wallets and Runtime.Cron. There are no public package-level CronRefreshPairs/CronLoadMarkets APIs. Pair refresh, loss checks, delay monitoring and order checks must not read another task's state.

## Shutdown contract

RuntimeLifecycle exposes Context, OnClose and OnCloseWait. Cancellation closes batch/provider/socket admission and waits for accepted callbacks/network listeners before trader Run returns. The entry then closes/joins Runtime and releases shared dependencies. Custom goroutines need real stop/join registration; Runtime does not automatically own all external work.

## Factors and mixed strategies

Factors use runner.NewLive, Observe/Flush and subscription generations. Mixed strategies can share an account while preserving budgets, targets and TS projections. Real execution requires verified transport, current revision/publication/funding evidence and reconciliation. Missing capabilities fail without paper fallback.

See [runtime](runtime.md), [biz](biz.md), [Web API](web.md) and the [factor guide](../guide/factor.md).
