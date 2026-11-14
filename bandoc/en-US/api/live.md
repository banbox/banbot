# live Package

live owns the time-series live controller. HTTP request/response types belong to web/live, account execution to execution and cross-sectional drivers to factor/runner.

## CLI entry and execution ownership

Ordinary `trade` in `prod`/`dry_run` requires explicit execution declarations. An omitted provider, `banexg` or `hub` selects Hub. `hub` (alias `spider`) starts public data and a private Unix socket. Robots register accounts and consume committed projections without owning private account streams or sender authority. A missing Hub fails without local fallback. The old direct live-sender branch in `runExplicitTradeSpecContext` is disabled for prod/dry_run; using a legacy constructor cannot restore it.

The default path supports linear perpetuals with one settlement currency. Time-series preflight requires positive capital, instrument units, durable execution declarations, sender leases and positive risk limits. Unmigrated legacy `fatal_stop` and dynamic `leverage` are rejected. Production uses `required-stream`; live paper uses `env: dry_run` and `explicit-zero`. Factor-containing `trade --dry-run` instead runs historical events replay; time-series-only entries reject that flag.

Hub account lifetime is independent of robots. Robot Stop/Join closes its own borrow and callbacks; Hub continues committed pending orders/protection. After restart Hub restores credential-free identities and awaits registration authentication, with local protection inactive in the meantime. See [Shared accounts](../guide/execution.md) for operation and failure boundaries.

## CryptoTrader construction

NewCryptoTraderWithRuntimeDeps(deps biz.RuntimeDeps, startup CryptoTraderStartupFunc) (*CryptoTrader, *errs.Error) is the explicit entry. startup is func(context.Context, *CryptoTrader) error. deps.Callbacks must provide the same task's RuntimeLifecycle; symbols, clock, exchange and callback tracking must share that owner. Missing dependencies fail rather than falling back to globals.

NewCryptoTrader() and NewCryptoTraderWith(startup) retain older embedding forms. New concurrent tasks use explicit construction. Trader uses Runtime.Accounts and its lock without mutating Config Snapshot.

## Startup and current records

entry creates Process/Runtime, binds the trader and starts notifications, instance Web API, providers and jobs. On the default path, Hub owns account recovery, private reports and reconciliation. CryptoTrader account-check errors can disable an account in process; withdrawal permission or unrestricted IP access only produces warnings. These checks do not replace Hub reconciliation. Warmup/current input use the task clock/state. Third-party Symbol/SID identities must belong to that task; foreign identities fail explicitly.

Cron jobs are runtime-bound controller methods using configuration, clock, symbols, orders/wallets and Runtime.Cron. There are no public package-level CronRefreshPairs/CronLoadMarkets APIs. Pair refresh, loss checks, delay monitoring and order checks must not read another task's state.

## Shutdown contract

RuntimeLifecycle exposes Context, OnClose and OnCloseWait. Cancellation closes batch/provider/socket admission and waits for accepted callbacks/network listeners before trader Run returns. The entry then closes/joins Runtime and releases shared dependencies. Custom goroutines need real stop/join registration; Runtime does not automatically own all external work.

## Factors and mixed strategies

Factors use runner.NewLive, Observe/Flush/FlushAt and subscription generations with a caller-provided clock. Mixed strategies can share an account while preserving budgets, targets and TS projections. Real execution requires verified transport, current revision/publication/funding evidence and reconciliation. Missing capabilities fail without paper fallback.

See [runtime](runtime.md), [biz](biz.md), [Web API](web.md) and the [factor guide](../guide/factor.md).
