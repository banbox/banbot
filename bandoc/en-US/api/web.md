# web and its subpackages

web assembles HTTP services; it does not implement a second engine/account model. Development replay and live monitoring use distinct typed dependencies/lifecycles.

## Development server

web.NewDevCommandWithFactory(factory) injects the entry-owned server factory. web/dev RuntimeFactory takes context, exchangeName and market, returning data.RuntimeDeps, cleanup and error. Temporary data-tool tasks keep private state and release external dependencies according to borrowing contracts.

Development APIs use the same entry backtest preflight/execution factory. They do not rebuild unified configuration, field origins or TS/CS/mixed routing. Loading configuration is read-only; explicit editor saves use conflict checks and atomic writes. Reading old YAML never migrates production data.

## Live monitoring

web.StartApiWithRuntimeDeps(lifecycle, deps biz.RuntimeDeps) and web/live.StartApiWithRuntimeDeps bind the current task. HTTP/auth/WebSocket/order/wallet operations use instance state. The lifecycle owner stops admission then joins handlers/writers.

Slow-client WebSocket monitoring is bounded and does not block trading callbacks. Shutdown rejects new handlers and waits for admitted work. web.StartApi() is a compatibility facade; new multi-task paths use explicit dependencies.

OrderArgs, ForceExitArgs, CloseArgs, JobItem and LoginRequest belong to web/live, not the live strategy package. Routes and validation follow the actual server registration.

See [entry](entry.md), [runtime](runtime.md) and [live trading](../guide/live_trading.md).
