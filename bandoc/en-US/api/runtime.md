# runtime Package

runtime assembles task state and resource borrowing. Context carries cancellation, deadlines and I/O lifecycles; concrete state travels through fields, receivers and RuntimeDeps, not Context.Value service lookup.

## Process and Runtime ownership

Process owns account registries/shared services, construction/shutdown coordination, scheduler claims and storage-identity SID allocators/registries. One Process can create several Runtimes; an account owner controls physical send authority. This does not authorize arbitrary concurrent sharing of mutable external services.

Runtime owns Core, Clock, Market, Symbols, Batch, Strategies, Orders, Trading, Catalog, Notifications and optional FactorState. Config is a read-only Snapshot; Accounts is separately mutable execution configuration coordinated by the same AccountsMu. Storage/Exchange fields are dependencies, not ownership claims.

## Construction and typed dependencies

- NewProcess() *Process constructs the coordination owner.
- (*Process).NewRuntime(opts Options) (*Runtime, error) creates a task and releases newly acquired resources/claims on construction failure.
- Runtime.BizDeps() projects biz.RuntimeDeps so traders, providers, wallets, reports and filters use the same owner's concrete state.
- Factor installation uses runner.CloneConfig: configuration containers are copied while Plan/ComputationGroup and callbacks remain borrowed.

Embedding should reuse normal entry assembly. Manual construction needs consistent configuration, clock, symbols, storage, exchange, callback owner and accounts; missing fields must not fall back to package globals.

## Stop, Close and Join

`Stop` closes admission and propagates cancellation only; it does not fully release resources. The resource owner requests shutdown with `Close`, then waits with `Join`. `Join` is a no-op until `Close` is requested. Calling `Close` inside a callback completes shutdown asynchronously; an external owner must call `Join` to avoid waiting on that callback itself. `Close` coordinates stop, registered work, reset and borrow release. `OnClose` registers stop work and `OnCloseWait` registers join work. Application goroutines are included only through callback/lifecycle registration. Web/RPC servers have their own Stop/Join contracts, distinct from Runtime shutdown.

Process.Close waits for tasks/construction and releases shared accounts/registries; the entry then closes its own Storage/Exchange. Borrowed schedulers are not stopped. Owned schedulers have one ownership claim. Releasing one account borrower must preserve other strategies' physical-account service.

## Compatibility and validation

Domain packages retain compatibility facades, but runtime exposes no WithLegacy/LockLegacy. runtimeplan.Inspect uses local state without installing globals. New business paths use explicit deps; legacy helpers provide no general multi-task isolation guarantee.

See [entry](entry.md), [biz](biz.md), [com](com.md), [rpc](rpc.md), [web](web.md) and [factor](factor.md). Local/default/race tests do not certify real database/venue sessions.
