# Hub: current implementation and cutover boundary

## Status

Default prod/dry-run TS, factor and mixed entries use the private Hub client
when `live_provider` is omitted, `banexg` or `hub`. `execution.hub_socket` supports
account overrides and defaults to `<datadir>/execution/hub.sock`. A missing Hub
fails closed; robots never silently fall back to a local sender. Robots own
public inputs, computation and projections, not execution accounts or private
streams. Explicit registered production embedding providers remain opt-in and
cannot be mixed with Hub accounts in one robot. Dry-run entries reject embedded
live adapters. Backtests remain in-process. Starting Hub
does not take over a running legacy sender: stop/migrate that owner first.
Default routing is implemented, but complete historical configuration parity
and real-venue production acceptance remain separate requirements.

## Commands and configuration

```
banbot hub --datadir /absolute/data
banbot hub --datadir /absolute/data --execution-socket /private/path/hub.sock
```

`spider` is a CLI alias for `hub`. The existing `RunSpiderWith` embedding API
remains a data-only compatibility entry point. Public market data still uses
`spider_addr`, default `127.0.0.1:6789`; existing data subscribers are unchanged.
The private endpoint defaults to `<datadir>/execution/hub.sock`.

Hub configuration supports `env: prod` or `env: dry_run`, not backtesting or
testnet. Production account registration requires an explicitly configured
`execution.sender_lease_dir` in the host configuration. It must resolve to the
same absolute lease directory used by the previous owner and every other
process capable of sending for that physical account. A lock in a different
directory is not a global sender fence.

Hub startup reads storage/public-market settings, but does not initialize private
accounts from its YAML. Public miners receive no YAML account credentials or SDK
options. Arbitrary exchange SDK options are deliberately not inherited; custom
endpoints/proxies from those options are not currently supported by hub.

## Private account protocol

`execution.NewHubClient(socketPath)` exposes:

- `ClaimStrategyWriter`: a stable per-session writer ID and expected epoch CAS
  claim a durable strategy epoch. Target, pending, TS-exit and policy commands
  carry this epoch and a positive, monotonically increasing strategy sequence.
  They share one highwater across command types. Retry an accepted command with
  the same identity/payload; do not issue a new sequence merely to retry a send.
- `RegisterContributor`: add a contributor to an already authenticated account,
  without resending another robot's entire account configuration. Capital comes
  only from unassigned account cash. Capital, risk cap, immutable instrument
  allowlist and receipt commit together. Instruments must already be installed
  in the account's adapter/market service; this is not dynamic symbol expansion.
- `AcceptPolicyBatch`, `PolicyEvidence`, `ResumePolicy`: atomically accept policy
  state, targets and receipts, including lost-ACK replay. Hub-observed quotes are
  frozen in the same transaction. State-only updates also require writer fencing.
- `CancelPolicyEntries`: a writer-fenced durable cancellation command. Completed
  retries never cancel a later order generation. An interrupted accepted command
  remains fenced across reconciliation/restart and requires controlled recovery;
  it is not blindly rerun. No operator completion tool is implemented yet.
- `RecoveryStatuses`: credential-free account lifecycle and outstanding
  reauthentication obligations. Known accounts are not silently forgotten on
  restart and are not reported as active before their factory succeeds.

- `Register`: `HubAccountRegistration{Account, Env, Configuration}`. The entry
  factory decodes Configuration as `entry.HubAccountConfig`: exchange name,
  explicit instrument units, strategy capital, account/strategy risk limits,
  and either production credentials or paper costs/cash.
- `RebalanceStrategy`: one strategy's absolute target patch/full update. The
  owner carries other strategies, gets its own visible quotes, and performs
  account-level netting, durable admission and sends. It never trusts the
  client's quote or send clock.
- `ResumeStrategyTarget`: restore an accepted target by its source proposal
  fingerprint, returning the latest source checkpoint. Reconnect/restart never
  recomputes an accepted quantity from a newer price or restores stale Full/Patch
  membership merely because an old command was retried.
- `Snapshot`, `ProjectionEvents`, `AdvanceProjection`: account state and
  replayable committed projections. These are request/response APIs, not yet
  server-pushed subscriptions. Consumers acknowledge only after applying events.
- `ObservePendingFeed`: durable, deduplicated feed-clock observations.
- `ApplyPendingCommand`: account-owned special-order admission with a stable
  command ID and expected per-lot revision. Supports `create`, `edit_limit`,
  `cancel_entry`, `set_protections`, `set_exit`, `edit_exit_limit` and
  `cancel_exit`. Identity, revision, pending book and
  strategy targets commit atomically. Venue cancellation and final fills settle
  before replacement targets. A retry resumes an unsent current plan, never an
  obsolete plan or an uncertain venue send.
- `PendingOrders`: authoritative pending definitions including `CommandRevision`
  for reconnecting clients. Runtime trigger/fill/native state is read-only;
  creation accepts `PendingCreateSpec`, not a caller-supplied runtime book.
- `ApplyTimeSeriesExit`: selects one strategy's TS lots inside account admission,
  consuming one quantity-unit budget across all selected lots. Supports symbol,
  timeframe, order ID, entry tag, direction, filled/unfilled selectors and close
  delay/force semantics. Cancellation final fills settle before sizing; partial
  remaining entry can resume. Metadata, revisions, acceptance events, receipt,
  pending book and target plan commit together. Retry the same command ID and
  payload after a lost ACK. This is not an all-strategy atomic batch.
- `TimeSeriesProjection`: captures snapshot, pending book, TS metadata and a
  bounded committed-event page inside account admission. An explicit terminal
  lot list retains access to closed presentation records. Reads do not advance
  the cursor; `AdvanceProjection` acknowledges successful application.

`cancel_entry` cancels the remaining entry only; it retains filled inventory and
its protection. It is not a position-close command. `edit_limit` changes only an
active entry's limit, retaining its stop/trailing latch and total quantity.
`set_protections` explicitly replaces the complete lot protection configuration
and resets its local anchors after settling any old native protection. An
explicit exit already in progress is not canceled by protection replacement.
`set_exit` cancels the remaining entry and replaces any existing exit after
settling native protection and ordinary cancellation final fills. Its size is
a reduction of final held inventory, capped at that inventory; zero requests
a full close. It never reverses a lot or resumes the canceled entry. An empty
lot accepts a full-close/reduction request as a no-op. `edit_exit_limit` retains
the original absolute goal and trigger/trailing latches; cancellation final fills
can satisfy that goal without creating a second reduction. `cancel_exit` retains
final held inventory and configured protection, but does not resume entry.
Protection may therefore trigger on a subsequent account evaluation. Exit
commands accept timestamp expiry, not quote-bar expiry. They use the lot's
existing placement policy and never accept client-authored runtime/venue state.
These per-lot commands remain distinct from `ApplyTimeSeriesExit`, which owns
cross-lot selection and budget consumption.

`PendingCommand.Metadata` now commits typed TS selector/presentation identity and
an opaque JSON payload with the pending plan. Typed identity cannot change on
edits, and short direction must agree with entry side. The payload retains the
original request and custom fields; it is not executable trigger state.
Commands do not yet implement entry quantity amendments or the complete legacy
facade. Bar expiry requires an explicit feed
identity and countdown; clients cannot introduce a quote-bar expiry clock.
Successful command acceptance is not proof of venue execution. On a transport
failure retry exactly the same ID/payload and inspect the book/snapshot; unknown
venue outcomes still require authoritative recovery, never blind resubmission.

First-time remote `StopBars` admission requires a host-provided public feed
validator. The CLI host validates timeframe and SID identity, pins and authorizes
the source before accepting the command. Unsupported clocks or an unready public
source are rejected before the pending book/target changes. Receipt retries keep
their original acceptance even when a source is temporarily unavailable. The
trusted in-process custom-feed contract remains general. New entries bind their
clock to owner acceptance time: queued older closed facts can advance older
orders without consuming a new order's lifetime, with the feed/deadline change
committed atomically.

Account registration is concurrent and idempotent for the same configuration;
inconsistent repeats fail. It initializes the physical account baseline. A
second robot can use `RegisterContributor` for its own incremental declaration
within that baseline, rather than attempting to replace account registration.

The first accepted request creates the account. Client disconnect or client
Close does not stop it. Hub shutdown cancels/joins account work and closes its
store and exchange. Live accounts have host-selected hashed ledger paths under
`<datadir>/execution`; clients cannot choose paths. Dry-run accounts use a
durable PaperAdapter and SQLite store, never a real execution adapter. An
append-only venue journal restores receipts/client IDs, resting orders,
cancellations, cash, fees, cost bases and quotes. Funding and terminal settlement
commit ledger and simulated venue changes atomically. Missing/corrupt journals
and changed initial paper cash/fees/slippage/model fail closed. Ordinary backtest
paper adapters remain memory-only. The Hub does not yet ingest simulated funding:
default dry-run entries require explicit `funding_policy: explicit-zero`, rather
than silently accepting an unimplemented `required-stream` declaration.

The CLI host persists a credential-free account manifest in
`<datadir>/execution/accounts.json` before account initialization. The directory
must be private and the manifest is mode 0600, atomically replaced and fsynced.
It contains account identities/environments, never registration configuration or
credentials. Restart loads these as `waiting_authentication`, emits an explicit
warning that local protection is NOT active, and exposes the obligations through
`RecoveryStatuses`. A robot must authenticate again with `Register`. No secure
credential provider is currently configured, so this does not promise autonomous
restoration when every robot is offline. Durable paper state is recovered after
registration; a manifest alone does not restart its public feed or protection.

Each account also has a private, credential-free `host-declaration-v1.json`
containing the original capital, risk and full instrument universe. It is
durably published before creating the venue or account. On restart a subset
robot authenticates the restored full host declaration, then joins through
atomic contributor admission; it cannot become a replacement bootstrap owner.
This preserves an offline initial robot's protection and public feed universe
when a later robot reconnects first. Existing ledgers lacking this declaration
fail closed rather than inferring ownership. Additional contributions remain
durable ledger facts; no credentials are saved in either manifest.

Rebalance retry identity and the first owner-observed effective request are
committed atomically with the account plan. Retry the same command identity and
payload after a lost ACK; never invent a new ID to resolve an uncertain send.

## Security and lifecycle

Private account traffic never enters public spider broadcasts. The Unix socket
is mode 0600; its directory must not be group/world writable. All clients running
as the same OS user are trusted to operate all registered accounts. This is not
multi-user or strategy-level authorization. No network/private TCP transport is
provided. Existing sockets are not unlinked or taken over, and shutdown only
unlinks the socket inode it created. After an unclean host crash, stale-socket
recovery requires verifying that the old owner is stopped; there is no automatic
takeover. Credential-bearing factory errors are not returned over RPC.

Recovered contributor/instrument bindings are validated before a market worker
can execute pending orders. Account-owned market subscriptions continue after
the requesting robot disconnects. Because the pinned SDK reuses mutable websocket
book pointers without an atomic snapshot contract, websocket messages are used
only as identity wake-ups; execution reads an independent REST book snapshot.
This avoids racing SDK mutations, but adds latency/request load and does NOT
guarantee capture of transient crossings between snapshots. Snapshots refresh
independently by symbol; source failure withdraws the affected quote and retries,
and a closed websocket subscription is reestablished. Transient public failures
do not themselves permanently freeze the ledger. Dormant, unexposed symbols
with unavailable quotes do not prevent healthy symbols from advancing pending
protection. Real carried exposure that cannot be valued still blocks account
risk admission: stale marks are never substituted to make protection proceed.

Hub also pins required public 1m closed-series sources independently of robot
subscriptions. Persisted immutable clock facts are delivered in per-SID order;
the saving callback only queues facts, never performs account networking.
Complete contiguous whole-minute buckets provide strategy clocks such as 5m or
1h. Incomplete first buckets, missing bars and unsupported sub-minute frames
are not presented as complete observations. Public SID/symbol identity is
validated separately for every account/instrument/feed binding. Failed account
advancement retries the already accepted fact without double-counting it.
Pins currently last for the host lifecycle; dynamic pin removal and historical
range replay over host downtime are not yet implemented/verified.

## Audit of the previous migration

Already owned by execution:

- Account ledger, netting/internal matching, durable sends, fills/recovery.
- Pending entry/exit policies, stops, trailing and exclusive-lot venue delegation.
- Volume/depth default limit pricing and proven external-trade attribution.
- Independent private-report worker and periodic order/funding reconciliation.
- Capability-gated normalized private balance and position subscriptions. Pushes only
  wake authoritative order/funding/account reconciliation, never overwrite cash
  from an incomplete push. A failed subscription or closed subscribed source
  fences admission. Trade/status hints include zero-fill normalized status events.

`factor/runner.NewHubAccountSink` supplies the same small query/command application
ports as the local sink. A remote sink has no `SharedAccountBorrow`, paper adapter,
store access or risk/market closures installed in the robot. Policy/source state
and quantity recovery cross the durable Hub contract. This constructor is tested
end to end but is not yet used by default `entry` live deployment assembly.

Still outside the required final boundary:

- `entry/time_series_execution.go` and `entry/factor_live.go` still create local
  adapters, account owners and private-report workers in the robot process.
- `biz/shared_order_mgr.go` still uses `WithState` closures to access the local
  Store, coordinate requests, commit TS/pending checkpoints and project orders.
  These closures cannot be transparently serialized or replaced with SDK RPC.
- Pure TS dry-run still selects `LocalLiveOrderMgr`; adding hub does not change
  that route. Factor and mixed routes also need an explicit local/remote seam.
- Serializable pending create/limit-edit/cancel/protection and per-lot exit
  commands, atomic TS metadata and strategy-scoped cross-lot exits now exist.
  `biz/time_series_command_bridge.go` converts defaulted strategy requests to
  serializable commands without robot-side lot selection. The local/remote
  projection port and callback delivery consumer are covered, but the complete
  `ormo.InOutOrder` hydration/event-time conversion and existing facade are NOT
  connected to them. Existing facades still use their local transaction path.
- The pinned normalized SDK has no independent `WatchOrders` API. Trade/status
  hints and balance pushes do not establish independent order-stream parity;
  position pushes also remain unwired. Hub-owned closed-series feed expiry is not wired to
  spider ingestion; forwarding observations from a robot is not equivalent.
- Spot/inverse/dated/hedge, dynamic leverage, fatal-stop windows and unproven
  native conditional/trailing scenarios retain the TS migration safety fences.

## Required before robot cutover

1. Wire the serializable TS commands into the complete facade, including durable
   order identity allocation, entry admission and default-price decisions.
   Metadata, pending definitions and target revisions must remain in the account
   owner's atomic transaction; do not introduce remote Store access.
2. Make robot projections consume snapshots/replayable committed events through
   a local/remote account interface. Route live prod and dry-run to hub; preserve
   in-process execution for backtests. No silent fallback to a robot sender.
3. Add safe SDK normalized quote snapshots and private order/balance streams,
   plus hub-owned closed-feed observations independent of robot callbacks.
4. Stop/join the previous owner, validate the shared sender lease domain and
   explicitly migrate its attributed ledger. Do not bootstrap a second empty
   ledger or infer ownership from a raw venue position.
5. Exercise two robots/netting, partial fills and lost ACKs, disconnect/restart,
   pending edits/cancel races, event replay, dry-run no-real-send and backtest
   no-hub-access end to end before calling this migration complete.

No live account operations or automatic ledger migrations are performed by the
test suite or by starting hub alone.

## TS projection delivery boundary

`biz.TimeSeriesProjectionConsumer` requires an explicit TS strategy scope.
Missing metadata for a declared TS event fails before hydration/acknowledgement;
factor contributors outside that scope are ignored. Hydration sees the latest
snapshot, which can be newer than the event page: historical callbacks must
derive their event-time state from committed payload/postings, never from the
hydrated latest order alone. The consumer is a delivery seam, not the legacy
order-view converter. New TS acceptance facts retain their submitted metadata
snapshot in the immutable event; acceptance callbacks use that snapshot rather
than a later command's overwritten exit tag or payload.

Callbacks run outside account admission. Delivery is at least once: callback
failure, a lost acknowledgement, or a later-lot callback failure replays the
unacknowledged event with stable per-event/per-strategy/per-lot IDs. Consumers
must deduplicate callback side effects durably. Restore hydrates first and then
acknowledges the captured snapshot without dispatching historical callbacks.
`biz.HubOrderMgr` now owns the robot-side remote facade. Its private journal under
`<datadir>/execution/robots` freezes commands, writer identity/sequence and
projection/callback progress; it is not an execution ledger. Keep it across
robot restarts, and do not invent new command identities to resolve lost ACKs.
Each journal has a single-process file lease. Default liquidity-based limit
prices come from the account owner's `DefaultLimitPrice` RPC, not robot-side
SDK calls. Legacy arbitrary RelayOrders imports are explicitly rejected.
Owner-wide historical order-count limits, dynamic leverage/fatal-stop, native
conditional parity and multi-leg execution groups remain compatibility work.
