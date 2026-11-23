# Time-series execution migration

## Hub process boundary

See [HUB.md](HUB.md) for the request-owned account host, private Unix RPC,
`hub` command (`spider` compatibility alias), and the current cutover checklist.
Default prod/dry-run TS, factor and mixed entries now use remote execution.
`biz.HubOrderMgr` retains strategy-facing commands and committed projections
without borrowing a Store/SharedAccount or owning a private adapter. Its private
robot journal freezes request sizing and writer sequence across lost ACK/restart.
A missing Hub fails closed, never selecting the old local manager. Explicit
embedding providers remain opt-in; backtests continue in-process. This default
route does not establish parity for every historical configuration.

Execution now exposes serializable `PendingCommand` admission over the private
hub RPC for create, entry-limit edit, remaining-entry cancellation, full
protection replacement and per-lot exit admission/limit-edit/cancellation.
Per-lot revisions are returned by `PendingOrders`.
Command receipts/revisions, book changes and target plans commit atomically;
current unsent plans resume on retry, superseded plans do not resurrect, and
unknown sends remain recovery-fenced. These commands now back the remote TS
facade, including its immutable metadata and event-time callbacks.
Exit commands settle cancellation final fills before sizing a reduction; edits
retain an absolute goal rather than applying the original reduction again.
Per-lot explicit exits cancel remaining entry permanently. The separate
`ApplyTimeSeriesExit` command now implements strategy-scoped cross-lot selection,
quantity-unit budget consumption and partial remaining-entry resume inside the
account owner. TS metadata and acceptance events commit with its plan. The remote
facade uses this command instead of sizing exits against robot-local fills.
Native final fills preserve/clamp an active explicit exit goal in the durable
book, including when the process reloads before an in-memory sync can run.
Normalized balance pushes now wake authoritative account reconciliation; the
pinned SDK still lacks an independent normalized order subscription.

## Completion boundary

The migration is not complete until every enabled legacy scenario has an
equivalent execution-owned implementation and regression coverage. Removing a
legacy constructor, rejecting its configuration, or changing a native stop into
a software stop does not establish parity.

`SharedOrderMgr` is the backtest-only strategy-facing request/projection adapter
(mixed factor + time-series replay); live and dry-run accounts, including the
time-series part of a mixed account, use `HubOrderMgr`. It does not send SDK orders. The execution account owner owns submission,
stable client IDs, durable attempts, fills, balances and reconciliation.
Pending entries, exits, protection checkpoints and placement policy now live in
`execution/pending_*.go`. The account-owned registry is committed atomically with
accepted plans. Private-report recovery and the account's periodic worker advance
it independently of strategy callbacks. `biz/shared_order_mgr.go` converts legacy
requests and hydrates projections from that authoritative registry.

Feed-clock entry expiry is also execution state (`feeds-v1` plus a per-entry
deadline). Data adapters forward closed, non-warmup observations through
`ObservePendingFeed`; duplicate and out-of-order observations do not consume
bars. Observations persist before quote collection, so quote or cancellation
failures cannot lose expiry progress. The account tick consumes the stored
deadline without a strategy facade. A stopped feed does not invent elapsed bars.
Startup upgrades both facade-only checkpoints and existing pending books without
resetting newer execution counters, deadlines or trigger state.

## Order placement

```yaml
execution:
  order_mode: local           # default; alternative: exchange_first
  near_limit_bps: '10'        # zero/omitted uses 10 basis points
  protection_mode: native_preferred # optional; software forces local protection
```

Account overrides use the same fields. These are additional fields, not a
replacement for durable store, instrument, capital, funding and risk declarations.
`protection_mode` accepts `software` and `native_preferred`. Omission follows
`order_mode`: local uses software, exchange-first prefers proven native protection.
A known unsupported capability falls back visibly to local protection; uncertain
submission/query/cancellation stops the round and never enables a duplicate local exit.

Local mode retains distant limits and all dormant special orders locally.
Strictly executable opposing strategy deltas can match internally, then the net
residual goes to the venue. A limit within the proximity threshold can rest at the
venue only when one economic lot owns that instrument. Exchange-first removes
the proximity requirement for such isolated limits. Multiple lots of the same
strategy also count as independent contributors. A new pending/factor contributor
revokes incompatible delegation; cancellation and its final fills settle before
replacement targets are built. Venue placement never permits an internal fill
outside the original limit or before a local stop activates.

Native fixed SL/TP reservations use immutable plan membership and the same
durable sender, client ID, recovery and trade ledger as ordinary orders. Dormant
remaining quantities do not subtract held exposure. Only one protection per
exclusive lot delegates; local siblings cancel it and settle final fills before
reducing. New contributors and explicit exits revoke that delegation.

The pinned banexg SDK cannot prove complete native conditional recovery or the
existing midpoint trigger semantics, so its adapter currently reports unsupported
and uses visible local fallback. Native trailing remains local until recovered
anchor semantics are proven. `Info["execution_protection"]` and the pending book
expose requested/effective placement plus the reason; native support in the core
does not imply that an unverified live venue is enabled.

Production legacy manager construction and unconfigured production TS startup
are now disabled. Unsupported scenarios fail explicitly instead of falling back
to the retired sender. This safety fence does not mean those scenarios have
been migrated. The retired legacy live order manager (`LiveOrderMgr`,
`LocalLiveOrderMgr`) has been deleted; the supported/refused/ignored matrix and
the migration notes per feature are in `doc/project/05_biz_orders_live.md`.

## Scenario acceptance matrix

| Legacy scenario | New-path regression coverage | Status |
| --- | --- | --- |
| Market, limit, maker; defaults; edits; relay; force exit; callback replay | `runtime/shared_capabilities_test.go` | Existing shared coverage |
| Partial entry, cancel/fill race, remaining size, selector semantics | `runtime/shared_partial_test.go` | Existing shared coverage |
| Global exit quantity consumed once across lots | `runtime/shared_exit_budget_test.go` | Existing shared coverage |
| Software SL/TP, relative levels, partial reduction and trailing | `execution/protection_test.go`, `runtime/shared_trigger_test.go` | Execution policy and integration coverage |
| Stop-limit latch and trailing anchor after restart/import | `runtime/shared_migration_semantics_test.go` | Existing shared coverage |
| Pending entry expiry across feeds and restart | `runtime/shared_feed_expiry_test.go` | Existing shared coverage |
| Environment end / adjustment / main-contract switch | `runtime/shared_env_end_test.go` | Exit symbol across timeframes, preserve close delay, cancel pending, final-fill race |
| Lost ACK, authoritative query, canceled late fill, fee correction | `execution/recovery_contract_test.go`, `execution/cancel_final_snapshot_test.go` | Existing execution coverage |
| Rejected order followed by contradictory late fill | `execution/rejected_late_fill_test.go`, `execution/RECOVERY.md` | Fail closed; audited recovery required |
| Attributed legacy positions/orders | `runtime/shared_migration_test.go`, `runtime/shared_migration_exit_test.go` | Explicit stopped-owner import, not automatic attribution |
| Same-time ordinary and special funding, repeated recovery | `execution/banexg_funding_test.go` | Separate IDs, idempotent recovery |
| CLI close of attributed strategy orders | `runtime/shared_close_tool_test.go`, `live/tools_test.go` | Shared owner cancellation/reduction; no raw account-wide SDK cancellation |
| Native fixed SL/TP lifecycle | `execution/conditional_test.go`, `execution/pending_native_test.go` | Capability-gated core; pinned SDK falls back visibly; trailing anchor recovery remains unsupported |
| Spot, inverse, dated and hedge-mode venue positions | Adapter/instrument validation currently rejects these | Not migrated; do not reinterpret as net linear positions |
| Leverage | `execution/banexg_leverage_test.go`, `entry/hub_test.go`, `entry/time_series_execution_test.go` | Fixed per-instrument leverage declared at account registration and set at the venue by the Hub owner before any order; requires `margin_rate >= 1/leverage`. Per-order leverage, venue leverage caps by notional tier and proportional resizing are not migrated and stay rejected. Real-venue acceptance of the SetLeverage call is still required |
| Order-book/volume-based default limit selection | `execution/limit_price_test.go`, `execution/banexg_limit_price_test.go`, `runtime/shared_order_mode_test.go` | Production SDK binding uses closed-minute volume, current depth and tick rounding; missing liquidity fails closed |
| Default local / isolated exchange-first limits | `runtime/shared_order_mode_test.go`, `execution/pending_revocation_test.go` | Account-scoped admission and cancel/final-fill transitions, including factor arrivals |
| Account-owned pending execution after restart | `execution/pending_registry_test.go` | Durable source state, entry/protection execution without biz callbacks |
| Execution-owned feed expiry | `execution/pending_feed_test.go`, `execution/pending_feed_upgrade_test.go`, `runtime/shared_feed_autonomous_test.go`, `runtime/shared_feed_upgrade_test.go`, `runtime/shared_feed_expiry_test.go` | Durable feed identity/countdown, replay deduplication, independent observation and expiry; old checkpoints upgraded without trading |
| Serializable pending commands / hub RPC | `execution/pending_command_test.go`, `execution/pending_command_exit_test.go`, `execution/pending_command_native_test.go`, `execution/hub_pending_test.go` | Atomic admission, per-lot exit/edit/cancel, cross-strategy preservation, revisions, final cancellation fills (ordinary/native), persisted explicit exit goals, replay/resume and uncertain-send fences; TS facade cutover still pending |
| Atomic TS metadata and cross-lot exit commands | `execution/time_series_command_test.go`, `execution/hub_time_series_test.go`, `biz/time_series_command_bridge_test.go` | Memory/SQLite atomic metadata, selector and global budget, different quantity steps, close delay, partial entry cancel/resume, cancel final fill, concurrent RPC replay and other-strategy isolation; existing facade not switched |
| Serializable TS projection and callback delivery | `execution/time_series_projection_test.go`, `biz/time_series_projection_test.go` | Local/remote page parity, terminal metadata, replay/ack and multi-lot callback failures; full legacy order-view/event-time conversion remains pending |
| Private balance and zero-fill status wakeups | `execution/private_balance_stream_test.go` | Capability-gated balance subscription, authoritative accounting, closure/setup cancellation and failure freezes; independent normalized order stream still unavailable in pinned SDK |
| Explicit external book takeover | `execution/external_attribution_test.go` | Atomic attributed inventory/economics transfer; requires an explicit destination and subsequent reconciliation |
| Automatic external trade discovery and takeover | `execution/banexg_external_test.go`, `execution/auto_takeover_test.go`, `runtime/hub_takeover_test.go` | Configured `take_over_strat`; only complete terminal-order trade history and provable opening/additions enter fresh lots; unknown active orders, ambiguous reductions and missing basis remain frozen |
| Take-profit ladders and OCO entry groups on the Hub path | `runtime/hub_ladder_oco_test.go`, `runtime/hub_ladder_review_test.go`, `biz/hub_order_ladder_test.go`, `biz/hub_order_ladder_review_test.go` | Robot-side orchestration over existing owner commands (`set_protections`, `cancel_entry`) with journaled `Plans`/`Followups`, retry/abandon/back-off, tiny-rung merging and break-even reconciliation. The owner itself resolves OCO groups before coordination (`execution/pending_oco.go`, `runtime/hub_oco_owner_test.go`): of several members executable in the same tick only the earliest-created survives (`oco_simultaneous`), and once a member has a fill its open unfilled siblings are cancelled (`oco_filled_sibling`); members with fills, live orders, reservations or exits are never cancelled (several filled members all stay, with a WARN). Known differences from in-process `SharedOrderMgr`: `set_protections` resets the owner's trailing anchor; `Val`/`Rate` rungs are exact only after the entry fill is observed; a ladder or stop exit carries no rung exit tag in the Hub view |
| Owner refusals of new entries on the Hub path (price tick, risk budget, spot short, self-cross, venue minimum) | `execution/hub_rejection_test.go`, `runtime/hub_rejection_test.go` | The owner returns a closed fixed code only for `create`; the robot drops the entry with a warning, keeps running and does not consume the bar's open-order capacity. Exits, edits and protection changes stay fail-closed. Order prices are tick-checked at admission; trigger levels are not |
| Opposite deltas of one strategy and refusal isolation in the autonomous tick | `execution/pending_sequence_test.go`, `execution/pending_isolation_test.go` | Reductions execute and the opposite increase waits one tick; an admitted, increasing, not-yet-working entry refused deterministically is canceled (audited) instead of freezing the account. Embedded `SharedOrderMgr` entries are not admitted by the owner and are not isolated |
| Account metrics of factor-only Hub robots and time-series Hub robots | `runtime/hub_account_reader_test.go` | Read-only cached view (1 s TTL, 3 s timeout); metrics inside sub-second callbacks may be up to 1 s old |
| Fill and status notifications, batch callbacks, strategy shutdown hooks | `runtime/order_notify_test.go`, `runtime/live_hooks_test.go` | Execution-backed runtimes: asynchronous bounded notification queue (drop on overflow), batch firing after `core.DelayBatchMS`, `OnShutDown`/`OnStratExit` at shutdown; per-order `strat_perf` score updates run in `runtime/perf_update.go` (`runtime/perf_update_test.go`): bounded per-job ring of `max_od_num` closed orders, deduplicated by lot, seeded from archived rows on the Hub path only (`SharedOrderMgr` does not archive, so its ring starts empty after a restart), `strat_perfs.yml` dumped in live mode, inert when `strat_perf` is disabled |
| Hub-facing facade for an embedded production time-series robot (the only facade; the former `execution.ts_facade` key was removed) | `execution/hub_port_test.go`, `execution/hub_error_observer_test.go`, `runtime/hub_port_parity_test.go`, `runtime/embedded_hub_facade_test.go`, `entry/ts_facade_test.go` | `HubOrderMgr` over an in-process `LocalHubPort` (same JSON round trip, error mapping and size limits as the socket) onto the robot's own `SharedAccount` through `EmbeddedHub`; used by every prod embedded time-series account with a custom `live_provider`; `ts_facade` is rejected at config load (`entry/ts_facade_test.go`) and `SharedOrderMgr` is no longer built for that path (`entry/ts_facade_default_test.go`); the first start without a journal imports the `__legacy_bridge` checkpoint once and leaves the legacy data untouched (`biz/hub_order_legacy_import.go`, `runtime/hub_legacy_migration_test.go`); `take_over_strat` is supported; journal `execution/robots/ts-local-<hash>.json` |
| Fatal-stop loss windows | `execution/loss_stop_test.go`, `biz/ts_loss_stop_test.go` | Account-owned equity loss breaker (`LossStopPolicy`): realized results, fees, funding and floating losses count; durable pause shared by all robots; robot-side admission enforces it before a command is frozen. Measured on equity change, not on closed-order profit, so values differ from the legacy cron |

## Cutover safety

1. Declare durable store, sender lease domain, instruments, account risk limits,
   strategy capital and visible-quote inputs explicitly. Do not synthesize
   balances or assign existing account inventory to a strategy by symbol.
2. Reject unsupported account modes and venue capabilities before any sender
   starts. An explicitly configured execution startup failure must never fall
   back to the legacy sender.
3. Stop and join the old runtime before importing its attributed snapshot.
   Revalidate venue position/order/cash highwaters at the atomic import gates.
4. Keep native protection and ambiguous external inventory fenced until their
   ownership, recovery and account-level risk contracts are implemented.
5. Keep unsupported production configurations disabled until the remaining
   rows have equivalent scenario tests. Delete reference implementations only
   after parity is established; do not delete examples to make the suite pass.

The generic `trade-close` runtime currently has no shared account binding. It
therefore returns an explicit production migration error rather than invoking
the raw venue closer. In an already-bound shared runtime, the close service
supports attributed strategy orders, but rejects raw external-position closing.

This document records implementation and validation boundaries; it is not an
operator authorization to migrate a live account or rewrite its ledger.

## Automatic External Takeover

The existing `take_over_strat` selects the destination strategy. Each proven
external opening/addition is transferred atomically into a fresh independent lot
with its recorded basis, fees, funding and provenance; it is not merged into an
existing strategy lot. Unknown private reports are hints only: execution fetches
a complete terminal order and validates stable trade IDs, quantity, cost and fees
before accepting its batch. Replays are idempotent and historical trades at or
before the bootstrap baseline cannot be counted again. Raw position snapshots
never supply missing acquisition history. Automatic adoption does not itself
unfreeze the account: authoritative reconciliation must succeed afterward.
