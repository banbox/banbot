# Rejected orders with late fills

## Safety boundary

An authoritative not-found result can close a transmitted zero-fill order as
`Rejected`. A later fill contradicts that evidence. `Store.ApplyFill` rejects
fills for `Prepared` or `Rejected` orders; the failed transaction must not
consume the event ID, change balances, create lots, or advance the checkpoint.
This is intentional fail-closed behavior, not permission to discard the fill.

There is currently no supported operator command to reopen a rejected order.
`OwnerExecutor.Recover` also skips zero-fill rejected orders with no exchange
identity. Restarting or repeatedly calling recovery cannot repair this case.
Do not edit the SQLite order state, delete dedup rows, resubmit the intent, or
book the same fill as both an external position and a strategy fill.

## Entries excluded by the owner are not rejected orders

When the autonomous pending tick is refused with a deterministic pre-commit
code (`limit_off_tick`, `risk_limit`, `spot_short`, `venue_minimum`), the owner
may end one admitted, increasing entry that has no live venue order or
reservation, in the same commit as the plan that excludes it
(`pending_isolation.go`). The lot ends `Canceled` with a `pending-isolated:<lot>`
checkpoint and a `StrategyAcceptedEvent` carrying `Rejected`. This is not an
exchange `Rejected` order: nothing was sent, no fill can contradict it, and no
repair or account freeze follows. Unclassified errors, errors after commit,
reductions, and entries that never passed owner admission (the backtest-only
`SharedOrderMgr` writes its pending book directly) are not isolated; they fail
the private report step and the account stays frozen until reconciled.

Transient venue read failures (network, timeout, rate limit, temporary ban,
overload, indeterminate result, deadline, account depth not yet visible) are not
failures of that step: the worker skips the tick, the account stays unready and
refuses new entries (`account_not_ready`) until a reconciliation succeeds, and no
freeze or repair follows. A freeze logs `account frozen by the private report worker`
with its reason and error.

## OCO siblings cancelled by the owner are not rejected orders

An OCO entry group member cancelled by the owner (`pending_oco.go`, reason
`oco_simultaneous` or `oco_filled_sibling`) ends `Canceled` in the same commit
as the plan, with a `pending-oco:<lot>` checkpoint and a `StrategyAcceptedEvent`
carrying `Rejected`. Nothing was sent for it, no fill can contradict it and no
repair follows. Members with fills, live venue orders, reservations or exits
are never cancelled.

## Incident procedure

1. Stop new strategy submissions for the affected physical account and stop
   every process that can submit for it. Preserve the account freeze and sender
   ownership boundary. A direct `Store.ApplyFill` error alone does not freeze
   the account; callers must stop admission explicitly. The managed private
   report service records report/recovery failures through `FreezeFailure`.
2. After all writers stop and join, preserve a consistent backup of the
   execution database, including any SQLite WAL sidecars. Keep the original
   database unchanged. Save redacted logs and the failed fill payload.
3. Collect venue evidence through the adapter: account and settlement domain,
   instrument/version, client ID, exchange order ID, trade IDs, event times,
   executed quantity, cost, fees/currencies, final order state, and the original
   authoritative-absence response and observation time. Obtain complete trade
   history as well as the final order snapshot; a single stream event is not
   proof of a complete cumulative snapshot.
4. Compare the evidence with `Store.Order`, `Store.OrderAttempts`,
   `Store.Snapshot`, and committed events. Check for another local order with
   the same venue identity and for replacement orders already sent after the
   rejection. Reconcile venue cash, positions and all active/conditional orders.
   Identify whether the adapter incorrectly asserted authoritative absence.
5. Escalate the evidence and backup for an audited repair. Remain stopped if
   attribution or history completeness is uncertain. If immediate venue risk
   reduction is necessary, use the account's established manual risk procedure,
   record its orders/trades separately, and include them in the repair evidence;
   do not launch a second bot sender while the owner is active.

## Repair acceptance criteria

A future controlled reopen operation must validate the account owner, original
submission attempt, immutable allocations and exact client/exchange identity;
retain the original rejection evidence; append a separately authorized repair
event; and replay complete cumulative quantity/cost/fee highwaters atomically
with fill dedup, lots and cash. It must reject a different order identity,
overfills, conflicting replays, already-attributed fills and insufficient
history. Test restart/replay and rollback on every supported storage backend.

Validate the repair on a backup with no network submissions first. Resume only
after cash, position, attribution and active-order reconciliation pass, the
adapter's absence contract is corrected if necessary, and the account's normal
recovery/reconciliation gates explicitly release admission. A zero unexplained
cash difference alone is not sufficient evidence to clear a risk freeze.
