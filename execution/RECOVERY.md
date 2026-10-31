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
