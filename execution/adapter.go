package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type AdapterCapabilities struct {
	PostOnly              bool
	QueryClientID         bool
	AuthoritativeNotFound bool
	StableTradeID         bool
	CumulativeReports     bool
}

type SubmitReceipt struct {
	ExchangeID string
	Rejected   bool
	Fills      []FillReport
}
type QueryResult struct {
	Found         bool
	Authoritative bool
	Complete      bool // quantity/cost/fee and fills are complete, including a confirmed zero-fill order
	Receipt       SubmitReceipt
	Canceled      bool
}
type VenueSnapshot struct {
	Cash       string
	Positions  map[string]int64
	OpenOrders []QueryResult
	Fills      []FillReport
}

// ExecutionAdapter returns reports instead of invoking synchronous callbacks
// into OwnerExecutor. Implementations must respect context cancellation and a
// bounded transport timeout. They may not invent authoritative absence from a
// transient error. No exchange-specific logic belongs in this package.
type ExecutionAdapter interface {
	Capabilities() AdapterCapabilities
	Submit(context.Context, OrderIntent, string) (SubmitReceipt, error)
	Cancel(context.Context, string) (bool, error)
	Query(context.Context, string, string) (QueryResult, error)
	Snapshot(context.Context) (VenueSnapshot, error)
}

type OwnerExecutor struct {
	Context context.Context // operation/borrower cancellation; owner admission still fences every call
	Handle  *AccountHandle
	Token   OwnerToken
	Store   *Store
	Adapter ExecutionAdapter
	Timeout time.Duration // zero uses a bounded 15 second operation timeout
}

func (e *OwnerExecutor) local(call func(context.Context) error) error {
	if e == nil || e.Handle == nil || e.Store == nil || e.Adapter == nil || e.Store.key != e.Token.Key {
		return ErrOwnerToken
	}
	if err := e.Store.beginOwnerOperation(); err != nil {
		return err
	}
	defer e.Store.work.Done()
	return e.Handle.DoLocal(e.Token, func(ownerCtx context.Context) error {
		timeout := e.Timeout
		if timeout <= 0 {
			timeout = 15 * time.Second
		}
		deadline := time.Now().Add(timeout)
		if e.Context != nil {
			if callerDeadline, ok := e.Context.Deadline(); ok && callerDeadline.Before(deadline) {
				deadline = callerDeadline
			}
		}
		ctx, cancel := context.WithDeadline(ownerCtx, deadline)
		defer cancel()
		if e.Context != nil {
			stop := context.AfterFunc(e.Context, cancel)
			defer stop()
			if e.Context.Err() != nil {
				cancel()
			}
		}
		return call(ctx)
	})
}

// Send claims Prepared in a committed transaction before touching the adapter.
// Sending/Unknown/CancelPending are never resubmitted by this method.
func (e *OwnerExecutor) Send(id string, nowMS int64) error {
	return e.local(func(ctx context.Context) error {
		preview, err := e.Store.Order(ctx, id)
		if err != nil {
			return err
		}
		if preview.Intent.PostOnly && !e.Adapter.Capabilities().PostOnly {
			return errors.New("execution: adapter does not prove post-only support")
		}
		if preview.Intent.Risk != nil {
			plan, err := e.Store.Plan(ctx, preview.Intent.PlanID)
			if err != nil {
				return err
			}
			snapshot, err := e.Store.Snapshot(ctx)
			if err != nil {
				return err
			}
			if err := validatePlanReservation(snapshot, plan, *preview.Intent.Risk, nowMS); err != nil {
				return err
			}
		}
		var order StoredOrder
		err = e.Store.commit(ctx, func(tx *storeTxn) error {
			var err error
			order, err = e.Store.readOrder(ctx, tx, id)
			if err != nil {
				return err
			}
			if order.State != OrderPrepared {
				return errors.New("execution: order is not prepared for first send")
			}
			for _, allocation := range order.Intent.Allocations {
				var body string
				if err := tx.QueryRow(opReadIntentRuntime, e.Store.accountID, string(allocation.IntentID)).Scan(&body); err != nil {
					return err
				}
				var intent EligibleIntent
				if err := json.Unmarshal([]byte(body), &intent); err != nil {
					return err
				}
				available, err := intent.Evaluate(order.Intent.Observation.Price, nowMS, order.Intent.Observation.Bar)
				if err != nil || available < allocation.Steps-order.AllocationFilled[allocation.ID] {
					return errors.Join(errors.New("execution: contributor condition expired or no longer eligible at send"), err)
				}
			}
			var planBody string
			if err := tx.QueryRow(opReadPlan, e.Store.accountID, order.Intent.PlanID).Scan(&planBody); err != nil {
				return err
			}
			var plan Plan
			if err := json.Unmarshal([]byte(planBody), &plan); err != nil {
				return err
			}
			var latest int64
			if err := tx.QueryRow(opReadLatestSequence, e.Store.accountID).Scan(&latest); err != nil {
				return err
			}
			if nowMS < plan.DecisionMS || nowMS >= plan.ExpiresMS || order.Intent.Observation.ValidUntilMS <= nowMS || plan.Sequence != latest {
				return errors.New("execution: prepared intent/observation expired or superseded")
			}
			for _, prerequisite := range order.Intent.RequiresFilled {
				prior, err := e.Store.readOrder(ctx, tx, prerequisite)
				if err != nil {
					return err
				}
				if prior.State != OrderFilled {
					return errors.New("execution: preceding decrease leg is not fully filled")
				}
			}
			if order.Intent.ReduceOnly {
				actual, err := e.Store.actualPosition(tx, order.Intent.Instrument)
				if err != nil {
					return err
				}
				if actual.SignedSteps == 0 || order.Intent.Side == Buy && actual.SignedSteps > 0 || order.Intent.Side == Sell && actual.SignedSteps < 0 || order.Intent.Steps > absSteps(actual.SignedSteps) {
					return errors.New("execution: reduce-only leg does not decrease actual net")
				}
			}
			var frozen bool
			if err := tx.QueryRow(opReadAccountFrozen, e.Store.accountID).Scan(&frozen); err != nil {
				return err
			}
			if frozen {
				return errors.New("execution: frozen account cannot submit")
			}
			var uncertain int
			if err := tx.QueryRow(opCountUncertainOrders, e.Store.accountID, string(OrderUnknown), string(OrderSending), string(OrderCancelPending)).Scan(&uncertain); err != nil {
				return err
			}
			if uncertain > 0 {
				return errors.New("execution: uncertain order blocks ordinary submission")
			}
			order.Attempt++
			order.Generation = e.Token.Generation
			if _, err := tx.Exec(opUpdateOrderAttempt, order.Attempt, fmt.Sprint(order.Generation), e.Store.accountID, id); err != nil {
				return err
			}
			if err := e.Store.recordOrderAttempt(tx, OrderAttempt{OrderID: id, Number: order.Attempt, Kind: SubmitAttempt, Generation: order.Generation, AtMS: nowMS, Phase: AttemptStarted, Result: "Sending"}); err != nil {
				return err
			}
			return e.Store.setOrderState(tx, id, OrderSending)
		})
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return errors.Join(err, e.markUncertain(id, OrderUnknown, err))
		}
		order.Intent.SubmitAtMS = nowMS
		receipt, err := e.Adapter.Submit(ctx, order.Intent, order.ClientID)
		if err != nil {
			if persistErr := e.markUncertain(id, OrderUnknown, err); persistErr != nil {
				return errors.Join(err, persistErr)
			}
			return err
		}
		return e.acknowledge(id, receipt)
	})
}

func (e *OwnerExecutor) markUncertain(id string, state RealOrderState, cause error) error {
	// Recording unresolved sends survives owner context cancellation. No network
	// call occurs here; shutdown joins this local final persistence.
	return e.Store.commit(context.Background(), func(tx *storeTxn) error {
		if err := e.Store.setOrderState(tx, id, state); err != nil {
			return err
		}
		return e.Store.recordAttemptResult(tx, id, -1, cause.Error())
	})
}

func (e *OwnerExecutor) acknowledge(id string, receipt SubmitReceipt) error {
	previous, err := e.Store.Order(context.Background(), id)
	if err != nil {
		return err
	}
	fail := func(cause error) error {
		if terminalOrder(string(previous.State)) {
			return cause
		}
		return errors.Join(cause, e.markUncertain(id, OrderUnknown, cause))
	}
	if !receipt.Rejected && !canonicalID(receipt.ExchangeID) {
		cause := errors.New("execution: acknowledgement lacks exchange identity")
		return fail(cause)
	}
	err = e.Store.atomically(context.Background(), func(ctx context.Context) error {
		if err := e.Store.commit(ctx, func(tx *storeTxn) error {
			order, err := e.Store.readOrder(ctx, tx, id)
			if err != nil {
				return err
			}
			state := OrderAcknowledged
			if order.FilledSteps == order.Intent.Steps {
				state = OrderFilled
			} else if terminalOrder(string(order.State)) {
				state = order.State
			} else if receipt.Rejected {
				state = OrderRejected
			} else if order.FilledSteps > 0 {
				state = OrderPartial
			}
			if order.State == state && order.ExchangeID == receipt.ExchangeID {
				return nil
			}
			if _, err := tx.Exec(opUpdateExchangeID, receipt.ExchangeID, e.Store.accountID, id); err != nil {
				return err
			}
			if err := e.Store.setOrderState(tx, id, state); err != nil {
				return err
			}
			return e.Store.recordAttemptResult(tx, id, order.Attempt, string(state))
		}); err != nil {
			return err
		}
		for _, fill := range receipt.Fills {
			if fill.OrderID != id {
				return errors.New("execution: acknowledgement contains foreign order fill")
			}
			if _, err := e.Store.ApplyFill(ctx, fill); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fail(err)
	}
	return nil
}

// Recover first persists Sending as Unknown, then queries the stable client ID.
// Unsupported/inconclusive queries retain uncertainty. Authoritative absence
// closes a zero-fill intent as Rejected; known fills require retained history.
// Retry requires a new reviewed plan/intent.
func (e *OwnerExecutor) Recover(id string) error {
	return e.local(func(ctx context.Context) error {
		order, err := e.Store.Order(ctx, id)
		if err != nil {
			return err
		}
		// A durable zero-fill rejection without a venue identity proves no
		// order was created (explicit submit refusal or authoritative absence).
		if order.State == OrderPrepared {
			return nil
		}
		if order.FilledSteps == 0 && order.ExchangeID == "" && (order.State == OrderRejected || terminalOrder(string(order.State)) && order.Attempt == 0) {
			return nil
		}
		if order.State == OrderSending {
			if err := e.markUncertain(id, OrderUnknown, errors.New("execution: interrupted send")); err != nil {
				return err
			}
			order.State = OrderUnknown
		}
		caps := e.Adapter.Capabilities()
		if !caps.QueryClientID && order.ExchangeID == "" {
			return errors.New("execution: adapter cannot query unresolved client identity")
		}
		result, err := e.Adapter.Query(ctx, order.ClientID, order.ExchangeID)
		if err != nil {
			return err
		}
		for n := range result.Receipt.Fills {
			if result.Receipt.Fills[n].Cumulative && result.Authoritative {
				result.Receipt.Fills[n].AuthoritativeSnapshot = true
			}
		}
		if !result.Found {
			if !result.Authoritative || !caps.AuthoritativeNotFound {
				return errors.New("execution: temporary/inconclusive order absence")
			}
			if order.FilledSteps > 0 || terminalOrder(string(order.State)) && order.State != OrderRejected {
				return errors.New("execution: terminal order history absent; recovery completeness unproven")
			}
			if order.State == OrderRejected {
				return nil // repeated authoritative absence preserves the rejection
			}
			return e.Store.commit(context.Background(), func(tx *storeTxn) error { return e.Store.setOrderState(tx, id, OrderRejected) })
		}
		if !result.Complete {
			return errors.New("execution: query identity found but fill snapshot is incomplete")
		}
		if terminalOrder(string(order.State)) && (!result.Authoritative || order.ExchangeID != "" && result.Receipt.ExchangeID != order.ExchangeID || order.FilledSteps > 0 && len(result.Receipt.Fills) == 0) {
			return errors.New("execution: terminal recovery requires authoritative matching fill history")
		}
		if err := e.acknowledge(id, result.Receipt); err != nil {
			return err
		}
		current, err := e.Store.Order(ctx, id)
		if err != nil {
			return err
		}
		if current.State == OrderFilled {
			return nil
		}
		if result.Canceled {
			return e.Store.commit(context.Background(), func(tx *storeTxn) error {
				if err := e.Store.setOrderState(tx, id, OrderCanceled); err != nil {
					return err
				}
				return e.Store.recordAttemptResult(tx, id, -1, string(OrderCanceled))
			})
		}
		if order.State == OrderCancelPending {
			return e.Store.commit(context.Background(), func(tx *storeTxn) error { return e.Store.setOrderState(tx, id, OrderCancelPending) })
		}
		return nil
	})
}

// ApplyTrade handles a streamed incremental trade after recovery has switched
// an order to cumulative snapshots. Such trades only wake an authoritative
// order query; persisting the query's quantity/cost/fee highwaters prevents the
// same historical trade from being credited twice. Periodic Recover handles
// eventual query visibility when a venue snapshot temporarily trails a trade.
func (e *OwnerExecutor) ApplyTrade(report FillReport) error {
	return e.local(func(ctx context.Context) error {
		var mode string
		if err := e.Store.commit(ctx, func(tx *storeTxn) error {
			return tx.QueryRow(opReadOrderReportMode, e.Store.accountID, report.OrderID).Scan(&mode)
		}); err != nil {
			return err
		}
		if mode != "cumulative" || report.Cumulative {
			_, err := e.Store.ApplyFill(ctx, report)
			return err
		}
		order, err := e.Store.Order(ctx, report.OrderID)
		if err != nil {
			return err
		}
		result, err := e.Adapter.Query(ctx, order.ClientID, order.ExchangeID)
		if err != nil {
			return err
		}
		if !result.Found || !result.Authoritative || !result.Complete {
			return errors.New("execution: streamed trade requires authoritative cumulative normalization")
		}
		if len(result.Receipt.Fills) == 0 {
			return errors.New("execution: cumulative query lacks quantity/cost/fee snapshot")
		}
		for _, fill := range result.Receipt.Fills {
			if !fill.Cumulative || fill.OrderID != report.OrderID || !fill.Cost.IsPositive() {
				return errors.New("execution: invalid cumulative normalization receipt")
			}
			fill.AuthoritativeSnapshot = true
			if _, err := e.Store.ApplyFill(ctx, fill); err != nil {
				return err
			}
		}
		return nil
	})
}

func (e *OwnerExecutor) Cancel(id string, nowMS int64) error {
	return e.local(func(ctx context.Context) error {
		var order StoredOrder
		err := e.Store.commit(ctx, func(tx *storeTxn) error {
			var err error
			order, err = e.Store.readOrder(ctx, tx, id)
			if err != nil {
				return err
			}
			if order.State == OrderPrepared {
				return e.Store.setOrderState(tx, id, OrderCanceled)
			}
			if order.State == OrderFilled || order.State == OrderCanceled || order.State == OrderRejected {
				return nil
			}
			if order.State == OrderUnknown || order.State == OrderSending || order.State == OrderCancelPending || order.ExchangeID == "" {
				return errors.New("execution: query uncertain order before cancellation")
			}
			order.Attempt++
			if _, err := tx.Exec(opUpdateOrderAttempt, order.Attempt, fmt.Sprint(e.Token.Generation), e.Store.accountID, id); err != nil {
				return err
			}
			if err := e.Store.recordOrderAttempt(tx, OrderAttempt{OrderID: id, Number: order.Attempt, Kind: CancelAttempt, Generation: e.Token.Generation, AtMS: nowMS, Phase: AttemptStarted, Result: "CancelPending"}); err != nil {
				return err
			}
			return e.Store.setOrderState(tx, id, OrderCancelPending)
		})
		if err != nil {
			return err
		}
		if order.State == OrderPrepared || order.State == OrderFilled || order.State == OrderCanceled || order.State == OrderRejected {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return errors.Join(err, e.markUncertain(id, OrderCancelPending, err))
		}
		confirmed, err := e.Adapter.Cancel(ctx, order.ExchangeID)
		if err != nil {
			return errors.Join(err, e.markUncertain(id, OrderCancelPending, err))
		}
		if !confirmed {
			return nil
		}
		result, err := e.Adapter.Query(ctx, order.ClientID, order.ExchangeID)
		if err != nil {
			return errors.Join(err, e.markUncertain(id, OrderCancelPending, err))
		}
		if !result.Found || !result.Authoritative || !result.Complete || result.Receipt.ExchangeID != order.ExchangeID || (order.FilledSteps > 0 && len(result.Receipt.Fills) == 0) {
			cause := errors.New("execution: cancellation requires complete authoritative final fill snapshot")
			return errors.Join(cause, e.markUncertain(id, OrderCancelPending, cause))
		}
		for n := range result.Receipt.Fills {
			if result.Receipt.Fills[n].Cumulative {
				result.Receipt.Fills[n].AuthoritativeSnapshot = true
			}
		}
		if err := e.acknowledge(id, result.Receipt); err != nil {
			return errors.Join(err, e.markUncertain(id, OrderCancelPending, err))
		}
		current, err := e.Store.Order(context.Background(), id)
		if err != nil {
			return err
		}
		if current.State == OrderFilled {
			return nil
		}
		if !result.Canceled {
			cause := errors.New("execution: final cancellation state not confirmed")
			return errors.Join(cause, e.markUncertain(id, OrderCancelPending, cause))
		}
		return e.Store.commit(context.Background(), func(tx *storeTxn) error {
			if err := e.Store.setOrderState(tx, id, OrderCanceled); err != nil {
				return err
			}
			return e.Store.recordAttemptResult(tx, id, -1, string(OrderCanceled))
		})
	})
}

func (e *OwnerExecutor) ApplyFill(report FillReport) error {
	return e.local(func(ctx context.Context) error { _, err := e.Store.ApplyFill(ctx, report); return err })
}
