package execution

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

func TestDefiniteSubmitRefusalCodeSet(t *testing.T) {
	definite := []int{errs.CodeOrderRejected, errs.CodeInsufficientFunds, errs.CodeInsufficientMargin, errs.CodeReduceOnlyRejected, errs.CodePrecisionViolation, errs.CodeRiskLimit, errs.CodeOrderWouldTrigger}
	for _, code := range definite {
		err := fmt.Errorf("wrap: %w", errs.NewMsg(code, "refused"))
		if !definiteSubmitRefusal(context.Background(), err) {
			t.Errorf("code %d must be a definite refusal", code)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if definiteSubmitRefusal(ctx, err) {
			t.Errorf("code %d must stay uncertain under a cancelled context", code)
		}
	}
	uncertain := []int{errs.CodeTimeout, errs.CodeNetFail, errs.CodeExecutionUnknown, errs.CodeDuplicateRequest, errs.CodeRateLimit, errs.CodeTemporarilyBanned,
		errs.CodeServerError, errs.CodeSystemBusy, errs.CodeParamInvalid, errs.CodeExchangeError, errs.CodeCancel, errs.CodeOrderNotFound}
	for _, code := range uncertain {
		if definiteSubmitRefusal(context.Background(), errs.NewMsg(code, "ambiguous")) {
			t.Errorf("code %d must not be a definite refusal", code)
		}
	}
	if definiteSubmitRefusal(context.Background(), errors.New("plain")) || definiteSubmitRefusal(context.Background(), nil) {
		t.Error("non-SDK errors must stay uncertain")
	}
}

func TestBanexgDefiniteSubmitRejectionEndsRejectedAndDoesNotBlockSends(t *testing.T) {
	s, h, exchange, _, config := banexgFixture(t)
	adapter, err := NewBanexgAdapter(context.Background(), exchange, config)
	if err != nil {
		t.Fatal(err)
	}
	owner := &OwnerExecutor{Store: s, Handle: h, Token: h.Token(), Adapter: adapter}
	first := planOrder(t, s, "refused", 1, Buy, EntryIntent, map[string]int64{"strategy": 10})
	if err := s.PrepareOrder(context.Background(), first, 10); err != nil {
		t.Fatal(err)
	}
	exchange.create = func(string, string, string, float64, float64, map[string]any) (*banexg.Order, *errs.Error) {
		return nil, errs.NewMsg(errs.CodeInsufficientMargin, "margin is insufficient")
	}
	if err := owner.Send(first.ID, 11); err != nil {
		t.Fatal("definite refusal must settle as a rejected outcome, not an error", err)
	}
	current, err := s.Order(context.Background(), first.ID)
	if err != nil || current.State != OrderRejected || current.ExchangeID != "" || current.FilledSteps != 0 {
		t.Fatal(current, err)
	}
	if err := owner.Recover(first.ID); err != nil {
		t.Fatal("recovery must keep the rejection", err)
	}
	second := planOrder(t, s, "after-refused", 2, Buy, EntryIntent, map[string]int64{"strategy2": 10})
	if err := s.PrepareOrder(context.Background(), second, 20); err != nil {
		t.Fatal(err)
	}
	var stored StoredOrder
	exchange.create = func(symbol, kind, side string, amount, price float64, params map[string]any) (*banexg.Order, *errs.Error) {
		stored, _ = s.Order(context.Background(), second.ID)
		return &banexg.Order{ID: "venue-after", ClientOrderID: stored.ClientID, Status: banexg.OdStatusOpen}, nil
	}
	if err := owner.Send(second.ID, 21); err != nil {
		t.Fatal("rejected order blocked the next send", err)
	}
	if exchange.creates != 2 {
		t.Fatal(exchange.creates)
	}
}

func TestBanexgAmbiguousSubmitErrorStaysUnknownAndBlocks(t *testing.T) {
	for _, code := range []int{errs.CodeTimeout, errs.CodeExecutionUnknown, errs.CodeDuplicateRequest, errs.CodeServerError} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			s, h, exchange, _, config := banexgFixture(t)
			adapter, err := NewBanexgAdapter(context.Background(), exchange, config)
			if err != nil {
				t.Fatal(err)
			}
			owner := &OwnerExecutor{Store: s, Handle: h, Token: h.Token(), Adapter: adapter}
			first := planOrder(t, s, "ambiguous", 1, Buy, EntryIntent, map[string]int64{"strategy": 10})
			if err := s.PrepareOrder(context.Background(), first, 10); err != nil {
				t.Fatal(err)
			}
			exchange.create = func(string, string, string, float64, float64, map[string]any) (*banexg.Order, *errs.Error) {
				return nil, errs.NewMsg(code, "ambiguous")
			}
			if err := owner.Send(first.ID, 11); err == nil {
				t.Fatal("ambiguous outcome accepted")
			}
			current, _ := s.Order(context.Background(), first.ID)
			if current.State != OrderUnknown {
				t.Fatal(current.State)
			}
			second := planOrder(t, s, "blocked", 2, Buy, EntryIntent, map[string]int64{"strategy2": 10})
			if err := s.PrepareOrder(context.Background(), second, 20); err != nil {
				t.Fatal(err)
			}
			if err := owner.Send(second.ID, 21); err == nil || exchange.creates != 1 {
				t.Fatal("uncertain order did not block submission", err, exchange.creates)
			}
		})
	}
}

// restingOrder sends a market intent that the fake venue acknowledges as open.
func restingOrder(t *testing.T, id string) (*Store, *OwnerExecutor, *fakeBanexgSession, OrderIntent, string) {
	s, h, exchange, _, config := banexgFixture(t)
	adapter, err := NewBanexgAdapter(context.Background(), exchange, config)
	if err != nil {
		t.Fatal(err)
	}
	intent := planOrder(t, s, id, 1, Buy, EntryIntent, map[string]int64{"strategy": 10})
	if err := s.PrepareOrder(context.Background(), intent, 10); err != nil {
		t.Fatal(err)
	}
	stored, _ := s.Order(context.Background(), intent.ID)
	exchange.create = func(string, string, string, float64, float64, map[string]any) (*banexg.Order, *errs.Error) {
		return &banexg.Order{ID: "venue-" + id, ClientOrderID: stored.ClientID, Status: banexg.OdStatusOpen}, nil
	}
	owner := &OwnerExecutor{Store: s, Handle: h, Token: h.Token(), Adapter: adapter}
	if err := owner.Send(intent.ID, 11); err != nil {
		t.Fatal(err)
	}
	return s, owner, exchange, intent, stored.ClientID
}

func TestBanexgCancelOfVenueFinalOrderSettlesThroughQuery(t *testing.T) {
	for _, code := range []int{errs.CodeOrderNotFound, errs.CodeOrderNotCancelable} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			s, owner, exchange, intent, client := restingOrder(t, "gone")
			exchange.cancel = func(string, string, map[string]any) (*banexg.Order, *errs.Error) {
				return nil, errs.NewMsg(code, "order already final")
			}
			exchange.query = func(symbol, id string, _ map[string]any) (*banexg.Order, *errs.Error) {
				return &banexg.Order{ID: id, ClientOrderID: client, Symbol: symbol, Side: banexg.OdSideBuy, Amount: 1, Filled: 1, Cost: 100, Fee: &banexg.Fee{Currency: "USDT", Cost: 0.1}, Status: banexg.OdStatusFilled, LastUpdateTimestamp: 13}, nil
			}
			if err := owner.Cancel(intent.ID, 13); err != nil {
				t.Fatal("cancel of a venue-final order must not error", err)
			}
			current, err := s.Order(context.Background(), intent.ID)
			if err != nil || current.State != OrderFilled || current.FilledSteps != 10 {
				t.Fatal(current, err)
			}
		})
	}
}

func TestBanexgCancelNotFoundWithoutAuthoritativeFinalStateStaysCancelPending(t *testing.T) {
	s, owner, exchange, intent, _ := restingOrder(t, "gone-unproven")
	exchange.cancel = func(string, string, map[string]any) (*banexg.Order, *errs.Error) {
		return nil, errs.NewMsg(errs.CodeOrderNotFound, "unknown order")
	}
	exchange.query = func(string, string, map[string]any) (*banexg.Order, *errs.Error) {
		return nil, errs.NewMsg(errs.CodeDataNotFound, "history window incomplete")
	}
	if err := owner.Cancel(intent.ID, 13); err == nil {
		t.Fatal("unproven final state accepted")
	}
	current, _ := s.Order(context.Background(), intent.ID)
	if current.State != OrderCancelPending {
		t.Fatal(current.State)
	}
}

func TestBanexgCancelPendingOrderCanBeCancelledAgain(t *testing.T) {
	s, owner, exchange, intent, client := restingOrder(t, "retry-cancel")
	var cancels atomic.Int32
	exchange.cancel = func(id, _ string, _ map[string]any) (*banexg.Order, *errs.Error) {
		if cancels.Add(1) == 1 {
			return nil, errs.NewMsg(errs.CodeNetFail, "connection reset")
		}
		return &banexg.Order{ID: id, Status: banexg.OdStatusCanceled}, nil
	}
	if err := owner.Cancel(intent.ID, 13); err == nil {
		t.Fatal("transport failure not reported")
	}
	current, _ := s.Order(context.Background(), intent.ID)
	if current.State != OrderCancelPending {
		t.Fatal(current.State)
	}
	// Unconfirmed final state keeps CancelPending but still lets a later tick retry.
	exchange.query = func(symbol, id string, _ map[string]any) (*banexg.Order, *errs.Error) {
		return &banexg.Order{ID: id, ClientOrderID: client, Symbol: symbol, Side: banexg.OdSideBuy, Amount: 1, Status: banexg.OdStatusOpen}, nil
	}
	if err := owner.Cancel(intent.ID, 14); err == nil {
		t.Fatal("still-open order accepted as cancelled")
	}
	current, _ = s.Order(context.Background(), intent.ID)
	if current.State != OrderCancelPending {
		t.Fatal(current.State)
	}
	exchange.query = func(symbol, id string, _ map[string]any) (*banexg.Order, *errs.Error) {
		return &banexg.Order{ID: id, ClientOrderID: client, Symbol: symbol, Side: banexg.OdSideBuy, Amount: 1, Status: banexg.OdStatusCanceled, LastUpdateTimestamp: 15}, nil
	}
	if err := owner.Cancel(intent.ID, 15); err != nil {
		t.Fatal("CancelPending retry refused", err)
	}
	current, _ = s.Order(context.Background(), intent.ID)
	if current.State != OrderCanceled || cancels.Load() != 3 {
		t.Fatal(current.State, cancels.Load())
	}
}

func TestCancelStillRejectsUnknownAndSendingOrders(t *testing.T) {
	s, owner, _, intent, _ := restingOrder(t, "uncertain-cancel")
	if err := owner.markUncertain(intent.ID, OrderUnknown, errors.New("test")); err != nil {
		t.Fatal(err)
	}
	if err := owner.Cancel(intent.ID, 20); err == nil {
		t.Fatal("unknown order cancelled without a query")
	}
	current, _ := s.Order(context.Background(), intent.ID)
	if current.State != OrderUnknown {
		t.Fatal(current.State)
	}
}

// flakySnapshotAdapter reports a reconciliation race a fixed number of times.
type flakySnapshotAdapter struct {
	*reportFake
	cause    error
	failures atomic.Int32
}

func (a *flakySnapshotAdapter) Snapshot(ctx context.Context) (VenueSnapshot, error) {
	if a.failures.Add(-1) >= 0 {
		return VenueSnapshot{}, a.cause
	}
	return a.reportFake.Snapshot(ctx)
}

func TestReportWorkerRetriesReconcileRaceBeforeFreezing(t *testing.T) {
	old := reconcileRacePause
	reconcileRacePause = 0
	t.Cleanup(func() { reconcileRacePause = old })
	for name, cause := range map[string]error{
		"order-absent":      fmt.Errorf("%w: order x", ErrReconciliationOrderAbsent),
		"position-mismatch": ErrReconciliationPositionMismatch,
	} {
		t.Run(name, func(t *testing.T) {
			rig := newReportRig(t)
			flaky := &flakySnapshotAdapter{reportFake: rig.fake, cause: cause}
			flaky.failures.Store(2)
			rig.b.service.opts.Adapter = flaky
			rig.wake()
			rig.assertHealthy("race that clears within the bounded retries")
			if !rig.ready() {
				t.Fatal("account not reconciled after retried race")
			}
		})
	}
}

func TestReportWorkerPersistentReconcileDisagreementStillFreezes(t *testing.T) {
	old := reconcileRacePause
	reconcileRacePause = 0
	t.Cleanup(func() { reconcileRacePause = old })
	rig := newReportRig(t)
	flaky := &flakySnapshotAdapter{reportFake: rig.fake, cause: fmt.Errorf("%w: order x", ErrReconciliationOrderAbsent)}
	flaky.failures.Store(1000)
	rig.b.service.opts.Adapter = flaky
	rig.wake()
	if !rig.frozen() {
		t.Fatal("persistent disagreement must still freeze the account")
	}
	if got := 1000 - flaky.failures.Load(); got != reconcileRaceAttempts {
		t.Fatalf("expected %d bounded snapshot attempts, got %d", reconcileRaceAttempts, got)
	}
}
