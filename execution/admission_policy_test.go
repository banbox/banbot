package execution

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// tsCreate is a time-series create (it carries TimeSeriesMetadata) of one lot.
// The limit is below the 100 quote, so the entry rests as an open pending lot.
func admTSCreate(strategy StrategyID, lot string, orderID int64, createdMS int64, tf string) PendingCommand {
	c := pendingCreate("create-"+lot, strategy)
	c.Lot = VirtualLotID(lot)
	c.Create.QuantitySteps = 1
	c.Metadata = &TimeSeriesMetadata{OrderID: orderID, StrategyName: string(strategy), Symbol: "BTC", TimeFrame: tf, CreatedMS: createdMS}
	return c
}

func requireAdmissionRejected(t *testing.T, err error) {
	t.Helper()
	code, ok := RejectionCode(err)
	if !ok || code != RejectAdmissionLimit {
		t.Fatalf("expected %s rejection, got %v", RejectAdmissionLimit, err)
	}
}

func TestAdmissionMaxOpenOrdersAcrossStrategies(t *testing.T) {
	for _, memory := range []bool{true, false} {
		t.Run(map[bool]string{true: "memory", false: "sqlite"}[memory], func(t *testing.T) {
			b, venue := strategyService(t, memory)
			ctx := context.Background()
			if err := b.SetAdmissionPolicy(ctx, "robot-1", AdmissionPolicy{MaxOpenOrders: 2}); err != nil {
				t.Fatal(err)
			}
			if _, err := b.ApplyPendingCommand(ctx, admTSCreate("a", "a1", 1, 60_000, "1m"), 100); err != nil {
				t.Fatal(err)
			}
			if _, err := b.ApplyPendingCommand(ctx, admTSCreate("b", "b1", 2, 60_000, "1m"), 101); err != nil {
				t.Fatal(err)
			}
			refused := admTSCreate("a", "a2", 3, 60_000, "1m")
			_, err := b.ApplyPendingCommand(ctx, refused, 102)
			requireAdmissionRejected(t, err)
			// A non time-series create is outside the account TS limit.
			plain := pendingCreate("plain", "b")
			plain.Lot = "plain-lot"
			if _, err := b.ApplyPendingCommand(ctx, plain, 103); err != nil {
				t.Fatalf("non time-series create must not be admission limited: %v", err)
			}
			book, _ := b.PendingOrders(ctx)
			if len(book) != 3 {
				t.Fatalf("refusal left state behind: %d pending", len(book))
			}
			// Freeing a slot admits the very same command identity (no receipt was spent).
			cancel := PendingCommand{ID: "cancel-a1", Strategy: "a", Lot: "a1", Action: PendingCancelEntry, ExpectedRevision: 1}
			if _, err := b.ApplyPendingCommand(ctx, cancel, 104); err != nil {
				t.Fatal(err)
			}
			if _, err := b.ApplyPendingCommand(ctx, refused, 105); err != nil {
				t.Fatalf("retry after a slot freed: %v", err)
			}
			if got := venue.Metrics().Fills; got != 0 {
				t.Fatalf("resting entries reached fills: %d", got)
			}
			// Disabling the policy lifts the limit.
			if err := b.SetAdmissionPolicy(ctx, "robot-1", AdmissionPolicy{}); err != nil {
				t.Fatal(err)
			}
			if _, err := b.ApplyPendingCommand(ctx, admTSCreate("b", "b2", 4, 60_000, "1m"), 106); err != nil {
				t.Fatalf("limit not lifted: %v", err)
			}
		})
	}
}

func TestAdmissionMaxSimulOpenRoundWindow(t *testing.T) {
	b, _ := strategyService(t, true)
	ctx := context.Background()
	if err := b.SetAdmissionPolicy(ctx, "robot-1", AdmissionPolicy{MaxSimulOpen: 2}); err != nil {
		t.Fatal(err)
	}
	minute := int64(60_000)
	apply := func(strategy StrategyID, lot string, id, at int64) error {
		_, err := b.ApplyPendingCommand(ctx, admTSCreate(strategy, lot, id, at, "1m"), 100+id)
		return err
	}
	// Two different robots/strategies spend the round; the third is refused.
	if err := apply("a", "l1", 1, 10*minute+1); err != nil {
		t.Fatal(err)
	}
	if err := apply("b", "l2", 2, 10*minute+59_000); err != nil {
		t.Fatal(err)
	}
	requireAdmissionRejected(t, apply("a", "l3", 3, 10*minute+30_000))
	// The next bar round is a fresh window.
	if err := apply("a", "l3", 3, 11*minute); err != nil {
		t.Fatalf("next round refused: %v", err)
	}
	// A lagging request of an older round counts in the newest round, never reopening it.
	if err := apply("b", "l4", 4, 11*minute+1); err != nil {
		t.Fatal(err)
	}
	requireAdmissionRejected(t, apply("a", "l5", 5, 10*minute))
	status, err := b.AdmissionStatus(ctx)
	if err != nil || status.WindowStartMS != 11*minute || status.WindowEntries != 2 || status.Policy.MaxSimulOpen != 2 {
		t.Fatalf("round state: %+v %v", status, err)
	}
	// "ws" requests have no bar round.
	for n, lot := range []string{"w1", "w2", "w3"} {
		c := admTSCreate("a", lot, int64(10+n), 11*minute, "ws")
		if _, err := b.ApplyPendingCommand(ctx, c, 200+int64(n)); err != nil {
			t.Fatalf("ws entry %d: %v", n, err)
		}
	}
}

func TestAdmissionConflictingRegistrationsKeepMostRestrictive(t *testing.T) {
	b, _ := strategyService(t, true)
	ctx := context.Background()
	status := func() AdmissionStatus {
		s, err := b.AdmissionStatus(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	set := func(who string, p AdmissionPolicy) {
		if err := b.SetAdmissionPolicy(ctx, who, p); err != nil {
			t.Fatal(err)
		}
	}
	set("r1", AdmissionPolicy{MaxOpenOrders: 5, MaxSimulOpen: 3})
	set("r2", AdmissionPolicy{MaxOpenOrders: 8})
	if p := status().Policy; p != (AdmissionPolicy{MaxOpenOrders: 5, MaxSimulOpen: 3}) {
		t.Fatalf("unlimited/looser registrant widened the limit: %+v", p)
	}
	set("r2", AdmissionPolicy{MaxOpenOrders: 2, MaxSimulOpen: 4})
	if p := status().Policy; p != (AdmissionPolicy{MaxOpenOrders: 2, MaxSimulOpen: 3}) {
		t.Fatalf("most restrictive per field expected: %+v", p)
	}
	// A registrant replaces only its own entry, so a robot may raise its limit.
	set("r2", AdmissionPolicy{MaxOpenOrders: 9})
	if p := status().Policy; p != (AdmissionPolicy{MaxOpenOrders: 5, MaxSimulOpen: 3}) {
		t.Fatalf("own entry not replaced: %+v", p)
	}
	set("r1", AdmissionPolicy{})
	if s := status(); s.Policy != (AdmissionPolicy{MaxOpenOrders: 9}) || len(s.Registrants) != 1 {
		t.Fatalf("deregistration: %+v", s)
	}
	for _, bad := range []AdmissionPolicy{{MaxOpenOrders: -1}, {MaxSimulOpen: 2_000_000}} {
		if err := b.SetAdmissionPolicy(ctx, "r3", bad); err == nil {
			t.Fatalf("invalid policy accepted: %+v", bad)
		}
	}
	if err := b.SetAdmissionPolicy(ctx, " ", AdmissionPolicy{MaxOpenOrders: 1}); err == nil {
		t.Fatal("invalid registrant accepted")
	}
}

func TestAdmissionPolicyAndRoundSurviveRestart(t *testing.T) {
	b, _ := strategyService(t, false)
	ctx := context.Background()
	if err := b.SetAdmissionPolicy(ctx, "robot-1", AdmissionPolicy{MaxOpenOrders: 3, MaxSimulOpen: 1}); err != nil {
		t.Fatal(err)
	}
	minute := int64(60_000)
	if _, err := b.ApplyPendingCommand(ctx, admTSCreate("a", "a1", 1, 5*minute, "1m"), 100); err != nil {
		t.Fatal(err)
	}
	opts := b.service.opts
	key := b.service.AccountKey()
	b.Release()
	if err := b.service.Close(); err != nil {
		t.Fatal(err)
	}
	registry := &AccountRegistry{}
	owner, err := registry.Acquire(key)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewSharedAccount(owner, opts)
	if err != nil {
		t.Fatal(err)
	}
	account := service.Borrow()
	t.Cleanup(func() { account.Release(); registry.Close(); service.Close() })
	if err := account.Reconcile("restart-admission", 101); err != nil {
		t.Fatal(err)
	}
	if err := account.RegisterAccountQuotes([]Instrument{ledgerInstrument()}, func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
		return VisibleQuote{Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: now, ReceivedMS: now, ValidUntilMS: 1000, Bar: now}, nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	status, err := account.AdmissionStatus(ctx)
	if err != nil || status.Policy != (AdmissionPolicy{MaxOpenOrders: 3, MaxSimulOpen: 1}) || status.WindowStartMS != 5*minute || status.WindowEntries != 1 {
		t.Fatalf("admission state lost across restart: %+v %v", status, err)
	}
	// The spent round still refuses after the restart; a new round admits.
	_, err = account.ApplyPendingCommand(ctx, admTSCreate("b", "b1", 2, 5*minute+1, "1m"), 102)
	requireAdmissionRejected(t, err)
	if _, err := account.ApplyPendingCommand(ctx, admTSCreate("b", "b1", 2, 6*minute, "1m"), 103); err != nil {
		t.Fatal(err)
	}
}

func TestAdmissionRPCRoundTripAndTypedRejection(t *testing.T) {
	// The wire carries the new code like the other typed refusals.
	if !hubRejectCodes[RejectAdmissionLimit] {
		t.Fatal("admission_limit missing from the closed rejection set")
	}
	err := hubReplyError(hubRPCResponse{Rejected: RejectAdmissionLimit})
	if code, ok := RejectionCode(err); !ok || code != RejectAdmissionLimit {
		t.Fatalf("reply decoding: %v", err)
	}
	if !errors.Is(fmt.Errorf("wrapped: %w", err), &RejectedError{Code: RejectAdmissionLimit}) {
		t.Fatal("errors.Is on the admission code failed")
	}
	request := hubRPCRequest{Pending: &PendingCommand{Action: PendingCreate}}
	if got := hubRPCRejection("/v1/pending-command", request, rejected(RejectAdmissionLimit, "x")); got != RejectAdmissionLimit {
		t.Fatalf("create rejection not transported: %q", got)
	}
	request.Pending.Action = PendingSetExit
	if got := hubRPCRejection("/v1/pending-command", request, rejected(RejectAdmissionLimit, "x")); got != "" {
		t.Fatalf("non-create rejection must stay generic: %q", got)
	}
}
