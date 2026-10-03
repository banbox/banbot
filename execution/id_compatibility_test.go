package execution

import (
	"context"
	"testing"

	"github.com/shopspring/decimal"
)

func TestFramedExecutionIDsSeparateValidSlashTuplesAndTypes(t *testing.T) {
	if legacyRebalanceID("a/b", "/", "c") == legacyRebalanceID("a", "/", "b/c") {
		if rebalanceID("a/b", "/", "c") == rebalanceID("a", "/", "b/c") {
			t.Fatal("slash tuples collide")
		}
	} else {
		t.Fatal("fixture does not reproduce old collision")
	}
	if rebalanceID(int64(1)) == rebalanceID("1") || rebalanceID("ab", "c") == rebalanceID("a", "bc") {
		t.Fatal("typed component boundaries collide")
	}
}

func TestAcceptedLegacyRetryAndNewCollisionRemainDistinct(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore"} {
		t.Run(backend, func(t *testing.T) {
			store, _, _, _ := testStore(t)
			ctx := context.Background()
			old := StrategyAcceptedEvent{Strategy: "a/b", Lot: "c", Kind: EntryIntent, CommandID: "command", AtMS: 10}
			other := old
			other.Strategy, other.Lot = "a", "b/c"
			body, _ := payload(old)
			oldID := "accepted-" + legacyRebalanceID(old.Strategy, "/", old.Lot, "/", old.Kind, "/", old.CommandID)
			if err := store.commit(ctx, func(tx *storeTxn) error {
				if _, err := store.recordEvent(tx, oldID, "StrategyAccepted", body); err != nil {
					return err
				}
				return store.commitAccountEvent(tx, oldID, decimal.Zero, nil)
			}); err != nil {
				t.Fatal(err)
			}
			checkpoint := StrategyCheckpoint{Strategy: "ts", Name: "acceptance", Payload: []byte(`{}`), Events: []StrategyAcceptedEvent{old, other}}
			for range 2 {
				if err := store.saveAcceptedCheckpoint(ctx, checkpoint); err != nil {
					t.Fatal(err)
				}
			}
			events, err := store.EventsAfter(ctx, 0, 10)
			if err != nil || len(events) != 2 || events[0].ID != oldID || events[0].ID == events[1].ID {
				t.Fatal("legacy retry duplicated or distinct command collided", events, err)
			}
			changed := old
			changed.AtMS++
			checkpoint.Events = []StrategyAcceptedEvent{changed}
			if err := store.saveAcceptedCheckpoint(ctx, checkpoint); err == nil {
				t.Fatal("legacy command identity accepted changed timestamp")
			}
		})
	}
}

func TestReopenPreservesOldAttemptDedupAndStableClientID(t *testing.T) {
	store, _, owner, path := testStore(t)
	ctx := context.Background()
	order := planOrder(t, store, "old/attempt", 1, Buy, EntryIntent, map[string]int64{"a": 10})
	stored, err := store.Order(ctx, order.ID)
	if err != nil {
		t.Fatal(err)
	}
	started := OrderAttempt{OrderID: order.ID, Number: 1, Kind: SubmitAttempt, Generation: owner.Token().Generation, AtMS: 11, Phase: AttemptStarted, Result: "Sending"}
	if err := store.commit(ctx, func(tx *storeTxn) error {
		if _, err := tx.Exec(opUpdateOrderAttempt, 1, "1", store.accountID, order.ID); err != nil {
			return err
		}
		if err := store.setOrderState(tx, order.ID, OrderUnknown); err != nil {
			return err
		}
		for _, phase := range []AttemptPhase{AttemptStarted, AttemptResult} {
			attempt := started
			attempt.Phase = phase
			if phase == AttemptResult {
				attempt.Result = "Acknowledged"
			}
			body, _ := payload(attempt)
			id := store.legacyAttemptIdentity(order.ID, 1, phase)
			if phase == AttemptResult {
				id += "-" + legacyRebalanceID(body)
			}
			if _, err := store.recordEvent(tx, id, "OrderAttempt", body); err != nil {
				return err
			}
			if err := store.commitAccountEvent(tx, id, decimal.Zero, nil); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	store.Close()
	reopened, err := OpenStore(path, store.key)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	adapter := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true}}
	adapter.query = func(_ context.Context, clientID, _ string) (QueryResult, error) {
		if clientID != stored.ClientID {
			t.Fatal("retry changed persisted client ID", clientID, stored.ClientID)
		}
		return QueryResult{Found: true, Authoritative: true, Complete: true, Receipt: SubmitReceipt{ExchangeID: "ack"}}, nil
	}
	if err := executorFor(reopened, owner, adapter).Recover(order.ID); err != nil {
		t.Fatal(err)
	}
	attempts, err := reopened.OrderAttempts(ctx, order.ID)
	if err != nil || len(attempts) != 2 {
		t.Fatal("old result replay added duplicate audit", attempts, err)
	}
	current, err := reopened.Order(ctx, order.ID)
	if err != nil || current.ClientID != stored.ClientID || current.State != OrderAcknowledged {
		t.Fatal("reopen changed existing identity", current, err)
	}
	if len(adapter.calls()) != 1 {
		t.Fatal("recovery resubmitted old uncertain order", adapter.calls())
	}
}
