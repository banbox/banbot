package execution

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestAuthoritativeAbsenceLateFillPreservesRejectedLedger(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			s, _, owner, _ := testStore(t)
			order := planOrder(t, s, "late-rejected", 1, Buy, EntryIntent, map[string]int64{"a": 10})
			adapter := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true, AuthoritativeNotFound: true}}
			adapter.submit = func(context.Context, OrderIntent, string) (SubmitReceipt, error) {
				return SubmitReceipt{}, errors.New("acknowledgement lost")
			}
			e := executorFor(s, owner, adapter)
			if err := e.Send(order.ID, 11); err == nil {
				t.Fatal("submission uncertainty hidden")
			}
			adapter.query = func(context.Context, string, string) (QueryResult, error) {
				return QueryResult{Authoritative: true}, nil
			}
			if err := e.Recover(order.ID); err != nil {
				t.Fatal(err)
			}
			before, err := s.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			rejected, err := s.Order(ctx, order.ID)
			if err != nil || rejected.State != OrderRejected {
				t.Fatal("authoritative absence did not reject order", rejected, err)
			}
			report := FillReport{EventID: "late-fill", OrderID: order.ID, Steps: 2, Price: intentPrice("100"), Fee: intentPrice("0.02"), AtMS: 12}
			for repeat := 0; repeat < 2; repeat++ {
				if err := e.ApplyTrade(report); err == nil || !strings.Contains(err.Error(), "unsent/rejected") {
					t.Fatal("late fill must require controlled recovery, including replay", err)
				}
				after, err := s.Snapshot(ctx)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatal("rejected late fill changed balances, lots, order or checkpoint", before, after, err)
				}
				stored, err := s.Order(ctx, order.ID)
				if err != nil || !reflect.DeepEqual(rejected, stored) {
					t.Fatal("late fill changed rejected terminal order", stored, err)
				}
			}
			if err := e.Send(order.ID, 13); err == nil {
				t.Fatal("rejected intent was resubmitted")
			}
		})
	}
}
