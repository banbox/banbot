package execution

import (
	"context"
	"testing"
)

func TestCancelIncompleteFinalQueryRetainsPendingUntilRecovery(t *testing.T) {
	s, _, h, _ := testStore(t)
	o := planOrder(t, s, "cancel-incomplete", 1, Buy, EntryIntent, map[string]int64{"a": 10})
	a := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true, CumulativeReports: true}}
	e := executorFor(s, h, a)
	if err := e.Send(o.ID, 11); err != nil {
		t.Fatal(err)
	}
	complete := false
	a.query = func(context.Context, string, string) (QueryResult, error) {
		return QueryResult{Found: true, Authoritative: true, Complete: complete, Canceled: true, Receipt: SubmitReceipt{ExchangeID: "exchange-" + o.ID, Fills: []FillReport{{EventID: "final", OrderID: o.ID, Steps: 8, Price: intentPrice("100"), Cost: intentPrice("80"), Fee: intentPrice("0.08"), Cumulative: true, AtMS: 13}}}}, nil
	}
	if err := e.Cancel(o.ID, 13); err == nil {
		t.Fatal("incomplete query accepted cancellation")
	}
	current, _ := s.Order(context.Background(), o.ID)
	if current.State != OrderCancelPending || current.FilledSteps != 0 {
		t.Fatal("incomplete query released/applied unverified fill", current)
	}
	complete = true
	e = executorFor(s, h, a)
	if err := e.Recover(o.ID); err != nil {
		t.Fatal(err)
	}
	current, _ = s.Order(context.Background(), o.ID)
	if current.State != OrderCanceled || current.FilledSteps != 8 {
		t.Fatal("recovery did not settle final highwater", current)
	}
}

func TestCancelSettlesConcurrentFinalFillBeforeRelease(t *testing.T) {
	s, _, h, _ := testStore(t)
	o := planOrder(t, s, "cancel-race", 1, Buy, EntryIntent, map[string]int64{"a": 10})
	a := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true, CumulativeReports: true}}
	e := executorFor(s, h, a)
	if err := e.Send(o.ID, 11); err != nil {
		t.Fatal(err)
	}
	if err := e.ApplyTrade(FillReport{EventID: "initial", OrderID: o.ID, Steps: 6, Price: intentPrice("100"), Fee: intentPrice("0.06"), AtMS: 12}); err != nil {
		t.Fatal(err)
	}
	a.query = func(context.Context, string, string) (QueryResult, error) {
		return QueryResult{Found: true, Complete: true, Authoritative: true, Canceled: true, Receipt: SubmitReceipt{ExchangeID: "exchange-" + o.ID, Fills: []FillReport{{EventID: "final", OrderID: o.ID, Steps: 8, Price: intentPrice("100"), Cost: intentPrice("80"), Fee: intentPrice("0.08"), Cumulative: true, AtMS: 13}}}}, nil
	}
	if err := e.Cancel(o.ID, 13); err != nil {
		t.Fatal(err)
	}
	current, _ := s.Order(context.Background(), o.ID)
	if current.State != OrderCanceled || current.FilledSteps != 8 || !current.ReportedFee.Equal(intentPrice("0.08")) {
		t.Fatal("cancel released before final cumulative fill", current)
	}
	snapshot, _ := s.Snapshot(context.Background())
	if snapshot.Lots[0].SignedSteps != 8 {
		t.Fatal("concurrent cancel fill lost", snapshot)
	}
	if err := e.Recover(o.ID); err != nil {
		t.Fatal(err)
	}
	again, _ := s.Snapshot(context.Background())
	if again.Checkpoint != snapshot.Checkpoint {
		t.Fatal("final query duplicate charged twice")
	}
	replacement := o
	replacement.ID = "remaining"
	replacement.Steps = 2
	replacement.Allocations = append([]FillAllocation(nil), o.Allocations...)
	replacement.Allocations[0].Steps = 2
	if err := s.PrepareOrder(context.Background(), replacement, 14); err != nil {
		t.Fatal(err)
	}
	replacement.ID = "too-much"
	replacement.Steps = 4
	replacement.Allocations[0].Steps = 4
	if err := s.PrepareOrder(context.Background(), replacement, 14); err == nil {
		t.Fatal("replacement ignored final fill highwater")
	}
}
