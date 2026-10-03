package execution

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func ledgerInstrument() Instrument {
	return Instrument{ID: "BTC", Version: "v1", Valuation: "linear_perpetual", SettlementCurrency: "USDT", QuantityStep: intentPrice("0.1"), ContractSize: intentPrice("1"), PriceTick: intentPrice("0.01"), MoneyScale: 8}
}

func testStore(t *testing.T) (*Store, *AccountRegistry, *AccountHandle, string) {
	t.Helper()
	key := testIntent(Buy).Account
	path := filepath.Join(t.TempDir(), "trades.db")
	var store *Store
	var err error
	if strings.Contains(t.Name(), "/MemoryHistory") {
		store, err = NewMemoryStoreWithHistory(key, path)
	} else if strings.Contains(t.Name(), "/MemoryStore") {
		store, err = NewMemoryStore(key)
	} else {
		store, err = OpenStore(path, key)
	}
	if err != nil {
		t.Fatal(err)
	}
	registry := &AccountRegistry{}
	handle, err := registry.Acquire(key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { registry.Close(); store.Close() })
	return store, registry, handle, path
}

func planOrder(t *testing.T, s *Store, id string, seq int64, side OrderSide, kind IntentKind, demands map[string]int64) OrderIntent {
	t.Helper()
	plan := Plan{ID: "plan-" + id, Sequence: seq, DecisionMS: 10, ExpiresMS: 1000}
	order := OrderIntent{ID: id, PlanID: plan.ID, Instrument: ledgerInstrument(), Side: side, Observation: ExecutionObservation{Price: intentPrice("100"), AtMS: 10, ValidUntilMS: 1000, Bar: 5}}
	for strategy, steps := range demands {
		intent := EligibleIntent{ID: VirtualIntentID(id + "-" + strategy), Account: s.key, Strategy: StrategyID(strategy), Lot: VirtualLotID("lot-" + strategy), Instrument: "BTC", Kind: kind, Side: side, QuantitySteps: steps, State: Eligible, Conditions: IntentConditions{CreatedBar: 5}}
		plan.Intents = append(plan.Intents, intent)
		order.Allocations = append(order.Allocations, FillAllocation{ID: strategy, IntentID: intent.ID, Strategy: intent.Strategy, Lot: intent.Lot, Side: side, Kind: kind, Steps: steps})
		order.Steps += steps
	}
	if err := s.SavePlan(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	if err := s.PrepareOrder(context.Background(), order, 10); err != nil {
		t.Fatal(err)
	}
	return order
}

type fakeExecutionAdapter struct {
	mu           sync.Mutex
	trace        []string
	capabilities AdapterCapabilities
	submit       func(context.Context, OrderIntent, string) (SubmitReceipt, error)
	cancel       func(context.Context, string) (bool, error)
	query        func(context.Context, string, string) (QueryResult, error)
}

func (a *fakeExecutionAdapter) add(s string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.trace = append(a.trace, s)
}
func (a *fakeExecutionAdapter) calls() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.trace...)
}
func (a *fakeExecutionAdapter) Capabilities() AdapterCapabilities { return a.capabilities }
func (a *fakeExecutionAdapter) Submit(ctx context.Context, o OrderIntent, client string) (SubmitReceipt, error) {
	a.add("Submit:" + client)
	if err := ctx.Err(); err != nil {
		return SubmitReceipt{}, err
	}
	if a.submit != nil {
		return a.submit(ctx, o, client)
	}
	return SubmitReceipt{ExchangeID: "exchange-" + o.ID}, nil
}
func (a *fakeExecutionAdapter) Cancel(ctx context.Context, id string) (bool, error) {
	a.add("Cancel:" + id)
	if a.cancel != nil {
		return a.cancel(ctx, id)
	}
	return true, nil
}
func (a *fakeExecutionAdapter) Query(ctx context.Context, client, id string) (QueryResult, error) {
	a.add("Query:" + client)
	if a.query != nil {
		return a.query(ctx, client, id)
	}
	return QueryResult{}, nil
}
func (a *fakeExecutionAdapter) Snapshot(context.Context) (VenueSnapshot, error) {
	a.add("Snapshot")
	return VenueSnapshot{}, nil
}

func executorFor(s *Store, h *AccountHandle, a *fakeExecutionAdapter) *OwnerExecutor {
	return &OwnerExecutor{Handle: h, Token: h.Token(), Store: s, Adapter: a}
}

func TestPersistentPrepareSendAckLossAndRecovery(t *testing.T) {
	s, _, h, path := testStore(t)
	planOrder(t, s, "order", 1, Buy, EntryIntent, map[string]int64{"a": 10})
	before, err := s.Order(context.Background(), "order")
	if err != nil {
		t.Fatal(err)
	}
	a := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true, AuthoritativeNotFound: true}}
	a.submit = func(ctx context.Context, o OrderIntent, client string) (SubmitReceipt, error) {
		stored, err := s.Order(ctx, o.ID)
		if err != nil || stored.State != OrderSending || stored.Attempt != 1 || stored.Generation != h.Token().Generation {
			t.Fatalf("network before durable attempt: %+v %v", stored, err)
		}
		return SubmitReceipt{}, errors.New("ack lost after venue accepted")
	}
	e := executorFor(s, h, a)
	if err := e.Send("order", 11); err == nil {
		t.Fatal("ack loss hidden")
	}
	if err := e.Send("order", 12); err == nil {
		t.Fatal("unknown blindly resubmitted")
	}
	s.Close()
	reopened, err := OpenStore(path, s.key)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	e.Store = reopened
	a.query = func(context.Context, string, string) (QueryResult, error) {
		return QueryResult{Found: true, Authoritative: true, Complete: true, Receipt: SubmitReceipt{ExchangeID: "venue-order"}}, nil
	}
	if err := e.Recover("order"); err != nil {
		t.Fatal(err)
	}
	after, err := reopened.Order(context.Background(), "order")
	if err != nil {
		t.Fatal(err)
	}
	if after.State != OrderAcknowledged || after.ClientID != before.ClientID || after.Attempt != 1 {
		t.Fatal(after)
	}
	if calls := a.calls(); len(calls) != 2 || calls[0] != "Submit:"+before.ClientID || calls[1] != "Query:"+before.ClientID {
		t.Fatal(calls)
	}
}

func TestFillAttributionDedupRollbackAndPlanReplacement(t *testing.T) {
	s, _, h, _ := testStore(t)
	order := planOrder(t, s, "order", 1, Buy, EntryIntent, map[string]int64{"a": 6, "b": 4})
	e := executorFor(s, h, &fakeExecutionAdapter{})
	if err := e.Send(order.ID, 11); err != nil {
		t.Fatal(err)
	}
	fill := FillReport{EventID: "trade1", OrderID: order.ID, Steps: 3, Price: intentPrice("100"), Fee: intentPrice("0.01"), AtMS: 12}
	if _, err := s.db.Exec("CREATE TRIGGER fail_ledger BEFORE INSERT ON exec_ledger BEGIN SELECT RAISE(ABORT,'fault'); END"); err != nil {
		t.Fatal(err)
	}
	if applied, err := s.ApplyFill(context.Background(), fill); err == nil || applied {
		t.Fatal("rollback injection did not fail")
	}
	stored, _ := s.Order(context.Background(), order.ID)
	if stored.FilledSteps != 0 {
		t.Fatal("rolled back highwater escaped")
	}
	var count int
	if err := s.db.QueryRow("SELECT count(*) FROM exec_event WHERE id=?", fill.EventID).Scan(&count); err != nil || count != 0 {
		t.Fatal("rolled back dedup escaped", count, err)
	}
	if _, err := s.db.Exec("DROP TRIGGER fail_ledger"); err != nil {
		t.Fatal(err)
	}
	if applied, err := s.ApplyFill(context.Background(), fill); err != nil || !applied {
		t.Fatal(applied, err)
	}
	if applied, err := s.ApplyFill(context.Background(), fill); err != nil || applied {
		t.Fatal("duplicate fill changed ledger", applied, err)
	}
	planOrder(t, s, "new-plan-order", 2, Sell, EntryIntent, map[string]int64{"c": 5})
	fill.EventID = "trade2"
	fill.Steps = 7
	fill.Fee = intentPrice("0.02")
	if applied, err := s.ApplyFill(context.Background(), fill); err != nil || !applied {
		t.Fatal(applied, err)
	}
	snapshot, err := s.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.AccountSettledCash.Equal(intentPrice("-0.03")) || len(snapshot.Lots) != 2 {
		t.Fatal(snapshot)
	}
	if snapshot.Lots[0].Strategy != "a" || snapshot.Lots[0].SignedSteps != 6 || snapshot.Lots[1].Strategy != "b" || snapshot.Lots[1].SignedSteps != 4 {
		t.Fatal("plan replacement reassigned old fills", snapshot.Lots)
	}
	strategyCash := snapshot.UnassignedCash
	for _, cash := range snapshot.SyntheticStrategyCash {
		strategyCash = strategyCash.Add(cash)
	}
	if !strategyCash.Equal(snapshot.AccountSettledCash) {
		t.Fatal("fee residual not conserved")
	}
}

func TestCumulativeRecoveryNormalizesLaterTrades(t *testing.T) {
	s, _, h, _ := testStore(t)
	order := planOrder(t, s, "order", 1, Buy, EntryIntent, map[string]int64{"a": 10})
	a := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true, CumulativeReports: true}}
	e := executorFor(s, h, a)
	if err := e.Send(order.ID, 11); err != nil {
		t.Fatal(err)
	}
	trade := FillReport{EventID: "trade1", OrderID: order.ID, Steps: 2, Price: intentPrice("100"), Fee: intentPrice("0.02"), AtMS: 12}
	if err := e.ApplyTrade(trade); err != nil {
		t.Fatal(err)
	}
	cumulative := FillReport{EventID: "query4", OrderID: order.ID, Steps: 4, Price: intentPrice("105"), Cost: intentPrice("42"), Fee: intentPrice("0.04"), Cumulative: true, AtMS: 13}
	a.query = func(context.Context, string, string) (QueryResult, error) {
		return QueryResult{Found: true, Authoritative: true, Complete: true, Receipt: SubmitReceipt{ExchangeID: "exchange-order", Fills: []FillReport{cumulative}}}, nil
	}
	if err := e.Recover(order.ID); err != nil {
		t.Fatal(err)
	}
	if err := e.ApplyTrade(trade); err != nil {
		t.Fatal("historical trade wakeup failed", err)
	}
	stored, _ := s.Order(context.Background(), order.ID)
	if stored.FilledSteps != 4 || !stored.ReportedCost.Equal(intentPrice("42")) {
		t.Fatal("query/trade double counted", stored)
	}
	cumulative.EventID = "query5"
	cumulative.Steps = 5
	cumulative.Cost = intentPrice("53")
	cumulative.Fee = intentPrice("0.05")
	cumulative.Price = intentPrice("106")
	cumulative.AtMS = 14
	trade.EventID = "trade-new"
	trade.Steps = 1
	trade.Price = intentPrice("110")
	if err := e.ApplyTrade(trade); err != nil {
		t.Fatal(err)
	}
	stored, _ = s.Order(context.Background(), order.ID)
	if stored.FilledSteps != 5 || !stored.ReportedCost.Equal(intentPrice("53")) {
		t.Fatal(stored)
	}
	snapshot, _ := s.Snapshot(context.Background())
	if !snapshot.Lots[0].CostBasis.Equal(intentPrice("53")) || !snapshot.AccountSettledCash.Equal(intentPrice("-0.05")) {
		t.Fatal(snapshot)
	}
	cumulative.EventID = "regressed"
	cumulative.Steps = 4
	if err := e.ApplyTrade(trade); err == nil {
		t.Fatal("regressed cumulative highwater accepted")
	}
}

func TestOwnerStopPersistsUnknownAndJoinsSend(t *testing.T) {
	s, r, h, _ := testStore(t)
	order := planOrder(t, s, "order", 1, Buy, EntryIntent, map[string]int64{"a": 10})
	started := make(chan struct{})
	a := &fakeExecutionAdapter{}
	a.submit = func(ctx context.Context, _ OrderIntent, _ string) (SubmitReceipt, error) {
		close(started)
		<-ctx.Done()
		return SubmitReceipt{}, ctx.Err()
	}
	e := executorFor(s, h, a)
	done := make(chan error, 1)
	go func() { done <- e.Send(order.ID, 11) }()
	<-started
	r.Stop()
	r.Join()
	if err := <-done; err == nil {
		t.Fatal("canceled network operation reported success")
	}
	stored, err := s.Order(context.Background(), order.ID)
	if err != nil || stored.State != OrderUnknown {
		t.Fatal("stop lost uncertain send", stored, err)
	}
	if err := e.Send(order.ID, 12); !errors.Is(err, ErrOwnerStopped) {
		t.Fatal(err)
	}
	if len(a.calls()) != 1 {
		t.Fatal(a.calls())
	}
}

func TestCancelLossAndAuthoritativeAbsence(t *testing.T) {
	s, _, h, _ := testStore(t)
	order := planOrder(t, s, "order", 1, Buy, EntryIntent, map[string]int64{"a": 10})
	a := &fakeExecutionAdapter{capabilities: AdapterCapabilities{QueryClientID: true, AuthoritativeNotFound: true}}
	e := executorFor(s, h, a)
	if err := e.Send(order.ID, 11); err != nil {
		t.Fatal(err)
	}
	a.cancel = func(context.Context, string) (bool, error) { return false, errors.New("cancel ack lost") }
	if err := e.Cancel(order.ID, 12); err == nil {
		t.Fatal("cancel loss hidden")
	}
	stored, _ := s.Order(context.Background(), order.ID)
	if stored.State != OrderCancelPending {
		t.Fatal(stored)
	}
	if err := e.Cancel(order.ID, 13); err == nil {
		t.Fatal("uncertain cancel repeated")
	}
	if err := e.Recover(order.ID); err == nil {
		t.Fatal("temporary absence treated definitive")
	}
	stored, _ = s.Order(context.Background(), order.ID)
	if stored.State != OrderCancelPending {
		t.Fatal(stored)
	}
	a.query = func(context.Context, string, string) (QueryResult, error) {
		return QueryResult{Authoritative: true}, nil
	}
	if err := e.Recover(order.ID); err != nil {
		t.Fatal(err)
	}
	stored, _ = s.Order(context.Background(), order.ID)
	if stored.State != OrderRejected {
		t.Fatal(stored)
	}
}
