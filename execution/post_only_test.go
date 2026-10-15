package execution

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

func TestPostOnlyUnsupportedRollsBackAndCannotDropCondition(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore"} {
		t.Run(backend, func(t *testing.T) {
			s, _, h, _ := testStore(t)
			fundStrategies(t, s)
			executor := executorFor(s, h, &fakeExecutionAdapter{})
			request := domainRequest("maker-unsupported", 1, 10, "100", 3, 0)
			condition := contributor(s, "a", "lot-a", Buy, 3, "99", "0")
			condition.Conditions.PostOnly = true
			request.Requests[0].IntentConstraints = []EligibleIntent{condition}
			before, _ := s.Snapshot(context.Background())
			if _, err := executor.PrepareRebalanceWithCheckpoint(request, StrategyCheckpoint{Strategy: "a", Name: "source", Payload: []byte(`{}`)}); err == nil {
				t.Fatal("unsupported maker accepted")
			}
			after, _ := s.Snapshot(context.Background())
			if before.Checkpoint != after.Checkpoint || len(after.Orders) != 0 {
				t.Fatal("rejected maker changed account")
			}
			if _, err := s.StrategyCheckpoint(context.Background(), "a", "source"); !errors.Is(err, sql.ErrNoRows) {
				t.Fatal("rejected maker committed strategy", err)
			}
			executor.Adapter = &fakeExecutionAdapter{capabilities: AdapterCapabilities{PostOnly: true}}
			prepared, err := executor.PrepareRebalance(request)
			if err != nil {
				t.Fatal(err)
			}
			order, _ := s.Order(context.Background(), prepared.OrderIDs[0])
			if !order.Intent.PostOnly || !order.Intent.Limit.Equal(intentPrice("99")) {
				t.Fatal("maker constraint lost", order.Intent)
			}
			executor.Adapter = &fakeExecutionAdapter{}
			if err := executor.Send(order.Intent.ID, 11); err == nil {
				t.Fatal("unproven replacement adapter sent maker")
			}
			order, _ = s.Order(context.Background(), order.Intent.ID)
			if order.Attempt != 0 || order.State != OrderPrepared {
				t.Fatal("unsupported send claimed an attempt", order)
			}
		})
	}
}

func TestPaperPostOnlyRestsThenSettlesFromVisibleQuote(t *testing.T) {
	for _, memory := range []bool{false, true} {
		name := "SQLite"
		if memory {
			name = "MemoryStore"
		}
		t.Run(name, func(t *testing.T) {
			borrow, paper := strategyService(t, memory)
			request := strategyRequest("a", "resting", 10, StrategyTargetsPatch, ExecutableTarget{Lot: "maker", SignedSteps: 3})
			condition := contributor(borrow.service.store, "a", "maker", Buy, 3, "99", "0")
			condition.Conditions.PostOnly = true
			request.Requests[0].IntentConstraints = []EligibleIntent{condition}
			if err := borrow.RebalanceStrategy(request, 11); err != nil {
				t.Fatal(err)
			}
			snapshot, _ := borrow.Snapshot(context.Background())
			if paper.Metrics().Fills != 0 || len(snapshot.Lots) != 0 || len(snapshot.Orders) != 1 || snapshot.Orders[0].State != OrderAcknowledged {
				t.Fatal("resting maker filled immediately", snapshot)
			}
			quote := VisibleQuote{Bid: intentPrice("98"), Ask: intentPrice("99"), AtMS: 12, ReceivedMS: 12, ValidUntilMS: 1000, Bar: 12}
			future := quote
			future.ReceivedMS = 20
			if err := borrow.AdvancePaperQuote(context.Background(), "BTC", future, 12); err == nil {
				t.Fatal("future quote filled order")
			}
			if err := borrow.AdvancePaperQuote(context.Background(), "BTC", quote, 12); err != nil {
				t.Fatal(err)
			}
			snapshot, _ = borrow.Snapshot(context.Background())
			if paper.Metrics().Fills != 1 || len(snapshot.Orders) != 0 || len(snapshot.Lots) != 1 || snapshot.Lots[0].SignedSteps != 3 || !snapshot.Lots[0].CostBasis.Equal(intentPrice("29.7")) {
				t.Fatal("visible maker fill not settled exactly", snapshot)
			}
			if err := borrow.AdvancePaperQuote(context.Background(), "BTC", quote, 12); err != nil || paper.Metrics().Fills != 1 {
				t.Fatal("quote replay double filled", err)
			}
			slippage, _ := paper.StrategyCosts("a")
			if slippage != 0 || len(paper.pending) != 0 {
				t.Fatal("maker charged taker slippage or retained active index")
			}
		})
	}
}

func TestPaperPostOnlyCrossedQuotesAndCancelNeverFill(t *testing.T) {
	ctx := context.Background()
	for _, side := range []OrderSide{Buy, Sell} {
		t.Run(string(side), func(t *testing.T) {
			paper, _ := NewPaperAdapter(intentPrice("1000"), intentPrice("0.01"), intentPrice("0.01"))
			order := OrderIntent{ID: "crossed", Instrument: ledgerInstrument(), Side: side, Steps: 3, PostOnly: true, Limit: intentPrice("100"), SubmitAtMS: 11, Observation: ExecutionObservation{Price: intentPrice("100"), Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: 10, ValidUntilMS: 1000}}
			receipt, err := paper.Submit(ctx, order, "crossed-client")
			if err != nil || !receipt.Rejected || paper.Metrics().Fills != 0 {
				t.Fatal("crossed maker became taker", receipt, err)
			}
			order.ID = "resting"
			order.Limit = intentPrice("99")
			if side == Sell {
				order.Limit = intentPrice("101")
			}
			receipt, err = paper.Submit(ctx, order, "resting-client")
			if err != nil || receipt.Rejected {
				t.Fatal(receipt, err)
			}
			if ok, err := paper.Cancel(ctx, receipt.ExchangeID); err != nil || !ok {
				t.Fatal(err)
			}
			quote := VisibleQuote{Bid: intentPrice("101"), Ask: intentPrice("101"), AtMS: 12, ReceivedMS: 12, ValidUntilMS: 1000}
			if side == Buy {
				quote.Bid, quote.Ask = intentPrice("99"), intentPrice("99")
			}
			if ids, err := paper.AdvanceQuote(ctx, "BTC", quote, 12); err != nil || len(ids) != 0 || paper.Metrics().Fills != 0 {
				t.Fatal("canceled maker filled", ids, err)
			}
			result, err := paper.Query(ctx, "resting-client", receipt.ExchangeID)
			if err != nil || !result.Canceled || !result.Complete || len(paper.pending) != 0 {
				t.Fatal(result, err)
			}
		})
	}
}

func TestBanexgPostOnlyRequiresProofAndDurableStore(t *testing.T) {
	s, _, venue, transport, config := banexgFixture(t)
	adapter, err := NewBanexgAdapter(context.Background(), venue, config)
	if err != nil {
		t.Fatal(err)
	}
	order := OrderIntent{ID: "maker", Instrument: ledgerInstrument(), Side: Buy, Steps: 3, Limit: intentPrice("99"), PostOnly: true}
	if _, err := adapter.Submit(context.Background(), order, "client"); err == nil || venue.creates != 0 {
		t.Fatal("unproven maker dispatched")
	}
	transport.proof.PostOnly = true
	adapter, err = NewBanexgAdapter(context.Background(), venue, config)
	if err != nil || !adapter.Capabilities().PostOnly {
		t.Fatal(err)
	}
	venue.create = func(_ string, kind, _ string, _, price float64, params map[string]any) (*banexg.Order, *errs.Error) {
		if kind != banexg.OdTypeLimitMaker || price != 99 {
			t.Fatal("maker used taker order type", kind, price)
		}
		return &banexg.Order{ID: "maker-venue", ClientOrderID: params[banexg.ParamClientOrderId].(string)}, nil
	}
	if _, err := adapter.Submit(context.Background(), order, "client"); err != nil {
		t.Fatal(err)
	}
	memory, _ := NewMemoryStore(s.Account())
	defer memory.Close()
	if err := adapter.BindStore(memory); err == nil {
		t.Fatal("real adapter bound memory store")
	}
	config.Store = memory
	if _, err := NewBanexgAdapter(context.Background(), venue, config); err == nil {
		t.Fatal("real adapter constructed with memory store")
	}
}

func TestPaperMakerFillPersistenceFailureRecoversOnSameQuote(t *testing.T) {
	borrow, paper := strategyService(t, false)
	request := strategyRequest("a", "resting-fault", 10, StrategyTargetsPatch, ExecutableTarget{Lot: "maker", SignedSteps: 3})
	condition := contributor(borrow.service.store, "a", "maker", Buy, 3, "99", "0")
	condition.Conditions.PostOnly = true
	request.Requests[0].IntentConstraints = []EligibleIntent{condition}
	if err := borrow.RebalanceStrategy(request, 11); err != nil {
		t.Fatal(err)
	}
	db := borrow.service.store.db
	if _, err := db.Exec(`CREATE TRIGGER fail_maker BEFORE INSERT ON exec_ledger BEGIN SELECT RAISE(ABORT,'maker ledger fault'); END`); err != nil {
		t.Fatal(err)
	}
	quote := VisibleQuote{Bid: intentPrice("98"), Ask: intentPrice("99"), AtMS: 12, ReceivedMS: 12, ValidUntilMS: 1000}
	if err := borrow.AdvancePaperQuote(context.Background(), "BTC", quote, 12); err == nil {
		t.Fatal("faulted fill committed")
	}
	if paper.Metrics().Fills != 1 {
		t.Fatal("simulated venue did not fill before persistence failure")
	}
	if _, err := db.Exec("DROP TRIGGER fail_maker"); err != nil {
		t.Fatal(err)
	}
	if err := borrow.AdvancePaperQuote(context.Background(), "BTC", quote, 12); err != nil {
		t.Fatal("same visible quote lost unpersisted venue fill", err)
	}
	snapshot, _ := borrow.Snapshot(context.Background())
	if len(snapshot.Lots) != 1 || snapshot.Lots[0].SignedSteps != 3 || paper.Metrics().Fills != 1 {
		t.Fatal("maker recovery double filled or lost allocation", snapshot)
	}
}
