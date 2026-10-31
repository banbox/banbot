package execution

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
)

func durablePaper(t *testing.T, store *Store) *PaperAdapter {
	t.Helper()
	p, err := NewDurablePaperAdapter(intentPrice("1000"), intentPrice("0.001"), intentPrice("0.001"), PaperBidAsk)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.BindStore(store); err != nil {
		t.Fatal(err)
	}
	return p
}

func durablePaperOrder(id string) OrderIntent {
	return OrderIntent{ID: id, Instrument: ledgerInstrument(), Side: Buy, Steps: 10, SubmitAtMS: 11, Observation: ExecutionObservation{Price: intentPrice("100"), Bid: intentPrice("99"), Ask: intentPrice("100"), AtMS: 10, ValidUntilMS: 1000}, Allocations: []FillAllocation{{Strategy: "a", Steps: 10}}}
}

func assertPaperReplay(t *testing.T, old, restored *PaperAdapter) {
	t.Helper()
	old.mu.Lock()
	defer old.mu.Unlock()
	restored.mu.Lock()
	defer restored.mu.Unlock()
	ids := make([]string, 0, len(old.orders))
	for id := range old.orders {
		ids = append(ids, id)
	}
	a, err := old.paperProofLocked(ids)
	if err != nil {
		t.Fatal(err)
	}
	b, err := restored.paperProofLocked(ids)
	if err != nil {
		t.Fatal(err)
	}
	if a != b || !reflect.DeepEqual(old.clients, restored.clients) || len(old.pending) != len(restored.pending) {
		t.Fatal("durable venue replay lost receipts, financial state, or resting orders")
	}
}

func TestDurablePaperRestartsReceiptsLimitsCashAndCosts(t *testing.T) {
	ctx := context.Background()
	s, _, _, path := testStore(t)
	p := durablePaper(t, s)
	market := durablePaperOrder("market")
	first, err := p.Submit(ctx, market, "market-client")
	if err != nil {
		t.Fatal(err)
	}
	resting := durablePaperOrder("resting")
	resting.Limit = intentPrice("98")
	resting.PostOnly = true
	if _, err := p.Submit(ctx, resting, "resting-client"); err != nil {
		t.Fatal(err)
	}
	canceled := resting
	canceled.ID = "canceled"
	receipt, err := p.Submit(ctx, canceled, "canceled-client")
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := p.Cancel(ctx, receipt.ExchangeID); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if err := p.ApplyCash(intentPrice("-2.50")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(path, testIntent(Buy).Account)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restored := durablePaper(t, reopened)
	assertPaperReplay(t, p, restored)
	duplicate, err := restored.Submit(ctx, market, "market-client")
	if err != nil || !reflect.DeepEqual(first, duplicate) || restored.Metrics().Fills != 1 {
		t.Fatal("client retry duplicated fill", duplicate, err)
	}
	quote := VisibleQuote{Bid: intentPrice("97"), Ask: intentPrice("98"), AtMS: 12, ReceivedMS: 12, ValidUntilMS: 1000}
	ids, err := restored.AdvanceQuote(ctx, "BTC", quote, 12)
	if err != nil || len(ids) != 1 || ids[0] != "resting" {
		t.Fatal(ids, err)
	}
	again := durablePaper(t, reopened)
	assertPaperReplay(t, restored, again)
	result, err := again.Query(ctx, "canceled-client", receipt.ExchangeID)
	if err != nil || !result.Canceled {
		t.Fatal(result, err)
	}
}

func rejectPaperJournal(t *testing.T, s *Store) {
	t.Helper()
	if _, err := s.db.Exec(`CREATE TRIGGER reject_paper_journal BEFORE INSERT ON exec_checkpoint WHEN NEW.strategy='execution-paper-venue' AND NEW.name LIKE 'operation:%' BEGIN SELECT RAISE(ABORT,'venue journal failure'); END`); err != nil {
		t.Fatal(err)
	}
}

func TestDurablePaperJournalFailureRollsBackSubmitAndQuote(t *testing.T) {
	ctx := context.Background()
	s, _, _, _ := testStore(t)
	p := durablePaper(t, s)
	rejectPaperJournal(t, s)
	if _, err := p.Submit(ctx, durablePaperOrder("failed"), "failed-client"); err == nil {
		t.Fatal("failed journal acknowledged")
	}
	if p.Metrics().Fills != 0 || len(p.orders) != 0 || len(p.clients) != 0 || !p.cash.Equal(intentPrice("1000")) {
		t.Fatal("failed submit leaked venue mutation")
	}
	if _, err := s.db.Exec("DROP TRIGGER reject_paper_journal"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"one", "two"} {
		order := durablePaperOrder(id)
		order.Limit = intentPrice("98")
		if _, err := p.Submit(ctx, order, id); err != nil {
			t.Fatal(err)
		}
	}
	rejectPaperJournal(t, s)
	quote := VisibleQuote{Bid: intentPrice("97"), Ask: intentPrice("98"), AtMS: 12, ReceivedMS: 12, ValidUntilMS: 1000}
	if _, err := p.AdvanceQuote(ctx, "BTC", quote, 12); err == nil {
		t.Fatal("failed quote journal accepted")
	}
	if p.Metrics().Fills != 0 || len(p.pending) != 2 || !p.cash.Equal(intentPrice("1000")) {
		t.Fatal("failed quote mutated finances or pending pointers")
	}
	assertPaperReplay(t, p, durablePaper(t, s))
	if _, err := s.db.Exec("DROP TRIGGER reject_paper_journal"); err != nil {
		t.Fatal(err)
	}
	if ids, err := p.AdvanceQuote(ctx, "BTC", quote, 12); err != nil || len(ids) != 2 || p.Metrics().Fills != 2 {
		t.Fatal(ids, err)
	}
	assertPaperReplay(t, p, durablePaper(t, s))
}

func TestDurablePaperRecoverVenueCommitBeforeLocalACK(t *testing.T) {
	ctx := context.Background()
	s, registry, h, path := testStore(t)
	p := durablePaper(t, s)
	fundStrategies(t, s)
	order := planOrder(t, s, "lost-ack", 1, Buy, EntryIntent, map[string]int64{"a": 10})
	adapter := &fakeExecutionAdapter{capabilities: p.Capabilities(), submit: func(ctx context.Context, o OrderIntent, client string) (SubmitReceipt, error) {
		o.Observation.Bid, o.Observation.Ask = o.Observation.Price, o.Observation.Price
		if _, err := p.Submit(ctx, o, client); err != nil {
			return SubmitReceipt{}, err
		}
		return SubmitReceipt{}, errors.New("lost ACK after venue commit")
	}}
	executor := executorFor(s, h, adapter)
	if err := executor.Send(order.ID, 11); err == nil {
		t.Fatal("lost ACK not surfaced")
	}
	stored, err := s.Order(ctx, order.ID)
	if err != nil || stored.State != OrderUnknown {
		t.Fatal(stored, err)
	}
	registry.Close()
	s.Close()
	reopened, err := OpenStore(path, testIntent(Buy).Account)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restored := durablePaper(t, reopened)
	var owners AccountRegistry
	owner, err := owners.Acquire(testIntent(Buy).Account)
	if err != nil {
		t.Fatal(err)
	}
	defer owners.Close()
	recovery := &OwnerExecutor{Handle: owner, Token: owner.Token(), Store: reopened, Adapter: restored}
	for range 2 {
		if err := recovery.Recover(order.ID); err != nil {
			t.Fatal(err)
		}
	}
	stored, err = reopened.Order(ctx, order.ID)
	if err != nil || stored.State != OrderFilled || stored.FilledSteps != 10 || restored.Metrics().Fills != 1 {
		t.Fatal("venue ACK recovery lost/doubled fill", stored, err)
	}
	snapshot, err := reopened.Snapshot(ctx)
	if err != nil || len(snapshot.Lots) != 1 || snapshot.Lots[0].SignedSteps != 10 {
		t.Fatal(snapshot, err)
	}
}

func TestDurablePaperRefusesMissingCorruptOrMismatchedJournal(t *testing.T) {
	for _, mode := range []string{"missing-operation", "corrupt-operation", "configuration", "old-ledger"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			s, _, _, _ := testStore(t)
			if mode == "old-ledger" {
				fundStrategies(t, s)
			} else {
				p := durablePaper(t, s)
				if _, err := p.Submit(ctx, durablePaperOrder("market"), "client"); err != nil {
					t.Fatal(err)
				}
				if mode == "missing-operation" {
					if _, err := s.db.Exec("DELETE FROM exec_checkpoint WHERE name=?", paperJournalOperationName(1)); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "corrupt-operation" {
					if _, err := s.db.Exec("UPDATE exec_checkpoint SET payload='{}' WHERE name=?", paperJournalOperationName(1)); err != nil {
						t.Fatal(err)
					}
				}
			}
			cash := intentPrice("1000")
			if mode == "configuration" {
				cash = intentPrice("999")
			}
			p, err := NewDurablePaperAdapter(cash, intentPrice("0.001"), intentPrice("0.001"), PaperBidAsk)
			if err != nil {
				t.Fatal(err)
			}
			if err := p.BindStore(s); err == nil {
				t.Fatal("unsafe reconstruction accepted")
			}
			if _, err := p.Snapshot(ctx); err == nil {
				t.Fatal("unbound venue readable after failed replay")
			}
		})
	}
}

func durablePaperService(t *testing.T, root string) (*SharedAccountBorrow, *PaperAdapter, func()) {
	t.Helper()
	var registry AccountRegistry
	owner, err := registry.Acquire(testIntent(Buy).Account)
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewDurablePaperAdapter(intentPrice("1000"), intentPrice("0"), intentPrice("0"), PaperBidAsk)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewSharedAccount(owner, SharedExecutionOptions{StorePath: filepath.Join(root, "ledger.db"), SenderLeaseDir: filepath.Join(root, "leases"), Adapter: p, AuthoritativeSnapshot: true})
	if err != nil {
		t.Fatal(err)
	}
	borrow := service.Borrow()
	return borrow, p, func() { borrow.Release(); registry.Close(); service.Close() }
}

func TestDurablePaperFundingAtomicRollbackAndRestart(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	borrow, p, closeAccount := durablePaperService(t, root)
	s := borrow.Service().Store()
	fundStrategies(t, s)
	if err := borrow.Reconcile("ready", 0); err != nil {
		t.Fatal(err)
	}
	if err := borrow.RegisterRiskPolicy("v1", domainRisk()); err != nil {
		t.Fatal(err)
	}
	if err := borrow.RegisterAccountQuotes([]Instrument{ledgerInstrument()}, func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
		return VisibleQuote{Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: now, ReceivedMS: now, ValidUntilMS: 1000, Bar: now}, nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	lease, err := borrow.ClaimStrategyWriter(ctx, StrategyWriterClaim{Strategy: "a", WriterID: "robot-session"})
	if err != nil {
		t.Fatal(err)
	}
	request := strategyRequest("a", "enter", 10, StrategyTargetsFull, ExecutableTarget{Lot: "first", SignedSteps: 10})
	request.StrategyEpoch, request.StrategySequence = lease.Epoch, 7
	if err := borrow.RebalanceStrategy(request, 11); err != nil {
		t.Fatal(err)
	}
	event := PaperFundingObservation{ID: "funding", Instrument: ledgerInstrument(), Mark: intentPrice("100"), Rate: intentPrice("0.01"), AtMS: 12, AvailableAt: 12}
	before, err := borrow.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	beforeBody, _ := json.Marshal(before)
	cash := p.cash
	rejectPaperJournal(t, s)
	if applied, err := borrow.ApplyPaperFunding(ctx, event, 12); err == nil || applied {
		t.Fatal("failed funding accepted", applied, err)
	}
	after, _ := borrow.Snapshot(ctx)
	afterBody, _ := json.Marshal(after)
	if string(beforeBody) != string(afterBody) || !p.cash.Equal(cash) {
		t.Fatal("venue journal failure split ledger and cash")
	}
	if _, err := s.db.Exec("DROP TRIGGER reject_paper_journal"); err != nil {
		t.Fatal(err)
	}
	if applied, err := borrow.ApplyPaperFunding(ctx, event, 12); err != nil || !applied {
		t.Fatal(applied, err)
	}
	final, _ := borrow.Snapshot(ctx)
	finalBody, _ := json.Marshal(final)
	closeAccount()
	restored, venue, closeRestored := durablePaperService(t, root)
	defer closeRestored()
	assertPaperReplay(t, p, venue)
	state, err := restored.Service().Store().strategyWriter(ctx, "a")
	if err != nil || state != (strategyWriterState{Epoch: lease.Epoch, Sequence: 7, Revision: 1}) {
		t.Fatal("restart lost writer epoch/highwater", state, err)
	}
	snapshot, err := restored.Snapshot(ctx)
	body, _ := json.Marshal(snapshot)
	if err != nil || string(finalBody) != string(body) {
		t.Fatal("restart lost funded ledger", err)
	}
	if applied, err := restored.ApplyPaperFunding(ctx, event, 12); err != nil || applied {
		t.Fatal("restart repeated funding", applied, err)
	}
}

func TestDurablePaperSettlementsAtomicRollbackAndRestart(t *testing.T) {
	for _, kind := range []string{"option", "terminal"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			borrow, p, closeAccount := durablePaperService(t, root)
			s := borrow.Service().Store()
			fundStrategies(t, s)
			if err := borrow.Reconcile("ready", 0); err != nil {
				t.Fatal(err)
			}
			if err := borrow.RegisterRiskPolicy("v1", domainRisk()); err != nil {
				t.Fatal(err)
			}
			instrument := ledgerInstrument()
			price := intentPrice("100")
			if kind == "option" {
				instrument = cashOption("put")
				price = intentPrice("5")
			}
			if err := borrow.RegisterAccountQuotes([]Instrument{instrument}, func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
				return VisibleQuote{Bid: price, Ask: price, AtMS: now, ReceivedMS: now, ValidUntilMS: 4000, Bar: now}, nil
			}, nil); err != nil {
				t.Fatal(err)
			}
			request := strategyRequest("a", "enter", 10, StrategyTargetsFull, ExecutableTarget{Lot: "first", SignedSteps: 2})
			request.Requests[0].Instrument = instrument
			request.Requests[0].Quote.Bid = price
			request.Requests[0].Quote.Ask = price
			if err := borrow.RebalanceStrategy(request, 11); err != nil {
				t.Fatal(err)
			}
			apply := func(b *SharedAccountBorrow) (bool, error) {
				if kind == "option" {
					return b.ApplyOptionExpiry(OptionExpiry{Instrument: instrument, Fixing: intentPrice("90"), FixingAtMS: 1000, PublishedMS: 1002, AtMS: 1005})
				}
				return b.ApplyTerminalSettlement(TerminalSettlement{Source: "terminal", SourceVersion: "v1", Instrument: instrument, CashValue: intentPrice("60"), EventMS: 20, PublishedMS: 21, ReceivedMS: 25, AtMS: 25})
			}
			before, _ := borrow.Snapshot(ctx)
			beforeBody, _ := json.Marshal(before)
			cash := p.cash
			rejectPaperJournal(t, s)
			if applied, err := apply(borrow); err == nil || applied {
				t.Fatal("failed settlement accepted", applied, err)
			}
			after, _ := borrow.Snapshot(ctx)
			afterBody, _ := json.Marshal(after)
			if string(beforeBody) != string(afterBody) || !p.cash.Equal(cash) {
				t.Fatal("failed settlement split ledger and venue")
			}
			if _, err := s.db.Exec("DROP TRIGGER reject_paper_journal"); err != nil {
				t.Fatal(err)
			}
			if applied, err := apply(borrow); err != nil || !applied {
				t.Fatal(applied, err)
			}
			settled, _ := borrow.Snapshot(ctx)
			settledBody, _ := json.Marshal(settled)
			closeAccount()
			restored, venue, closeRestored := durablePaperService(t, root)
			defer closeRestored()
			assertPaperReplay(t, p, venue)
			snapshot, err := restored.Snapshot(ctx)
			body, _ := json.Marshal(snapshot)
			if err != nil || string(settledBody) != string(body) {
				t.Fatal("settled ledger lost on restart", err)
			}
			if applied, err := apply(restored); err != nil || applied {
				t.Fatal("restarted settlement repeated", applied, err)
			}
		})
	}
}

func TestDurablePaperUnavailableCommitProofFailsClosed(t *testing.T) {
	ctx := context.Background()
	s, _, _, path := testStore(t)
	p := durablePaper(t, s)
	if _, err := p.Submit(ctx, durablePaperOrder("committed"), "committed-client"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.ApplyCash(intentPrice("-100")); err == nil {
		t.Fatal("unverifiable commit accepted")
	}
	if _, err := p.Query(ctx, "committed-client", ""); err == nil {
		t.Fatal("latched venue published unverified receipt")
	}
	if _, err := p.Snapshot(ctx); err == nil {
		t.Fatal("latched venue published unverified snapshot")
	}
	reopened, err := OpenStore(path, testIntent(Buy).Account)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restored := durablePaper(t, reopened)
	if result, err := restored.Query(ctx, "committed-client", ""); err != nil || !result.Found || len(result.Receipt.Fills) != 1 {
		t.Fatal(result, err)
	}
	if !restored.cash.Equal(p.cash) {
		t.Fatal("failed commit changed cash")
	}
}
