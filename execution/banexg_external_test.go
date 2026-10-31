package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

func completeExternalOrder() *banexg.Order {
	return &banexg.Order{
		ID: "manual-order", ClientOrderID: "manual-client", Symbol: "BTC/USDT:USDT", Side: string(Buy),
		Status: banexg.OdStatusFilled, Amount: .3, Filled: .3, Cost: 30, Fee: &banexg.Fee{Currency: "USDT", Cost: .03},
		Trades: []*banexg.Trade{
			{ID: "trade-2", Symbol: "BTC/USDT:USDT", Side: string(Buy), Amount: .2, Price: 100, Cost: 20, Order: "manual-order", Timestamp: 13, Fee: &banexg.Fee{Currency: "USDT", Cost: .02}},
			{ID: "trade-1", Symbol: "BTC/USDT:USDT", Side: string(Buy), Amount: .1, Price: 100, Cost: 10, Order: "manual-order", Timestamp: 12, Fee: &banexg.Fee{Currency: "USDT", Cost: .01}},
		},
	}
}

func externalHint() BanexgStreamReport {
	return BanexgStreamReport{UnassignedExchangeID: "manual-order", Err: sql.ErrNoRows,
		ExternalOrder: &ExternalOrderHint{Symbol: "BTC/USDT:USDT", ExchangeID: "manual-order", ClientID: "manual-client"}}
}

func TestBanexgExternalRecoveryRequiresCompleteTerminalEconomics(t *testing.T) {
	for _, scenario := range []string{"filled", "canceled-partial", "active", "missing-trades", "duplicate-trade", "foreign-fee", "missing-trade-fee", "quantity", "trade-order", "symbol", "cost", "sum-cost", "sum-fee", "client", "bot-client", "hint-conflict", "unknown-symbol"} {
		t.Run(scenario, func(t *testing.T) {
			_, _, exchange, _, config := banexgFixture(t)
			adapter, err := NewBanexgAdapter(context.Background(), exchange, config)
			if err != nil {
				t.Fatal(err)
			}
			raw, report := completeExternalOrder(), externalHint()
			switch scenario {
			case "canceled-partial":
				raw.Status, raw.Amount = banexg.OdStatusCanceled, .5
			case "active":
				raw.Status = banexg.OdStatusPartFilled
			case "missing-trades":
				raw.Trades = nil
			case "duplicate-trade":
				raw.Trades[1].ID = raw.Trades[0].ID
			case "foreign-fee":
				raw.Fee.Currency = "BTC"
			case "missing-trade-fee":
				raw.Trades[0].Fee = nil
			case "quantity":
				raw.Trades[0].Amount = .3
			case "trade-order":
				raw.Trades[0].Order = "another"
			case "symbol":
				raw.Symbol = "ETH/USDT:USDT"
			case "cost":
				raw.Trades[0].Cost = 21
			case "sum-cost":
				raw.Cost = 31
			case "sum-fee":
				raw.Fee.Cost = .04
			case "client":
				raw.ClientOrderID = "another-client"
			case "bot-client":
				raw.ClientOrderID, report.ExternalOrder.ClientID = "exec-unknown", "exec-unknown"
			case "hint-conflict":
				report.Err = errors.New("conflicting known identity")
			case "unknown-symbol":
				report.ExternalOrder.Symbol = "unknown"
			}
			exchange.query = func(symbol, id string, params map[string]any) (*banexg.Order, *errs.Error) {
				ctx, ok := params[banexg.ParamContext].(context.Context)
				if !ok || ctx.Err() != nil || params[banexg.ParamAccount] != config.Account.Account || params[banexg.ParamCompleteOrder] != true || symbol != "BTC/USDT:USDT" || id != "manual-order" {
					t.Fatal("external query lost owned complete-query contract", params, symbol, id)
				}
				return raw, nil
			}
			events, err := adapter.RecoverExternal(context.Background(), report)
			if scenario != "filled" && scenario != "canceled-partial" {
				if err == nil || len(events) != 0 {
					t.Fatal("incomplete/ambiguous external order admitted", events, err)
				}
				return
			}
			if err != nil || len(events) != 2 || events[0].AtMS != 12 || events[0].Steps != 1 || events[1].Steps != 2 || !events[0].Fee.Equal(intentPrice("0.01")) || !events[1].Price.Equal(intentPrice("100")) {
				t.Fatal("terminal complete history not recovered exactly", events, err)
			}
		})
	}
}

func TestBanexgExternalRecoveryRejectsKnownClientAndHonorsCancellation(t *testing.T) {
	s, _, exchange, transport, config := banexgFixture(t)
	adapter, err := NewBanexgAdapter(context.Background(), exchange, config)
	if err != nil {
		t.Fatal(err)
	}
	order := planOrder(t, s, "bot-order", 1, Buy, EntryIntent, map[string]int64{"a": 1})
	stored, err := s.Order(context.Background(), order.ID)
	if err != nil {
		t.Fatal(err)
	}
	raw := completeExternalOrder()
	raw.ClientOrderID = stored.ClientID
	// A missing stream client ID must not hide a locally owned queried identity.
	report := externalHint()
	report.ExternalOrder.ClientID = ""
	exchange.query = func(string, string, map[string]any) (*banexg.Order, *errs.Error) { return raw, nil }
	if events, err := adapter.RecoverExternal(context.Background(), report); err == nil || len(events) != 0 {
		t.Fatal("locally owned client was adopted", events, err)
	}
	called := false
	transport.invoke = func(ctx context.Context, _ func() error) error {
		called = true
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if events, err := adapter.RecoverExternal(ctx, externalHint()); !errors.Is(err, context.DeadlineExceeded) || len(events) != 0 || !called {
		t.Fatal("external query escaped cancellation", events, err, called)
	}
}

func TestBanexgExternalCompletedOrderBatchIsAtomicAndReplaySafe(t *testing.T) {
	// A real venue adapter intentionally rejects process-local storage. Its
	// recovery integration therefore runs against the supported durable store.
	s, _, h, path := testStore(t)
	attributionPolicy(t, s)
	exchange := &fakeBanexgSession{}
	transport := &verifiedTestTransport{proof: BanexgExecutionProof{Account: s.key, EvidenceID: "external-test", ContextBound: true, StableClientID: true, QueryClientID: true, CompleteCumulativeReports: true, CompleteAccountSnapshot: true, SettledCash: true, NetLinearPositions: true}}
	config := BanexgAdapterConfig{Account: s.key, Store: s, Transport: transport, Instruments: []BanexgInstrument{{Symbol: "BTC/USDT:USDT", Instrument: ledgerInstrument()}}}
	exchange.query = func(string, string, map[string]any) (*banexg.Order, *errs.Error) { return completeExternalOrder(), nil }
	makeBorrow := func(store *Store) *SharedAccountBorrow {
		config.Store = store
		adapter, err := NewBanexgAdapter(context.Background(), exchange, config)
		if err != nil {
			t.Fatal(err)
		}
		service := &SharedAccount{ctx: context.Background(), store: store, owner: h, opts: SharedExecutionOptions{Adapter: adapter, TakeoverStrategy: "a"}, ready: true}
		borrow := service.Borrow()
		t.Cleanup(borrow.Release)
		return borrow
	}
	borrow := makeBorrow(s)
	if err := borrow.RecoverExternalReport(context.Background(), externalHint()); err != nil {
		t.Fatal(err)
	}
	before, err := s.Snapshot(context.Background())
	if err != nil || len(before.Lots) != 1 || before.Lots[0].SignedSteps != 3 || !before.Lots[0].CostBasis.Equal(intentPrice("30")) || !before.Lots[0].Fees.Equal(intentPrice("0.03")) || !before.RiskFrozen || borrow.service.ready {
		t.Fatal("batch did not preserve complete owned economics and freeze", before, err)
	}
	records, err := s.AutoTakeoverRecords(context.Background())
	if err != nil || len(records) != 1 {
		t.Fatal("one external order split into multiple takeover lots", records, err)
	}
	{
		borrow.Release()
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		s, err = OpenStore(path, config.Account)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		borrow = makeBorrow(s)
	}
	if err := borrow.RecoverExternalReport(context.Background(), externalHint()); err != nil {
		t.Fatal(err)
	}
	after, err := s.Snapshot(context.Background())
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("repeated complete history changed book or cash", before, after, err)
	}
	// A corrected immutable trade is not booked twice under a new identity.
	exchange.query = func(string, string, map[string]any) (*banexg.Order, *errs.Error) {
		raw := completeExternalOrder()
		raw.Trades[1].Fee.Cost, raw.Fee.Cost = .02, .04
		return raw, nil
	}
	if err := borrow.RecoverExternalReport(context.Background(), externalHint()); err == nil {
		t.Fatal("conflicting history silently overwrote original trade")
	}
	unchanged, err := s.Snapshot(context.Background())
	if err != nil || !reflect.DeepEqual(after, unchanged) {
		t.Fatal("failed conflicting replay partially committed", unchanged, err)
	}
	// Financial snapshots already contain older trade cash. Known trades
	// may replay, but an unseen historical order must not be rebooked.
	for _, baseline := range []struct {
		id, kind string
		value    any
	}{
		{"live-capital-bootstrap-v1", string(Reconciliation), CashEvent{AtMS: 20}},
		{"migration-baseline", "LegacyMigration", LegacyMigration{VenueSnapshot: LegacyVenueSnapshot{AtMS: 30}}},
	} {
		body, err := json.Marshal(baseline.value)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.commit(context.Background(), func(tx *storeTxn) error {
			if _, err := s.recordEvent(tx, baseline.id, baseline.kind, string(body)); err != nil {
				return err
			}
			return s.commitAccountEvent(tx, baseline.id, intentPrice("0"), nil)
		}); err != nil {
			t.Fatal(err)
		}
		exchange.query = func(string, string, map[string]any) (*banexg.Order, *errs.Error) { return completeExternalOrder(), nil }
		if err := borrow.RecoverExternalReport(context.Background(), externalHint()); err != nil {
			t.Fatal("known history could not replay after baseline", err)
		}
		before, err := s.Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		exchange.query = func(string, string, map[string]any) (*banexg.Order, *errs.Error) {
			raw := completeExternalOrder()
			raw.ID = "older-unseen"
			for _, trade := range raw.Trades {
				trade.Order = raw.ID
				trade.Timestamp += 10 // after bootstrap, before migration
			}
			if baseline.kind != "LegacyMigration" {
				for _, trade := range raw.Trades {
					trade.Timestamp -= 10
				}
			}
			return raw, nil
		}
		hint := externalHint()
		hint.UnassignedExchangeID, hint.ExternalOrder.ExchangeID = "older-unseen", "older-unseen"
		if err := borrow.RecoverExternalReport(context.Background(), hint); err == nil {
			t.Fatal("unseen pre-baseline history was booked twice")
		}
		after, err := s.Snapshot(context.Background())
		if err != nil || !reflect.DeepEqual(before, after) {
			t.Fatal("historical rejection changed account", before, after, err)
		}
	}
}

func TestBanexgReportsOnlyOfferUnknownIdentityRecovery(t *testing.T) {
	s, _, exchange, _, config := banexgFixture(t)
	adapter, err := NewBanexgAdapter(context.Background(), exchange, config)
	if err != nil {
		t.Fatal(err)
	}
	order := planOrder(t, s, "owned", 1, Buy, EntryIntent, map[string]int64{"a": 1})
	stored, err := s.Order(context.Background(), order.ID)
	if err != nil {
		t.Fatal(err)
	}
	exchange.trades = make(chan *banexg.MyTrade, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reports, err := adapter.Reports(ctx)
	if err != nil {
		t.Fatal(err)
	}
	exchange.trades <- &banexg.MyTrade{ClientID: "manual-client", Trade: banexg.Trade{Order: "manual-order", Symbol: "BTC/USDT:USDT", Amount: 999}}
	exchange.trades <- &banexg.MyTrade{ClientID: stored.ClientID, Trade: banexg.Trade{Order: "manual-order", Symbol: "BTC/USDT:USDT"}}
	for index := 0; index < 2; index++ {
		select {
		case report := <-reports:
			if index == 0 && (report.ExternalOrder == nil || report.ExternalOrder.Symbol != "BTC/USDT:USDT" || !errors.Is(report.Err, sql.ErrNoRows)) {
				t.Fatal("unknown identity lost complete-query hint", report)
			}
			if index == 1 && (report.ExternalOrder != nil || report.Err == nil) {
				t.Fatal("conflicting known identity offered external recovery", report)
			}
		case <-time.After(time.Second):
			t.Fatal("private hint translation stalled")
		}
	}
}
