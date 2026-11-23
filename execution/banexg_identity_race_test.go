package execution

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

func nextIdentityReport(t *testing.T, reports <-chan BanexgStreamReport) BanexgStreamReport {
	t.Helper()
	select {
	case r := <-reports:
		return r
	case <-time.After(2 * time.Second):
		t.Fatal("private stream report stalled")
	}
	return BanexgStreamReport{}
}

// A private-stream trade can outrun the submit receipt of an instantly filled
// order. With our client ID it is the same order, not a conflicting identity.
func TestBanexgReportsAssignEarlyExchangeIDToOurClientOrder(t *testing.T) {
	s, _, exchange, _, config := banexgFixture(t)
	adapter, err := NewBanexgAdapter(context.Background(), exchange, config)
	if err != nil {
		t.Fatal(err)
	}
	intent := planOrder(t, s, "early-id", 1, Buy, EntryIntent, map[string]int64{"strategy": 10})
	if err := s.PrepareOrder(context.Background(), intent, 10); err != nil {
		t.Fatal(err)
	}
	stored, _ := s.Order(context.Background(), intent.ID)
	if stored.ExchangeID != "" {
		t.Fatal("fixture order must have no recorded exchange ID", stored.ExchangeID)
	}
	exchange.trades = make(chan *banexg.MyTrade, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reports, err := adapter.Reports(ctx)
	if err != nil {
		t.Fatal(err)
	}
	exchange.trades <- &banexg.MyTrade{ClientID: stored.ClientID, Trade: banexg.Trade{Symbol: "BTC/USDT:USDT", Order: "venue-early"}}
	r := nextIdentityReport(t, reports)
	if r.Err != nil || r.OrderID != intent.ID || r.ExternalOrder != nil || r.UnassignedExchangeID != "" {
		t.Fatalf("early exchange ID not assigned to our order: %+v", r)
	}
	// Unknown client and unknown exchange ID: still an external hint (ErrNoRows).
	exchange.trades <- &banexg.MyTrade{ClientID: "foreign-client", Trade: banexg.Trade{Symbol: "BTC/USDT:USDT", Order: "venue-foreign"}}
	r = nextIdentityReport(t, reports)
	if !errors.Is(r.Err, sql.ErrNoRows) || r.OrderID != "" || r.UnassignedExchangeID != "venue-foreign" || r.ExternalOrder == nil || r.ExternalOrder.ExchangeID != "venue-foreign" || r.ExternalOrder.ClientID != "foreign-client" {
		t.Fatalf("unknown identity must stay an external hint: %+v", r)
	}
}

// Two different recorded exchange IDs for one client ID still conflict.
func TestBanexgReportsConflictWhenRecordedExchangeIDDiffers(t *testing.T) {
	s, h, exchange, _, config := banexgFixture(t)
	adapter, err := NewBanexgAdapter(context.Background(), exchange, config)
	if err != nil {
		t.Fatal(err)
	}
	intent := planOrder(t, s, "recorded-id", 1, Buy, EntryIntent, map[string]int64{"strategy": 10})
	if err := s.PrepareOrder(context.Background(), intent, 10); err != nil {
		t.Fatal(err)
	}
	stored, _ := s.Order(context.Background(), intent.ID)
	exchange.create = func(string, string, string, float64, float64, map[string]any) (*banexg.Order, *errs.Error) {
		return &banexg.Order{ID: "venue-recorded", ClientOrderID: stored.ClientID, Status: banexg.OdStatusOpen}, nil
	}
	owner := &OwnerExecutor{Store: s, Handle: h, Token: h.Token(), Adapter: adapter}
	if err := owner.Send(intent.ID, 11); err != nil {
		t.Fatal(err)
	}
	if recorded, _ := s.Order(context.Background(), intent.ID); recorded.ExchangeID != "venue-recorded" {
		t.Fatal("submit receipt did not record the exchange ID", recorded.ExchangeID)
	}
	exchange.trades = make(chan *banexg.MyTrade, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reports, err := adapter.Reports(ctx)
	if err != nil {
		t.Fatal(err)
	}
	exchange.trades <- &banexg.MyTrade{ClientID: stored.ClientID, Trade: banexg.Trade{Symbol: "BTC/USDT:USDT", Order: "venue-other"}}
	r := nextIdentityReport(t, reports)
	if r.Err == nil || !strings.Contains(r.Err.Error(), "conflicting banexg order identity") || r.OrderID != "" || r.ExternalOrder != nil {
		t.Fatalf("different recorded exchange ID must conflict: %+v", r)
	}
	// The recorded ID itself still resolves to the order.
	exchange.trades <- &banexg.MyTrade{ClientID: stored.ClientID, Trade: banexg.Trade{Symbol: "BTC/USDT:USDT", Order: "venue-recorded"}}
	if r = nextIdentityReport(t, reports); r.Err != nil || r.OrderID != intent.ID {
		t.Fatalf("recorded identity must resolve: %+v", r)
	}
}
