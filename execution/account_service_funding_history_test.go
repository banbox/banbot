package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/shopspring/decimal"
)

func TestFundingActivityInstrumentUsesTypedHistoryAndRetainsErrors(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStoreWithLeaseDir(filepath.Join(dir, "ledger.db"), AccountKey{VenueSessionIdentity: dir, Account: "test", SettlementDomain: "USDT"}, filepath.Join(dir, "leases"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	instrument := Instrument{ID: "ETH", Version: "v1", Valuation: "linear_perpetual", SettlementCurrency: "USDT", QuantityStep: decimal.RequireFromString("0.1"), ContractSize: decimal.NewFromInt(1), PriceTick: decimal.NewFromInt(1), MoneyScale: 3}
	for _, kind := range []string{"InternalFill", string(ExternalCashChange), string(Liquidation)} {
		var body []byte
		if kind == "InternalFill" {
			body, _ = json.Marshal(InternalMatch{Instrument: instrument})
		} else {
			body, _ = json.Marshal(ExternalPositionEvent{Instrument: instrument})
		}
		got, err := fundingActivityInstrument(context.Background(), store, CommittedEvent{Kind: kind, Payload: body})
		if err != nil || got.ID != "ETH" {
			t.Fatal("typed instrument lost", kind, got, err)
		}
	}
	for _, event := range []CommittedEvent{{Kind: "ExchangeFill", Payload: []byte("{")}, {Kind: "ExchangeFill", Payload: []byte("{}")}, {Kind: string(ExternalCashChange), Payload: []byte("{}")}, {Kind: "InternalFill", Payload: []byte("[]")}, {Kind: "unknown", Payload: []byte("{}")}} {
		if _, err := fundingActivityInstrument(context.Background(), store, event); err == nil {
			t.Fatal("unclassifiable activity guessed", event)
		}
	}
	missing, _ := json.Marshal(FillReport{OrderID: "missing-native-order"})
	event := CommittedEvent{Kind: "ExchangeFill", Payload: missing}
	if _, err := fundingActivityInstrument(context.Background(), store, event); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("missing order error replaced", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := fundingActivityInstrument(ctx, store, event); !errors.Is(err, context.Canceled) {
		t.Fatal("storage cancellation treated as missing metadata", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := fundingActivityInstrument(context.Background(), store, event); err == nil || !strings.Contains(err.Error(), "store closed") {
		t.Fatal("store error replaced by instrument guess", err)
	}
}
