package execution

import (
	"context"
	"math"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/shopspring/decimal"
)

func TestPaperExactCostBasisMatchesStore(t *testing.T) {
	for _, option := range []bool{false, true} {
		for _, short := range []bool{false, true} {
			t.Run(map[bool]string{false: "perpetual", true: "option"}[option]+map[bool]string{false: "long", true: "short"}[short], func(t *testing.T) {
				ctx := context.Background()
				s, err := OpenStoreWithLeaseDir(filepath.Join(t.TempDir(), "ledger.db"), testIntent(Buy).Account, t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				registry := &AccountRegistry{}
				h, err := registry.Acquire(s.key)
				if err != nil {
					t.Fatal(err)
				}
				defer registry.Close()
				fundStrategies(t, s)
				paper, _ := NewPaperAdapter(intentPrice("1000"), intentPrice("0.001"), decimal.Zero)
				i := ledgerInstrument()
				if option {
					i = cashOption("call")
					i.QuantityStep = intentPrice("0.1")
				}
				opening, closing := Buy, Sell
				if short {
					opening, closing = Sell, Buy
				}
				trades := []struct {
					side  OrderSide
					steps int64
					price string
				}{{opening, 2, "100"}, {opening, 1, "101"}, {closing, 1, "110"}, {closing, 3, "111"}, {opening, 1, "112"}, {opening, 1, "113"}, {opening, 2, "114"}}
				for n, trade := range trades {
					o := optionOrder(t, s, h, i, int64(n+1), trade.side, trade.steps, trade.price)
					o.SubmitAtMS = int64(n+1) * 10
					receipt, err := paper.Submit(ctx, o, "client-"+o.ID)
					if err != nil {
						t.Fatal(err)
					}
					if applied, err := s.ApplyFill(ctx, receipt.Fills[0]); err != nil || !applied {
						t.Fatal(applied, err)
					}
					ledger, err := s.Snapshot(ctx)
					if err != nil {
						t.Fatal(err)
					}
					venue, _ := paper.Snapshot(ctx)
					if venue.Cash != ledger.AccountSettledCash.String() {
						t.Fatalf("trade %d cash differs: paper=%s store=%s", n, venue.Cash, ledger.AccountSettledCash)
					}
				}
				if option {
					event := OptionExpiry{Instrument: i, Fixing: intentPrice("120"), FixingAtMS: 1000, PublishedMS: 1001, AtMS: 1001}
					if _, err := s.ApplyOptionExpiry(ctx, event); err != nil {
						t.Fatal(err)
					}
					if _, err := paper.ApplyOptionExpiry(ctx, event); err != nil {
						t.Fatal(err)
					}
				} else {
					event := TerminalSettlement{Source: "test", SourceVersion: "v1", Instrument: i, CashValue: intentPrice("60"), EventMS: 1000, PublishedMS: 1001, ReceivedMS: 1001, AtMS: 1001}
					if _, err := s.ApplyTerminalSettlement(ctx, event); err != nil {
						t.Fatal(err)
					}
					if _, err := paper.ApplyTerminalSettlement(ctx, event); err != nil {
						t.Fatal(err)
					}
				}
				ledger, err := s.Snapshot(ctx)
				if err != nil {
					t.Fatal(err)
				}
				venue, _ := paper.Snapshot(ctx)
				if venue.Cash != ledger.AccountSettledCash.String() || venue.Positions[i.ID] != 0 {
					t.Fatal("settlement differs", venue, ledger.AccountSettledCash)
				}
			})
		}
	}
}

func TestPaperPositionOverflowIsAtomic(t *testing.T) {
	for _, side := range []OrderSide{Buy, Sell} {
		paper, _ := NewPaperAdapter(intentPrice("1000"), decimal.Zero, decimal.Zero)
		o := OrderIntent{ID: "max", Instrument: ledgerInstrument(), Side: side, Steps: math.MaxInt64, Observation: ExecutionObservation{Price: intentPrice("100"), AtMS: 10}, SubmitAtMS: 10}
		if _, err := paper.Submit(context.Background(), o, "max-client"); err != nil {
			t.Fatal(err)
		}
		before, _ := paper.Snapshot(context.Background())
		metrics := paper.Metrics()
		position := paper.positions[o.Instrument.ID]
		o.ID, o.Steps = "overflow", 2
		if _, err := paper.Submit(context.Background(), o, "overflow-client"); err == nil {
			t.Fatal("position overflow accepted")
		}
		after, _ := paper.Snapshot(context.Background())
		if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(metrics, paper.Metrics()) || !reflect.DeepEqual(position, paper.positions[o.Instrument.ID]) || len(paper.orders) != 1 || len(paper.clients) != 1 {
			t.Fatal("failed fill mutated paper state")
		}
	}
}
