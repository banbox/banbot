package execution

import (
	"context"
	"encoding/json"
	"testing"
)

func TestPaperHistoryPreservesReceiptsAndDelayedFundingEvidence(t *testing.T) {
	t.Run("MemoryHistory", func(t *testing.T) {
		ctx := context.Background()
		borrow, venue := strategyService(t, true)
		if err := borrow.RebalanceStrategy(strategyRequest("a", "enter", 10, StrategyTargetsFull, ExecutableTarget{Lot: "first", SignedSteps: 10}), 11); err != nil {
			t.Fatal(err)
		}
		page, err := borrow.Service().Store().EventsAfter(ctx, 0, 100)
		if err != nil {
			t.Fatal(err)
		}
		var fill FillReport
		for _, event := range page {
			if event.Kind == "ExchangeFill" {
				if err := json.Unmarshal(event.Payload, &fill); err != nil {
					t.Fatal(err)
				}
				break
			}
		}
		order, err := borrow.Service().Store().Order(ctx, fill.OrderID)
		if err != nil {
			t.Fatal(err)
		}
		before, err := venue.Query(ctx, order.ClientID, order.ExchangeID)
		if err != nil || len(before.Receipt.Fills) != 1 {
			t.Fatal("paper fixture did not fill", before, err)
		}
		venue.mu.Lock()
		original := venue.orders[order.Intent.ID].intent
		venue.mu.Unlock()
		if err := borrow.RebalanceStrategy(strategyRequest("a", "reduce", 20, StrategyTargetsFull, ExecutableTarget{Lot: "first", SignedSteps: 5}), 21); err != nil {
			t.Fatal(err)
		}
		venue.mu.Lock()
		_, hot := venue.orders[order.Intent.ID]
		retained := len(venue.orders)
		venue.mu.Unlock()
		if hot || retained > 1 {
			t.Fatal("settled paper orders accumulated", hot, retained)
		}
		after, err := venue.Query(ctx, order.ClientID, order.ExchangeID)
		beforeBody, _ := payload(before)
		afterBody, _ := payload(after)
		if err != nil || beforeBody != afterBody {
			t.Fatal("cold venue query changed exact receipt", before, after, err)
		}
		receipt, err := venue.Submit(ctx, original, order.ClientID)
		receiptBody, _ := payload(receipt)
		originalBody, _ := payload(before.Receipt)
		if err != nil || receiptBody != originalBody || venue.Metrics().Fills != 2 {
			t.Fatal("cold duplicate submitted again", receipt, err, venue.Metrics())
		}
		original.Steps++
		if _, err := venue.Submit(ctx, original, order.ClientID); err == nil {
			t.Fatal("cold paper client reused with conflicting intent")
		}
		// At 12 the position was larger; charging it to today's reduced lot would
		// invent attribution. The cold fill/order evidence must still reject it.
		if _, err := borrow.ApplyFunding(FundingSettlement{ID: "late-funding", Instrument: ledgerInstrument(), Mark: intentPrice("100"), Rate: intentPrice("0.01"), AccountAmount: intentPrice("0"), AtMS: 12}); err == nil {
			t.Fatal("cold history lost the intervening position change")
		}
		if err := borrow.Service().Store().memory.history.db.Close(); err != nil {
			t.Fatal(err)
		}
		if result, err := venue.Query(ctx, order.ClientID, ""); err == nil || result.Authoritative {
			t.Fatal("cold read failure became authoritative absence", result, err)
		}
	})
}
