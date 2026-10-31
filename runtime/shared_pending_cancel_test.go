package runtime

import (
	"context"
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

func TestSharedUnfilledEntryCancellationKeepsValidTerminalIntent(t *testing.T) {
	for _, mode := range []execution.OrderMode{execution.LocalFirst, execution.ExchangeFirst} {
		t.Run(string(mode), func(t *testing.T) {
			f := newSharedTriggerFixture(t)
			f.bridge.OrderPolicy.Mode = mode
			od := f.entry(t, &strat.EnterReq{Tag: "unfilled-cancel", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90})
			if _, err := f.manager.ExitOrder(od, &strat.ExitReq{Tag: "cancel", UnFillOnly: true}); err != nil {
				t.Fatal("completely unfilled entry cancellation failed", err)
			}
			if err := f.rt.SharedExecution().WithState(func(account *execution.SharedAccount) error {
				book, err := account.Store().LoadPendingBook(context.Background())
				if err != nil {
					return err
				}
				for _, definition := range book {
					if err := definition.Order.Entry.Validate(); err != nil {
						t.Fatal("cancellation persisted invalid entry", err)
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			f.price, f.bar = decimal.NewFromInt(80), 2
			f.rt.Clock.SetTimeMS(102)
			if err := f.rt.SharedExecution().AdvancePendingExecution(context.Background(), 102); err != nil {
				t.Fatal(err)
			}
			if f.steps(t) != 0 || f.adapter.Metrics().Fills != 0 || od.Enter.Filled != 0 {
				t.Fatal("canceled entry resurrected when its original limit became marketable")
			}
		})
	}
}

func TestSharedPartialUnfilledCancellationPreservesRemainingEntry(t *testing.T) {
	f := newSharedTriggerFixture(t)
	od := f.entry(t, &strat.EnterReq{Tag: "partial-unfilled-cancel", Amount: 1, OrderType: core.OrderTypeLimit, Limit: 90})
	if _, err := f.manager.ExitOrder(od, &strat.ExitReq{Tag: "cancel-half", UnFillOnly: true, Amount: 0.5}); err != nil {
		t.Fatal(err)
	}
	f.price, f.bar = decimal.NewFromInt(80), 2
	f.rt.Clock.SetTimeMS(102)
	if err := f.rt.SharedExecution().AdvancePendingExecution(context.Background(), 102); err != nil {
		t.Fatal(err)
	}
	if f.steps(t) != 5 || od.Enter.Filled != 0.5 || f.adapter.Metrics().Fills != 1 {
		t.Fatalf("partial cancellation changed remaining entry: steps=%d filled=%v venue=%+v", f.steps(t), od.Enter.Filled, f.adapter.Metrics())
	}
}
