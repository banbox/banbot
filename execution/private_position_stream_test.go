package execution

import (
	"context"
	"testing"
	"time"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

func TestBanexgPositionPushOnlyWakesAuthoritativeRecovery(t *testing.T) {
	store, _, exchange, _, config := banexgFixture(t)
	exchange.trades = make(chan *banexg.MyTrade)
	exchange.positionStream = make(chan []*banexg.Position, 2)
	adapter, err := NewBanexgAdapter(context.Background(), exchange, config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reports, err := adapter.Reports(ctx)
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// A nonsensical partial position is a wake-up, not trusted account state.
	exchange.positionStream <- []*banexg.Position{{Symbol: "unknown", Contracts: 999999, Hedged: true}}
	// A complete close can also be represented by an empty non-nil update.
	exchange.positionStream <- []*banexg.Position{}
	for range 2 {
		report := nextPrivateReport(t, reports)
		if !report.AccountChanged || report.Err != nil || report.OrderID != "" {
			t.Fatalf("position update was not a recovery hint: %+v", report)
		}
	}
	after, err := store.Snapshot(ctx)
	if err != nil || after.Checkpoint != before.Checkpoint || len(after.ActualPositions) != len(before.ActualPositions) || exchange.snapshots != 0 {
		t.Fatalf("push changed accounting or bypassed account owner: %+v %v", after, err)
	}
}

func TestBanexgPositionSubscriptionFailureCancelsSiblingSources(t *testing.T) {
	for _, mode := range []string{"error", "nil"} {
		t.Run(mode, func(t *testing.T) {
			_, _, exchange, _, config := banexgFixture(t)
			exchange.trades = make(chan *banexg.MyTrade)
			var streamCtx context.Context
			exchange.watchPositions = func(params map[string]any) (chan []*banexg.Position, *errs.Error) {
				streamCtx = params[banexg.ParamContext].(context.Context)
				if mode == "error" {
					return nil, errs.NewMsg(errs.CodeRunTime, "private position subscription rejected")
				}
				return nil, nil
			}
			adapter, err := NewBanexgAdapter(context.Background(), exchange, config)
			if err != nil {
				t.Fatal(err)
			}
			reports, err := adapter.Reports(context.Background())
			if err == nil || reports != nil || streamCtx == nil || streamCtx.Err() == nil {
				t.Fatal("failed position setup leaked sibling subscriptions", reports, err)
			}
		})
	}
}

func TestBanexgPositionSourceClosureAndNilPayloadFenceAdmission(t *testing.T) {
	for _, mode := range []string{"closed", "nil"} {
		t.Run(mode, func(t *testing.T) {
			_, _, exchange, _, config := banexgFixture(t)
			exchange.trades = make(chan *banexg.MyTrade)
			position := make(chan []*banexg.Position, 1)
			var streamCtx context.Context
			exchange.watchPositions = func(params map[string]any) (chan []*banexg.Position, *errs.Error) {
				streamCtx = params[banexg.ParamContext].(context.Context)
				return position, nil
			}
			adapter, err := NewBanexgAdapter(context.Background(), exchange, config)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reports, err := adapter.Reports(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "closed" {
				close(position)
			} else {
				position <- nil
			}
			report := nextPrivateReport(t, reports)
			if report.Err == nil || report.AccountChanged {
				t.Fatal("invalid position source did not fence admission", report)
			}
			if mode == "nil" {
				cancel()
			}
			select {
			case _, ok := <-reports:
				if ok || streamCtx.Err() == nil {
					t.Fatal("position failure retained subscriptions")
				}
			case <-time.After(time.Second):
				t.Fatal("position translator did not stop")
			}
		})
	}
}
