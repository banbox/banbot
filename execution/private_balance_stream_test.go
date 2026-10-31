package execution

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/shopspring/decimal"
)

func nextPrivateReport(t *testing.T, reports <-chan BanexgStreamReport) BanexgStreamReport {
	t.Helper()
	select {
	case report, ok := <-reports:
		if !ok {
			t.Fatal("private source closed before report")
		}
		return report
	case <-time.After(time.Second):
		t.Fatal("private report stalled")
	}
	return BanexgStreamReport{}
}

func TestBanexgBalanceAndZeroFillStatusAreRecoveryHints(t *testing.T) {
	store, _, exchange, _, config := banexgFixture(t)
	intent := planOrder(t, store, "balance-status", 1, Buy, EntryIntent, map[string]int64{"strategy": 1})
	if err := store.PrepareOrder(context.Background(), intent, 10); err != nil {
		t.Fatal(err)
	}
	stored, _ := store.Order(context.Background(), intent.ID)
	exchange.trades = make(chan *banexg.MyTrade, 1)
	exchange.balanceStream = make(chan *banexg.Balances, 1)
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
	// Stream payloads may be partial/mutable. They must never supply ledger cash.
	exchange.balanceStream <- &banexg.Balances{Total: map[string]float64{"USDT": 999999}}
	report := nextPrivateReport(t, reports)
	if !report.AccountChanged || report.Err != nil || report.OrderID != "" {
		t.Fatal(report)
	}
	if exchange.snapshots != 0 {
		t.Fatal("translation performed account reconciliation outside its owner")
	}
	exchange.trades <- &banexg.MyTrade{ClientID: stored.ClientID, State: banexg.OdStatusCanceled}
	report = nextPrivateReport(t, reports)
	if report.OrderID != intent.ID || report.AccountChanged || report.Err != nil {
		t.Fatal("zero-fill order status lost its recovery identity", report)
	}
	current, err := store.Order(ctx, intent.ID)
	if err != nil || current.State != OrderPrepared {
		t.Fatal("stream status directly mutated ledger", current, err)
	}
	cancel()
	select {
	case _, ok := <-reports:
		if ok {
			t.Fatal("canceled source still emitted")
		}
	case <-time.After(time.Second):
		t.Fatal("canceled source did not close")
	}
}

func TestBanexgPrivateSourceClosureCancelsSiblingSubscription(t *testing.T) {
	for _, source := range []string{"trade", "balance"} {
		t.Run(source, func(t *testing.T) {
			_, _, exchange, _, config := banexgFixture(t)
			exchange.trades = make(chan *banexg.MyTrade)
			balanceStream := make(chan *banexg.Balances)
			var subscriptionCtx context.Context
			exchange.watchBalance = func(params map[string]any) (chan *banexg.Balances, *errs.Error) {
				subscriptionCtx = params[banexg.ParamContext].(context.Context)
				return balanceStream, nil
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
			if source == "trade" {
				close(exchange.trades)
			} else {
				close(balanceStream)
			}
			report := nextPrivateReport(t, reports)
			if report.Err == nil || report.AccountChanged {
				t.Fatal("closed private source did not fence admission", report)
			}
			select {
			case _, ok := <-reports:
				if ok {
					t.Fatal("closed source did not finish")
				}
			case <-time.After(time.Second):
				t.Fatal("closed source leaked translator")
			}
			if subscriptionCtx.Err() == nil {
				t.Fatal("sibling subscription not canceled")
			}
		})
	}
}

func TestBanexgBalanceRegistrationFailureCancelsAcceptedTradeSource(t *testing.T) {
	for _, mode := range []string{"error", "nil"} {
		t.Run(mode, func(t *testing.T) {
			_, _, exchange, _, config := banexgFixture(t)
			exchange.trades = make(chan *banexg.MyTrade)
			var subscriptionCtx context.Context
			exchange.watchBalance = func(params map[string]any) (chan *banexg.Balances, *errs.Error) {
				subscriptionCtx = params[banexg.ParamContext].(context.Context)
				if mode == "error" {
					return nil, errs.NewMsg(errs.CodeRunTime, "balance setup failed")
				}
				return nil, nil
			}
			adapter, err := NewBanexgAdapter(context.Background(), exchange, config)
			if err != nil {
				t.Fatal(err)
			}
			reports, err := adapter.Reports(context.Background())
			if err == nil || reports != nil || subscriptionCtx.Err() == nil {
				t.Fatal("failed optional registration retained an accepted source", reports, err)
			}
		})
	}
}

func TestBanexgNilBalanceReportFailsClosed(t *testing.T) {
	_, _, exchange, _, config := banexgFixture(t)
	exchange.trades = make(chan *banexg.MyTrade)
	exchange.balanceStream = make(chan *banexg.Balances, 1)
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
	exchange.balanceStream <- nil
	report := nextPrivateReport(t, reports)
	if report.Err == nil || report.AccountChanged {
		t.Fatal("nil balance fabricated a valid observation", report)
	}
}

type balanceWakeAdapter struct {
	*fakeExecutionAdapter
	reports      chan BanexgStreamReport
	snapshots    chan struct{}
	failSnapshot bool
}

func (a *balanceWakeAdapter) Reports(ctx context.Context) (<-chan BanexgStreamReport, error) {
	go func() { <-ctx.Done(); close(a.reports) }()
	return a.reports, nil
}
func (a *balanceWakeAdapter) Snapshot(context.Context) (VenueSnapshot, error) {
	a.snapshots <- struct{}{}
	if a.failSnapshot {
		return VenueSnapshot{}, errors.New("authoritative balance unavailable")
	}
	return VenueSnapshot{Cash: "1000", Positions: map[string]int64{}}, nil
}

func TestPrivateBalanceWakeupReconcilesAuthoritativeAccount(t *testing.T) {
	for _, failSnapshot := range []bool{false, true} {
		t.Run(map[bool]string{false: "reconcile", true: "failure-freezes"}[failSnapshot], func(t *testing.T) {
			registry := &AccountRegistry{}
			owner, err := registry.Acquire(testIntent(Buy).Account)
			if err != nil {
				t.Fatal(err)
			}
			adapter := &balanceWakeAdapter{fakeExecutionAdapter: &fakeExecutionAdapter{}, reports: make(chan BanexgStreamReport, 1), snapshots: make(chan struct{}, 4)}
			service, err := NewSharedAccount(owner, SharedExecutionOptions{StorePath: filepath.Join(t.TempDir(), "execution.db"), SenderLeaseDir: t.TempDir(), Adapter: adapter, AuthoritativeSnapshot: true})
			if err != nil {
				t.Fatal(err)
			}
			borrow := service.Borrow()
			defer func() { borrow.Release(); registry.Close(); _ = service.Close() }()
			if err := borrow.BootstrapCapital(context.Background(), map[StrategyID]decimal.Decimal{"strategy": decimal.NewFromInt(1000)}, 1); err != nil {
				t.Fatal(err)
			}
			<-adapter.snapshots // Discard the bootstrap snapshot, before starting the worker.
			adapter.failSnapshot = failSnapshot
			committed := make(chan struct{}, 4)
			unsubscribe, err := borrow.SubscribeCommitted(func() { committed <- struct{}{} })
			if err != nil {
				t.Fatal(err)
			}
			defer unsubscribe()
			if err := borrow.StartReports(); err != nil {
				t.Fatal(err)
			}
			reportErrors, err := borrow.ReportErrors()
			if err != nil {
				t.Fatal(err)
			}
			adapter.reports <- BanexgStreamReport{AccountChanged: true}
			select {
			case <-adapter.snapshots:
			case <-time.After(time.Second):
				t.Fatal("balance wakeup did not query authoritative snapshot")
			}
			if failSnapshot {
				select {
				case err := <-reportErrors:
					if err == nil || !strings.Contains(err.Error(), "authoritative balance unavailable") {
						t.Fatal(err)
					}
				case <-time.After(time.Second):
					t.Fatal("failed snapshot did not freeze")
				}
				select {
				case <-service.reportDone:
				case <-time.After(time.Second):
					t.Fatal("failed account recovery did not cancel/join source")
				}
			} else {
				select {
				case <-committed:
				case <-time.After(time.Second):
					t.Fatal("balance reconciliation not projected")
				}
			}
			snapshot, err := borrow.Snapshot(context.Background())
			if err != nil || snapshot.RiskFrozen != failSnapshot || !snapshot.AccountSettledCash.Equal(decimal.NewFromInt(1000)) {
				t.Fatal("account wakeup bypassed authoritative reconciliation", snapshot, err)
			}
			select {
			case err := <-reportErrors:
				t.Fatal("valid balance wakeup treated as unassigned order", err)
			default:
			}
		})
	}
}
