package execution

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
)

func coreConditionalService(t *testing.T) (*SharedAccount, *coreConditionalAdapter, ConditionalReservation) {
	t.Helper()
	store, executor, adapter, _, reservation := coreConditionalFixture(t)
	risk := domainRisk()
	if err := store.RegisterAccountPolicy(context.Background(), AccountPolicy{Version: "native-test", Currency: "USDT", MarginRate: risk.MarginRate, MaxAccountMargin: risk.MaxAccountMargin, MaxVirtualGross: risk.MaxVirtualGross, StrategyGrossLimits: risk.StrategyGrossLimits}); err != nil {
		t.Fatal(err)
	}
	service := &SharedAccount{ctx: context.Background(), store: store, executor: executor, owner: executor.Handle, ready: true,
		markets: map[string]accountMarket{"BTC": {instrument: ledgerInstrument(), quote: func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
			return VisibleQuote{Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: now, ReceivedMS: now, ValidUntilMS: 1000, Bar: now}, nil
		}}}}
	return service, adapter, reservation
}

func TestNativeRetainedTargetsFollowEconomicFill(t *testing.T) {
	for _, zeroTarget := range []bool{false, true} {
		name := "Nonzero"
		if zeroTarget {
			name = "WithZero"
		}
		t.Run(name, func(t *testing.T) {
			service, _, reservation := coreConditionalService(t)
			prepared := prepareCoreConditional(t, service.executor, reservation)
			if err := service.executor.Send(prepared.OrderIDs[0], 13); err != nil {
				t.Fatal(err)
			}
			if err := service.executor.ApplyTrade(FillReport{EventID: "retained-native-fill", OrderID: prepared.OrderIDs[0], Steps: 4, Price: intentPrice("95"), AtMS: 14}); err != nil {
				t.Fatal(err)
			}
			plan := prepared.Plan
			if !zeroTarget {
				plan.Targets = plan.Targets[:1]
			}
			snapshot, err := service.store.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			targets, err := service.retainedPlanTargets(context.Background(), plan, snapshot)
			if err != nil || len(targets) == 0 || targets[0].SignedSteps != 6 {
				t.Fatal("inherited target resurrects native-filled exposure", targets, err)
			}
			if prepared.Plan.Targets[0].SignedSteps != 10 {
				t.Fatal("immutable plan mutated")
			}
		})
	}
}

func TestNativeCancellationFillCannotBeOverwrittenByStalePendingCheckpoint(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			service, adapter, reservation := coreConditionalService(t)
			prepared := prepareCoreConditional(t, service.executor, reservation)
			if err := service.executor.Send(prepared.OrderIDs[0], 13); err != nil {
				t.Fatal(err)
			}
			book, err := service.store.LoadPendingBook(ctx)
			if err != nil {
				t.Fatal(err)
			}
			stale, err := PendingBookCheckpoint(book)
			if err != nil {
				t.Fatal(err)
			}
			adapter.capabilities = AdapterCapabilities{QueryClientID: true, CumulativeReports: true}
			adapter.query = func(context.Context, string, string) (QueryResult, error) {
				return QueryResult{Found: true, Complete: true, Authoritative: true, Canceled: true, Receipt: SubmitReceipt{ExchangeID: "exchange-" + prepared.OrderIDs[0], Fills: []FillReport{{EventID: "native-cancel-race", OrderID: prepared.OrderIDs[0], Steps: 4, Price: intentPrice("95"), Cost: intentPrice("38"), AtMS: 14, Cumulative: true}}}}, nil
			}
			updates := []StrategyRebalance{
				strategyRequest("a", "after-native-cancel", 14, StrategyTargetsFull, ExecutableTarget{Lot: "lot-a", SignedSteps: 10}),
				strategyRequest("b", "after-native-cancel", 14, StrategyTargetsFull, ExecutableTarget{Lot: "lot-b", SignedSteps: 1}),
			}
			result, err := service.PrepareStrategiesWithCheckpoints(updates, ctx, []StrategyCheckpoint{stale})
			if err != nil {
				t.Fatal(err)
			}
			book, err = service.store.LoadPendingBook(ctx)
			if err != nil || len(book) != 1 || book[0].Order.Desired != 6 || book[0].Order.LastNativeFill != "native-cancel-race" || !book[0].Order.Canceled {
				t.Fatal("stale pending checkpoint erased economic fill", book, err)
			}
			for _, target := range result.Plan.Targets {
				if target.Strategy == "a" && target.SignedSteps != 6 {
					t.Fatal("stale request reopens native-filled exposure", target)
				}
			}
			if updates[0].Requests[0].Targets[0].SignedSteps != 10 {
				t.Fatal("preflight mutated caller request and retry identity")
			}
			if _, err := service.PrepareStrategiesWithCheckpoints(updates, ctx, []StrategyCheckpoint{stale}); err != nil {
				t.Fatal("original revision cannot be retried", err)
			}
		})
	}
}

func TestNativePreflightPreservesExplicitExitGoal(t *testing.T) {
	for _, goal := range []int64{0, 3, 8, -1} {
		t.Run(strconv.FormatInt(goal, 10), func(t *testing.T) {
			ctx := context.Background()
			service, _, reservation := coreConditionalService(t)
			prepared := prepareCoreConditional(t, service.executor, reservation)
			if err := service.executor.Send(prepared.OrderIDs[0], 13); err != nil {
				t.Fatal(err)
			}
			book, err := service.store.LoadPendingBook(ctx)
			if err != nil {
				t.Fatal(err)
			}
			exit := reservation.Intent
			exit.ID = "explicit-after-native"
			book[0].Order.Exit, book[0].Order.Desired = &exit, goal
			stale, err := PendingBookCheckpoint(book)
			if err != nil {
				t.Fatal(err)
			}
			if err := service.executor.ApplyTrade(FillReport{EventID: "preflight-native-fill", OrderID: prepared.OrderIDs[0], Steps: 4, Price: intentPrice("95"), AtMS: 14}); err != nil {
				t.Fatal(err)
			}
			updates := []StrategyRebalance{strategyRequest("a", "manual-post-fill", 14, StrategyTargetsFull, ExecutableTarget{Lot: "lot-a", SignedSteps: goal})}
			merged, checkpoints, err := service.mergeNativePreflightState(ctx, updates, []StrategyCheckpoint{stale})
			if err != nil {
				t.Fatal(err)
			}
			want := max(int64(0), min(goal, int64(6)))
			if merged[0].Requests[0].Targets[0].SignedSteps != want {
				t.Fatal("native fill replaced the explicit bounded exit goal", merged)
			}
			if err := json.Unmarshal(checkpoints[0].Payload, &book); err != nil {
				t.Fatal(err)
			}
			if book[0].Order.Desired != want || book[0].Order.Exit == nil || book[0].Order.Exit.ID != exit.ID || book[0].Order.LastNativeFill != "preflight-native-fill" {
				t.Fatal("pending checkpoint lost explicit exit or native economics", book)
			}
		})
	}
}

func TestNativeTerminalCallbackHydratesStaleProtectionCaller(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			service, _, reservation := coreConditionalService(t)
			prepared := prepareCoreConditional(t, service.executor, reservation)
			if err := service.executor.Send(prepared.OrderIDs[0], 13); err != nil {
				t.Fatal(err)
			}
			stale, err := service.store.LoadPendingBook(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := service.executor.ApplyTrade(FillReport{EventID: "terminal-callback-native", OrderID: prepared.OrderIDs[0], Steps: 10, Price: intentPrice("95"), AtMS: 14}); err != nil {
				t.Fatal(err)
			}
			definition := &stale[0]
			if err := service.prepareNativeProtections(ctx, []PendingProtectionRequest{{Order: &definition.Order, Instrument: definition.Instrument, Specs: definition.Specs, Policy: definition.Policy}}, 15); err != nil {
				t.Fatal(err)
			}
			pending := definition.Order
			if pending.Desired != 0 || !pending.Canceled || pending.LastNativeFill != "terminal-callback-native" || !pending.ProtectionDone[string(StopLossProtection)] || pending.NativeProtectionID != "" {
				t.Fatal("terminal callback economics were skipped because no native order remains live", pending)
			}
		})
	}
}
