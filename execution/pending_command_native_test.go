package execution

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strconv"
	"testing"
)

func pendingNativeCommandBorrow(t *testing.T, service *SharedAccount) *SharedAccountBorrow {
	t.Helper()
	borrow := service.Borrow()
	t.Cleanup(borrow.Release)
	if err := borrow.RegisterRiskPolicy("native-command-test", domainRisk()); err != nil {
		t.Fatal(err)
	}
	if err := borrow.RegisterAccountQuotes([]Instrument{ledgerInstrument()}, func(_ context.Context, _ string, now int64) (VisibleQuote, error) {
		return VisibleQuote{Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: now, ReceivedMS: now, ValidUntilMS: 1000, Bar: now}, nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	return borrow
}

func TestPendingCommandProtectionReplacementSettlesNativeFinalFill(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		t.Run(backend, func(t *testing.T) {
			for _, filled := range []int64{4, 10} {
				t.Run(strconv.FormatInt(filled, 10), func(t *testing.T) {
					ctx := context.Background()
					service, adapter, definitions := pendingNativeFixture(t)
					order := coordinateNative(t, service, definitions)
					borrow := pendingNativeCommandBorrow(t, service)
					adapter.query = func(context.Context, string, string) (QueryResult, error) {
						return QueryResult{Found: true, Complete: true, Authoritative: true, Canceled: true, Receipt: SubmitReceipt{ExchangeID: order.ExchangeID, Fills: []FillReport{{EventID: "command-native-final", OrderID: order.Intent.ID, Steps: filled, Price: intentPrice("95"), Cost: intentPrice("9.5").Mul(intentPrice(strconv.FormatInt(filled, 10))), Cumulative: true, AtMS: 14}}}}, nil
					}
					command := PendingCommand{ID: "replace-native", Strategy: "a", Lot: "lot-a", Action: PendingSetProtections,
						Protections: []PositionProtection{{Kind: TrailingProtection, TrailingPercent: intentPrice("5")}}}
					if revision, err := borrow.ApplyPendingCommand(ctx, command, 14); err != nil || revision != 1 {
						t.Fatal(revision, err)
					}
					book, err := borrow.PendingOrders(ctx)
					if err != nil || len(book) != 1 {
						t.Fatal(book, err)
					}
					pending := book[0].Order
					if pending.Desired != 10-filled || !pending.Canceled || pending.NativeProtectionID != "" || book[0].CommandRevision != 1 || len(book[0].Specs) != 1 || book[0].Specs[0].Kind != TrailingProtection || !book[0].Specs[0].TrailingPercent.Equal(intentPrice("5")) {
						t.Fatal("replacement ignored final native fill or lost command state", book)
					}
					plan, err := borrow.LatestPlan(ctx)
					if err != nil {
						t.Fatal(err)
					}
					found := false
					for _, target := range plan.Targets {
						if target.Strategy == "a" && target.Lot == "lot-a" {
							found = true
							if target.SignedSteps != 10-filled {
								t.Fatal("replacement plan reopens native reduction", plan.Targets)
							}
						}
					}
					if !found {
						t.Fatal("replacement omitted retained lot target", plan.Targets)
					}
					final, err := service.store.Order(ctx, order.Intent.ID)
					if err != nil || final.FilledSteps != filled || !terminalOrder(string(final.State)) {
						t.Fatal(final, err)
					}
					before, err := borrow.Snapshot(ctx)
					if err != nil {
						t.Fatal(err)
					}
					var held int64
					for _, lot := range before.Lots {
						if lot.Strategy == "a" && lot.ID == "lot-a" {
							held += lot.SignedSteps
						}
					}
					if held != 10-filled {
						t.Fatal("native final fill not settled in attributed inventory", before.Lots)
					}
					calls := adapter.calls()
					if revision, err := borrow.ApplyPendingCommand(ctx, command, 15); err != nil || revision != 1 {
						t.Fatal(revision, err)
					}
					after, err := borrow.Snapshot(ctx)
					if err != nil || !reflect.DeepEqual(before, after) || !reflect.DeepEqual(calls, adapter.calls()) {
						t.Fatal("command replay repeated native fill or venue operations")
					}
				})
			}
		})
	}
}

func TestPendingCommandUncertainNativeCancellationRetainsProtection(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			service, adapter, definitions := pendingNativeFixture(t)
			order := coordinateNative(t, service, definitions)
			borrow := pendingNativeCommandBorrow(t, service)
			bookBefore, err := borrow.PendingOrders(ctx)
			if err != nil {
				t.Fatal(err)
			}
			planBefore, err := borrow.LatestPlan(ctx)
			if err != nil {
				t.Fatal(err)
			}
			adapter.query = func(context.Context, string, string) (QueryResult, error) {
				return QueryResult{Found: true, Complete: true, Authoritative: true, Receipt: SubmitReceipt{ExchangeID: order.ExchangeID}}, nil
			}
			adapter.cancel = func(context.Context, string) (bool, error) { return false, context.DeadlineExceeded }
			command := PendingCommand{ID: "uncertain-native-replace", Strategy: "a", Lot: "lot-a", Action: PendingSetProtections,
				Protections: []PositionProtection{{Kind: TrailingProtection, TrailingPercent: intentPrice("5")}}}
			if _, err := borrow.ApplyPendingCommand(ctx, command, 14); err == nil {
				t.Fatal("uncertain native cancellation accepted replacement")
			}
			bookAfter, err := borrow.PendingOrders(ctx)
			if err != nil || !reflect.DeepEqual(bookBefore, bookAfter) {
				t.Fatal("uncertain cancellation changed protection configuration", bookAfter, err)
			}
			planAfter, err := borrow.LatestPlan(ctx)
			if err != nil || !reflect.DeepEqual(planBefore, planAfter) {
				t.Fatal("uncertain cancellation admitted replacement plan", planAfter, err)
			}
			if _, err := service.store.StrategyCheckpoint(ctx, command.Strategy, "pending-command:"+command.ID); !errors.Is(err, sql.ErrNoRows) {
				t.Fatal("failed cancellation consumed command identity", err)
			}
			current, err := service.store.Order(ctx, order.Intent.ID)
			if err != nil || terminalOrder(string(current.State)) {
				t.Fatal("uncertain cancellation invented terminal protection", current, err)
			}
		})
	}
}

func TestPendingNativeCancelFinalFillPreservesPersistedExplicitExitGoal(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		t.Run(backend, func(t *testing.T) {
			for _, goal := range []int64{0, 3, 8} {
				t.Run(strconv.FormatInt(goal, 10), func(t *testing.T) {
					ctx := context.Background()
					service, adapter, definitions := pendingNativeFixture(t)
					order := coordinateNative(t, service, definitions)
					pending := &definitions[0].Order
					pending.Desired = goal
					pending.Canceled = true
					pending.LastProtection = TakeProfitProtection
					pending.Exit = &EligibleIntent{ID: "persisted-explicit-exit", Account: service.AccountKey(), Strategy: "a", Lot: "lot-a", Instrument: "BTC", Kind: ExitIntent, Side: Sell, QuantitySteps: 10 - goal, State: Eligible}
					saveTestPendingBook(t, service.store, definitions)
					adapter.query = func(context.Context, string, string) (QueryResult, error) {
						return QueryResult{Found: true, Complete: true, Authoritative: true, Canceled: true, Receipt: SubmitReceipt{ExchangeID: order.ExchangeID, Fills: []FillReport{{EventID: "persisted-exit-native-final", OrderID: order.Intent.ID, Steps: 4, Price: intentPrice("95"), Cost: intentPrice("38"), Cumulative: true, AtMS: 14}}}}, nil
					}
					if err := service.cancelPendingOrder(ctx, order.Intent.ID, 14); err != nil {
						t.Fatal(err)
					}
					// Read only the committed book: a crash can discard the command's
					// in-memory copy before its replacement plan is admitted.
					book, err := service.store.LoadPendingBook(ctx)
					if err != nil || len(book) != 1 {
						t.Fatal(book, err)
					}
					want := min(goal, int64(6))
					stored := book[0].Order
					if stored.Desired != want || stored.Exit == nil || stored.Exit.ID != "persisted-explicit-exit" || stored.LastProtection != TakeProfitProtection {
						t.Fatalf("native final fill replaced persisted explicit exit goal: goal=%d want=%d pending=%+v", goal, want, stored)
					}
					latest, err := service.store.LatestPlan(ctx)
					if err != nil {
						t.Fatal(err)
					}
					targets, err := service.nativeFillTargets(ctx, latest, latest.Targets)
					if err != nil {
						t.Fatal(err)
					}
					for _, target := range targets {
						if target.Strategy == "a" && target.Lot == "lot-a" && target.SignedSteps != want {
							t.Fatal("reloaded target lost explicit exit goal", targets)
						}
					}
				})
			}
		})
	}
}

func TestPendingCommandSetExitSettlesNativeFinalFill(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		t.Run(backend, func(t *testing.T) {
			for _, filled := range []int64{4, 10} {
				for _, reduction := range []int64{0, 3} {
					t.Run(strconv.FormatInt(filled, 10)+"/"+strconv.FormatInt(reduction, 10), func(t *testing.T) {
						ctx := context.Background()
						service, adapter, definitions := pendingNativeFixture(t)
						order := coordinateNative(t, service, definitions)
						borrow := pendingNativeCommandBorrow(t, service)
						adapter.query = func(context.Context, string, string) (QueryResult, error) {
							return QueryResult{Found: true, Complete: true, Authoritative: true, Canceled: true, Receipt: SubmitReceipt{ExchangeID: order.ExchangeID, Fills: []FillReport{{EventID: "set-exit-native-final", OrderID: order.Intent.ID, Steps: filled, Price: intentPrice("95"), Cost: intentPrice("9.5").Mul(intentPrice(strconv.FormatInt(filled, 10))), Cumulative: true, AtMS: 14}}}}, nil
						}
						command := PendingCommand{ID: "exit-after-native", Strategy: "a", Lot: "lot-a", Action: PendingSetExit, Exit: &PendingExitSpec{QuantitySteps: reduction}}
						if revision, err := borrow.ApplyPendingCommand(ctx, command, 14); err != nil || revision != 1 {
							t.Fatal(revision, err)
						}
						remaining := 10 - filled
						want := int64(0)
						if reduction > 0 {
							want = max(int64(0), remaining-reduction)
						}
						book, err := borrow.PendingOrders(ctx)
						if err != nil || len(book) != 1 || book[0].Order.Desired != want || book[0].Order.NativeProtectionID != "" {
							t.Fatal("exit goal ignored native cancellation fill", book, err)
						}
						if remaining == 0 && book[0].Order.Exit != nil {
							t.Fatal("exit recreated a fully closed lot", book)
						}
						plan, err := borrow.LatestPlan(ctx)
						if err != nil {
							t.Fatal(err)
						}
						for _, target := range plan.Targets {
							if target.Strategy == "a" && target.Lot == "lot-a" && target.SignedSteps != want {
								t.Fatal("set exit plan lost post-cancel goal", plan.Targets)
							}
						}
						snapshot, err := borrow.Snapshot(ctx)
						if err != nil {
							t.Fatal(err)
						}
						var exitSteps int64
						for _, submitted := range snapshot.Orders {
							if submitted.Intent.PlanID != plan.ID {
								continue
							}
							if submitted.Intent.Side != Sell || !submitted.Intent.ReduceOnly {
								t.Fatal("exit submitted an opening or reversed order", submitted)
							}
							exitSteps += submitted.Intent.Steps
						}
						if exitSteps != remaining-want {
							t.Fatal("exit quantity not capped to post-cancel inventory", exitSteps, remaining-want)
						}
					})
				}
			}
		})
	}
}
