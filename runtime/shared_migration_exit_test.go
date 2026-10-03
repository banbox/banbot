package runtime

import (
	"context"
	"strings"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

func TestSharedCutoverRejectsUnmappedEntryAlongsideSourceExit(t *testing.T) {
	for _, terminal := range []bool{true, false} {
		name := "active-partial-entry"
		fill := .5
		if terminal {
			name = "terminal-entry"
			fill = 1
		}
		t.Run(name, func(t *testing.T) {
			row := legacyExitRow(false, .2)
			row.Enter.Filled = fill
			row.Enter.OrderID = "original-entry-exchange"
			f := semanticCutoverFixtureWithOrders(t, row, true, func(request *execution.LegacyMigration, adapter *semanticMigrationAdapter, key execution.AccountKey) {
				attachNativeExit(row)(request, adapter, key)
				request.Plans[0].Sequence = 1
				request.Plans[0].Targets[0].SignedSteps = decimal.NewFromFloat(fill - .5).Div(request.Lots[0].Instrument.QuantityStep).IntPart()
				filled := decimal.NewFromFloat(fill).Div(request.Lots[0].Instrument.QuantityStep).IntPart()
				virtual := execution.EligibleIntent{ID: "original-entry-intent", Account: key, Strategy: "ts", Lot: "old-42", Instrument: "BTC", Side: execution.Buy, Kind: execution.EntryIntent, QuantitySteps: 10, FilledSteps: filled, Conditions: execution.IntentConditions{Limit: decimal.NewFromInt(90)}}
				plan := execution.Plan{ID: "original-entry-plan", Sequence: 0, DecisionMS: 1, ExpiresMS: 1000, Intents: []execution.EligibleIntent{virtual}, Targets: []execution.ExecutableTarget{{Strategy: "ts", Lot: "old-42", Instrument: "BTC", SignedSteps: 10}}}
				intent := execution.OrderIntent{ID: "original-entry", PlanID: plan.ID, Instrument: request.Lots[0].Instrument, Side: execution.Buy, Steps: 10, Limit: decimal.NewFromInt(90), Observation: execution.ExecutionObservation{Price: decimal.NewFromInt(100), AtMS: 1, ValidUntilMS: 1000}, Allocations: []execution.FillAllocation{{ID: "original-entry-allocation", IntentID: virtual.ID, Strategy: "ts", Lot: "old-42", Side: execution.Buy, Kind: execution.EntryIntent, Steps: 10}}}
				state := execution.OrderPartial
				if terminal {
					state = execution.OrderFilled
				}
				order := execution.StoredOrder{Intent: intent, ClientID: "original-entry-client", ExchangeID: row.Enter.OrderID, State: state, FilledSteps: filled, ReportedCost: decimal.NewFromFloat(fill * 100), AllocationFilled: map[string]int64{"original-entry-allocation": filled}}
				request.Plans = append([]execution.Plan{plan}, request.Plans...)
				request.Orders = append(request.Orders, order)
				request.RawLegacyMap[0].ExOrderIDs = append(request.RawLegacyMap[0].ExOrderIDs, order.ExchangeID)
				if !terminal {
					request.VenueSnapshot.OpenOrders = append(request.VenueSnapshot.OpenOrders, execution.LegacyVenueOrder{ExchangeID: order.ExchangeID, ClientID: order.ClientID, Instrument: "BTC", Side: execution.Buy, Steps: 10, FilledSteps: filled, Cost: order.ReportedCost})
				}
				adapter.venue = request.VenueSnapshot
			})
			want := "overlapping legacy entry/exit"
			if terminal {
				want = "only completely attributed confirmed active legacy orders may migrate"
			}
			if f.cutoverErr == nil || !strings.Contains(f.cutoverErr.Error(), want) {
				t.Fatal("wrong source refusal", f.cutoverErr)
			}
		})
	}
}

func TestSharedCutoverNativeExitCallbacksPreserveEventTimeMetadata(t *testing.T) {
	for _, scenario := range []struct {
		short      bool
		correction string
	}{{false, "0"}, {true, "0"}, {false, "0.02"}, {true, "0.02"}, {false, "-0.01"}, {true, "-0.01"}} {
		short := scenario.short
		correction := decimal.RequireFromString(scenario.correction)
		name := "long"
		if short {
			name = "short"
		}
		t.Run(name+"/correction/"+scenario.correction, func(t *testing.T) {
			row := legacyExitRow(short, .2)
			f := semanticCutoverFixtureWithOrders(t, row, false, attachNativeExit(row), true)
			var managerEvents, jobEvents []*ormo.InOutOrder
			deps := f.rt.BizDeps()
			manager, err := biz.NewSharedOrderMgr(deps, deps.SharedExecution, deps.SharedOrderBridge, func(od *ormo.InOutOrder, entry bool) {
				if entry {
					t.Error("exit callback marked entry")
				}
				managerEvents = append(managerEvents, od.Clone())
			})
			if err != nil {
				t.Fatal(err)
			}
			f.manager = manager
			job := &strat.StratJob{Strat: &strat.TradeStrat{Name: "legacy"}, Symbol: &orm.ExSymbol{ID: 1, Symbol: "BTC"}, TimeFrame: "1m", Account: "default"}
			job.BindRuntimeState(f.rt.Strategies, f.rt.Core, f.rt.Clock)
			job.BindRuntimeMarket(f.rt.Market.Prices, f.rt.Clock)
			job.Strat.OnOrderChange = func(_ *strat.StratJob, od *ormo.InOutOrder, kind int) {
				if kind == strat.OdChgExit || kind == strat.OdChgOrderChanged {
					return
				}
				if kind != strat.OdChgExitFill {
					t.Error("wrong change kind", kind)
				}
				jobEvents = append(jobEvents, od.Clone())
			}
			if err := manager.BindJobs([]*strat.StratJob{job}); err != nil {
				t.Fatal(err)
			}
			if len(managerEvents) != 0 || len(jobEvents) != 0 {
				t.Fatal("restore replayed callbacks")
			}
			adapter := f.opts.Adapter.(*semanticMigrationAdapter)
			for _, delta := range []int64{1, 2} {
				o := adapter.active.Intent
				o.ID += "/callback/" + decimal.NewFromInt(adapter.active.FilledSteps).String()
				o.Steps, o.SubmitAtMS = delta, 103
				if _, err := adapter.PaperAdapter.Submit(context.Background(), o, o.ID); err != nil {
					t.Fatal(err)
				}
				adapter.active.FilledSteps += delta
				adapter.active.ReportedCost = adapter.active.ReportedCost.Add(decimal.NewFromInt(delta).Mul(o.Instrument.QuantityStep).Mul(o.Observation.Price))
				fee := decimal.NewFromInt(delta).Mul(decimal.RequireFromString("0.01"))
				adapter.active.ReportedFee = adapter.active.ReportedFee.Add(fee)
				adapter.PaperAdapter.ApplyCash(fee.Neg())
				adapter.active.State = execution.OrderPartial
				if adapter.active.FilledSteps == 5 {
					adapter.active.State = execution.OrderFilled
				}
				for n := 0; n < 2; n++ {
					if err := f.rt.SharedExecution().Recover("original-exit"); err != nil {
						t.Fatal(err)
					}
				}
				if adapter.active.FilledSteps == 3 && !correction.IsZero() {
					adapter.active.ReportedFee = adapter.active.ReportedFee.Add(correction)
					adapter.PaperAdapter.ApplyCash(correction.Neg())
					report := execution.FillReport{EventID: "native-exit-fee-correction", OrderID: "original-exit", Steps: 3, Cost: adapter.active.ReportedCost, Fee: adapter.active.ReportedFee, Price: o.Observation.Price, Cumulative: true, AtMS: 103}
					for n := 0; n < 2; n++ {
						if err := f.rt.SharedExecution().ApplyTrade(report); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			if err := f.rt.SharedExecution().Reconcile("callback-progress", 103); err != nil {
				t.Fatal(err)
			}
			f.observe(t, row.Exit.Price, 4)
			for _, callbacks := range [][]*ormo.InOutOrder{managerEvents, jobEvents} {
				if len(callbacks) != 2 {
					t.Fatal("missing or duplicate callbacks", len(callbacks))
				}
				for n, od := range callbacks {
					wantFill, wantFee, wantEventFee, wantStatus := .3, .03, "0.01", ormo.OdStatusPartOK
					if n == 1 {
						wantFill, wantFee, wantEventFee, wantStatus = .5, decimal.RequireFromString("0.05").Add(correction).InexactFloat64(), "0.02", ormo.OdStatusClosed
					}
					exit := od.Exit
					if exit == nil || exit.Amount != row.Exit.Amount || exit.Filled != wantFill || exit.Price != row.Exit.Price || exit.OrderType != row.Exit.OrderType || exit.OrderID != row.Exit.OrderID || exit.Side != row.Exit.Side || exit.FeeType != row.Exit.FeeType || exit.Fee != wantFee || exit.FeeQuote != wantFee || od.ExitTag != row.ExitTag {
						t.Fatalf("callback %d lost original/event-time metadata: %+v", n, exit)
					}
					if exit.Status != int64(wantStatus) {
						t.Fatal("future status leaked into callback", n, exit.Status)
					}
					if od.Info["shared_event_fee"] != wantEventFee || od.Info["shared_cumulative_fee"] != decimal.NewFromFloat(wantFee).String() {
						t.Fatal("event fee progression lost", od.Info)
					}
				}
			}
			if err := f.rt.SharedExecution().Recover("original-exit"); err != nil {
				t.Fatal(err)
			}
			if err := f.rt.SharedExecution().Reconcile("callback-duplicate", 104); err != nil {
				t.Fatal(err)
			}
			f.observe(t, row.Exit.Price, 5)
			f.process.Close()
			f.process = NewProcess()
			t.Cleanup(f.process.Close)
			f.rt, err = f.process.NewRuntime(Options{AccountOwnerKey: &f.key, SharedExecution: f.opts, SharedOrderBridge: f.bridge})
			if err != nil {
				t.Fatal(err)
			}
			if err := f.rt.SharedExecution().RecoverPersisted(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := f.rt.SharedExecution().Reconcile("callback-restart", 106); err != nil {
				t.Fatal(err)
			}
			biz.InitLocalOrderMgrWithRuntimeDeps(f.rt.BizDeps(), func(od *ormo.InOutOrder, _ bool) { managerEvents = append(managerEvents, od.Clone()) }, false)
			manager = biz.GetOdMgrWithState(f.rt.Trading, "default").(*biz.SharedOrderMgr)
			job.BindRuntimeState(f.rt.Strategies, f.rt.Core, f.rt.Clock)
			job.BindRuntimeMarket(f.rt.Market.Prices, f.rt.Clock)
			if err := manager.BindJobs([]*strat.StratJob{job}); err != nil {
				t.Fatal(err)
			}
			if len(managerEvents) != 2 || len(jobEvents) != 2 {
				t.Fatal("duplicate/restore replayed callbacks")
			}
		})
	}
}

func legacyExitRow(short bool, filled float64) *ormo.InOutOrder {
	row := semanticLegacyRow(1)
	row.Short, row.Status, row.ExitTag, row.ExitAt = short, ormo.InOutStatusPartExit, "original-exit", 99
	side, limit := "sell", float64(110)
	if short {
		row.Enter.Side, side, limit = "sell", "buy", 90
	}
	fee := decimal.NewFromFloat(filled).Mul(decimal.RequireFromString("0.1")).InexactFloat64()
	row.Exit = &ormo.ExOrder{ID: 7, Enter: false, Symbol: "BTC", Side: side, OrderType: "limit", Amount: .5, Filled: filled, Price: limit, Status: ormo.OdStatusPartOK, OrderID: "original-exit-exchange", CreateAt: 99, Fee: fee, FeeQuote: fee, FeeType: "USDT"}
	return row
}

func attachNativeExit(row *ormo.InOutOrder) func(*execution.LegacyMigration, *semanticMigrationAdapter, execution.AccountKey) {
	return func(request *execution.LegacyMigration, adapter *semanticMigrationAdapter, key execution.AccountKey) {
		instrument := request.Lots[0].Instrument
		side := execution.OrderSide(row.Exit.Side)
		quantity := decimal.NewFromFloat(row.Exit.Amount).Div(instrument.QuantityStep).IntPart()
		filled := decimal.NewFromFloat(row.Exit.Filled).Div(instrument.QuantityStep).IntPart()
		virtual := execution.EligibleIntent{ID: "original-exit-intent", Account: key, Strategy: "ts", Lot: "old-42", Instrument: instrument.ID, Side: side, Kind: execution.ExitIntent, QuantitySteps: quantity, FilledSteps: filled, Conditions: execution.IntentConditions{Limit: decimal.NewFromFloat(row.Exit.Price)}}
		plan := execution.Plan{ID: "original-exit-plan", Sequence: 0, DecisionMS: 99, ExpiresMS: 1000, Intents: []execution.EligibleIntent{virtual}, Targets: []execution.ExecutableTarget{{Strategy: "ts", Lot: "old-42", Instrument: "BTC", SignedSteps: 5}}}
		if row.Short {
			plan.Targets[0].SignedSteps = -5
		}
		intent := execution.OrderIntent{ID: "original-exit", PlanID: plan.ID, Instrument: instrument, Side: side, Steps: quantity, Limit: decimal.NewFromFloat(row.Exit.Price), ReduceOnly: true, Observation: execution.ExecutionObservation{Price: decimal.NewFromFloat(row.Exit.Price), AtMS: 99, ValidUntilMS: 1000}, Allocations: []execution.FillAllocation{{ID: "original-exit-allocation", IntentID: virtual.ID, Strategy: "ts", Lot: "old-42", Side: side, Kind: execution.ExitIntent, Steps: quantity}}}
		state := execution.OrderAcknowledged
		if filled > 0 {
			state = execution.OrderPartial
		}
		order := execution.StoredOrder{Intent: intent, ClientID: "original-exit-client", ExchangeID: row.Exit.OrderID, State: state, FilledSteps: filled, ReportedCost: decimal.NewFromFloat(row.Exit.Filled * row.Exit.Price), ReportedFee: decimal.NewFromFloat(row.Exit.FeeQuote), AllocationFilled: map[string]int64{"original-exit-allocation": filled}}
		request.Plans, request.Orders = []execution.Plan{plan}, []execution.StoredOrder{order}
		request.RawLegacyMap[0].ExOrderIDs = []string{order.ExchangeID}
		request.VenueSnapshot.OpenOrders = []execution.LegacyVenueOrder{{ExchangeID: order.ExchangeID, ClientID: order.ClientID, Instrument: "BTC", Side: side, Steps: quantity, FilledSteps: filled, Cost: order.ReportedCost, Fee: order.ReportedFee}}
		adapter.venue, adapter.active = request.VenueSnapshot, &order
	}
}

func TestSharedCutoverNativeExitKeepsReductionAndHighwaterAfterRestart(t *testing.T) {
	for _, short := range []bool{false, true} {
		for _, fill := range []float64{0, .2} {
			for _, protection := range []bool{false, true} {
				name := "long/" + decimal.NewFromFloat(fill).String()
				if short {
					name = "short/" + decimal.NewFromFloat(fill).String()
				}
				if protection {
					name += "/protection"
				} else {
					name += "/unprotected"
				}
				t.Run(name, func(t *testing.T) {
					row := legacyExitRow(short, fill)
					// A crossed protection must not replace this explicit live exit.
					stop := float64(105)
					if short {
						stop = 95
					}
					if protection {
						row.Info[ormo.OdInfoStopLoss] = &ormo.TriggerState{ExitTrigger: &ormo.ExitTrigger{Price: stop}}
					}
					f := semanticCutoverFixtureWithOrders(t, row, false, attachNativeExit(row))
					adapter := f.opts.Adapter.(*semanticMigrationAdapter)
					f.observe(t, 100, 2)
					if len(adapter.submits) != 0 {
						t.Fatal("cutover duplicated exit or compensated exposure", adapter.submits)
					}
					plan, err := f.rt.SharedExecution().LatestPlan(context.Background())
					want := int64(5)
					if short {
						want = -5
					}
					if err != nil || len(plan.Targets) != 1 || plan.Targets[0].SignedSteps != want {
						t.Fatal("original final reduction target lost", plan, err)
					}
					rows, lock := f.rt.Orders.GetOpenODs("default")
					lock.Lock()
					projected := rows[42]
					lock.Unlock()
					if projected.Exit == nil || projected.Exit.Amount != .5 || projected.Exit.Filled != fill || projected.Exit.Price != row.Exit.Price || projected.Exit.OrderID != row.Exit.OrderID || projected.Exit.OrderType != "limit" || projected.Exit.Side != row.Exit.Side || projected.ExitTag != "original-exit" {
						t.Fatal("original exit facade lost source contract", projected.Exit)
					}
					for _, delta := range []int64{1, 5 - adapter.active.FilledSteps - 1} {
						if delta <= 0 {
							continue
						}
						o := adapter.active.Intent
						o.ID += "/completion/" + decimal.NewFromInt(adapter.active.FilledSteps).String()
						o.Steps, o.SubmitAtMS = delta, 103
						if _, err := adapter.PaperAdapter.Submit(context.Background(), o, o.ID); err != nil {
							t.Fatal(err)
						}
						adapter.active.FilledSteps += delta
						adapter.active.ReportedCost = adapter.active.ReportedCost.Add(decimal.NewFromInt(delta).Mul(o.Instrument.QuantityStep).Mul(o.Observation.Price))
						fee := decimal.NewFromInt(delta).Mul(decimal.RequireFromString("0.01"))
						adapter.active.ReportedFee = adapter.active.ReportedFee.Add(fee)
						adapter.PaperAdapter.ApplyCash(fee.Neg())
						adapter.active.State = execution.OrderPartial
						if adapter.active.FilledSteps == 5 {
							adapter.active.State = execution.OrderFilled
						}
						for n := 0; n < 2; n++ {
							if err := f.rt.SharedExecution().Recover("original-exit"); err != nil {
								t.Fatal(err)
							}
						}
						if err := f.rt.SharedExecution().Reconcile("exit-progress", 103); err != nil {
							t.Fatal(err)
						}
						f.observe(t, row.Exit.Price, 4)
						if len(adapter.submits) != 0 {
							t.Fatal("native cumulative fill emitted duplicate/compensating order", adapter.submits)
						}
					}
					if f.steps(t) != want {
						t.Fatal("exit cumulative fill target mismatch", f.steps(t), want)
					}
					rows, lock = f.rt.Orders.GetOpenODs("default")
					lock.Lock()
					finalExit := rows[42].Exit
					lock.Unlock()
					if finalExit.Filled != .5 || !decimal.NewFromFloat(finalExit.FeeQuote).Equal(adapter.active.ReportedFee) {
						t.Fatal("native exit cumulative quantity/fee projection lost", finalExit, adapter.active.ReportedFee)
					}
					state, err := f.rt.SharedExecution().Snapshot(context.Background())
					if err != nil || len(state.Lots) != 1 || !state.Lots[0].Fees.Equal(adapter.active.ReportedFee) {
						t.Fatal("duplicate report changed exact ledger fee", state, err)
					}
					if protection {
						f.observe(t, 100, 6)
						if f.steps(t) != 0 || len(adapter.submits) != 1 {
							t.Fatal("completed explicit exit suppressed remaining protection")
						}
					} else {
						f.rt.Clock.SetTimeMS(60110)
						if _, err := f.manager.ExitOpenOrders("BTC", &strat.ExitReq{StratName: "legacy", OrderID: 42, Amount: .2}); err != nil {
							t.Fatal(err)
						}
						want = 3
						if short {
							want = -3
						}
						if f.steps(t) != want || len(adapter.submits) != 1 {
							t.Fatal("completed source exit blocked new selector", f.steps(t), adapter.submits)
						}
					}
				})
			}
		}
	}
}

func TestSharedCutoverNativeExitCancellationDoesNotRetryRemainder(t *testing.T) {
	row := legacyExitRow(false, .2)
	f := semanticCutoverFixtureWithOrders(t, row, false, attachNativeExit(row))
	adapter := f.opts.Adapter.(*semanticMigrationAdapter)
	adapter.active.State = execution.OrderCanceled
	for n := 0; n < 2; n++ {
		if err := f.rt.SharedExecution().Recover("original-exit"); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.rt.SharedExecution().Reconcile("native-canceled", 103); err != nil {
		t.Fatal(err)
	}
	f.observe(t, 110, 4)
	f.observe(t, 110, 5)
	if f.steps(t) != 8 || len(adapter.submits) != 0 {
		t.Fatal("canceled original exit retried remainder", f.steps(t), adapter.submits)
	}
	rows, lock := f.rt.Orders.GetOpenODs("default")
	lock.Lock()
	exit := rows[42].Exit
	lock.Unlock()
	if exit == nil || exit.Status != ormo.OdStatusClosed || exit.Filled != .2 {
		t.Fatal("native canceled exit facade/highwater lost", exit)
	}
	f.rt.Clock.SetTimeMS(60110)
	if _, err := f.manager.ExitOpenOrders("BTC", &strat.ExitReq{StratName: "legacy", OrderID: 42, Amount: .2}); err != nil {
		t.Fatal(err)
	}
	if f.steps(t) != 6 || len(adapter.submits) != 1 {
		t.Fatal("canceled native linkage blocked new exit", f.steps(t), adapter.submits)
	}
}

func TestSharedCutoverRejectsNativeExitSourceMismatchBeforeReadiness(t *testing.T) {
	for _, name := range []string{"missing-source", "limit", "highwater", "quantity"} {
		t.Run(name, func(t *testing.T) {
			row := legacyExitRow(false, .2)
			source := row.Exit
			prepare := attachNativeExit(row)
			if name == "missing-source" {
				row.Exit = nil
			}
			semanticCutoverFixtureWithOrders(t, row, true, func(request *execution.LegacyMigration, adapter *semanticMigrationAdapter, key execution.AccountKey) {
				if name == "missing-source" {
					row.Exit = source
					prepare(request, adapter, key)
					row.Exit = nil
				} else {
					prepare(request, adapter, key)
				}
				switch name {
				case "limit":
					request.Orders[0].Intent.Limit = decimal.NewFromInt(111)
				case "highwater":
					request.Orders[0].AllocationFilled["original-exit-allocation"] = 1
				case "quantity":
					request.Plans[0].Intents[0].QuantitySteps = 6
				}
			})
		})
	}
}

func TestSharedCutoverLocalPendingAndCanceledExitRemainFaithful(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		name := "pending"
		if canceled {
			name = "canceled"
		}
		t.Run(name, func(t *testing.T) {
			row := legacyExitRow(false, .2)
			row.Exit.OrderID = ""
			if canceled {
				row.Exit.Status = ormo.OdStatusClosed
			}
			f := migratedSemanticFixture(t, row)
			adapter := f.opts.Adapter.(*semanticMigrationAdapter)
			f.observe(t, 100, 2)
			if len(adapter.submits) != 0 || f.steps(t) != 8 {
				t.Fatal("local pending/canceled exit traded early")
			}
			f.observe(t, 110, 3)
			want := int64(5)
			if canceled {
				want = 8
			}
			if f.steps(t) != want {
				t.Fatal("pending/canceled exit contract lost", f.steps(t), want)
			}
		})
	}
}
