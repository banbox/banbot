package runtime

import (
	"context"
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

func TestSharedDelayedFundingExternalActivityAndFeeOnlyBoundaries(t *testing.T) {
	for _, activity := range []string{"external-ETH", "external-BTC", "fee-only-BTC"} {
		t.Run(activity, func(t *testing.T) {
			var adapter *semanticMigrationAdapter
			f := newSharedTriggerFixtureWithAdapter(t, func(p *runner.PaperAdapter) execution.ExecutionAdapter {
				adapter = &semanticMigrationAdapter{PaperAdapter: p}
				return adapter
			})
			f.entry(t, &strat.EnterReq{Tag: "funding-boundary", Amount: 1})
			account := f.rt.SharedExecution()
			btc := f.bridge.Instruments["BTC"]
			if activity == "fee-only-BTC" {
				order := adapter.submits[0]
				report := execution.FillReport{EventID: "after-cutoff-fee-only", OrderID: order.ID, Steps: order.Steps, Price: decimal.NewFromInt(100), Cost: decimal.NewFromInt(100), Fee: decimal.RequireFromString("0.01"), Cumulative: true, AuthoritativeSnapshot: true, AtMS: 102}
				if err := account.ApplyTrade(report); err != nil {
					t.Fatal(err)
				}
				f.adapter.ApplyCash(report.Fee.Neg())
			} else {
				instrument := btc
				if activity == "external-ETH" {
					instrument.ID = "ETH"
				}
				if _, err := account.ApplyExternalPosition(execution.ExternalPositionEvent{ID: "after-cutoff-external", Kind: execution.ExternalCashChange, Instrument: instrument, Side: execution.Buy, Steps: 2, Price: decimal.NewFromInt(100), AtMS: 102}); err != nil {
					t.Fatal(err)
				}
			}
			funding := execution.FundingSettlement{ID: "boundary-delayed-funding", Instrument: btc, Mark: decimal.NewFromInt(100), Rate: decimal.RequireFromString("0.01"), AccountAmount: decimal.NewFromInt(-1), AtMS: 101}
			applied, err := account.ApplyFunding(funding)
			if activity == "external-BTC" {
				if applied || err == nil {
					t.Fatal("same instrument external activity accepted late funding", applied, err)
				}
				requireOccurrenceFrozen(t, account, err)
				return
			}
			if !applied || err != nil {
				t.Fatal("unrelated/fee-only activity blocked funding", activity, applied, err)
			}
			state, err := account.Snapshot(context.Background())
			if err != nil || len(state.Lots) != 1 || !state.Lots[0].Funding.Equal(decimal.NewFromInt(-1)) {
				t.Fatal("funding attribution changed", state, err)
			}
			if activity == "fee-only-BTC" && (state.RiskFrozen || !state.Lots[0].Fees.Equal(decimal.RequireFromString("0.01"))) {
				t.Fatal("fee-only funding changed fee/freeze", state)
			}
		})
	}
}

func TestSharedDelayedFundingClassifiesNativeInstrumentHistory(t *testing.T) {
	for _, changed := range []string{"ETH", "BTC"} {
		t.Run(changed, func(t *testing.T) {
			f := newSharedTriggerFixture(t)
			eth := f.bridge.Instruments["BTC"]
			eth.ID = "ETH"
			f.bridge.Instruments["ETH"] = eth
			if err := f.rt.SharedExecution().RegisterAccountQuotes([]execution.Instrument{eth}, func(_ context.Context, id string, now int64) (execution.VisibleQuote, error) {
				return f.bridge.Quote(id, now)
			}, f.rt.Clock.TimeMS); err != nil {
				t.Fatal(err)
			}
			f.rt.Market.Prices.SetBarPriceAt(100, "ETH", 100)
			f.entry(t, &strat.EnterReq{Tag: "funding-holder", Amount: 1})
			f.rt.Clock.SetTimeMS(102)
			f.rt.Market.Prices.SetBarPriceAt(102, changed, 100)
			sid := int32(1)
			if changed == "ETH" {
				sid = 2
			}
			if _, err := f.manager.EnterOrder(&orm.ExSymbol{ID: sid, Symbol: changed}, "ws", &strat.EnterReq{StratName: "legacy", Tag: "after-cutoff", Amount: 1}); err != nil {
				t.Fatal(err)
			}
			account := f.rt.SharedExecution()
			funding := execution.FundingSettlement{ID: "delayed-btc-funding", Instrument: f.bridge.Instruments["BTC"], Mark: decimal.NewFromInt(100), Rate: decimal.RequireFromString("0.01"), AccountAmount: decimal.NewFromInt(-1), AtMS: 101}
			applied, err := account.ApplyFunding(funding)
			if changed == "BTC" {
				if applied || err == nil {
					t.Fatal("same-instrument activity accepted historical funding", applied, err)
				}
				requireOccurrenceFrozen(t, account, err)
				return
			}
			if err != nil || !applied {
				t.Fatal("unrelated native fill blocked delayed funding", applied, err)
			}
			f.adapter.ApplyCash(funding.AccountAmount)
			check := func(account interface {
				Snapshot(context.Context) (execution.AccountSnapshot, error)
			}) {
				state, err := account.Snapshot(context.Background())
				if err != nil || state.RiskFrozen || !state.AccountSettledCash.Equal(decimal.NewFromInt(999)) || !state.SyntheticStrategyCash["ts"].Equal(decimal.NewFromInt(999)) || !state.UnassignedCash.IsZero() || len(state.Lots) != 2 {
					t.Fatal("funding books changed or froze", state, err)
				}
				for _, lot := range state.Lots {
					want := decimal.Zero
					if lot.Instrument.ID == "BTC" {
						want = decimal.NewFromInt(-1)
					}
					if !lot.Funding.Equal(want) {
						t.Fatal("funding attributed to wrong instrument", lot)
					}
				}
			}
			check(account)
			if applied, err := account.ApplyFunding(funding); err != nil || applied {
				t.Fatal("funding duplicated", applied, err)
			}
			check(account)
			f.process.Close()
			f.process = NewProcess()
			t.Cleanup(f.process.Close)
			f.rt, err = f.process.NewRuntime(Options{Mode: core.RunModeBackTest, AccountOwnerKey: &f.key, SharedExecution: f.opts, SharedOrderBridge: f.bridge})
			if err != nil {
				t.Fatal(err)
			}
			check(f.rt.SharedExecution())
			if applied, err := f.rt.SharedExecution().ApplyFunding(funding); err != nil || applied {
				t.Fatal("restart duplicated funding", applied, err)
			}
			check(f.rt.SharedExecution())
		})
	}
}
