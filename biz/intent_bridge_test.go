package biz

import (
	"math"
	"reflect"
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

func bridgeContext() IntentBridgeContext {
	return IntentBridgeContext{Account: execution.AccountKey{VenueSessionIdentity: "paper", Account: "account", SettlementDomain: "USDT"}, Strategy: "strategy-id", StrategyName: "strategy-name", Lot: "lot-id", Intent: "intent-id", Instrument: "BTC", QuantityStep: decimal.RequireFromString("0.1"), ContractSize: decimal.NewFromInt(1), ReferencePrice: decimal.NewFromInt(100), StrategyNAV: decimal.NewFromInt(1000), StakeNAVFraction: decimal.RequireFromString("0.1"), MaxNotional: decimal.NewFromInt(1000), NowMS: 10, Bar: 5}
}

func TestIntentBridgeEntryEligibility(t *testing.T) {
	c := bridgeContext()
	for _, tc := range []struct {
		req   strat.EnterReq
		state execution.IntentState
	}{
		{strat.EnterReq{Amount: 1}, execution.Eligible},
		{strat.EnterReq{Amount: 1, Limit: 90, StopBars: 2}, execution.PendingCondition},
		{strat.EnterReq{Amount: 1, Stop: 110}, execution.PendingCondition},
		{strat.EnterReq{Amount: 1, Short: true, Limit: 110}, execution.PendingCondition},
		{strat.EnterReq{Amount: 1, Short: true, Stop: 90}, execution.PendingCondition},
	} {
		got, err := BridgeEntryReq(c, &tc.req)
		if err != nil {
			t.Fatal(err)
		}
		if got.Intent.State != tc.state || got.Intent.QuantitySteps != 10 {
			t.Fatal(got.Intent)
		}
	}
	got, err := BridgeEntryReq(c, &strat.EnterReq{Amount: 1, Limit: 90, StopBars: 2})
	if err != nil {
		t.Fatal(err)
	}
	if n, err := got.Intent.Evaluate(decimal.NewFromInt(90), 11, 6); err != nil || n != 10 {
		t.Fatal(n, err)
	}
	if n, err := got.Intent.Evaluate(decimal.NewFromInt(90), 12, 7); err != nil || n != 0 || got.Intent.State != execution.Expired {
		t.Fatal(n, err)
	}
}

func TestIntentBridgeEntryPreservesAllFields(t *testing.T) {
	req := strat.EnterReq{Tag: "entry", StratName: "strategy-name", OrderType: core.OrderTypeLimit, Limit: 90, Stop: 110, CostRate: 2, LegalCost: 300, Leverage: 3, Amount: 1.29, StopLossVal: 1, StopLoss: 80, StopLossLimit: 79, StopLossRate: 0.5, StopLossTag: "sl", ActivationPrice: 120, CallbackPct: 1, TakeProfitVal: 2, TakeProfit: 130, TakeProfitLimit: 129, TakeProfitRate: 0.7, TakeProfitTag: "tp", StopBars: 2, ClientID: "client", Infos: map[string]string{"x": "y"}, Log: true}
	got, err := BridgeEntryReq(bridgeContext(), &req)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(req, got.Original) {
		t.Fatal("request fields lost")
	}
	if !reflect.DeepEqual(got.DeferredProtections, []string{"stop_loss", "take_profit", "trailing"}) {
		t.Fatal("unsupported protections unreported")
	}
	if got.Intent.QuantitySteps != 12 || !got.Intent.Conditions.TrailingPercent.IsZero() {
		t.Fatal("trailing exit protection affected entry")
	}
	req.Infos["x"] = "changed"
	if got.Original.Infos["x"] != "y" {
		t.Fatal("metadata snapshot aliases mutable request")
	}
}

func TestIntentBridgeEntrySizingAndBudget(t *testing.T) {
	c := bridgeContext()
	for _, tc := range []struct {
		req   strat.EnterReq
		units int64
	}{{strat.EnterReq{}, 10}, {strat.EnterReq{CostRate: 2}, 20}, {strat.EnterReq{LegalCost: 99.99, CostRate: 5, Leverage: 10}, 9}, {strat.EnterReq{Amount: 0.29, LegalCost: 500, CostRate: 5}, 2}} {
		got, err := BridgeEntryReq(c, &tc.req)
		if err != nil || got.Intent.QuantitySteps != tc.units {
			t.Fatal(got, err)
		}
	}
	c.ContractSize = decimal.NewFromInt(2)
	got, err := BridgeEntryReq(c, &strat.EnterReq{LegalCost: 100})
	if err != nil || got.Intent.QuantitySteps != 5 {
		t.Fatal(got, err)
	}
	c.MaxNotional = decimal.NewFromInt(99)
	if _, err := BridgeEntryReq(c, &strat.EnterReq{Amount: 1}); err == nil {
		t.Fatal("risk allowance ignored")
	}
	for _, req := range []strat.EnterReq{{Amount: 0.01}, {Amount: math.NaN()}, {Amount: 1, Limit: math.Inf(1)}, {Amount: 1, StopLoss: -1}, {Amount: 1, OrderType: core.OrderTypeLimit}, {Amount: 1, OrderType: 99, Limit: 90}, {Amount: 1, StratName: "foreign"}} {
		if _, err := BridgeEntryReq(bridgeContext(), &req); err == nil {
			t.Fatal("invalid/unsupported entry accepted", req)
		}
	}
	c = bridgeContext()
	c.QuantityStep = decimal.Zero
	if _, err := BridgeEntryReq(c, &strat.EnterReq{Amount: 1}); err == nil {
		t.Fatal("invalid precision accepted")
	}
}

func TestIntentBridgeExitOwnershipFiltersAndQuantity(t *testing.T) {
	c := bridgeContext()
	lot := LegacyIntentLot{Selection: execution.LotSelection{Account: c.Account, Strategy: c.Strategy, Lot: c.Lot, Instrument: c.Instrument, FilledSteps: 4, PendingEntrySteps: 6}, OrderID: 42, EnterTag: "entry"}
	for _, tc := range []struct {
		req            strat.ExitReq
		reduce, cancel int64
	}{{strat.ExitReq{}, 4, 6}, {strat.ExitReq{FilledOnly: true}, 4, 0}, {strat.ExitReq{UnFillOnly: true}, 0, 6}, {strat.ExitReq{ExitRate: 0.5}, 2, 3}, {strat.ExitReq{Amount: 0.6, ExitRate: 0.1}, 4, 2}, {strat.ExitReq{Amount: 0.2}, 2, 0}} {
		got, err := BridgeExitReq(c, &tc.req, lot)
		if err != nil {
			t.Fatal(err)
		}
		var reduce int64
		if got.Reduction != nil {
			reduce = got.Reduction.QuantitySteps
		}
		if !got.Matched || reduce != tc.reduce || got.CancelEntrySteps != tc.cancel {
			t.Fatal(got)
		}
	}
	req := strat.ExitReq{Tag: "exit", StratName: c.StrategyName, EnterTag: "entry", OrderID: 42, Dirt: core.OdDirtLong, OrderType: core.OrderTypeLimit, Limit: 110, FilledOnly: true, Log: true}
	got, err := BridgeExitReq(c, &req, lot)
	if err != nil || !reflect.DeepEqual(req, got.Original) || got.Reduction.State != execution.PendingCondition {
		t.Fatal(got, err)
	}
	for _, req := range []strat.ExitReq{{OrderID: 43}, {EnterTag: "other"}, {Dirt: core.OdDirtShort}} {
		got, err := BridgeExitReq(c, &req, lot)
		if err != nil || got.Matched || got.Reduction != nil || got.CancelEntrySteps != 0 {
			t.Fatal("identity filter ignored", got, err)
		}
	}
	foreign := lot
	foreign.Selection.Strategy = "foreign"
	if _, err := BridgeExitReq(c, &strat.ExitReq{}, foreign); err == nil {
		t.Fatal("foreign strategy lot allowed")
	}
	for _, req := range []strat.ExitReq{{FilledOnly: true, UnFillOnly: true}, {ExitRate: 1.1}, {Amount: math.Inf(1)}, {Limit: 1, OrderType: 99}} {
		if _, err := BridgeExitReq(c, &req, lot); err == nil {
			t.Fatal("unsupported exit accepted", req)
		}
	}
}
