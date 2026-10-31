package biz

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

func TestTimeSeriesEntryCommandPreservesMetadataProtectionsAndFeedClock(t *testing.T) {
	req := strat.EnterReq{CommandID: "entry-1", Tag: "signal", Amount: 1.29, OrderType: core.OrderTypeLimitMaker, Limit: 90, Stop: 110, StopBars: 3, StopLoss: 80, StopLossTag: "sl", TakeProfitVal: 20, TakeProfitRate: 0.5, CallbackPct: 1, ActivationPrice: 120, ClientID: "custom", Infos: map[string]string{"source": "strategy"}}
	original, _ := json.Marshal(req)
	info := map[string]any{"custom": "retained", "null": nil}
	command, err := BridgeTimeSeriesEntryCommand(bridgeContext(), &orm.ExSymbol{ID: 7, Symbol: "BTC/USDT:USDT"}, "1m", 42, &req, execution.OrderPolicy{Mode: execution.LocalFirst}, info)
	if err != nil {
		t.Fatal(err)
	}
	if command.Create.QuantitySteps != 12 || command.Create.Conditions.StopBars != 0 || command.Create.Conditions.CreatedBar != 0 || command.Create.Feed != "7/1m" || command.Create.StopBars != 3 || !command.Create.Conditions.PostOnly || len(command.Create.Protections) != 3 {
		t.Fatal(command.Create)
	}
	if command.Metadata.OrderID != 42 || command.Metadata.EnterTag != "signal" || command.Metadata.StrategyName != bridgeContext().StrategyName || command.Lot != bridgeContext().Lot {
		t.Fatal(command.Metadata)
	}
	var payload TimeSeriesEntryPayload
	if err := json.Unmarshal(command.Metadata.Payload, &payload); err != nil || !reflect.DeepEqual(payload.Request, req) || !reflect.DeepEqual(payload.SourceInfo, info) || payload.SID != 7 {
		t.Fatal(payload, err)
	}
	after, _ := json.Marshal(req)
	if string(after) != string(original) {
		t.Fatal("caller request mutated")
	}
	req.Infos["source"], info["custom"] = "changed", "changed"
	if err := json.Unmarshal(command.Metadata.Payload, &payload); err != nil || payload.Request.Infos["source"] != "strategy" || payload.SourceInfo["custom"] != "retained" {
		t.Fatal("command aliases caller metadata", payload, err)
	}
	encoded, err := json.Marshal(command)
	var decoded execution.PendingCommand
	if err != nil || json.Unmarshal(encoded, &decoded) != nil {
		t.Fatal("command is not serializable", err)
	}
	roundTrip, err := json.Marshal(decoded)
	if err != nil || string(roundTrip) != string(encoded) {
		t.Fatal("command changed after transport", err)
	}
}

func TestTimeSeriesExitCommandForwardsOneBudgetAndSelector(t *testing.T) {
	req := strat.ExitReq{CommandID: "exit-1", StratName: "ts", Tag: "close", EnterTag: "signal", Dirt: core.OdDirtShort, OrderType: core.OrderTypeLimitMaker, Limit: 90, Amount: 1.3, ExitRate: 0.5, OrderID: 42, FilledOnly: true, Force: true}
	command, err := BridgeTimeSeriesExitCommand("strategy", "ts", "BTC,ETH", "1m", &req)
	if err != nil || command.Strategy != "strategy" || command.Side != execution.Sell || !command.Amount.Equal(decimal.RequireFromString("1.3")) || !command.Rate.Equal(decimal.RequireFromString("0.5")) || command.OrderID != 42 || !command.FilledOnly || !command.Force || !command.Conditions.PostOnly || !reflect.DeepEqual(command.Symbols, []string{"BTC", "ETH"}) {
		t.Fatal(command, err)
	}
	if command.EnterTag != "signal" || command.ExitTag != "close" || command.TimeFrame != "1m" || req.StratName != "ts" {
		t.Fatal(command)
	}
}

func TestTimeSeriesCommandBridgeRejectsAmbiguityAndInvalidNumbers(t *testing.T) {
	for _, req := range []strat.ExitReq{
		{}, {CommandID: "x", StratName: "other"}, {CommandID: "x", Amount: math.NaN()}, {CommandID: "x", Dirt: 99}, {CommandID: "x", FilledOnly: true, UnFillOnly: true}, {CommandID: "x", OrderType: core.OrderTypeLimit},
	} {
		if _, err := BridgeTimeSeriesExitCommand("strategy", "ts", "", "", &req); err == nil {
			t.Fatal("invalid exit accepted", req)
		}
	}
	if _, err := BridgeTimeSeriesEntryCommand(bridgeContext(), &orm.ExSymbol{ID: 7, Symbol: "BTC"}, "1m", 1, &strat.EnterReq{Amount: 1}, execution.OrderPolicy{}, nil); err == nil {
		t.Fatal("entry without retry identity accepted")
	}
}
