package biz

import (
	"testing"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
)

func TestBridgeTimeSeriesEntryCommandCarriesOCOGroup(t *testing.T) {
	for _, group := range []string{"", "bracket-1"} {
		req := strat.EnterReq{CommandID: "c1", Amount: 1, OCOGroup: group}
		command, err := BridgeTimeSeriesEntryCommand(bridgeContext(), &orm.ExSymbol{ID: 7, Symbol: "BTC/USDT:USDT"}, "1m", 42, &req, execution.OrderPolicy{Mode: execution.LocalFirst}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if command.Create == nil || command.Create.OCOGroup != group {
			t.Fatalf("group %q not carried: %+v", group, command.Create)
		}
	}
}
