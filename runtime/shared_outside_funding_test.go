package runtime

import (
	"context"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
	"testing"
)

func TestAccountOutsideFactorFundingSettlesDeduplicatesAndRestores(t *testing.T) {
	f := newSharedTriggerFixture(t)
	f.entry(t, &strat.EnterReq{Tag: "outside-factor", Amount: 1})
	btc := f.bridge.Instruments["BTC"]
	eth := btc
	eth.ID = "ETH"
	sink := &runner.AccountSink{Account: f.rt.SharedExecution(), Instruments: map[int32]execution.Instrument{2: eth}, FundingInstruments: map[int32]execution.Instrument{1: btc}, AuthoritativeFunding: true}
	before, err := sink.Account.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	record := factor.VersionRecord{Series: orm.DataSeries{Sid: 1, Source: "funding", TimeFrame: "event", Values: map[string]any{"settlement_id": "outside-settlement", "mark": "100", "rate": "0.0001", "account_amount": "-0.015"}}, EventTime: 101, AvailableAt: 101, IngestedAt: 101}
	if err := sink.ObserveFundingRecord(context.Background(), record, 101); err != nil {
		t.Fatal(err)
	}
	after, err := sink.Account.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !after.AccountSettledCash.Equal(before.AccountSettledCash.Sub(decimal.RequireFromString(".015"))) || !after.SyntheticStrategyCash["ts"].Equal(before.SyntheticStrategyCash["ts"].Sub(decimal.RequireFromString(".01"))) {
		t.Fatal("outside account/virtual settlement missing", before, after)
	}
	if err := sink.ObserveFundingRecord(context.Background(), record, 102); err != nil {
		t.Fatal(err)
	}
	dedup, _ := sink.Account.Snapshot(context.Background())
	if dedup.Checkpoint != after.Checkpoint {
		t.Fatal("funding duplicate reposted")
	}
	record.Series.Sid = 3
	if err := sink.ObserveFundingRecord(context.Background(), record, 102); err == nil {
		t.Fatal("undeclared outside funding accepted")
	}
	record.Series.Sid = 1
	f.process.Close()
	p := NewProcess()
	defer p.Close()
	rt, err := p.NewRuntime(Options{AccountOwnerKey: &f.key, SharedExecution: f.opts})
	if err != nil {
		t.Fatal(err)
	}
	sink.Account = rt.SharedExecution()
	if err := sink.ObserveFundingRecord(context.Background(), record, 102); err != nil {
		t.Fatal(err)
	}
	restored, _ := sink.Account.Snapshot(context.Background())
	if restored.Checkpoint != after.Checkpoint || !restored.AccountSettledCash.Equal(after.AccountSettledCash) {
		t.Fatal("restart funding duplicate changed cash", restored)
	}
}
