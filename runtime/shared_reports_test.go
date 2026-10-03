package runtime

import (
	"context"
	"errors"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
	"sync/atomic"
	"testing"
	"time"
)

type reportRuntimeAdapter struct {
	*runner.PaperAdapter
	input  chan execution.BanexgStreamReport
	joined chan struct{}
	starts atomic.Int64
	closed atomic.Bool
}

func (a *reportRuntimeAdapter) Reports(ctx context.Context) (<-chan execution.BanexgStreamReport, error) {
	a.starts.Add(1)
	output := make(chan execution.BanexgStreamReport, 4)
	go func() {
		defer close(output)
		defer close(a.joined)
		for {
			select {
			case <-ctx.Done():
				return
			case report := <-a.input:
				select {
				case output <- report:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return output, nil
}
func (a *reportRuntimeAdapter) Close() error {
	select {
	case <-a.joined:
		a.closed.Store(true)
		return nil
	default:
		return errors.New("private source closed before join")
	}
}

func TestSharedAccountPrivateStreamAndEmergencyFunding(t *testing.T) {
	var adapter *reportRuntimeAdapter
	f := newSharedTriggerFixtureWithAdapter(t, func(p *runner.PaperAdapter) execution.ExecutionAdapter {
		adapter = &reportRuntimeAdapter{PaperAdapter: p, input: make(chan execution.BanexgStreamReport, 4), joined: make(chan struct{})}
		return adapter
	})
	f.entry(t, &strat.EnterReq{Tag: "funding", Amount: 1})
	account := f.rt.SharedExecution()
	i := f.bridge.Instruments["BTC"]
	event := execution.FundingSettlement{ID: "settlement", Instrument: i, Mark: decimal.NewFromInt(100), Rate: decimal.RequireFromString("0.01"), AccountAmount: decimal.RequireFromString("-1.2"), AtMS: 101}
	if applied, err := account.ApplyFunding(event); err != nil || !applied {
		t.Fatal(applied, err)
	}
	snap, _ := account.Snapshot(context.Background())
	if snap.AccountSettledCash.String() != "998.8" || snap.SyntheticStrategyCash["ts"].String() != "999" || snap.UnassignedCash.String() != "-0.2" {
		t.Fatal("authoritative funding discrepancy not isolated", snap)
	}
	if applied, err := account.ApplyFunding(event); err != nil || applied {
		t.Fatal("funding duplicated", applied, err)
	}
	external := execution.ExternalPositionEvent{ID: "manual", Kind: execution.ExternalCashChange, Instrument: i, Side: execution.Buy, Steps: 2, Price: decimal.NewFromInt(100), AtMS: 102}
	if applied, err := account.ApplyExternalPosition(external); err != nil || !applied {
		t.Fatal(applied, err)
	}
	snap, _ = account.Snapshot(context.Background())
	if !snap.RiskFrozen || snap.Lots[0].SignedSteps != 10 || len(snap.ExternalPositions) != 1 || snap.ExternalPositions[0].SignedSteps != 2 {
		t.Fatal("manual trade stole strategy attribution", snap)
	}
	event.ID = "late"
	if _, err := account.ApplyFunding(event); err == nil {
		t.Fatal("late settlement used changed actual ownership")
	}
	external.ID = "forced"
	external.Kind = execution.Liquidation
	external.Side = execution.Sell
	external.Steps = 1
	external.AtMS = 103
	if _, err := account.ApplyExternalPosition(external); err != nil {
		t.Fatal(err)
	}
	snap, _ = account.Snapshot(context.Background())
	if !snap.RiskFrozen || snap.Lots[0].SignedSteps != 10 || snap.ExternalPositions[0].SignedSteps != 1 {
		t.Fatal("forced liquidation guessed TS lot", snap)
	}
	if err := account.StartReports(); err != nil {
		t.Fatal(err)
	}
	if err := account.StartReports(); err != nil {
		t.Fatal(err)
	}
	failures, err := account.ReportErrors()
	if err != nil {
		t.Fatal(err)
	}
	adapter.input <- execution.BanexgStreamReport{UnassignedExchangeID: "venue-manual", Err: errors.New("unmatched identity")}
	select {
	case err := <-failures:
		if err == nil {
			t.Fatal("missing report error")
		}
	case <-time.After(time.Second):
		t.Fatal("unknown private report was swallowed")
	}
	if adapter.starts.Load() != 1 {
		t.Fatal("multiple private streams", adapter.starts.Load())
	}
	f.process.Close()
	if !adapter.closed.Load() {
		t.Fatal("source and report worker were not joined before adapter close")
	}
	if err := account.CashEvent(execution.CashEvent{ID: "after-close", Kind: execution.ExternalCashChange}); err == nil {
		t.Fatal("account mutated after process close")
	}
}
