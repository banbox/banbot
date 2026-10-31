package data_test

import (
	"context"
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

func TestTimeSeriesManagedGenerationWarmupFillEnvironmentEndAndJoin(t *testing.T) {
	rt, _, symbols := generationRuntimeFixture(t)
	account := rt.SharedExecution()
	if err := account.CashEvent(execution.CashEvent{ID: "ts-capital", Kind: execution.CapitalTransfer, AtMS: rt.Clock.TimeMS(), Postings: []execution.CashPosting{{Amount: decimal.NewFromInt(-1000)}, {Strategy: "ts", Amount: decimal.NewFromInt(1000)}}}); err != nil {
		t.Fatal(err)
	}
	warm, live := 0, 0
	var entered []*ormo.InOutOrder
	sub := &strat.DataSub{Source: "kline", ExSymbol: symbols[1], TimeFrame: "1m", WarmupNum: 2, Fields: []string{"close"}}
	job := &strat.StratJob{Account: "default", Symbol: symbols[1], TimeFrame: "1m", CloseLong: true, CloseShort: true, Strat: &strat.TradeStrat{Name: "legacy"}}
	job.Strat.OnDataSubs = func(*strat.StratJob) []*strat.DataSub { return []*strat.DataSub{sub} }
	job.Strat.OnData = func(job *strat.StratJob, event strat.DataEvent) {
		if event.IsWarmUp {
			warm++
			return
		}
		live++
		if live == 1 {
			if err := job.OpenOrder(&strat.EnterReq{Amount: 1, Tag: "managed-ts"}); err != nil {
				t.Error(err)
			}
			orders, _, err := biz.ProcessJobOrders(job)
			if err != nil {
				t.Error(err)
			}
			entered = append(entered, orders...)
		}
	}
	if err := rt.BindFactorLegacyJobs([]*strat.StratJob{job}, []*strat.DataSub{sub}); err != nil {
		t.Fatal(err)
	}
	harness, provider := data.NewFactorLiveGenerationHarness(rt.DataDeps())
	harness.History = func(_ int, symbol *orm.ExSymbol, tf string, count int) ([]*orm.DataSeries, error) {
		var rows []*orm.DataSeries
		for i := count; i > 0; i-- {
			at := int64(180000 - i*60000)
			rows = append(rows, &orm.DataSeries{Source: "kline", Sid: symbol.ID, ExSymbol: symbol, TimeFrame: tf, TimeMS: at, EndMS: at + 60000, Closed: true, Values: map[string]any{"close": 100.0}})
		}
		return rows, nil
	}
	installation, err := rt.InstallTimeSeriesLive(provider)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { installation.Stop(); _ = installation.Join() }()
	if warm != 2 || live != 0 {
		t.Fatalf("warmup traded or callbacks lost: warm=%d live=%d", warm, live)
	}
	rt.Clock.SetTimeMS(240002)
	row := &orm.DataSeries{Source: "kline", Sid: 1, ExSymbol: symbols[1], TimeFrame: "1m", TimeMS: 180000, EndMS: 240000, Closed: true, Values: map[string]any{"close": 100.0}}
	if err := harness.Emit(0, row); err != nil {
		t.Fatal(err)
	}
	snapshot, err := account.Snapshot(context.Background())
	if err != nil || len(snapshot.Lots) != 1 || snapshot.Lots[0].SignedSteps != 10 || live != 1 {
		t.Fatalf("real TS callback did not reach shared ledger: lots=%+v live=%d err=%v", snapshot.Lots, live, err)
	}
	rt.Clock.SetTimeMS(1_000_000)
	if err := harness.EndEnvironment(0, row); err != nil {
		t.Fatal(err)
	}
	snapshot, err = account.Snapshot(context.Background())
	if err != nil || len(snapshot.Lots) != 0 {
		t.Fatalf("managed feeder environment end left exposure: %+v err=%v", snapshot.Lots, err)
	}
	if len(entered) != 1 || entered[0].ExitTag != core.ExitTagEnvEnd {
		t.Fatalf("environment end exit not projected: %+v", entered)
	}
	installation.Stop()
	if err := installation.Join(); err != nil {
		t.Fatal(err)
	}
	if !harness.Stopped(0) {
		t.Fatal("physical generation not stopped/joined")
	}
	if err := harness.Emit(0, row); err == nil || live != 1 {
		t.Fatal("stopped generation delivered a strategy callback")
	}
	if _, ok := biz.GetOdMgrWithState(rt.Trading, "default").(*biz.SharedOrderMgr); !ok {
		t.Fatal("managed generation installed legacy sender")
	}
}
