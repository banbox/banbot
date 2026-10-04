package runtime

import (
	"context"
	"encoding/json"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

type outsidePoolFundingSource struct {
	done           chan error
	subscriptions  []*strat.DataSub
	subscribeCount int
}

func (*outsidePoolFundingSource) Info() *orm.SeriesInfo {
	return orm.NewSeriesInfo("account_funding", "event", []orm.SeriesField{{Name: "rate", Type: "string"}, {Name: "mark", Type: "string"}, {Name: "account_amount", Type: "string"}, {Name: "settlement_id", Type: "string"}})
}
func (*outsidePoolFundingSource) FetchHistory(context.Context, *strat.DataSub, int64, int64) ([]*orm.DataRecord, error) {
	return nil, nil
}
func (s *outsidePoolFundingSource) SubscribeLive(_ context.Context, subs []*strat.DataSub, sink data.DataSink) error {
	s.subscribeCount++
	s.subscriptions = subs
	for _, sub := range subs {
		if sub.ExSymbol.ID != 1 {
			continue
		}
		row := &orm.DataRecord{Sid: 1, TimeMS: 101, EndMS: 102, Closed: true, Values: map[string]any{"settlement_id": "outside-pool-funding", "mark": "100", "rate": "0.01", "account_amount": "-1"}}
		for i := 0; i < 2; i++ {
			if err := sink.Emit(sub, []*orm.DataRecord{row}); err != nil {
				s.done <- err
				return err
			}
		}
	}
	s.done <- nil
	return nil
}

func TestFactorLiveSourceSubscribesAndSettlesFundingOutsideFactorPool(t *testing.T) {
	f := newSharedTriggerFixture(t)
	f.entry(t, &strat.EnterReq{Tag: "funded", Amount: 1})
	f.rt.Clock.SetTimeMS(104)
	f.rt.Symbols = orm.NewSymbolStateWithIdentity("fixture", "linear")
	for _, symbol := range []*orm.ExSymbol{{ID: 1, Symbol: "BTC", Exchange: "fixture", Market: "linear"}, {ID: 2, Symbol: "factor-only", Exchange: "fixture", Market: "linear"}} {
		if err := f.rt.Symbols.CacheExSymbolChecked(symbol); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := os.ReadFile("../factor/runner/example.json")
	if err != nil {
		t.Fatal(err)
	}
	var c runner.Config
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	c.Snapshot.Universe = factor.Universe{Version: "factor-only", Static: true, Investable: []int32{2}, Reference: []int32{2}, Tradable: []int32{2}, Tracked: []int32{2}, Evaluation: []int32{2}}
	c.Snapshot.SIDMap = map[int32]string{2: "factor-only"}
	c.Prices = runner.PriceStream{Source: "account_funding", TimeFrame: "event", Field: "mark"}
	c.FundingSource = "account_funding"
	c.Snapshot.Schemas[c.FundingSource], c.Snapshot.SourceVersions[c.FundingSource] = "schema-v1", "v1"
	c.Plan, err = factor.New().Add("close", factor.Field("account_funding", "mark", "event")).Compile()
	if err != nil {
		t.Fatal(err)
	}
	c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"close"}, Weights: map[string]float64{"close": 1}}
	unit := f.bridge.Instruments["BTC"]
	factorUnit := unit
	factorUnit.ID = "factor-only"
	sink := &runner.AccountSink{Account: f.rt.SharedExecution(), AccountID: "default", StrategyID: "cs", Currency: "USDT", Instruments: map[int32]execution.Instrument{2: factorUnit}, FundingInstruments: map[int32]execution.Instrument{1: unit}, AuthoritativeFunding: true}
	engine, err := runner.NewLive(c, sink, f.rt.Clock.TimeMS, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Stop()
	secondCfg := c
	secondCfg.StrategyID = "second-cs"
	secondEngine, err := runner.NewLive(secondCfg, sink, f.rt.Clock.TimeMS, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer secondEngine.Stop()
	source := &outsidePoolFundingSource{done: make(chan error, 1)}
	if err := f.rt.Catalog.RegisterDataSource(source); err != nil {
		t.Fatal(err)
	}
	mapped := 0
	mapper := func(s *orm.DataSeries, received int64) (factor.VersionRecord, error) {
		mapped++
		return factor.VersionRecord{Series: *s, EventTime: s.EndMS, AvailableAt: s.EndMS, IngestedAt: received, Revision: 1, SourceVersion: "v1"}, nil
	}
	_, err = f.rt.SubscribeFactorsLive(data.NewLiveSourceProvider(f.rt.Catalog), []*runner.Live{engine, secondEngine}, []runner.Config{c, secondCfg}, []func(*orm.DataSeries, int64) (factor.VersionRecord, error){mapper, mapper})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-source.done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("funding source did not complete")
	}
	if mapped != 4 || len(source.subscriptions) != 2 || source.subscribeCount != 1 {
		t.Fatalf("outside funding not dispatched: mapped=%d subscriptions=%d", mapped, len(source.subscriptions))
	}
	for _, sub := range source.subscriptions {
		for _, field := range []string{"rate", "mark", "account_amount", "settlement_id"} {
			if !slices.Contains(sub.Fields, field) {
				t.Fatalf("funding field %s missing", field)
			}
		}
	}
	snapshot, err := f.rt.SharedExecution().Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.AccountSettledCash.Equal(decimal.NewFromInt(999)) || !snapshot.SyntheticStrategyCash["ts"].Equal(decimal.NewFromInt(999)) {
		t.Fatalf("outside funding missing or duplicated: %+v", snapshot)
	}
}
