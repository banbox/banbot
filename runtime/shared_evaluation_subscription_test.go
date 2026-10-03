package runtime

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
)

type missingEvaluationHistorySource struct {
	liveHistorySource
	missingFetches int
}

func (s *missingEvaluationHistorySource) FetchHistory(ctx context.Context, sub *orm.Subscription, from, to int64) ([]*orm.DataRecord, error) {
	if sub.ExSymbol.ID == 99 {
		s.missingFetches++
		return nil, nil
	}
	return s.liveHistorySource.FetchHistory(ctx, sub, from, to)
}

func TestFactorLiveEvaluationOnlyHistoryCannotBlockInstallation(t *testing.T) {
	for _, reference := range []bool{false, true} {
		name := "evaluation-only"
		if reference {
			name = "required-reference"
		}
		t.Run(name, func(t *testing.T) {
			f := newSharedTriggerFixture(t)
			raw, err := os.ReadFile("../factor/runner/example.json")
			if err != nil {
				t.Fatal(err)
			}
			var c runner.Config
			if err := json.Unmarshal(raw, &c); err != nil {
				t.Fatal(err)
			}
			c.Manifest.Costs.FundingPolicy = "explicit-zero"
			c.Factor.Source, c.Factor.Window = "side", 2
			c.Snapshot.Schemas["side"], c.Snapshot.SourceVersions["side"] = "schema-v1", "v1"
			c.Snapshot.Universe.Evaluation = append(c.Snapshot.Universe.Evaluation, 99)
			if reference {
				c.Snapshot.Universe.Reference = append(c.Snapshot.Universe.Reference, 99)
			}
			c.Snapshot.SIDMap[99] = "evaluation-only"
			for sid, symbol := range c.Snapshot.SIDMap {
				if err := f.rt.Symbols.CacheExSymbolChecked(&orm.ExSymbol{ID: sid, Symbol: symbol, Exchange: "<runtime-unconfigured>", Market: "<runtime-unconfigured>"}); err != nil {
					t.Fatal(err)
				}
			}
			f.rt.Clock.SetTimeMS(24*3600000 + 2)
			engine, err := runner.NewLive(c, &closedGridSink{}, f.rt.Clock.TimeMS, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				engine.Stop()
				if err := engine.Join(context.Background()); err != nil {
					t.Error(err)
				}
			}()
			side := &missingEvaluationHistorySource{liveHistorySource: liveHistorySource{name: "side", frequency: "1h", field: "close"}}
			tick := &liveHistorySource{name: "tick", frequency: "event", field: "price"}
			for _, source := range []data.DataSource{side, tick} {
				if err := f.rt.Catalog.RegisterDataSource(source); err != nil {
					t.Fatal(err)
				}
			}
			mapper := func(row *orm.DataSeries, received int64) (factor.VersionRecord, error) {
				return factor.VersionRecord{Series: *row, EventTime: row.EndMS, AvailableAt: row.EndMS, IngestedAt: received, Revision: 1, SourceVersion: "v1"}, nil
			}
			_, err = f.rt.SubscribeFactorLive(data.NewLiveSourceProvider(f.rt.Catalog), engine, c, mapper)
			if reference {
				if err == nil || side.missingFetches == 0 {
					t.Fatal("missing reference history did not block startup")
				}
			} else if err != nil || side.missingFetches != 0 {
				t.Fatalf("unused evaluation history blocked trading: fetches=%d err=%v", side.missingFetches, err)
			}
		})
	}
}
