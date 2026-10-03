package runner

import (
	"context"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/orm"
	"path/filepath"
	"reflect"
	"testing"
)

type parityOutput struct {
	capture
	frames []factor.Frame
}

func (o *parityOutput) Decision(f factor.Frame, p *factor.TargetPortfolio, d []factor.Diagnostic) error {
	o.frames = append(o.frames, f)
	return o.capture.Decision(f, p, d)
}
func TestArchiveDecisionDelayMatchesLiveVisibilityAndRejectsFutureRevision(t *testing.T) {
	c := archiveConfig(t, false)
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	c.Manifest.Portfolio.K = 1
	c.Mode = Research
	c.DecisionDelayMS = 3
	c.Snapshot.ReplayTime = 1
	u := &c.Snapshot.Universe
	u.Investable = u.Investable[:3]
	u.Reference = u.Reference[:3]
	u.Tradable = u.Tradable[:3]
	u.Tracked = u.Tracked[:3]
	u.Evaluation = u.Evaluation[:3]
	plan, err := factor.New().Add("close", factor.Field("kline", "close", "1h")).Compile()
	if err != nil {
		t.Fatal(err)
	}
	c.Plan = plan
	c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"close"}, Weights: map[string]float64{"close": 1}}
	const grid int64 = 3600000
	rows := []factor.VersionRecord{}
	store, _ := factor.NewVersionStore(10)
	for sid := int32(1); sid <= 3; sid++ {
		r := factor.VersionRecord{Series: orm.DataSeries{Source: "kline", Sid: sid, TimeFrame: "1h", TimeMS: 0, EndMS: grid, Closed: true, Values: map[string]any{"close": float64(sid) * 100, "nullable": nil, "custom": int64(sid)}}, EventTime: grid, AvailableAt: grid + 2, IngestedAt: grid + 2, Revision: 1, SourceVersion: "v1"}
		rows = append(rows, r)
		if err = store.Put(r); err != nil {
			t.Fatal(err)
		}
	}
	future := rows[0]
	future.Revision = 2
	future.AvailableAt = grid + 100
	future.IngestedAt = grid + 100
	future.Series.Values = map[string]any{"close": 99999.0, "nullable": nil, "custom": int64(1)}
	if err = store.Put(future); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "delayed.gob")
	if _, err = store.Export(path); err != nil {
		t.Fatal(err)
	}
	c.Chunks = []Chunk{{Path: path, From: grid, To: grid + 4}}
	o := &parityOutput{capture: capture{targets: map[int64]map[int32]float64{}}}
	r, err := Run(context.Background(), c, nil, o)
	if err != nil {
		t.Fatal(err)
	}
	if r.Decisions != 1 || o.frames[0].GridTime != grid || o.frames[0].DecisionTime != grid+3 || o.frames[0].Values["close"][1].Value != 100 {
		t.Fatalf("grid/cutoff or future isolation failed: %+v", r)
	}
	clock := grid + 2
	lo := &parityOutput{capture: capture{targets: map[int64]map[int32]float64{}}}
	live, err := NewLive(c, &liveSink{}, func() int64 { return clock }, lo)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Stop()
	for _, row := range rows {
		if err = live.Observe(context.Background(), row); err != nil {
			t.Fatal(err)
		}
	}
	clock = grid + 3
	if err = live.Flush(context.Background(), grid); err != nil {
		t.Fatal(err)
	}
	if len(lo.frames) != 1 || !reflect.DeepEqual(o.frames, lo.frames) || !reflect.DeepEqual(o.targets, lo.targets) {
		t.Fatalf("archive/live visibility parity failed: archive=%+v live=%+v", o.frames, lo.frames)
	}
	c.DecisionDelayMS = 0
	early, err := Run(context.Background(), c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if early.Incomplete != 1 || early.Decisions != 0 || early.StrategyHash != r.StrategyHash || early.ManifestID == r.ManifestID {
		t.Fatal("delay assumption leaked future data or changed strategy identity")
	}
}
