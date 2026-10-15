package runner

import (
	"context"
	"github.com/banbox/banbot/factor/research"
	"reflect"
	"testing"
)

func TestMultiHorizonReplayIndependentMaturityAndFutureIsolation(t *testing.T) {
	const hour = int64(3600000)
	var baseline *capture
	for _, changed := range []bool{false, true} {
		c := archiveConfig(t, changed)
		c.MaxPending = 32
		c.Manifest.Labels = append(c.Manifest.Labels, research.LabelSpec{Name: "4h", Kind: research.ExecutableReturn, Horizon: 4 * hour, PeriodsPerYear: 2190, Overlapping: true})
		out := &capture{targets: map[int64]map[int32]float64{}}
		result, err := Run(context.Background(), c, nil, out)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Summary["score"]) != 2 || result.UnresolvedByHorizon["4h"] <= result.UnresolvedByHorizon["1h"] {
			t.Fatalf("horizon maturity not independent: %+v", result)
		}
		if !changed {
			baseline = out
			continue
		}
		for at, targets := range baseline.targets {
			if at < 27*hour && !reflect.DeepEqual(targets, out.targets[at]) {
				t.Fatalf("future labels leaked at %d", at)
			}
		}
	}
}

func TestMultiHorizonBoundAndExplicitHistoryLabel(t *testing.T) {
	c := archiveConfig(t, false)
	c.Manifest.Labels = append(c.Manifest.Labels, research.LabelSpec{Name: "4h", Kind: research.ExecutableReturn, Horizon: 4 * 3600000, PeriodsPerYear: 2190})
	c.MaxPending = 1
	if _, err := Run(context.Background(), c, nil, nil); err == nil {
		t.Fatal("accepted unbounded label queue")
	}
	c.MaxPending = 32
	c.Combo = research.ComboSpec{Method: research.HistoryRankIC, Columns: []string{"momentum"}, Label: "absent"}
	if _, err := Run(context.Background(), c, nil, nil); err == nil {
		t.Fatal("history accepted undeclared horizon")
	}
}

func TestHistoryDefaultHorizonIsCanonicalAndLegacySingleLabelUnchanged(t *testing.T) {
	c := archiveConfig(t, false)
	c.Combo = research.ComboSpec{Method: research.HistoryIC, Columns: []string{"momentum"}}
	_, legacy, err := compileDecision(c)
	if err != nil || legacy.Label != "" {
		t.Fatalf("single-label identity changed: %+v %v", legacy, err)
	}
	c.Manifest.Labels = append(c.Manifest.Labels, research.LabelSpec{Name: "4h", Kind: research.ExecutableReturn, Horizon: 4 * 3600000, PeriodsPerYear: 2190})
	for i := 0; i < 2; i++ {
		c.Manifest.Labels[0], c.Manifest.Labels[1] = c.Manifest.Labels[1], c.Manifest.Labels[0]
		_, combo, err := compileDecision(c)
		if err != nil || combo.Label != "1h" {
			t.Fatalf("default depends on label order: %+v %v", combo, err)
		}
	}
}
