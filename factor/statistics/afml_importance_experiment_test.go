package statistics

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestImportanceExperimentSeedGroupsAndBudget(t *testing.T) {
	c := DefaultImportanceExperimentConfig()
	c.Rows = 500
	c.Features = 8
	c.Informative = 2
	c.Redundant = 2
	c.Trees = 20
	c.Instruments = 1
	d, err := GenerateSyntheticImportanceData(c)
	if err != nil {
		t.Fatal(err)
	}
	e, _ := GenerateSyntheticImportanceData(c)
	if !reflect.DeepEqual(d, e) {
		t.Fatal("seed not reproducible")
	}
	d.Rows[0][0] = 999
	if e.Rows[0][0] == 999 {
		t.Fatal("shared generated storage")
	}
	c.MaxWork = 1
	if _, err = RunImportanceExperiment(c); err == nil {
		t.Fatal("budget accepted")
	}
	c.MaxWork = 100000000
	r, err := RunImportanceExperiment(c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = json.Marshal(r); err != nil {
		t.Fatal("undefined score not JSON-safe", err)
	}
	means := map[string]float64{}
	counts := map[string]int{}
	for _, f := range r.Analyses[0].Features {
		if f.MDI.Mean.Value != nil {
			means[f.Group] += *f.MDI.Mean.Value
			counts[f.Group]++
		}
	}
	if means["I"]/float64(counts["I"]) <= means["N"]/float64(counts["N"]) {
		t.Fatalf("informative group MDI <= noise: %v", means)
	}
	for _, method := range []string{"MDA", "SFI"} {
		group := map[string]float64{}
		for _, f := range r.Analyses[0].Features {
			s := f.MDA
			if method == "SFI" {
				s = f.SFI
			}
			if s.Mean.Value == nil {
				t.Fatalf("unexpected undefined %s mean", method)
			}
			group[f.Group] += *s.Mean.Value
		}
		if group["I"]/float64(c.Informative) <= group["N"]/float64(c.Features-c.Informative-c.Redundant) {
			t.Fatalf("informative %s <= noise: %v", method, group)
		}
	}
	if len(r.Analyses[0].Folds) != c.Folds || len(r.Analyses[0].Features) != c.Features {
		t.Fatal("missing complete evidence")
	}
}

func TestImportancePCAFitExcludesHeldOut(t *testing.T) {
	c := DefaultImportanceExperimentConfig()
	c.Rows = 80
	c.Features = 6
	c.Informative = 2
	c.Redundant = 1
	c.Trees = 3
	c.Folds = 2
	c.Instruments = 1
	d, err := GenerateSyntheticImportanceData(c)
	if err != nil {
		t.Fatal(err)
	}
	split := PurgedSplit{}
	for i := 0; i < c.Rows; i++ {
		if i < 40 {
			split.Train = append(split.Train, i)
		} else {
			split.Test = append(split.Test, i)
		}
	}
	a, err := importancePCAFold(d.Rows, d.Labels, split, c)
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range split.Test {
		for j := range d.Rows[i] {
			d.Rows[i][j] += 1000 * float64(j+1)
		}
	}
	b, err := importancePCAFold(d.Rows, d.Labels, split, c)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a.PCA, b.PCA) {
		t.Fatal("held-out rows affected PCA fit")
	}
	for _, i := range a.Train {
		for _, j := range a.Test {
			if i == j {
				t.Fatal("train/test overlap")
			}
		}
	}
}
