package statistics

import (
	"encoding/json"
	"math"
	"math/rand"
	"testing"
)

func TestAFMLIntrinsicMDIOriginalBootstrapAndRoundtrip(t *testing.T) {
	x := [][]float64{{0, 1}, {1, 1}, {2, 1}, {3, 1}, {4, 1}, {5, 1}}
	y := []int{0, 0, 0, 1, 1, 1}
	w := []float64{1, 2, 3, 4, 5, 6}
	c := RandomForestConfig{Trees: 12, MaxDepth: 2, MinSamplesLeaf: 1, MaxFeatures: 2, Seed: 37, Criterion: "gini"}
	f, err := TrainRandomForest(x, y, w, c)
	if err != nil {
		t.Fatal(err)
	}
	// Reconstruct actual first bootstrap, rather than routing the original dataset
	// through the fitted tree (which would lose repeated sample multiplicities).
	rng := rand.New(rand.NewSource(c.Seed))
	mass, pos := 0., 0.
	for range x {
		i := rng.Intn(len(x))
		mass += w[i] / 6
		if y[i] == 1 {
			pos += w[i] / 6
		}
	}
	afmlNear(t, f.artifact.Trees[0][0].Mass, mass)
	afmlNear(t, f.artifact.Trees[0][0].Impurity, 2*(pos/mass)*(1-pos/mass))
	imp, err := f.MeanDecreaseImpurity()
	if err != nil {
		t.Fatal(err)
	}
	afmlNear(t, imp[0].Mean, 1)
	if !math.IsNaN(imp[1].Mean) || imp[1].ValidFolds != 0 {
		t.Fatalf("constant feature: %+v", imp)
	}
	raw, _ := f.Snapshot()
	restored, err := RestoreRandomForest(raw)
	if err != nil {
		t.Fatal(err)
	}
	again, _ := restored.MeanDecreaseImpurity()
	afmlNear(t, again[0].Mean, imp[0].Mean)
	// Legacy artifacts preserve predictions but cannot invent training metadata.
	legacy := f.artifact
	legacy.Version = "afml-rf-v1"
	old, _ := json.Marshal(legacy)
	v1, err := RestoreRandomForest(old)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := f.PredictProbability(x[0])
	got, _ := v1.PredictProbability(x[0])
	afmlNear(t, got, want)
	if _, err = v1.MeanDecreaseImpurity(); err == nil {
		t.Fatal("legacy MDI invented")
	}
	var bad afmlForestArtifact
	json.Unmarshal(raw, &bad)
	bad.Trees[0][0].Mass *= 2
	broken, _ := json.Marshal(bad)
	if _, err = RestoreRandomForest(broken); err == nil {
		t.Fatal("inconsistent mass accepted")
	}
}

func TestAFMLIntrinsicMDISourceZeroExclusion(t *testing.T) {
	// Two independently normalized trees use separate features. A pooled mean
	// including zeros gives .5/.5; source zero exclusion gives .5/.5 after final
	// normalization, but each feature has only one observation and undefined SE.
	tree := func(feature int) []afmlForestNode {
		return []afmlForestNode{
			{Feature: feature, Left: 1, Right: 2, Mass: 4, Probability: .5, Impurity: .5},
			{Feature: -1, Left: -1, Right: -1, Mass: 2, Probability: 0},
			{Feature: -1, Left: -1, Right: -1, Mass: 2, Probability: 1},
		}
	}
	f := &RandomForest{artifact: afmlForestArtifact{Version: "afml-rf-v2", Features: 2, Trees: [][]afmlForestNode{tree(0), tree(1)}}}
	r, err := f.MeanDecreaseImpurity()
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range r {
		afmlNear(t, v.Mean, .5)
		if v.ValidFolds != 1 || !math.IsNaN(v.StdError) {
			t.Fatalf("zero exclusion %+v", v)
		}
	}
}

func TestAFMLBiasVarianceAndIndependentVotes(t *testing.T) {
	r, err := PredictionBiasVariance([]float64{1, 3, 5}, 2, .25)
	if err != nil {
		t.Fatal(err)
	}
	afmlNear(t, r.MeanPrediction, 3)
	afmlNear(t, r.BiasSquared, 1)
	afmlNear(t, r.Variance, 8./3)
	afmlNear(t, r.ExpectedMSE, 11./3+.25)
	v, err := IndependentBaggingAccuracy(3, 2, .6)
	if err != nil {
		t.Fatal(err)
	}
	afmlNear(t, v.SufficientProbability, .648)
	afmlNear(t, v.NecessaryProbability, .648)
	even, _ := IndependentBaggingAccuracy(2, 2, .5)
	afmlNear(t, even.SufficientProbability, .25)
	multi, _ := IndependentBaggingAccuracy(10, 3, .5)
	afmlNear(t, multi.NecessaryProbability, 1-176./1024)
	afmlNear(t, multi.SufficientProbability, 386./1024)
	weak, _ := IndependentBaggingAccuracy(101, 2, .4)
	strong, _ := IndependentBaggingAccuracy(101, 2, .6)
	if weak.SufficientProbability >= .4 || strong.SufficientProbability <= .6 {
		t.Fatal("independent ensemble amplification")
	}
	tiny, _ := IndependentBaggingAccuracy(100, 2, .01)
	if tiny.SufficientProbability <= 0 || tiny.SufficientProbability > 1e-60 {
		t.Fatal("tail cancellation", tiny)
	}
	for _, p := range []float64{-1, math.NaN(), 2} {
		if _, err = IndependentBaggingAccuracy(10, 2, p); err == nil {
			t.Fatal("bad p")
		}
	}
	if _, err = PredictionBiasVariance([]float64{1}, 2, -1); err == nil {
		t.Fatal("negative noise")
	}
}
