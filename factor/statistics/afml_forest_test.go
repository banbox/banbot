package statistics

import (
	"bytes"
	"encoding/json"
	"math"
	"testing"
)

func TestAFMLForestLearnsNonlinearInteractionAndRestores(t *testing.T) {
	var features [][]float64
	var labels []int
	for repeat := 0; repeat < 80; repeat++ {
		for a := -1; a <= 1; a += 2 {
			for b := -1; b <= 1; b += 2 {
				features = append(features, []float64{float64(a), float64(b)})
				label := 0
				if a*b > 0 {
					label = 1
				}
				labels = append(labels, label)
			}
		}
	}
	config := RandomForestConfig{Trees: 80, MaxDepth: 3, MinSamplesLeaf: 2, MaxFeatures: 2, Seed: 17, Criterion: "entropy"}
	model, err := TrainRandomForest(features, labels, nil, config)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		p, err := model.PredictProbability(features[i])
		if err != nil {
			t.Fatal(err)
		}
		if (labels[i] == 1 && p < .8) || (labels[i] == 0 && p > .2) {
			t.Fatalf("nonlinear XOR feature %v label %d probability %g", features[i], labels[i], p)
		}
	}
	raw, err := model.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	again, _ := TrainRandomForest(features, labels, nil, config)
	same, _ := again.Snapshot()
	if !bytes.Equal(raw, same) {
		t.Fatal("seed not deterministic")
	}
	restored, err := RestoreRandomForest(raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range features[:4] {
		want, _ := model.PredictProbability(x)
		got, _ := restored.PredictProbability(x)
		afmlNear(t, got, want)
	}
	// Neither caller-owned training data nor snapshot bytes can mutate the fit.
	want, _ := model.PredictProbability([]float64{1, 1})
	features[0][0] = 100
	raw[0] = 0
	got, _ := model.PredictProbability([]float64{1, 1})
	afmlNear(t, got, want)
}

func TestAFMLForestWeightsAndInputOwnership(t *testing.T) {
	x := [][]float64{{1}, {1}}
	y := []int{0, 1}
	config := RandomForestConfig{Trees: 200, MaxDepth: 1, MinSamplesLeaf: 1, Seed: 9, ClassWeights: map[int]float64{1: 20}}
	weighted, err := TrainRandomForest(x, y, []float64{1, 2}, config)
	if err != nil {
		t.Fatal(err)
	}
	p, _ := weighted.PredictProbability([]float64{1})
	if p < .65 {
		t.Fatalf("weights did not affect leaves %g", p)
	}
	config.ClassWeights[1] = .01
	after, _ := weighted.PredictProbability([]float64{1})
	afmlNear(t, after, p)
	omit, err := TrainRandomForest(x, y, []float64{0, 1}, config)
	if err != nil {
		t.Fatal(err)
	}
	p, _ = omit.PredictProbability([]float64{1})
	afmlNear(t, p, 1)
	if _, err = TrainRandomForest(x, y, []float64{0, 0}, config); err == nil {
		t.Fatal("zero total weight accepted")
	}
	if _, err = TrainRandomForest(x, y, []float64{math.SmallestNonzeroFloat64, math.MaxFloat64}, config); err == nil {
		t.Fatal("unrepresentable normalized weight accepted")
	}
	config.ClassWeights[0] = math.SmallestNonzeroFloat64
	if _, err = TrainRandomForest(x, y, []float64{math.SmallestNonzeroFloat64, 1}, config); err == nil {
		t.Fatal("effective weight underflow accepted")
	}
	if _, err = TrainRandomForest([][]float64{{math.NaN()}}, []int{1}, nil, config); err == nil {
		t.Fatal("NaN feature accepted")
	}
	if _, err = weighted.PredictProbability([]float64{1, 2}); err == nil {
		t.Fatal("wrong prediction dimensions accepted")
	}
	if _, err = weighted.PredictProbability([]float64{math.Inf(1)}); err == nil {
		t.Fatal("infinite prediction accepted")
	}
}

func TestAFMLForestRestoreRejectsBrokenArtifact(t *testing.T) {
	model, err := TrainRandomForest([][]float64{{0}, {1}, {2}, {3}}, []int{0, 0, 1, 1}, nil, RandomForestConfig{Trees: 1, MaxDepth: 2, MinSamplesLeaf: 1, Seed: 4})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := model.Snapshot()
	var artifact afmlForestArtifact
	if err = json.Unmarshal(raw, &artifact); err != nil {
		t.Fatal(err)
	}
	artifact.Trees[0][0].Feature = 0
	artifact.Trees[0][0].Left = 0
	artifact.Trees[0][0].Right = 0
	broken, _ := json.Marshal(artifact)
	if _, err = RestoreRandomForest(broken); err == nil {
		t.Fatal("cycle accepted")
	}
	artifact.Version = "unknown"
	broken, _ = json.Marshal(artifact)
	if _, err = RestoreRandomForest(broken); err == nil {
		t.Fatal("unknown version accepted")
	}
}

func TestAFMLForestBalancedSubsampleAndSequentialSampling(t *testing.T) {
	x, y := make([][]float64, 100), make([]int, 100)
	spans := make([]EventSpan, 100)
	for i := range x {
		x[i] = []float64{1}
		if i >= 90 {
			y[i] = 1
		}
		spans[i] = EventSpan{i, i + 2}
	}
	c := RandomForestConfig{Trees: 100, MaxDepth: 1, MinSamplesLeaf: 1, Seed: 23}
	ordinary, err := TrainRandomForest(x, y, nil, c)
	if err != nil {
		t.Fatal(err)
	}
	p, _ := ordinary.PredictProbability([]float64{1})
	if p < .05 || p > .15 {
		t.Fatalf("unbalanced class frequency lost %g", p)
	}
	c.BalancedSubsample = true
	balanced, err := TrainRandomForest(x, y, nil, c)
	if err != nil {
		t.Fatal(err)
	}
	p, _ = balanced.PredictProbability([]float64{1})
	afmlNear(t, p, .5)
	sequential, err := TrainSequentialBootstrapForest(x, y, nil, c, 102, spans)
	if err != nil {
		t.Fatal(err)
	}
	copy, err := TrainSequentialBootstrapForest(x, y, nil, c, 102, spans)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := sequential.Snapshot()
	b, _ := copy.Snapshot()
	if string(a) != string(b) {
		t.Fatal("sequential forest seed is not deterministic")
	}
	p, _ = sequential.PredictProbability([]float64{1})
	afmlNear(t, p, .5)
	if _, err = TrainSequentialBootstrapForest(x, y, nil, c, 101, spans); err == nil {
		t.Fatal("invalid event lifespan accepted")
	}
}
