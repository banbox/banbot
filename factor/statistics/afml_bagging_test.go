package statistics

import (
	"math"
	"reflect"
	"testing"
)

func TestAFMLBagSpecificAttribution(t *testing.T) {
	close := []float64{1, math.Exp(.1), 1, math.Exp(.2)}
	spans := []EventSpan{{1, 2}, {2, 3}}
	weights, err := BootstrapReturnWeights(close, spans, []int{0, 0, 1}, 1)
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []float64{.25, .25, 2.5} {
		afmlMicroNear(t, weights[i], want, 1e-12)
	}
	global, _ := AttributedReturnWeights(close, spans)
	if weights[0] == global[0] {
		t.Fatal("bag reused original fixed weights")
	}
	decay, err := BootstrapReturnWeights(close, spans, []int{1, 0, 0}, .5)
	if err != nil || decay[0] != 2.5 || decay[1] >= .25 || decay[2] >= .25 {
		t.Fatalf("chronological bag decay %v %v", decay, err)
	}
}

func TestAFMLThreeClassBaggingAndOwnership(t *testing.T) {
	close := []float64{1}
	var x [][]float64
	var labels []int
	var spans []EventSpan
	for i := 0; i < 45; i++ {
		label := i%3 - 1
		x = append(x, []float64{float64(label)})
		labels = append(labels, label)
		spans = append(spans, EventSpan{i + 1, i + 1})
		close = append(close, math.Exp(float64(i+1)*.01))
	}
	cfg := SequentialBaggingConfig{Trees: 12, MaxDepth: 3, MinSamplesLeaf: 1, SampleFraction: 1, DecayC: 1, Seed: 7, MaxWork: 1000000}
	model, err := TrainSequentialBaggedClassifier(x, labels, close, spans, cfg)
	if err != nil {
		t.Fatal(err)
	}
	other, err := TrainSequentialBaggedClassifier(x, labels, close, spans, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(model.Audits(), other.Audits()) {
		t.Fatal("seed not reproducible")
	}
	for label := -1; label <= 1; label++ {
		p, err := model.PredictProbabilities([]float64{float64(label)})
		if err != nil || p[label+1] < .99 {
			t.Fatalf("class%d not learned %v %v", label, p, err)
		}
	}
	audit := model.Audits()
	audit[0].Draws[0] = -1
	audit[0].Weights[0] = math.NaN()
	if model.Audits()[0].Draws[0] < 0 || math.IsNaN(model.Audits()[0].Weights[0]) {
		t.Fatal("audit aliases owned model")
	}
	x[0][0] = math.NaN()
	close[0] = math.NaN()
	labels[0] = 8
	p, err := model.PredictProbabilities([]float64{0})
	if err != nil || p[1] < .99 {
		t.Fatal("caller mutated model")
	}
	cfg.MaxWork = 1
	if _, err := TrainSequentialBaggedClassifier([][]float64{{1}, {2}}, []int{-1, 1}, []float64{1, 2, 3}, []EventSpan{{0, 1}, {1, 2}}, cfg); err == nil {
		t.Fatal("budget not enforced")
	}
}

func TestAFMLFFDTrainingSelection(t *testing.T) {
	var levels []float64
	sum := 0.
	for i := 0; i < 512; i++ {
		sum += 4.6 + .5*math.Sin(float64(i)*.39) + .015*math.Sin(float64(i)*1.7)
		levels = append(levels, sum)
	}
	spec := FFDSelectionSpec{DStep: .1, Threshold: 1e-5, ADFCritical: -2.8623, MaxWeights: 10000, MinSamples: 64, MaxWork: 40000000}
	fit, err := SelectStationaryFFD(levels, spec)
	if err != nil {
		t.Fatal(err)
	}
	if fit.D <= 0 || fit.D > 2 || fit.ADFStatistic >= spec.ADFCritical {
		t.Fatalf("no actual ADF rejection %+v", fit)
	}
	for _, trial := range fit.Trials[:len(fit.Trials)-1] {
		if trial.Reason == "" && trial.Statistic < spec.ADFCritical {
			t.Fatal("did not select minimum feasible grid d")
		}
	}
	values, _ := FractionalDifference(levels, fit.Weights)
	adf, err := AugmentedDickeyFuller(values[len(fit.Weights)-1:], ADFSpec{Lags: 1, Intercept: true})
	if err != nil {
		t.Fatal(err)
	}
	afmlMicroNear(t, adf.TStatistic, fit.ADFStatistic, 0)
	spec.MaxWork = 1
	if _, err := SelectStationaryFFD(levels, spec); err == nil {
		t.Fatal("selection budget not enforced")
	}
}
