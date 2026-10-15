package statistics

import (
	"math"
	"testing"
)

func TestChanCARTSSEAndGiniLeaves(t *testing.T) {
	x := [][]float64{{-3, 0}, {-2, 1}, {-1, 0}, {1, 1}, {2, 0}, {3, 1}}
	y := []float64{-2, -2, -2, 3, 3, 3}
	c := CARTConfig{MinLeaf: 2, MaxDepth: 8, MaxSplits: 10, MaxFeatures: 2, Criterion: "sse", Seed: 4}
	m, err := FitCART(x, y, c)
	if err != nil {
		t.Fatal(err)
	}
	for i, row := range x {
		p, _ := m.Predict(row)
		if p != y[i] {
			t.Fatal("SSE split/leaf")
		}
	}
	if m.Splits != 1 || m.Low != -2 || m.High != 3 {
		t.Fatal("extreme leaves")
	}
	c.Criterion = "gini"
	binary := []float64{0, 0, 0, 1, 1, 1}
	classifier, err := FitCART(x, binary, c)
	if err != nil {
		t.Fatal(err)
	}
	p, _ := classifier.Predict([]float64{2, 0})
	if p != 1 {
		t.Fatal("Gini split")
	}
	if _, err = FitCART(x, y, c); err == nil {
		t.Fatal("nonbinary Gini labels accepted")
	}
	c.MinLeaf = 4
	c.Criterion = "sse"
	constant, err := FitCART(x, y, c)
	if err != nil || constant.Splits != 0 {
		t.Fatal("minimum leaf violated")
	}
}

func TestChanEnsemblesAndCV(t *testing.T) {
	var x [][]float64
	var y []float64
	for i := 0; i < 100; i++ {
		v := float64(i) / 10
		x = append(x, []float64{v, math.Sin(v)})
		y = append(y, v*v)
	}
	c := CARTConfig{MinLeaf: 3, MaxDepth: 3, MaxSplits: 20, MaxFeatures: 2, Criterion: "sse", Seed: 3}
	selected, loss, err := SelectCARTFold(x, y, c, 5)
	if err != nil || selected == nil || !finite(loss) {
		t.Fatalf("CV %v", err)
	}
	for _, boost := range []bool{false, true} {
		config := TreeEnsembleConfig{Tree: c, Trees: 20, Bootstrap: !boost, Boost: boost, LearningRate: .1, Seed: 7}
		a, err := FitTreeEnsemble(x, y, config)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := FitTreeEnsemble(x, y, config)
		for _, row := range x {
			p, _ := a.Predict(row)
			q, _ := b.Predict(row)
			if p != q {
				t.Fatal("ensemble seed reproducibility")
			}
		}
		if boost {
			initial := 0.0
			for _, v := range y {
				initial += v / float64(len(y))
			}
			baseLoss, fitLoss := 0.0, 0.0
			for i, row := range x {
				p, _ := a.Predict(row)
				baseLoss += (y[i] - initial) * (y[i] - initial)
				fitLoss += (y[i] - p) * (y[i] - p)
			}
			if fitLoss >= baseLoss/10 {
				t.Fatal("LSBoost residual aggregation failed")
			}
		}
	}
}

func TestChanCVSelectsHeldOutBestSingleTree(t *testing.T) {
	x := make([][]float64, 10)
	y := make([]float64, 10)
	for i := range x {
		x[i] = []float64{float64(i)}
	}
	y[0] = 10
	c := CARTConfig{MinLeaf: 100, MaxDepth: 2, MaxSplits: 10, MaxFeatures: 1, Criterion: "sse", Seed: 3}
	m, loss, err := SelectCARTFold(x, y, c, 5)
	if err != nil {
		t.Fatal(err)
	}
	p, _ := m.Predict([]float64{0})
	if math.Abs(p-1.25) > 1e-12 || math.Abs(loss-1.5625) > 1e-12 {
		t.Fatalf("selected tree refit or wrong validation loss: %g %g", p, loss)
	}
	c.Criterion = "gini"
	y[0] = 1
	m, loss, err = SelectCARTFold(x, y, c, 5)
	if err != nil {
		t.Fatal(err)
	}
	p, _ = m.Predict([]float64{0})
	if math.Abs(p-.125) > 1e-12 || loss != 0 {
		t.Fatalf("classification CV must use majority error, not probability MSE: %g %g", p, loss)
	}
}
