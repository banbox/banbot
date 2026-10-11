package statistics

import (
	"math"
	"reflect"
	"testing"
)

func TestChanNeuralNonlinearTrainingAndCheckpoint(t *testing.T) {
	analytic := &NeuralNetwork{activation: "logistic", center: []float64{0}, scale: []float64{1}, targetScale: 1, layers: []neuralLayer{{Weights: [][]float64{{2, 0}}}, {Weights: [][]float64{{3, -1}}}}}
	v, err := analytic.Predict([]float64{.5})
	if err != nil || math.Abs(v-(3/(1+math.Exp(-1))-1)) > 1e-14 {
		t.Fatal("source logistic activation")
	}
	x := make([][]float64, 120)
	y := make([]float64, len(x))
	for i := range x {
		v := -2 + 4*float64(i)/119
		x[i] = []float64{v}
		y[i] = .01 * math.Tanh(2*v)
	}
	c := NeuralConfig{Hidden: []int{3}, Epochs: 2000, Patience: 100, Seed: 4}
	m, err := FitNeuralNetwork(x, y, c)
	if err != nil {
		t.Fatal(err)
	}
	again, err := FitNeuralNetwork(x, y, c)
	if err != nil || !reflect.DeepEqual(m, again) {
		t.Fatal("neural seeded fit differs")
	}
	loss := 0.0
	for i, row := range x {
		v, err := m.Predict(row)
		if err != nil {
			t.Fatal(err)
		}
		loss += (v - y[i]) * (v - y[i]) / 120
	}
	if loss > 1e-6 {
		t.Fatalf("nonlinear neural loss %g validation %g", loss, m.ValidationLoss)
	}
	if _, err := m.Predict([]float64{math.NaN()}); err == nil {
		t.Fatal("nonfinite prediction")
	}
	c.Networks = 3
	ensemble, err := FitNeuralEnsemble(x, y, c)
	if err != nil {
		t.Fatal(err)
	}
	for i, n := range ensemble.Models {
		if n.Epochs != c.Epochs {
			t.Fatal("external ensemble validation caused early stop")
		}
		if n.ValidationLoss < ensemble.Models[ensemble.Best].ValidationLoss {
			t.Fatalf("best selection%d", i)
		}
	}
	v, err = ensemble.Predict([]float64{.2})
	want, _ := ensemble.Models[ensemble.Best].Predict([]float64{.2})
	if err != nil || v != want {
		t.Fatal("best-model prediction")
	}
	ensemble.Average = true
	v, err = ensemble.Predict([]float64{.2})
	want = 0
	for _, n := range ensemble.Models {
		p, _ := n.Predict([]float64{.2})
		want += p / 3
	}
	if err != nil || v != want {
		t.Fatal("prediction average differs from parameter average")
	}
}

func TestChanNeuralSourceArchitectureFamilies(t *testing.T) {
	x := make([][]float64, 60)
	y := make([]float64, 60)
	for i := range x {
		v := -1 + 2*float64(i)/59
		x[i] = []float64{v}
		y[i] = v
	}
	for layers := 1; layers <= 3; layers++ {
		for units := 1; units <= 3; units++ {
			hidden := make([]int, layers)
			for j := range hidden {
				hidden[j] = units
			}
			m, err := FitNeuralNetwork(x, y, NeuralConfig{Hidden: hidden, Epochs: 2000, Patience: 100, LearningRate: .03, Seed: 6})
			if err != nil {
				t.Fatal(err)
			}
			loss := 0.0
			for i, row := range x {
				v, err := m.Predict(row)
				if err != nil {
					t.Fatal(err)
				}
				loss += (v - y[i]) * (v - y[i]) / 60
			}
			if loss > .03 {
				t.Fatalf("source %d layers/%d units failed nonlinear training path: loss%g", layers, units, loss)
			}
		}
	}
}
