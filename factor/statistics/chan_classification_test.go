package statistics

import (
	"reflect"
	"testing"
)

func TestChanClassificationForestAndFixedSubspace(t *testing.T) {
	x := make([][]float64, 100)
	y := make([]float64, 100)
	for i := range x {
		a, b := float64(i%2), float64((i/2)%2)
		x[i] = []float64{a, b}
		if a == 1 && b == 1 {
			y[i] = 1
		}
	}
	for _, subspace := range []int{0, 2} {
		c := ClassificationEnsembleConfig{Tree: CARTConfig{MinLeaf: 2, MaxDepth: 4, MaxSplits: 20, MaxFeatures: 2}, Trees: 11, SubspaceFeatures: subspace, Seed: 41}
		m, err := FitClassificationEnsemble(x, y, c)
		if err != nil {
			t.Fatal(err)
		}
		again, err := FitClassificationEnsemble(x, y, c)
		if err != nil || !reflect.DeepEqual(m, again) {
			t.Fatal("classification seed differs")
		}
		for i, row := range x {
			v, err := m.Predict(row)
			if err != nil || (v > .5) != (y[i] == 1) {
				t.Fatal("classification nonlinear AND/voting failed")
			}
		}
		if subspace > 0 {
			for _, tree := range m.trees {
				if len(tree.features) != subspace {
					t.Fatal("subspace not fixed per learner")
				}
			}
		}
	}
	soft := &ClassificationEnsemble{dimensions: 1, trees: []classificationTree{{&CART{features: 1, root: &cartNode{Feature: -1, Prediction: .51}}, []int{0}}, {&CART{features: 1, root: &cartNode{Feature: -1, Prediction: .51}}, []int{0}}, {&CART{features: 1, root: &cartNode{Feature: -1, Prediction: 0}}, []int{0}}}}
	v, err := soft.Predict([]float64{1})
	if err != nil || v != 2.0/3 {
		t.Fatal("hard class vote")
	}
	soft.probabilityVote = true
	v, err = soft.Predict([]float64{1})
	if err != nil || v != .34 {
		t.Fatal("probability averaging must differ from class averaging")
	}
}
