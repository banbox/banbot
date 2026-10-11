package statistics

import (
	"math"
	"reflect"
	"testing"
)

func TestAFMLTrainingEncoding(t *testing.T) {
	train := []float64{-3, -2, -1, 0, 1, 2, 3, 4}
	e, err := FitQuantileEncoder(train, 4)
	if err != nil {
		t.Fatal(err)
	}
	train[0] = 1e9 // model owns boundaries, not caller memory
	got, err := e.Encode([]float64{-3, -2, -1, 0, 1, 2, 3, 4})
	if err != nil || !reflect.DeepEqual(got, []int{0, 0, 1, 1, 2, 2, 3, 3}) {
		t.Fatal(got, err)
	}
	prefix, _ := e.Encode([]float64{-4, 1})
	extended, _ := e.Encode([]float64{-4, 1, 1e10})
	if !reflect.DeepEqual(prefix, extended[:2]) {
		t.Fatal("future observations changed encoding")
	}
	s, _ := FitSigmaEncoder([]float64{-2, 2}, 1)
	codes, _ := s.Encode([]float64{-3, -2, -1.1, -1, 2, 3})
	if !reflect.DeepEqual(codes, []int{-1, 0, 0, 1, 4, 5}) {
		t.Fatal(codes)
	}
	codes, indices, err := SignEncoding([]float64{0, -1, 2, 0, -3})
	if err != nil || !reflect.DeepEqual(codes, []int{0, 1, 0}) || !reflect.DeepEqual(indices, []int{1, 2, 4}) {
		t.Fatal(codes, indices, err)
	}
	if _, err = e.Encode([]float64{math.NaN()}); err == nil {
		t.Fatal("accepted NaN")
	}
}

func TestAFMLInformationAnalytic(t *testing.T) {
	x := []int{0, 0, 1, 1}
	r, _ := DiscreteInformation(x, x)
	if math.Abs(r.MutualInformation-math.Log(2)) > 1e-12 || r.NormalizedVariation != 0 {
		t.Fatal(r)
	}
	r, _ = DiscreteInformation(x, []int{0, 1, 0, 1})
	if math.Abs(r.MutualInformation) > 1e-12 || math.Abs(r.NormalizedVariation-1) > 1e-12 {
		t.Fatal(r)
	}
	kl, err := KLDivergence([]float64{.5, .5}, []float64{.25, .75})
	if err != nil || math.Abs(kl-.5*math.Log(4./3)) > 1e-12 {
		t.Fatal(kl, err)
	}
	kl, err = KLDivergence([]float64{1, 0}, []float64{0, 1})
	if err != nil || !math.IsInf(kl, 1) {
		t.Fatal(kl, err)
	}
	d, err := HillDiversity([]float64{.5, .5}, 2)
	if err != nil || math.Abs(d-2) > 1e-12 {
		t.Fatal(d, err)
	}
	d, err = HillDiversity([]float64{.5, .5, 0}, 1)
	if err != nil || math.Abs(d-2) > 1e-12 {
		t.Fatal(d, err)
	}
	d, _ = HillDiversity([]float64{.75, .25}, math.Inf(1))
	if math.Abs(d-4./3) > 1e-12 {
		t.Fatal(d)
	}
	h, err := GaussianDifferentialEntropy(1)
	if err != nil || math.Abs(h-.5*math.Log(2*math.Pi*math.E)) > 1e-12 {
		t.Fatal(h, err)
	}
	for _, fixture := range []struct{ q, want float64 }{{1, 2.5}, {0, 2}, {-1, 1.6}, {math.Inf(1), 4}, {math.Inf(-1), 1}} {
		got, err := GeneralizedWeightedMean([]float64{1, 4}, []float64{.5, .5}, fixture.q)
		if err != nil || math.Abs(got-fixture.want) > 1e-12 {
			t.Fatal(fixture, got, err)
		}
	}
}

func TestAFMLFixedHorizonAndMulticlass(t *testing.T) {
	labels, err := FixedHorizonLabels([]float64{100, 125, 100, 80, math.NaN()}, []int{0, 1, 2}, 1, []float64{.25, .1, .3})
	if err != nil || labels[0].Label != 0 || labels[1].Label != -1 || labels[2].Label != 0 {
		t.Fatal(labels, err)
	}
	if _, err = FixedHorizonLabels([]float64{100}, []int{0}, 1, []float64{.1}); err == nil {
		t.Fatal("accepted immature label")
	}
	r, err := MulticlassClassification([]int{-1, 1}, []int{-1, 0, 1}, [][]float64{{.8, .1, .1}, {.6, .1, .3}}, []float64{1, 3}, 1e-10)
	if err != nil || r.Accuracy != .25 || math.Abs(r.LogLoss-(-math.Log(.8)-3*math.Log(.3))/4) > 1e-12 || r.Confusion[2][0] != 3 {
		t.Fatal(r, err)
	}
}

func TestAFMLPurgedWeightedScoring(t *testing.T) {
	spans := []EventSpan{{0, 1}, {2, 3}, {4, 5}, {6, 7}, {8, 9}, {10, 11}}
	labels := []int{-1, 1, -1, 1, -1, 1}
	weights := []float64{1, 2, 3, 4, 5, 6}
	calls := 0
	reports, err := PurgedClassificationScores(spans, labels, []int{-1, 0, 1}, weights, 3, 1, 1e-6, func(train, test []int, w []float64, classes []int) ([][]float64, error) {
		calls++
		if !reflect.DeepEqual(classes, []int{-1, 0, 1}) {
			t.Fatal(classes)
		}
		for j, a := range train {
			if w[j] != weights[a] {
				t.Fatal(w)
			}
			for _, b := range test {
				if spans[a].Start <= spans[b].End && spans[a].End >= spans[b].Start {
					t.Fatal("overlap")
				}
			}
		}
		out := make([][]float64, len(test))
		for j, b := range test {
			out[j] = []float64{.8, .1, .1}
			if labels[b] == 1 {
				out[j] = []float64{.1, .1, .8}
			}
		}
		return out, nil
	})
	if err != nil || calls != 3 {
		t.Fatal(calls, err)
	}
	for _, r := range reports {
		if r.Accuracy != 1 {
			t.Fatal(r)
		}
	}
}
