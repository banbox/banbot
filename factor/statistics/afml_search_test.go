package statistics

import (
	"math"
	"reflect"
	"testing"
)

func TestAFMLLogUniformAndSampling(t *testing.T) {
	line, err := Linspace(-1, 1, 5)
	if err != nil || !reflect.DeepEqual(line, []float64{-1, -.5, 0, .5, 1}) {
		t.Fatal(line, err)
	}
	x, err := LogUniformQuantile(.001, 1000, .5)
	if err != nil || math.Abs(x-1) > 1e-12 {
		t.Fatal(x, err)
	}
	p, _ := LogUniformCDF(.001, 1000, x)
	if math.Abs(p-.5) > 1e-12 {
		t.Fatal(p)
	}
	x, err = LogUniformQuantile(math.SmallestNonzeroFloat64, math.MaxFloat64, .5)
	if err != nil || !afmlFinite(x) || x <= 0 {
		t.Fatal(x, err)
	}
	a, _ := UniformDownsample(100, 15, 42)
	b, _ := UniformDownsample(100, 15, 42)
	if !reflect.DeepEqual(a, b) {
		t.Fatal(a, b)
	}
	for i, v := range a {
		if v < 0 || v >= 100 || (i > 0 && v <= a[i-1]) {
			t.Fatal(a)
		}
	}
	if _, err := LogUniformQuantile(0, 1, .5); err == nil {
		t.Fatal("accepted invalid range")
	}
}

func TestAFMLSourcePartitions(t *testing.T) {
	p, _ := LinearPartitions(10, 3)
	if !reflect.DeepEqual(p, []int{0, 4, 7, 10}) {
		t.Fatal(p)
	}
	p, _ = NestedPartitions(10, 3, false)
	if !reflect.DeepEqual(p, []int{0, 6, 8, 10}) {
		t.Fatal(p)
	}
	p, _ = NestedPartitions(10, 3, true)
	if !reflect.DeepEqual(p, []int{0, 2, 4, 10}) {
		t.Fatal(p)
	}
	for n := 1; n < 30; n++ {
		for m := 1; m < 35; m++ {
			p, err := NestedPartitions(n, m, false)
			if err != nil || p[0] != 0 || p[len(p)-1] != n {
				t.Fatal(n, m, p, err)
			}
			for i := 1; i < len(p); i++ {
				if p[i] < p[i-1] {
					t.Fatal(p)
				}
			}
		}
	}
}

func TestAFMLPurgedParameterSearchPreservesTrials(t *testing.T) {
	spans := []EventSpan{{0, 1}, {2, 3}, {4, 5}, {6, 7}, {8, 9}, {10, 11}}
	y := []int{0, 1, 0, 1, 0, 1}
	w := []float64{1, 2, 3, 4, 5, 6}
	candidates := []map[string]float64{{"confidence": .5}, {"confidence": .9}, {"confidence": .9}}
	calls := 0
	r, err := PurgedParameterSearch(candidates, 3, spans, y, []int{0, 1}, w, 3, 1, "neg_log_loss", func(parameters map[string]float64, train, test []int, weights []float64, classes []int) ([][]float64, error) {
		calls++
		for i, row := range train {
			if weights[i] != w[row] {
				t.Fatal(weights)
			}
		}
		p := parameters["confidence"]
		parameters["confidence"] = 0 // callback mutations cannot affect trial or next fold
		out := make([][]float64, len(test))
		for i, row := range test {
			out[i] = []float64{p, 1 - p}
			if y[row] == 1 {
				out[i] = []float64{1 - p, p}
			}
		}
		return out, nil
	})
	if err != nil || r.Best != 1 || len(r.Trials) != 3 || calls != 9 || r.Trials[1].Parameters["confidence"] != .9 || math.Abs(r.Trials[1].MeanScore-math.Log(.9)) > 1e-12 {
		t.Fatal(r, calls, err)
	}
	candidates[1]["confidence"] = 1
	if r.Trials[1].Parameters["confidence"] != .9 {
		t.Fatal("retained caller params")
	}
	if _, err = PurgedParameterSearch(candidates, 2, spans, y, []int{0, 1}, w, 3, 1, "accuracy", nil); err == nil {
		t.Fatal("accepted budget/fit")
	}
}
