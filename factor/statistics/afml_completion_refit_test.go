package statistics

import (
	"reflect"
	"testing"
)

func TestAFMLSearchRefitWeightedBagsAndOwnership(t *testing.T) {
	x := [][]float64{{1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}, {6, 60}}
	y := []int{0, 0, 1, 0, 1, 1}
	w := []float64{1, 2, 3, 4, 5, 6}
	classes := []int{0, 1}
	spans := make([]EventSpan, len(x))
	for i := range spans {
		spans[i] = EventSpan{Start: i, End: i}
	}
	result, err := PurgedParameterSearch([]map[string]float64{{"p": .6}, {"p": .9}}, 2, spans, y, classes, w, 3, 0, "neg_log_loss", func(params map[string]float64, train, test []int, ww []float64, cc []int) ([][]float64, error) {
		out := make([][]float64, len(test))
		for i, j := range test {
			p := params["p"]
			if y[j] == 0 {
				out[i] = []float64{p, 1 - p}
			} else {
				out[i] = []float64{1 - p, p}
			}
		}
		return out, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Best != 1 {
		t.Fatal("search winner", result)
	}
	cfg := SearchBaggingConfig{Estimators: 7, SampleFraction: .75, FeatureFraction: .5, Seed: 22}
	var fittedRows [][][]float64
	fit := func(params map[string]float64, xx [][]float64, yy []int, ww []float64, cc []int) (AFMLProbabilityPredictor, error) {
		if params["p"] != .9 || !reflect.DeepEqual(cc, classes) {
			t.Fatal("lost winner/classes")
		}
		copyRows := make([][]float64, len(xx))
		mass, pos := 0., 0.
		for i := range xx {
			copyRows[i] = append([]float64(nil), xx[i]...)
			mass += ww[i]
			pos += float64(yy[i]) * ww[i]
		}
		fittedRows = append(fittedRows, copyRows)
		p := pos / mass
		params["p"] = 0
		xx[0][0] = 999
		ww[0] = 999
		cc[0] = 99
		return func(rows [][]float64) ([][]float64, error) {
			out := make([][]float64, len(rows))
			for i := range rows {
				out[i] = []float64{1 - p, p}
			}
			return out, nil
		}, nil
	}
	predict, audit, err := RefitPurgedSearch(result, x, y, classes, w, cfg, fit)
	if err != nil {
		t.Fatal(err)
	}
	want := 0.
	duplicate := false
	for b, a := range audit {
		if len(a.Rows) != 4 || len(a.Columns) != 1 {
			t.Fatal("source fractions", a)
		}
		mass, pos := 0., 0.
		seen := map[int]bool{}
		for i, j := range a.Rows {
			duplicate = duplicate || seen[j]
			seen[j] = true
			mass += w[j]
			pos += float64(y[j]) * w[j]
			afmlNear(t, fittedRows[b][i][0], x[j][a.Columns[0]])
		}
		want += pos / mass / 7
	}
	if !duplicate {
		t.Fatal("no replacement sample")
	}
	audit[0].Columns[0] = 100
	audit[0].Rows[0] = 100
	got, err := predict(x)
	if err != nil {
		t.Fatal(err)
	}
	afmlNear(t, got[0][1], want)
	if result.Trials[1].Parameters["p"] != .9 || x[0][0] != 1 || w[0] != 1 || classes[0] != 0 {
		t.Fatal("callback mutated caller")
	}
	_, again, err := RefitPurgedSearch(result, x, y, classes, w, cfg, fit)
	if err != nil {
		t.Fatal(err)
	}
	audit[0] = again[0] // undo intentional audit mutation
	if !reflect.DeepEqual(audit, again) {
		t.Fatal("seed changes bags")
	}
	one, full, err := RefitPurgedSearch(result, x, y, classes, w, SearchBaggingConfig{}, fit)
	if err != nil {
		t.Fatal(err)
	}
	pp, _ := one(x)
	afmlNear(t, pp[0][1], 14./21)
	if len(full) != 1 || len(full[0].Rows) != 6 || len(full[0].Columns) != 2 {
		t.Fatal("unbagged refit")
	}
}

func TestAFMLSearchRefitRejectsInvalidPredictionAndBudget(t *testing.T) {
	s := PurgedSearchResult{Best: 0, Trials: []ParameterTrial{{Parameters: map[string]float64{}}}}
	fit := func(map[string]float64, [][]float64, []int, []float64, []int) (AFMLProbabilityPredictor, error) {
		return func(rows [][]float64) ([][]float64, error) { return [][]float64{{.7, .7}}, nil }, nil
	}
	p, _, err := RefitPurgedSearch(s, [][]float64{{1}}, []int{0}, []int{0, 1}, nil, SearchBaggingConfig{}, fit)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p([][]float64{{1}}); err == nil {
		t.Fatal("unnormalized probabilities")
	}
	if _, _, err = RefitPurgedSearch(s, [][]float64{{1}}, []int{0}, []int{0, 1}, nil, SearchBaggingConfig{Estimators: 1, SampleFraction: .5, FeatureFraction: 1}, fit); err == nil {
		t.Fatal("empty bootstrap")
	}
}
