package statistics

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
)

func cpcvExercise() ([]EventSpan, []int, []CPCVExecution, CPCVResearchConfig) {
	spans, labels, execution := make([]EventSpan, 24), make([]int, 24), make([]CPCVExecution, 24)
	for i := range spans {
		spans[i] = EventSpan{Start: i * 3, End: i*3 + 2}
		labels[i] = 1
		if i%2 == 0 {
			labels[i] = -1
		}
		execution[i] = CPCVExecution{i * 3, i*3 + 1, i*3 + 2, float64(i%5-2) * .01}
	}
	return spans, labels, execution, CPCVResearchConfig{Groups: 6, TestGroups: 2, EmbargoRows: 1, MaxSplits: 15, Epsilon: 1e-6, TurnoverCost: .001}
}

func TestAFMLCPCVResearchProvenanceExecutionOwnership(t *testing.T) {
	spans, labels, execution, config := cpcvExercise()
	var buffer [][]float64
	calls := 0
	fit := func(train, test []int, weights []float64, classes []int) ([][]float64, error) {
		calls++
		for _, row := range train {
			for _, held := range test {
				if spans[row].Start <= spans[held].End && spans[row].End >= spans[held].Start {
					t.Fatal("label overlap")
				}
				after := held + 1 // next row starts after End in this fixture
				if row >= after && row-after < config.EmbargoRows {
					t.Fatal("embargo violation")
				}
			}
		}
		for _, p := range buffer {
			p[0], p[1] = .5, .5
		}
		buffer = make([][]float64, len(test))
		for j, row := range test {
			buffer[j] = []float64{.1, .9}
			if labels[row] == -1 {
				buffer[j] = []float64{.9, .1}
			}
		}
		train[0], test[0], classes[0], weights[0] = -99, -99, -99, -99
		return buffer, nil
	}
	got, err := RunCPCVResearch(spans, labels, []int{-1, 1}, nil, execution, config, fit)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 15 || len(got.Plan.Splits) != 15 || len(got.Paths) != 5 || !got.ResearchOnly || len(got.SharpeDistribution) != 5 {
		t.Fatalf("wrong shape: %d %d", calls, len(got.Paths))
	}
	seen := map[[2]int]bool{}
	future := 0
	for _, forecast := range got.Forecasts {
		if forecast.FutureTraining {
			future++
		}
		if forecast.Score.Accuracy != 1 {
			t.Fatal("score")
		}
	}
	if future == 0 || future == len(got.Forecasts) {
		t.Fatal("future training markers")
	}
	for p, path := range got.Paths {
		for group, split := range path.GroupSplits {
			key := [2]int{split, group}
			if seen[key] {
				t.Fatal("reused split-group occurrence")
			}
			seen[key] = true
			found := false
			for _, g := range got.Plan.Splits[split].TestGroups {
				if g == group {
					found = true
				}
			}
			if !found {
				t.Fatal("group not tested by supplier")
			}
			for _, row := range got.Plan.Groups[group] {
				if path.RowSplits[row] != split {
					t.Fatal("row provenance")
				}
			}
		}
		previous := 0
		for row, position := range path.Positions {
			if position != labels[row] {
				t.Fatal("callback probabilities aliased or wrong reconstructed signal")
			}
			turnover := math.Abs(float64(position - previous))
			flatten := row == len(labels)-1 || execution[row].ExitBar < execution[row+1].FillBar
			if flatten {
				turnover += math.Abs(float64(position))
			}
			want := float64(position)*execution[row].AssetReturn - config.TurnoverCost*turnover
			if path.Returns[row] != want {
				t.Fatal("portfolio cost/execution mismatch")
			}
			previous = position
			if flatten {
				previous = 0
			}
		}
		if !got.CorrelationValid[p][0] || math.Abs(got.Correlation[p][0]-1) > 1e-12 {
			t.Fatal("dependent paths not reported")
		}
	}
	if len(seen) != 30 || !reflect.DeepEqual(spans, got.Spans) {
		t.Fatal("lost provenance")
	}
}

func TestAFMLCPCVResearchFailuresAndUndefinedSharpe(t *testing.T) {
	spans, labels, execution, config := cpcvExercise()
	calls := 0
	fit := func(train, test []int, weights []float64, classes []int) ([][]float64, error) {
		calls++
		return nil, errors.New("model failed")
	}
	config.MaxSplits = 14
	if _, err := RunCPCVResearch(spans, labels, []int{-1, 1}, nil, execution, config, fit); err == nil || calls != 0 {
		t.Fatal("budget applied after fit")
	}
	config.MaxSplits = 15
	if _, err := RunCPCVResearch(spans, labels, []int{-1, 1}, nil, execution, config, fit); err == nil || !strings.Contains(err.Error(), "split 0 fit: model failed") {
		t.Fatal(err)
	}
	fit = func(train, test []int, weights []float64, classes []int) ([][]float64, error) {
		return [][]float64{{2, -1}}, nil
	}
	if _, err := RunCPCVResearch(spans, labels, []int{-1, 1}, nil, execution, config, fit); err == nil {
		t.Fatal("bad callback dimensions accepted")
	}
	for i := range execution {
		execution[i].AssetReturn = 0
	}
	config.TurnoverCost = 0
	fit = func(train, test []int, weights []float64, classes []int) ([][]float64, error) {
		p := make([][]float64, len(test))
		for i := range p {
			p[i] = []float64{0, 1}
		}
		return p, nil
	}
	got, err := RunCPCVResearch(spans, labels, []int{-1, 1}, nil, execution, config, fit)
	if err != nil || len(got.SharpeDistribution) != 0 {
		t.Fatal("undefined Sharpe silently included", err)
	}
	for i := range got.Paths {
		if got.Paths[i].SharpeValid || got.CorrelationValid[i][i] {
			t.Fatal("constant series assigned defined metric")
		}
	}
	execution[1].FillBar = execution[0].FillBar
	if _, err := RunCPCVResearch(spans, labels, []int{-1, 1}, nil, execution, config, fit); err == nil {
		t.Fatal("overlapping fills accepted")
	}
}

func TestAFMLCPCVResearchTouchingIntervalsCarryPosition(t *testing.T) {
	spans, labels, execution, config := cpcvExercise()
	for i := range execution {
		spans[i] = EventSpan{Start: i, End: i + 1}
		execution[i] = CPCVExecution{DecisionBar: i, FillBar: i + 1, ExitBar: i + 2, AssetReturn: .01 + float64(i)*.001}
	}
	fit := func(train, test []int, weights []float64, classes []int) ([][]float64, error) {
		p := make([][]float64, len(test))
		for i := range p {
			p[i] = []float64{0, 1}
		}
		return p, nil
	}
	got, err := RunCPCVResearch(spans, labels, []int{-1, 1}, nil, execution, config, fit)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range got.Paths {
		for i, turnover := range path.Turnover {
			want := 0.
			if i == 0 || i == len(labels)-1 {
				want = 1
			}
			if turnover != want {
				t.Fatal("touching intervals liquidated instead of carried")
			}
		}
	}
}
