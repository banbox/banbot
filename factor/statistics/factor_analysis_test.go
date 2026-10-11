package statistics

import (
	"context"
	"math"
	"math/rand"
	"testing"
)

func TestFactorScoresSourceGLSAndPosteriorAreDistinct(t *testing.T) {
	model := FactorAnalysisModel{Mean: []float64{0, 0, 0}, Loadings: [][]float64{{1, 0}, {0, 2}, {1, 1}}, Uniqueness: []float64{1, 4, 1}}
	row := []float64{2, -2, 1}
	gls, err := model.FactorScores(row, false)
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, gls.Scores[0], 2)
	closeTo(t, gls.Scores[1], -1)
	for _, residual := range gls.Residuals {
		closeTo(t, residual, 0)
	}
	posterior, err := model.FactorScores(row, true)
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, posterior.Scores[0], 9.0/8)
	closeTo(t, posterior.Scores[1], -3.0/8)
	model.Loadings = [][]float64{{1, 1}, {2, 2}, {3, 3}}
	if _, err := model.FactorScores(row, false); err == nil {
		t.Fatal("unidentified GLS factors accepted")
	}
}

func TestGaussianFactorAnalysisRecoversModelCovarianceAndSpecificRisk(t *testing.T) {
	rng := rand.New(rand.NewSource(73))
	trueLoadings := []float64{1, .8, 1.2, -.5}
	trueUniqueness := []float64{.2, .4, .3, .1}
	var rows [][]float64
	for range 4000 {
		f := rng.NormFloat64()
		row := make([]float64, 4)
		for i := range row {
			row[i] = trueLoadings[i]*f + math.Sqrt(trueUniqueness[i])*rng.NormFloat64()
		}
		rows = append(rows, row)
	}
	config := FactorAnalysisConfig{Components: 1, MaxIterations: 2000, Tolerance: 1e-9, MinUniqueness: 1e-6, ZeroMean: true}
	model, err := FitFactorAnalysis(context.Background(), rows, config)
	if err != nil {
		t.Fatal(err)
	}
	if model.Iterations < 2 || model.FloorActive {
		t.Fatalf("unexpected calibration %+v", model)
	}
	for i := range 4 {
		if math.Abs(model.Uniqueness[i]-trueUniqueness[i]) > .06 {
			t.Fatalf("specific risk asset%d: %g", i, model.Uniqueness[i])
		}
		for j := range 4 {
			cov := model.Loadings[i][0] * model.Loadings[j][0]
			want := trueLoadings[i] * trueLoadings[j]
			if i == j {
				cov += model.Uniqueness[i]
				want += trueUniqueness[i]
			}
			if math.Abs(cov-want) > .08 {
				t.Fatalf("covariance%d,%d %g want%g", i, j, cov, want)
			}
		}
	}
	// Adding different future rows cannot mutate the fitted training model.
	before := model.Loadings[0][0]
	rows[0][0] = 1e6
	if model.Loadings[0][0] != before {
		t.Fatal("model aliases caller data")
	}
	config.MaxIterations = 1
	if _, err := FitFactorAnalysis(context.Background(), rows, config); err == nil {
		t.Fatal("exhausted EM budget reported success")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := FitFactorAnalysis(ctx, rows, config); err == nil {
		t.Fatal("cancelled factor fit reported success")
	}
}
