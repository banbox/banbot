package statistics

import (
	"math"
	"math/rand"
	"slices"
	"testing"
)

func TestChanStepwiseRejectsNoiseAndKeepsIntercept(t *testing.T) {
	r := rand.New(rand.NewSource(557))
	var x [][]float64
	var y []float64
	for i := 0; i < 600; i++ {
		row := []float64{r.NormFloat64(), r.NormFloat64(), r.NormFloat64(), r.NormFloat64()}
		x = append(x, row)
		y = append(y, 2+3*row[1]+.05*r.NormFloat64())
	}
	for _, criterion := range []string{"pvalue", "bic", "aic"} {
		fit, err := StepwiseLinear(x, y, StepwiseConfig{Criterion: criterion, EnterP: .001, RemoveP: .01, MaxSteps: 100})
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Contains(fit.Features, 1) || math.Abs(fit.Regression.Intercept-2) > .01 {
			t.Fatalf("selection %s %+v", criterion, fit)
		}
		pred, err := fit.Predict([]float64{0, 2, 0, 0})
		if err != nil || math.Abs(pred-8) > .03 {
			t.Fatal("selected prediction")
		}
		if criterion == "pvalue" && len(fit.Features) != 1 {
			t.Fatal("partial-F kept pure noise")
		}
	}
	fit, err := FitSelectedLinear(x, y, nil)
	if err != nil || len(fit.Regression.Beta) != 0 || fit.Regression.Rank != 1 {
		t.Fatal("intercept-only model")
	}
	if _, err = StepwiseLinear(x, y, StepwiseConfig{Criterion: "bic", EnterP: .01, RemoveP: .1, MaxSteps: 1}); err == nil {
		t.Fatal("step budget accepted as converged")
	}
}
