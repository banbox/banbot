package statistics

import (
	"math"
	"math/rand"
	"testing"
)

func TestJohansenRecoversCointegratingDirectionAndSequentialRank(t *testing.T) {
	rng := rand.New(rand.NewSource(83))
	var levels [][]float64
	walk, noise := 0.0, 0.0
	for range 1400 {
		walk += rng.NormFloat64()
		noise = .4*noise + .3*rng.NormFloat64()
		levels = append(levels, []float64{2*walk + noise, walk})
	}
	result, err := Johansen(levels, JohansenSpec{DifferenceLags: 1, Intercept: true})
	if err != nil {
		t.Fatal(err)
	}
	beta := result.CointegratingVectors[0]
	if math.Abs(beta[1]/beta[0]+2) > .03 {
		t.Fatalf("wrong cointegrating vector %v", beta)
	}
	// Explicit test critical values, not an API claim about every VEC
	// deterministic specification or finite-sample significance table.
	rank, err := result.TraceRank([]float64{20, 8})
	if err != nil || rank != 1 {
		t.Fatalf("rank=%d trace=%v err=%v", rank, result.Trace, err)
	}
	for i, value := range result.Eigenvalues {
		closeTo(t, result.MaximumEigenvalue[i], -float64(result.Samples)*math.Log1p(-value))
		if i > 0 && value > result.Eigenvalues[i-1] {
			t.Fatal("eigenvalue order")
		}
	}
	closeTo(t, result.Trace[0], result.MaximumEigenvalue[0]+result.MaximumEigenvalue[1])
	// Scalar unit changes preserve canonical correlations and hedge economics.
	for _, row := range levels {
		row[1] *= 10
	}
	scaled, err := Johansen(levels, JohansenSpec{DifferenceLags: 1, Intercept: true})
	if err != nil {
		t.Fatal(err)
	}
	for i := range scaled.Eigenvalues {
		if math.Abs(scaled.Eigenvalues[i]-result.Eigenvalues[i]) > 1e-9 {
			t.Fatal("asset units altered rank statistic")
		}
	}
	if math.Abs(10*scaled.CointegratingVectors[0][1]/scaled.CointegratingVectors[0][0]+2) > .03 {
		t.Fatal("unit-adjusted hedge lost")
	}
}

func TestJohansenRejectsSingularAndInvalidMatrices(t *testing.T) {
	var singular [][]float64
	for i := range 40 {
		singular = append(singular, []float64{float64(i), 2 * float64(i)})
	}
	if _, err := Johansen(singular, JohansenSpec{}); err == nil {
		t.Fatal("singular covariance accepted")
	}
	if _, err := Johansen([][]float64{{1, 2}, {2, 3}, {3, 4}}, JohansenSpec{DifferenceLags: 1}); err == nil {
		t.Fatal("insufficient sample accepted")
	}
	if _, err := (JohansenResult{Trace: []float64{10, 1}}).TraceRank([]float64{5, math.NaN()}); err == nil {
		t.Fatal("nonfinite critical value accepted")
	}
}
