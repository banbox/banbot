package statistics

import (
	"context"
	"math"
	"math/rand"
	"testing"
)

func TestKalmanLikelihoodAnalyticInnovation(t *testing.T) {
	k := KalmanPair{Beta: [2]float64{2, 1}, ObservationVariance: 4}
	nll, err := KalmanLikelihood([]float64{3}, []float64{9}, k)
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, nll, .5*(math.Log(2*math.Pi)+math.Log(4)+1))
	if k.Beta != [2]float64{2, 1} {
		t.Fatal("likelihood mutated prior")
	}
}

func TestKalmanMLEImprovesLikelihoodAndRespectsBudget(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	var x, y []float64
	beta, offset := 1.2, 3.0
	for i := range 180 {
		beta += .002 * rng.NormFloat64()
		offset += .02 * rng.NormFloat64()
		price := 20 + 5*math.Sin(float64(i)*.17)
		x = append(x, price)
		y = append(y, beta*price+offset+.2*rng.NormFloat64())
	}
	initial := KalmanPair{Beta: [2]float64{1.2, 3}, Covariance: [2][2]float64{{1, 0}, {0, 1}}, ProcessVariance: .02, ObservationVariance: 4}
	spec := KalmanFitSpec{Initial: initial, MaxEvaluations: 3000, MaxIterations: 2000}
	f, err := FitKalmanNoise(context.Background(), x, y, spec)
	if err != nil {
		t.Fatal(err)
	}
	if !f.Converged || f.NegativeLogLikelihood >= f.InitialNegativeLogLikelihood-10 || f.Evaluations > 3000 || !pairPSD(f.Initial.ProcessCovariance) {
		t.Fatalf("bad likelihood fit %+v", f)
	}
	spec.MaxEvaluations = 5
	limited, err := FitKalmanNoise(context.Background(), x, y, spec)
	if err == nil || limited.Converged {
		t.Fatal("evaluation budget treated as successful MLE")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = FitKalmanNoise(ctx, x, y, spec); err != context.Canceled {
		t.Fatal("fit ignored canceled context")
	}
}
