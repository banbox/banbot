package statistics

import (
	"context"
	"math"
	"math/rand"
	"testing"
)

func TestChanKalmanDiagonalMLEConstraint(t *testing.T) {
	r := rand.New(rand.NewSource(5))
	var x, y []float64
	beta, offset := 1.2, 3.0
	for i := 0; i < 180; i++ {
		beta += .002 * r.NormFloat64()
		offset += .02 * r.NormFloat64()
		price := 20 + 5*math.Sin(float64(i)*.17)
		x = append(x, price)
		y = append(y, beta*price+offset+.2*r.NormFloat64())
	}
	spec := KalmanFitSpec{Initial: KalmanPair{Beta: [2]float64{1.2, 3}, Covariance: [2][2]float64{{1, 0}, {0, 1}}, ProcessVariance: .02, ObservationVariance: 4}, DiagonalProcessNoise: true, MaxEvaluations: 3000}
	fit, err := FitKalmanNoise(context.Background(), x, y, spec)
	if err != nil || !fit.Converged || fit.NegativeLogLikelihood >= fit.InitialNegativeLogLikelihood-10 {
		t.Fatalf("diagonal likelihood %+v %v", fit, err)
	}
	q := fit.Initial.ProcessCovariance
	if q[0][1] != 0 || q[1][0] != 0 || q[0][0] <= 0 || q[1][1] <= 0 {
		t.Fatal("diagonalQ constraint lost")
	}
	spec.Initial.ProcessCovariance = [2][2]float64{{.02, .001}, {.001, .02}}
	if _, err := FitKalmanNoise(context.Background(), x, y, spec); err == nil {
		t.Fatal("incompatible initial crossnoise accepted")
	}
}
