package statistics

import (
	"context"
	"math"
	"reflect"
	"testing"

	"gonum.org/v1/gonum/mat"
)

func trajectoryFixture() []TrajectoryHorizon {
	return []TrajectoryHorizon{
		{Mean: []float64{.04}, Cost: []float64{.07}, Covariance: [][]float64{{.01}}},
		{Mean: []float64{-.08}, Cost: []float64{.07}, Covariance: [][]float64{{.01}}},
		{Mean: []float64{-.08}, Cost: []float64{.07}, Covariance: [][]float64{{.01}}},
	}
}

func TestTrajectoryExactDynamicVersusStatic(t *testing.T) {
	p := trajectoryFixture()
	budget := TrajectoryBudget{2, 8, 72}
	d, err := OptimizeTrajectory(context.Background(), p, []float64{0}, 3, budget)
	if err != nil {
		t.Fatal(err)
	}
	s, err := StaticTrajectory(p, []float64{0})
	if err != nil {
		t.Fatal(err)
	}
	// Four possible tail combinations and both initial signs are independently
	// enumerated here. Dynamic anticipates two negative means and avoids a flip.
	if !reflect.DeepEqual(d.Weights, [][]float64{{-1}, {-1}, {-1}}) || d.Evaluated != 8 || d.Allocations != 2 {
		t.Fatalf("unexpected optimum: %+v", d)
	}
	want := .05 / math.Sqrt(.03)
	if math.Abs(d.Sharpe-want) > 1e-12 || d.Sharpe <= s.Sharpe {
		t.Fatalf("dynamic=%+v static=%+v", d, s)
	}
	for _, a := range []float64{-1, 1} {
		for _, b := range []float64{-1, 1} {
			for _, c := range []float64{-1, 1} {
				r, e := EvaluateTrajectory(p, []float64{0}, [][]float64{{a}, {b}, {c}})
				if e != nil || r.Sharpe > d.Sharpe+1e-12 {
					t.Fatalf("oracle mismatch %+v %v", r, e)
				}
			}
		}
	}
	if math.Abs(s.Costs[1]-.07*math.Sqrt(2)) > 1e-12 {
		t.Fatal("flip must pay square-root absolute turnover")
	}
}

func TestTrajectoryMeshAndBudgets(t *testing.T) {
	c, err := TrajectoryMeshSize(6, 3)
	if err != nil || c.Int64() != 146 {
		t.Fatalf("signed composition count %v %v", c, err)
	}
	p := []TrajectoryHorizon{{Mean: []float64{1, 0}, Cost: []float64{0, 0}, Covariance: [][]float64{{1, 0}, {0, 1}}}}
	r, err := OptimizeTrajectory(context.Background(), p, []float64{0, 0}, 2, TrajectoryBudget{8, 8, 64})
	if err != nil || r.Evaluated != 8 {
		t.Fatalf("two-asset signed compositions: %+v %v", r, err)
	}
	if math.Abs(r.Sharpe-1) > 1e-12 {
		t.Fatal(r)
	}
	for _, b := range []TrajectoryBudget{{7, 8, 64}, {8, 7, 64}, {8, 8, 63}, {}} {
		if _, err := OptimizeTrajectory(context.Background(), p, []float64{0, 0}, 2, b); err == nil {
			t.Fatal("budget accepted", b)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := OptimizeTrajectory(ctx, p, []float64{0, 0}, 2, TrajectoryBudget{8, 8, 64}); err != context.Canceled {
		t.Fatal(err)
	}
	// Reject enormous meshes without generating a single allocation.
	bigp := TrajectoryHorizon{Mean: make([]float64, 64), Cost: make([]float64, 64), Covariance: make([][]float64, 64)}
	for i := range bigp.Covariance {
		bigp.Covariance[i] = make([]float64, 64)
		bigp.Covariance[i][i] = 1
	}
	if _, err := OptimizeTrajectory(context.Background(), []TrajectoryHorizon{bigp, bigp}, make([]float64, 64), 1000000, TrajectoryBudget{100, 100, 100}); err == nil {
		t.Fatal("huge search accepted")
	}
}

func TestTrajectorySourceObjectiveInitialAndSingularity(t *testing.T) {
	p := []TrajectoryHorizon{{Mean: []float64{.1, -.2}, Cost: []float64{.03, .04}, Covariance: [][]float64{{.04, .02}, {.02, .09}}}}
	r, err := EvaluateTrajectory(p, []float64{.5, -.5}, [][]float64{{.5, -.5}})
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(r.ExpectedReturn-.15) > 1e-12 || math.Abs(r.Variance-.0225) > 1e-12 || math.Abs(r.Sharpe-1) > 1e-12 {
		t.Fatal(r)
	}
	p[0].Covariance = [][]float64{{1, 1}, {1, 1}}
	r, err = OptimizeTrajectory(context.Background(), p, []float64{0, 0}, 2, TrajectoryBudget{8, 8, 64})
	if err != nil || r.Undefined != 2 {
		t.Fatalf("singular covariance supported: %+v %v", r, err)
	}
	if _, err = StaticTrajectory(p, []float64{0, 0}); err == nil {
		t.Fatal("source inverse cannot solve singular covariance")
	}
	p[0].Covariance = [][]float64{{1, 2}, {2, 1}}
	if _, err = EvaluateTrajectory(p, []float64{0, 0}, [][]float64{{1, 0}}); err == nil {
		t.Fatal("indefinite covariance accepted")
	}
}

func TestGaussianRankAndSourceForecastGeneration(t *testing.T) {
	for _, rank := range []int{1, 3, 5} {
		x, err := GaussianMatrixWithRank(100, 5, rank, 0, true, 71)
		if err != nil {
			t.Fatal(err)
		}
		flat := make([]float64, 0, 500)
		for _, row := range x {
			flat = append(flat, row...)
		}
		var sv mat.SVD
		if !sv.Factorize(mat.NewDense(100, 5, flat), mat.SVDNone) {
			t.Fatal("svd failed")
		}
		if sv.Rank(1e-12) != rank {
			t.Fatalf("rank=%d actual=%d", rank, sv.Rank(1e-12))
		}
	}
	x, _ := GaussianMatrixWithRank(100, 5, 1, .1, false, 71)
	flat := make([]float64, 0, 500)
	for _, row := range x {
		flat = append(flat, row...)
	}
	var sv mat.SVD
	sv.Factorize(mat.NewDense(100, 5, flat), mat.SVDNone)
	if sv.Rank(1e-12) != 5 {
		t.Fatal("noise must not claim rank one")
	}
	p, err := RandomTrajectoryForecasts(4, 2, 17)
	if err != nil {
		t.Fatal(err)
	}
	again, _ := RandomTrajectoryForecasts(4, 2, 17)
	if !reflect.DeepEqual(p, again) || len(p[0].Mean) != 4 {
		t.Fatal("seed or size=4 violated")
	}
	if _, err = StaticTrajectory(p, make([]float64, 4)); err != nil {
		t.Fatal(err)
	}
}

func TestGaussianRankSourceDistribution(t *testing.T) {
	// Image00480 uses randn for the loading basis, latent factors and noise.
	// In one dimension the SVD loading is +/-1, so its sign cannot change
	// the standard-normal law. Uniform factors would instead have bounded,
	// one-sided support and variance 1/12; this oracle distinguishes them.
	for _, sigma := range []float64{0, 2} {
		x, err := GaussianMatrixWithRank(10000, 1, 1, sigma, true, 71)
		if err != nil {
			t.Fatal(err)
		}
		mean, squares, positive, negative := 0., 0., 0, 0
		for _, row := range x {
			mean += row[0] / float64(len(x))
			squares += row[0] * row[0] / float64(len(x))
			if row[0] > 1 {
				positive++
			}
			if row[0] < -1 {
				negative++
			}
		}
		variance, want := squares-mean*mean, 1+sigma*sigma
		if math.Abs(mean) > .05*math.Sqrt(want) || math.Abs(variance-want) > .08*want || positive == 0 || negative == 0 {
			t.Fatalf("sigma=%g: Gaussian oracle mean=%g variance=%g want=%g tails=%d/%d", sigma, mean, variance, want, positive, negative)
		}
		t.Logf("sigma=%g: mean=%g variance=%g", sigma, mean, variance)
	}
}

func TestGaussianRankSeedAndGuards(t *testing.T) {
	x, err := GaussianMatrixWithRank(20, 3, 2, .1, false, 71)
	if err != nil {
		t.Fatal(err)
	}
	again, err := GaussianMatrixWithRank(20, 3, 2, .1, false, 71)
	if err != nil || !reflect.DeepEqual(x, again) {
		t.Fatal("rank experiment must reproduce its seed", err)
	}
	other, err := GaussianMatrixWithRank(20, 3, 2, .1, false, 72)
	if err != nil || reflect.DeepEqual(x, other) {
		t.Fatal("rank experiment must use its seed", err)
	}
	for _, args := range []struct {
		samples, columns, rank int
		sigma                  float64
	}{
		{0, 1, 1, 0}, {1, 0, 1, 0}, {257, 257, 1, 0},
		{2, 2, 0, 0}, {2, 2, 3, 0}, {1, 2, 2, 0},
		{2000001, 1, 1, 0}, {2, 2, 1, -1},
		{2, 2, 1, math.NaN()}, {2, 2, 1, math.Inf(1)},
	} {
		if _, err := GaussianMatrixWithRank(args.samples, args.columns, args.rank, args.sigma, true, 71); err == nil {
			t.Fatalf("invalid rank experiment accepted: %+v", args)
		}
	}
}
