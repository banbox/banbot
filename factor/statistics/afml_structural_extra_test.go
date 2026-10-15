package statistics

import (
	"math"
	"math/rand"
	"testing"
)

func TestAFMLRecursiveResidualCUSUM(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	x := make([][]float64, 100)
	y := make([]float64, 100)
	for i := range x {
		x[i] = []float64{1, float64(i) / 100}
		y[i] = 2 + 3*x[i][1] + rng.NormFloat64()*.1
		if i >= 70 {
			y[i] += 1
		}
	}
	r, err := RecursiveResidualCUSUM(y, x, 10)
	if err != nil {
		t.Fatal(err)
	}
	// Independently recompute recursive OLS and leverage by prediction variance
	// for a two-column [1,z] design. This tests actual RLS, not its own recurrence.
	for end := 10; end < 100; end++ {
		fit, err := Regress(y[:end], x[:end], false)
		if err != nil {
			t.Fatal(err)
		}
		sum, sumSq := 0., 0.
		for _, row := range x[:end] {
			sum += row[1]
			sumSq += row[1] * row[1]
		}
		n := float64(end)
		z := x[end][1]
		lev := (sumSq - 2*z*sum + n*z*z) / (n*sumSq - sum*sum)
		w := (y[end] - fit.Beta[0] - fit.Beta[1]*z) / math.Sqrt(1+lev)
		if math.Abs(w-r.Residuals[end]) > 1e-9 {
			t.Fatalf("%d got %g want %g", end, r.Residuals[end], w)
		}
	}
	if r.CUSUM[99] < 15 {
		t.Fatal("missed mean break", r.CUSUM[99])
	}
	prefix, err := RecursiveResidualCUSUM(y[:70], x[:70], 10)
	if err != nil {
		t.Fatal(err)
	}
	for i := 10; i < 70; i++ {
		if prefix.Residuals[i] != r.Residuals[i] {
			t.Fatal("noncausal forecast residual")
		}
	}
}

func TestAFMLLevelCUSUMAndChow(t *testing.T) {
	r, err := LevelCUSUM([]float64{0, 1, 2, 3}, 1, 4.6)
	if err != nil || math.Abs(r.Statistic-math.Sqrt(3)) > 1e-12 || r.Reference != 0 || math.Abs(r.Critical-math.Sqrt(4.6+math.Log(3))) > 1e-12 {
		t.Fatal(r, err)
	}
	rng := rand.New(rand.NewSource(5))
	values := make([]float64, 150)
	values[0] = 1
	for i := 1; i < len(values); i++ {
		values[i] = values[i-1] + .03*rng.NormFloat64()
		if i >= 75 {
			values[i] = 1.04*values[i-1] + .03*rng.NormFloat64()
		}
	}
	stat, err := ChowDickeyFuller(values, 75)
	if err != nil || stat < 10 {
		t.Fatal(stat, err)
	}
	mesh, err := SupChowDickeyFuller(values, 30, 120, 91)
	if err != nil || mesh.Statistic < stat || mesh.BreakIndex < 30 || mesh.BreakIndex > 120 {
		t.Fatal(mesh, err)
	}
	if _, err = SupChowDickeyFuller(values, 30, 120, 90); err == nil {
		t.Fatal("ignored budget")
	}
	if _, err = LevelCUSUM([]float64{1, 1, 1}, 1, 4.6); err == nil {
		t.Fatal("accepted zero scale")
	}
}

func TestAFMLRobustADFPopulation(t *testing.T) {
	r, err := RobustADF([]float64{1, 2, 3, 4, 5}, .75, .125)
	if err != nil || r.Quantile != 4 || r.QuantileDispersion != 1 || r.TailMean != 4.5 || r.TailStd != .5 || r.TailCount != 2 {
		t.Fatal(r, err)
	}
	// Robust summaries use the same actual backward regression statistics,
	// rather than fitting unrelated quantile regressions under the QADF name.
	rng := rand.New(rand.NewSource(3))
	x := make([]float64, 50)
	for i := 1; i < len(x); i++ {
		x[i] = x[i-1] + rng.NormFloat64()
	}
	population, err := BackwardSupADF(x, 20, 1, "c", 100)
	if err != nil {
		t.Fatal(err)
	}
	r, err = RobustADF(population.Statistics, .95, .025)
	if err != nil || r.TailMean > population.Statistic+1e-12 || r.Maximum != population.Statistic {
		t.Fatal(r, err)
	}
	if _, err = RobustADF([]float64{1}, 1, .1); err == nil {
		t.Fatal("accepted undefined extreme dispersion")
	}
}

func TestAFMLMartingaleTrendFunctionalForms(t *testing.T) {
	rng := rand.New(rand.NewSource(34))
	noise := make([]float64, 80)
	for i := range noise {
		noise[i] = rng.NormFloat64() * .03
	}
	for _, model := range []string{"poly1", "poly2", "exp", "power"} {
		for _, side := range []float64{-1, 1} {
			values := make([]float64, len(noise))
			for i, e := range noise {
				clock := float64(i + 1)
				z := 2 + side*.001*clock*clock + e
				switch model {
				case "poly1":
					values[i] = z
				case "poly2":
					values[i] = math.Exp(z)
				case "exp":
					values[i] = math.Exp(2 + side*.02*clock + e)
				case "power":
					values[i] = math.Exp(2 + side*.5*math.Log(clock) + e)
				}
			}
			a, err := SupMartingaleTrend(values, 20, model, 0, 61)
			if err != nil || a.Statistic < 20 {
				t.Fatal(model, side, a, err)
			}
			b, err := SupMartingaleTrend(values, 20, model, .5, 61)
			if err != nil {
				t.Fatal(err)
			}
			for i, s := range a.Statistics {
				if math.Abs(b.Statistics[i]-s/math.Sqrt(float64(len(values)-i))) > 1e-10 {
					t.Fatal("wrong window penalty")
				}
			}
		}
	}
	if _, err := SupMartingaleTrend([]float64{0, 1, 2, 3, 4}, 4, "exp", 0, 2); err == nil {
		t.Fatal("accepted log zero")
	}
}
