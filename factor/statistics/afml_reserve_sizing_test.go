package statistics

import (
	"math"
	"math/rand"
	"reflect"
	"testing"
)

func TestReserveBudgetAndPowerSizingSourceEquations(t *testing.T) {
	m := GaussianMixture2{[2]float64{0, 0}, [2]float64{1, 1}, [2]float64{.4, .6}}
	for _, x := range []float64{-3, -1, 0, 1, 3} {
		v, err := m.ReserveBetSize(x)
		want := math.Erf(x / math.Sqrt2)
		if err != nil || math.Abs(v-want) > 1e-14 {
			t.Fatal(x, v, want, err)
		}
	}
	skew := GaussianMixture2{[2]float64{-1, 2}, [2]float64{.5, 1.5}, [2]float64{.3, .7}}
	base, _ := skew.CDF(0)
	cdf, _ := skew.CDF(-2)
	v, err := skew.ReserveBetSize(-2)
	if err != nil || math.Abs(v-(cdf-base)/base) > 1e-14 {
		t.Fatal(v, err)
	}
	budget, err := BudgetBetSize(3, 2, 6, 8)
	if err != nil || budget != .25 {
		t.Fatal(budget, err)
	}
	if _, err := BudgetBetSize(7, 0, 6, 8); err == nil {
		t.Fatal("unseen excessive concurrency accepted")
	}
	bets := []ActiveBet{{1, 4, false, .5}, {2, 3, false, -1}, {3, 0, true, 1}, {1, 9, false, 0}}
	longs, shorts, err := BetConcurrency(bets, []int64{1, 2, 3, 4}, 16)
	if err != nil || !reflect.DeepEqual(longs, []int{1, 1, 2, 1}) || !reflect.DeepEqual(shorts, []int{0, 1, 0, 0}) {
		t.Fatal(longs, shorts, err)
	}
	if _, _, err := BetConcurrency(bets, []int64{1, 2, 3, 4}, 15); err == nil {
		t.Fatal("work bound ignored")
	}
	omega, err := CalibratePowerBet(.5, .25)
	if err != nil || omega != 2 {
		t.Fatal(omega, err)
	}
	for _, x := range []float64{-1, -.5, 0, .5, 1} {
		size, err := PowerBetSize(omega, x)
		if err != nil {
			t.Fatal(err)
		}
		inverse, err := InversePowerBet(size, omega)
		if err != nil || inverse != x {
			t.Fatal(x, size, inverse, err)
		}
	}
	zero, _ := PowerBetSize(0, 0)
	endpoint, _ := PowerBetSize(0, -.1)
	if zero != 0 || endpoint != -1 {
		t.Fatal("sign zero/omega zero", zero, endpoint)
	}
	if _, err := InversePowerBet(.5, 0); err == nil {
		t.Fatal("noninvertible step accepted")
	}
}

func TestGaussianMixtureFitFrozenOwnershipAndFailure(t *testing.T) {
	rng := rand.New(rand.NewSource(102))
	data := make([]float64, 1000)
	for i := range data {
		if i < 400 {
			data[i] = -3 + .4*rng.NormFloat64()
		} else {
			data[i] = 2 + .7*rng.NormFloat64()
		}
	}
	saved := append([]float64(nil), data...)
	spec := GaussianMixtureSpec{400, 400000, 1e-9, .05}
	fit, err := FitGaussianMixture2(data, spec)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(data, saved) || math.Abs(fit.Model.Means[0]+3) > .1 || math.Abs(fit.Model.Means[1]-2) > .1 || math.Abs(fit.Model.Weights[0]-.4) > .03 {
		t.Fatal(fit)
	}
	before, _ := fit.Model.ReserveBetSize(1)
	for i := range data {
		data[i] = 1000
	}
	after, _ := fit.Model.ReserveBetSize(1)
	if before != after {
		t.Fatal("training ownership escaped")
	}
	if _, err := FitGaussianMixture2(data, spec); err == nil {
		t.Fatal("constant training accepted")
	}
	spec.MaxIterations = 1
	spec.MaxWork = 1000
	if _, err := FitGaussianMixture2(saved, spec); err == nil {
		t.Fatal("unconverged fit accepted")
	}
	spec.MaxWork = 999
	if _, err := FitGaussianMixture2(saved, spec); err == nil {
		t.Fatal("oversized fitting accepted")
	}
	bad := GaussianMixture2{[2]float64{}, [2]float64{1, 1}, [2]float64{.2, .2}}
	if _, err := bad.ReserveBetSize(0); err == nil {
		t.Fatal("invalid snapshot accepted")
	}
}
