package statistics

import (
	"math"
	"math/rand"
	"testing"
)

func TestBandTraderHysteresisStopAndMaxHold(t *testing.T) {
	cases := []struct {
		name string
		cfg  BandConfig
		zs   []float64
		want []int
	}{
		{"enter and exit with hysteresis", BandConfig{Entry: 2, Exit: 0.5},
			[]float64{0, -1.9, -2.1, -1.5, -0.6, -0.4, -2.5, 2.2, 1, 0.2},
			[]int{0, 0, 1, 1, 1, 0, 1, 1, 1, 0}},
		{"stop blocks re-entry until reversion", BandConfig{Entry: 2, Exit: 0.5, Stop: 4},
			[]float64{-2.5, -4.5, -2.5, -1, -0.2, -2.5},
			[]int{1, 0, 0, 0, 0, 1}},
		{"beyond stop never enters", BandConfig{Entry: 2, Exit: 0.5, Stop: 4},
			[]float64{5, -5, 4.5},
			[]int{0, 0, 0}},
		{"max hold closes and blocks", BandConfig{Entry: 2, Exit: 0.5, MaxHold: 2},
			[]float64{-2.5, -2.4, -2.4, -2.4, 0, -2.4},
			[]int{1, 1, 0, 0, 0, 1}},
		{"long only", BandConfig{Entry: 2, Exit: 0.5, LongOnly: true},
			[]float64{2.5, -2.5}, []int{0, 1}},
		{"short only", BandConfig{Entry: 2, Exit: 0.5, ShortOnly: true},
			[]float64{-2.5, 2.5}, []int{0, -1}},
		{"nan keeps state", BandConfig{Entry: 2, Exit: 0.5},
			[]float64{-3, math.NaN(), math.Inf(1), 0}, []int{1, 1, 1, 0}},
	}
	for _, c := range cases {
		bt, err := NewBandTrader(c.cfg)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		for i, z := range c.zs {
			if got := bt.Step(z); got != c.want[i] {
				t.Fatalf("%s step %d z=%v: got %d want %d", c.name, i, z, got, c.want[i])
			}
		}
	}
}

func TestBandConfigValidateRejectsAmbiguousRules(t *testing.T) {
	bad := []BandConfig{
		{}, {Entry: 2, Exit: 2}, {Entry: 2, Exit: -1}, {Entry: 2, Exit: 0.5, Stop: 2},
		{Entry: 2, Exit: 0.5, MaxHold: -1}, {Entry: 2, LongOnly: true, ShortOnly: true},
		{Entry: math.NaN()},
	}
	for i, c := range bad {
		if _, err := NewBandTrader(c); err == nil {
			t.Fatalf("case %d accepted: %+v", i, c)
		}
	}
}

func TestRollingSpreadMatchesFullRegression(t *testing.T) {
	const window = 30
	rng := rand.New(rand.NewSource(7))
	rs, err := NewRollingSpread(window)
	if err != nil {
		t.Fatal(err)
	}
	var xs, ys []float64
	x := 4.6
	for i := 0; i < 200; i++ {
		x += rng.NormFloat64() * 0.01
		y := 1.5 + 1.2*x + rng.NormFloat64()*0.02
		xs, ys = append(xs, x), append(ys, y)
		p, ok := rs.Observe(x, y)
		if i < window-1 {
			if ok {
				t.Fatalf("ready before window at %d", i)
			}
			continue
		}
		if !ok {
			t.Fatalf("not ready at %d", i)
		}
		ex := make([][]float64, window)
		for j := range ex {
			ex[j] = []float64{xs[i-window+1+j]}
		}
		fit, err := Regress(ys[i-window+1:], ex, true)
		if err != nil {
			t.Fatal(err)
		}
		_, std, _ := MeanStd(fit.Residuals, 2)
		last := fit.Residuals[window-1]
		if math.Abs(p.Beta-fit.Beta[0]) > 1e-8 || math.Abs(p.Alpha-fit.Intercept) > 1e-8 ||
			math.Abs(p.Residual-last) > 1e-8 || math.Abs(p.Std-std) > 1e-8 || math.Abs(p.Z-last/std) > 1e-6 {
			t.Fatalf("step %d mismatch: got %+v want beta=%v alpha=%v resid=%v std=%v", i, p, fit.Beta[0], fit.Intercept, last, std)
		}
	}
}

func TestRollingSpreadSkipsInvalidAndConstantInputs(t *testing.T) {
	if _, err := NewRollingSpread(2); err == nil {
		t.Fatal("tiny window accepted")
	}
	rs, _ := NewRollingSpread(5)
	if _, ok := rs.Observe(math.NaN(), 1); ok {
		t.Fatal("nan accepted")
	}
	for i := 0; i < 10; i++ {
		if _, ok := rs.Observe(3, float64(i)); ok {
			t.Fatal("constant x must have no beta")
		}
	}
}
