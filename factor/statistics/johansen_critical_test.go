package statistics

import (
	"math"
	"math/rand"
	"testing"
)

func TestJohansenCriticalValueTablesAndValidation(t *testing.T) {
	spec := JohansenSpec{Intercept: true, Trend: false, DifferenceLags: 1}

	trace95, err := JohansenTraceCriticalValues(12, spec, 0.95)
	if err != nil {
		t.Fatal(err)
	}
	wantTrace95 := []float64{
		334.9795, // r=0 -> m=12
		285.1402, // r=1 -> m=11
		239.2468, // r=2 -> m=10
		197.3772, // r=3 -> m=9
		159.5290, // r=4 -> m=8
		125.6185, // r=5 -> m=7
		95.7542,  // r=6 -> m=6
		69.8189,  // r=7 -> m=5
		47.8545,  // r=8 -> m=4
		29.7961,  // r=9 -> m=3
		15.4943,  // r=10 -> m=2
		3.8415,   // r=11 -> m=1
	}
	if len(trace95) != len(wantTrace95) {
		t.Fatalf("len=%d want=%d", len(trace95), len(wantTrace95))
	}
	for i, w := range wantTrace95 {
		if math.Abs(trace95[i]-w) > 1e-9 {
			t.Fatalf("trace95[%d]=%v want=%v", i, trace95[i], w)
		}
	}

	maxEig95, err := JohansenMaxEigCriticalValues(4, spec, 0.95)
	if err != nil {
		t.Fatal(err)
	}
	wantMaxEig95 := []float64{27.5858, 21.1314, 14.2639, 3.8415}
	for i, w := range wantMaxEig95 {
		if math.Abs(maxEig95[i]-w) > 1e-9 {
			t.Fatalf("maxEig95[%d]=%v want=%v", i, maxEig95[i], w)
		}
	}

	// Verify strict monotonicity across confidence levels (90% < 95% < 99%) and across m=d-r, including m > 12 extrapolation.
	for _, detSpec := range []JohansenSpec{
		{Intercept: false, Trend: false},
		{Intercept: true, Trend: false},
		{Intercept: true, Trend: true},
	} {
		t90, err := JohansenTraceCriticalValues(16, detSpec, 0.90)
		if err != nil {
			t.Fatal(err)
		}
		t95, err := JohansenTraceCriticalValues(16, detSpec, 0.95)
		if err != nil {
			t.Fatal(err)
		}
		t99, err := JohansenTraceCriticalValues(16, detSpec, 0.99)
		if err != nil {
			t.Fatal(err)
		}
		m90, err := JohansenMaxEigCriticalValues(16, detSpec, 0.90)
		if err != nil {
			t.Fatal(err)
		}
		m95, err := JohansenMaxEigCriticalValues(16, detSpec, 0.95)
		if err != nil {
			t.Fatal(err)
		}
		m99, err := JohansenMaxEigCriticalValues(16, detSpec, 0.99)
		if err != nil {
			t.Fatal(err)
		}
		for r := range 16 {
			if !(t90[r] < t95[r] && t95[r] < t99[r]) {
				t.Fatalf("trace confidence order broken at r=%d: %v < %v < %v", r, t90[r], t95[r], t99[r])
			}
			if !(m90[r] < m95[r] && m95[r] < m99[r]) {
				t.Fatalf("maxEig confidence order broken at r=%d: %v < %v < %v", r, m90[r], m95[r], m99[r])
			}
			if r > 0 && !(t95[r] < t95[r-1] && m95[r] < m95[r-1]) {
				t.Fatalf("m-monotonicity broken at r=%d: trace=%v/%v max=%v/%v", r, t95[r], t95[r-1], m95[r], m95[r-1])
			}
		}
	}

	// Reject invalid dimensions, specs, and confidence levels.
	if _, err := JohansenTraceCriticalValues(1, spec, 0.95); err == nil {
		t.Fatal("d=1 accepted")
	}
	if _, err := JohansenTraceCriticalValues(65, spec, 0.95); err == nil {
		t.Fatal("d=65 accepted")
	}
	if _, err := JohansenTraceCriticalValues(3, JohansenSpec{Intercept: false, Trend: true}, 0.95); err == nil {
		t.Fatal("Trend without Intercept accepted")
	}
	for _, badConf := range []float64{0, 0.80, 0.975, 1.0, math.NaN(), math.Inf(1)} {
		if _, err := JohansenTraceCriticalValues(3, spec, badConf); err == nil {
			t.Fatalf("bad confidence %v accepted", badConf)
		}
		if _, err := JohansenMaxEigCriticalValues(3, spec, badConf); err == nil {
			t.Fatalf("bad confidence %v accepted in MaxEig", badConf)
		}
	}
}

func TestJohansenThreeLegCointegratedVsRandomWalkRank(t *testing.T) {
	spec := JohansenSpec{Intercept: true, DifferenceLags: 1}

	// 1. 3-leg system with 2 common stochastic trends (rank == 1): y0 = 1.5*w1 - 0.8*w2 + u_t.
	rng := rand.New(rand.NewSource(202604))
	levelsRank1 := make([][]float64, 0, 900)
	var w1, w2, u float64
	for range 900 {
		w1 += rng.NormFloat64() * 0.03
		w2 += rng.NormFloat64() * 0.03
		u = 0.45*u + rng.NormFloat64()*0.012
		levelsRank1 = append(levelsRank1, []float64{1.5*w1 - 0.8*w2 + u, w1, w2})
	}
	res1, err := Johansen(levelsRank1, spec)
	if err != nil {
		t.Fatal(err)
	}
	rank1, err := res1.CointegrationRank(spec, 0.95)
	if err != nil || rank1 != 1 {
		t.Fatalf("expected rank=1 on 3-leg single-spread system, got rank=%d trace=%v err=%v", rank1, res1.Trace, err)
	}
	isCoint1, err := res1.IsCointegrated(spec, 0.99)
	if err != nil || !isCoint1 {
		t.Fatalf("expected IsCointegrated=true at 99%%, got %v err=%v", isCoint1, err)
	}
	maxCV, err := JohansenMaxEigCriticalValues(3, spec, 0.99)
	if err != nil {
		t.Fatal(err)
	}
	maxRank1, err := res1.MaxEigRank(maxCV)
	if err != nil || maxRank1 != 1 {
		t.Fatalf("expected MaxEigRank=1 at 99%%, got %d maxEig=%v err=%v", maxRank1, res1.MaximumEigenvalue, err)
	}
	beta := res1.CointegratingVectors[0]
	if math.Abs(beta[1]/beta[0]+1.5) > 0.06 || math.Abs(beta[2]/beta[0]-0.8) > 0.06 {
		t.Fatalf("unexpected 3-leg cointegrating vector ratios: beta=%v", beta)
	}

	// 2. 3-leg system with 1 common stochastic trend (rank == 2): y0 = w + u1, y1 = 0.7*w + u2, y2 = w.
	levelsRank2 := make([][]float64, 0, 900)
	var wCommon, u1, u2 float64
	for range 900 {
		wCommon += rng.NormFloat64() * 0.03
		u1 = 0.40*u1 + rng.NormFloat64()*0.01
		u2 = 0.35*u2 + rng.NormFloat64()*0.01
		levelsRank2 = append(levelsRank2, []float64{wCommon + u1, 0.7*wCommon + u2, wCommon})
	}
	res2, err := Johansen(levelsRank2, spec)
	if err != nil {
		t.Fatal(err)
	}
	rank2, err := res2.CointegrationRank(spec, 0.95)
	if err != nil || rank2 != 2 {
		t.Fatalf("expected rank=2 on 1-common-trend 3-leg system, got rank=%d trace=%v err=%v", rank2, res2.Trace, err)
	}

	// 3. 3 independent random walks: rank == 0, IsCointegrated == false.
	rngRW := rand.New(rand.NewSource(71))
	levelsRW := make([][]float64, 0, 600)
	var rw0, rw1, rw2 float64
	for range 600 {
		rw0 += rngRW.NormFloat64() * 0.02
		rw1 += rngRW.NormFloat64() * 0.02
		rw2 += rngRW.NormFloat64() * 0.02
		levelsRW = append(levelsRW, []float64{rw0, rw1, rw2})
	}
	resRW, err := Johansen(levelsRW, spec)
	if err != nil {
		t.Fatal(err)
	}
	isCointRW, err := resRW.IsCointegrated(spec, 0.95)
	if err != nil || isCointRW {
		t.Fatalf("independent random walks falsely flagged as cointegrated: trace=%v err=%v", resRW.Trace, err)
	}
}
