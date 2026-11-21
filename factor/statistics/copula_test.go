package statistics

import (
	"math"
	"math/rand"
	"testing"
)

func TestNormCDFAndNormInvAccuracy(t *testing.T) {
	probs := []float64{1e-10, 1e-7, 1e-4, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1 - 1e-4, 1 - 1e-7, 1 - 1e-10}
	for _, p := range probs {
		z, err := NormInv(p)
		if err != nil {
			t.Fatalf("NormInv(%g) error: %v", p, err)
		}
		got := NormCDF(z)
		tol := math.Max(1e-14, 1e-12*math.Min(p, 1-p))
		if math.Abs(got-p) > tol {
			t.Fatalf("NormCDF(NormInv(%g)) = %.16g, diff=%g > %g", p, got, math.Abs(got-p), tol)
		}
	}
	for _, bad := range []float64{0, 1, -0.1, 1.1, math.NaN(), math.Inf(1)} {
		if _, err := NormInv(bad); err == nil {
			t.Fatalf("NormInv(%v) must fail", bad)
		}
	}
}

func TestRankUniformsAndKendallTau(t *testing.T) {
	u, ok := RankUniforms([]float64{10, 30, 20, 40}, nil)
	if !ok || len(u) != 4 {
		t.Fatalf("RankUniforms failed: ok=%v u=%v", ok, u)
	}
	want := []float64{0.125, 0.625, 0.375, 0.875}
	for i := range want {
		if math.Abs(u[i]-want[i]) > 1e-15 {
			t.Fatalf("u[%d]=%g want %g", i, u[i], want[i])
		}
		if u[i] <= 0 || u[i] >= 1 {
			t.Fatalf("u[%d]=%g outside (0, 1)", i, u[i])
		}
	}
	// Tie handling: mid-ranks average tied slots and remain symmetric.
	uTie, ok := RankUniforms([]float64{5, 10, 10, 20}, nil)
	if !ok || math.Abs(uTie[1]-0.5) > 1e-15 || math.Abs(uTie[2]-0.5) > 1e-15 {
		t.Fatalf("unexpected tied mid-ranks: %v", uTie)
	}
	// Constant or non-finite samples must return ok=false.
	if _, ok := RankUniforms([]float64{7, 7, 7, 7}, nil); ok {
		t.Fatal("constant sample must return ok=false")
	}
	if _, ok := RankUniforms([]float64{1, math.NaN(), 3}, nil); ok {
		t.Fatal("NaN sample must return ok=false")
	}

	tauPos, err := KendallTau([]float64{1, 2, 3, 4, 5}, []float64{2, 4, 6, 8, 10})
	if err != nil || math.Abs(tauPos-1.0) > 1e-15 {
		t.Fatalf("expected tau=1, got %v err=%v", tauPos, err)
	}
	tauNeg, err := KendallTau([]float64{1, 2, 3, 4, 5}, []float64{10, 8, 6, 4, 2})
	if err != nil || math.Abs(tauNeg+1.0) > 1e-15 {
		t.Fatalf("expected tau=-1, got %v err=%v", tauNeg, err)
	}
	if _, err := KendallTau([]float64{1, 1, 1}, []float64{1, 2, 3}); err == nil {
		t.Fatal("constant x must fail KendallTau")
	}
}

func TestFrankDebyeInversionAndBounds(t *testing.T) {
	taus := []float64{-0.92, -0.75, -0.4, -0.1, -1e-5, 0, 1e-5, 0.1, 0.4, 0.75, 0.92}
	for _, tau := range taus {
		theta, err := FrankThetaFromTau(tau)
		if err != nil {
			t.Fatalf("FrankThetaFromTau(%g): %v", tau, err)
		}
		var gotTau float64
		if math.Abs(theta) < 1e-7 {
			gotTau = theta / 9.0
		} else {
			gotTau = 1.0 - (4.0/theta)*(1.0-FrankDebye1(theta))
		}
		if math.Abs(gotTau-tau) > 1e-11 {
			t.Fatalf("tau=%g -> theta=%g -> roundtrip tau=%.15g (diff=%g)", tau, theta, gotTau, math.Abs(gotTau-tau))
		}
	}
	// Odd symmetry: theta(-tau) == -theta(tau).
	thP, _ := FrankThetaFromTau(0.65)
	thN, _ := FrankThetaFromTau(-0.65)
	if math.Abs(thP+thN) > 1e-12 {
		t.Fatalf("Frank theta odd symmetry broken: %g vs %g", thP, thN)
	}
}

// copulaCDF evaluates the bivariate CDF C(u, v) directly for finite-difference derivative checks.
func copulaCDFForTest(fam CopulaFamily, param, u, v float64) float64 {
	switch fam {
	case CopulaClayton:
		if param == 0 {
			return u * v
		}
		return math.Pow(math.Pow(u, -param)+math.Pow(v, -param)-1.0, -1.0/param)
	case CopulaGumbel:
		x, y := -math.Log(u), -math.Log(v)
		return math.Exp(-math.Pow(math.Pow(x, param)+math.Pow(y, param), 1.0/param))
	case CopulaFrank:
		if param == 0 {
			return u * v
		}
		return -(1.0 / param) * math.Log(1.0+(math.Expm1(-param*u)*math.Expm1(-param*v))/math.Expm1(-param))
	default:
		return math.NaN()
	}
}

func TestCopulaAnalyticalBoundsMonotonicityAndSymmetry(t *testing.T) {
	grid := []float64{0.02, 0.08, 0.25, 0.5, 0.75, 0.92, 0.98}
	cases := []struct {
		fam    CopulaFamily
		params []float64
		radial bool
	}{
		{CopulaGaussian, []float64{-0.8, 0.0, 0.6, 0.92}, true},
		{CopulaClayton, []float64{0.0, 0.8, 3.0, 12.0}, false},
		{CopulaGumbel, []float64{1.0, 1.5, 3.5, 10.0}, false},
		{CopulaFrank, []float64{-10.0, -2.5, 0.0, 2.5, 10.0, 40.0}, true},
	}

	for _, tc := range cases {
		for _, param := range tc.params {
			for _, v := range grid {
				prevPU := 0.0
				for _, u := range grid {
					pUV, pVU, err := CopulaConditionalCDF(tc.fam, param, u, v)
					if err != nil {
						t.Fatalf("%s(param=%g, u=%g, v=%g): %v", tc.fam, param, u, v, err)
					}
					if !(pUV > 0 && pUV < 1 && pVU > 0 && pVU < 1) {
						t.Fatalf("%s conditional prob out of (0,1): pUV=%g pVU=%g", tc.fam, pUV, pVU)
					}
					if pUV < prevPU || (prevPU < 1-1e-13 && pUV > 1e-13 && pUV <= prevPU) {
						t.Fatalf("%s(param=%g, v=%g) not strictly increasing in u at u=%g: %g <= %g", tc.fam, param, v, u, pUV, prevPU)
					}
					prevPU = pUV

					// Exchange symmetry: swapping (u, v) swaps (pUV, pVU).
					pSW1, pSW2, err := CopulaConditionalCDF(tc.fam, param, v, u)
					if err != nil || math.Abs(pUV-pSW2) > 1e-13 || math.Abs(pVU-pSW1) > 1e-13 {
						t.Fatalf("%s exchange symmetry broken: (%g,%g) vs (%g,%g)", tc.fam, pUV, pVU, pSW1, pSW2)
					}

					// Radial symmetry for Gaussian and Frank: C_{1|2}(1-u | 1-v) == 1 - C_{1|2}(u | v).
					if tc.radial {
						pR, _, err := CopulaConditionalCDF(tc.fam, param, 1-u, 1-v)
						if err != nil || math.Abs(pR-(1-pUV)) > 1e-11 {
							t.Fatalf("%s radial symmetry broken at (%g,%g,param=%g): got %g want %g", tc.fam, u, v, param, pR, 1-pUV)
						}
					}

					// Finite-difference check on analytical Archimedean partial derivatives.
					if (tc.fam == CopulaClayton || tc.fam == CopulaGumbel || tc.fam == CopulaFrank) && math.Abs(param) <= 12.0 {
						eps := 1e-6
						cPlus := copulaCDFForTest(tc.fam, param, u, v+eps)
						cMinus := copulaCDFForTest(tc.fam, param, u, v-eps)
						fd := (cPlus - cMinus) / (2 * eps)
						if math.Abs(pUV-fd) > 1e-6 {
							t.Fatalf("%s(param=%g, u=%g, v=%g) partial derivative mismatch: analytic=%g fd=%g", tc.fam, param, u, v, pUV, fd)
						}
					}
				}
			}
		}
	}

	// Empirical copula bounds, strict monotonicity, and radial symmetry.
	uSamp := []float64{0.1, 0.2, 0.35, 0.5, 0.65, 0.8, 0.9}
	vSamp := []float64{0.12, 0.22, 0.30, 0.5, 0.70, 0.78, 0.88}
	for _, v := range grid {
		prev := 0.0
		for _, u := range grid {
			pUV, pVU, err := EmpiricalCopulaConditionalCDF(uSamp, vSamp, u, v)
			if err != nil {
				t.Fatal(err)
			}
			if !(pUV > 0 && pUV < 1 && pVU > 0 && pVU < 1) {
				t.Fatalf("empirical bounds broken: %g %g", pUV, pVU)
			}
			if pUV <= prev {
				t.Fatalf("empirical not strictly monotonic in u at (%g,%g): %g <= %g", u, v, pUV, prev)
			}
			prev = pUV

			pR, _, err := EmpiricalCopulaConditionalCDF(uSamp, vSamp, 1-u, 1-v)
			if err != nil || math.Abs(pR-(1-pUV)) > 1e-12 {
				t.Fatalf("empirical radial symmetry broken: got %g want %g", pR, 1-pUV)
			}
		}
	}

	// Tail dependence property at identical Kendall's tau = 0.6:
	// Clayton has stronger lower-tail crash clustering P(U <= 0.05 | V = 0.05) than Gumbel;
	// Gumbel has stronger upper-tail spike clustering P(U > 0.95 | V = 0.95) = 1 - P(U <= 0.95 | V = 0.95) than Clayton.
	thClay, _ := CopulaParameterFromTau(CopulaClayton, 0.6)
	thGumb, _ := CopulaParameterFromTau(CopulaGumbel, 0.6)
	cLowClay, _, _ := CopulaConditionalCDF(CopulaClayton, thClay, 0.05, 0.05)
	cLowGumb, _, _ := CopulaConditionalCDF(CopulaGumbel, thGumb, 0.05, 0.05)
	if cLowClay <= cLowGumb {
		t.Fatalf("expected Clayton lower-tail clustering (%g) > Gumbel (%g)", cLowClay, cLowGumb)
	}
	cHighClay, _, _ := CopulaConditionalCDF(CopulaClayton, thClay, 0.95, 0.95)
	cHighGumb, _, _ := CopulaConditionalCDF(CopulaGumbel, thGumb, 0.95, 0.95)
	if (1 - cHighGumb) <= (1 - cHighClay) {
		t.Fatalf("expected Gumbel upper-tail clustering (%g) > Clayton (%g)", 1-cHighGumb, 1-cHighClay)
	}
}

func TestRollingCopulaAutoSelectionAndZeroLookahead(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	const n = 150

	// 1. Exact Clayton sample via conditional inversion: V ~ U(0,1), W ~ U(0,1),
	//    U = (1 + V^(-theta) * (W^(-theta/(1+theta)) - 1))^(-1/theta) with theta = 5 (strong lower tail).
	rcAutoLow, err := NewRollingCopula(n, CopulaAuto)
	if err != nil {
		t.Fatal(err)
	}
	const thetaClay = 5.0
	var ptLow CopulaPoint
	for i := 0; i < n; i++ {
		v := math.Max(1e-9, math.Min(1-1e-9, rng.Float64()))
		w := math.Max(1e-9, math.Min(1-1e-9, rng.Float64()))
		u := math.Pow(1.0+math.Pow(v, -thetaClay)*(math.Pow(w, -thetaClay/(1.0+thetaClay))-1.0), -1.0/thetaClay)
		ptLow, _ = rcAutoLow.Observe(v, u)
	}
	if ptLow.Family != CopulaClayton {
		t.Fatalf("expected auto copula to select clayton on Clayton sample, got %q (tau=%g)", ptLow.Family, ptLow.Tau)
	}

	// 2. Reflected Clayton (= survival Clayton, strong upper-tail dependence like Gumbel): (1-v, 1-u).
	rcAutoHigh, _ := NewRollingCopula(n, CopulaAuto)
	var ptHigh CopulaPoint
	rng2 := rand.New(rand.NewSource(42))
	for i := 0; i < n; i++ {
		v := math.Max(1e-9, math.Min(1-1e-9, rng2.Float64()))
		w := math.Max(1e-9, math.Min(1-1e-9, rng2.Float64()))
		u := math.Pow(1.0+math.Pow(v, -thetaClay)*(math.Pow(w, -thetaClay/(1.0+thetaClay))-1.0), -1.0/thetaClay)
		ptHigh, _ = rcAutoHigh.Observe(1-v, 1-u)
	}
	if ptHigh.Family != CopulaGumbel {
		t.Fatalf("expected auto copula to select gumbel on upper-tail sample, got %q (tau=%g)", ptHigh.Family, ptHigh.Tau)
	}

	// 3. Negative dependence (tau < 0): auto falls back cleanly to Gaussian or Frank without error.
	rcAutoNeg, _ := NewRollingCopula(30, CopulaAuto)
	var ptNeg CopulaPoint
	var okNeg bool
	for i := 0; i < 30; i++ {
		x := float64(i) + 0.1*math.Sin(float64(i))
		y := -float64(i) + 0.1*math.Cos(float64(i))
		ptNeg, okNeg = rcAutoNeg.Observe(x, y)
	}
	if !okNeg || ptNeg.Tau >= 0 || (ptNeg.Family != CopulaGaussian && ptNeg.Family != CopulaFrank) {
		t.Fatalf("expected clean fallback on tau < 0, got ok=%v tau=%g fam=%q", okNeg, ptNeg.Tau, ptNeg.Family)
	}

	// 4. Strict zero-lookahead prefix invariance across all 6 families.
	families := []CopulaFamily{CopulaEmpirical, CopulaGaussian, CopulaClayton, CopulaGumbel, CopulaFrank, CopulaAuto}
	xs, ys := make([]float64, 50), make([]float64, 50)
	for i := range xs {
		xs[i] = 3.0 + 0.05*float64(i) + rng.NormFloat64()*0.08
		ys[i] = 1.5*xs[i] + rng.NormFloat64()*0.04
	}
	for _, fam := range families {
		cShort, err := NewRollingCopula(20, fam)
		if err != nil {
			t.Fatal(err)
		}
		cLong, _ := NewRollingCopula(20, fam)
		var p24 CopulaPoint
		for i := 0; i < 25; i++ {
			p, ok := cShort.Observe(xs[i], ys[i])
			if i < 19 && ok {
				t.Fatalf("%s ready before window full at %d", fam, i)
			}
			if i == 24 {
				if !ok {
					t.Fatalf("%s not ready at 24", fam)
				}
				p24 = p
			}
		}
		for i := 0; i < 50; i++ {
			xv, yv := xs[i], ys[i]
			if i >= 25 {
				xv += 50.0
				yv -= 80.0
			}
			p, _ := cLong.Observe(xv, yv)
			if i == 24 && (p.ProbYGivenX != p24.ProbYGivenX || p.ProbXGivenY != p24.ProbXGivenY || p.Z != p24.Z) {
				t.Fatalf("%s lookahead detected at bar 24: %+v vs %+v", fam, p, p24)
			}
		}
	}
}

func TestCopulaTraderHysteresisOvershootAndStop(t *testing.T) {
	tr, err := NewCopulaTrader(
		CopulaConfig{Family: CopulaClayton, LowerProb: 0.05, UpperProb: 0.95, ExitBand: 0.0, StopProb: 0.005},
		BandConfig{MaxHold: 4},
	)
	if err != nil {
		t.Fatal(err)
	}
	// One-sided low pYX=0.03 without high pXY=0.80 (< 0.95) must NOT enter.
	if pos := tr.Step(0.03, 0.80); pos != 0 {
		t.Fatalf("must require both conditional probs, got %d", pos)
	}
	// Joint mispricing pYX=0.03 <= 0.05 && pXY=0.97 >= 0.95 opens long spread (+1).
	if tr.Preview(0.03, 0.97) != 1 || tr.Position() != 0 {
		t.Fatal("Preview must return +1 without mutating Position")
	}
	if pos := tr.Step(0.03, 0.97); pos != 1 {
		t.Fatalf("expected +1 entry, got %d", pos)
	}
	// Partial reversion (0.25, 0.75) holds +1.
	if pos := tr.Step(0.25, 0.75); pos != 1 {
		t.Fatalf("expected +1 hold, got %d", pos)
	}
	// Discrete-bar overshoot across 0.5 directly to (0.68, 0.32) with ExitBand=0 exits cleanly!
	if pos := tr.Step(0.68, 0.32); pos != 0 || tr.LastCloseReason() != "exit" {
		t.Fatalf("expected overshoot across 0.5 to exit, got pos=%d reason=%s", pos, tr.LastCloseReason())
	}

	// Short entry + extreme tail StopProb=0.005 blocks immediate re-entry until crossing 0.5.
	if pos := tr.Step(0.96, 0.04); pos != -1 {
		t.Fatalf("expected -1 entry, got %d", pos)
	}
	if pos := tr.Step(0.998, 0.002); pos != 0 || tr.LastCloseReason() != "stop" {
		t.Fatalf("expected tail stop close, got pos=%d reason=%s", pos, tr.LastCloseReason())
	}
	// Still overvalued at (0.97, 0.03) -> blocked!
	if pos := tr.Step(0.97, 0.03); pos != 0 {
		t.Fatalf("expected blocked after stop, got %d", pos)
	}
	// Crosses 0.5 to (0.48, 0.52) -> clears block; next spike (0.97, 0.03) re-enters -1!
	tr.Step(0.48, 0.52)
	if pos := tr.Step(0.97, 0.03); pos != -1 {
		t.Fatalf("expected re-entry after clearing block, got %d", pos)
	}
}

func TestHurstExponentWilderRSIAndRollingReturnCorr(t *testing.T) {
	// 1. Hurst exponent across mean-reverting, random-walk, and trending series.
	rng := rand.New(rand.NewSource(123))
	mr := make([]float64, 120)
	rw := make([]float64, 120)
	tr := make([]float64, 120)
	for i := 1; i < len(mr); i++ {
		e := rng.NormFloat64()
		mr[i] = -0.4*mr[i-1] + e // anti-persistent / fast mean reversion
		rw[i] = rw[i-1] + e
		tr[i] = float64(i)*0.8 + 0.1*e // strong deterministic trend
	}
	hMR, err := HurstExponent(mr)
	if err != nil || hMR >= 0.35 {
		t.Fatalf("expected mean-reverting Hurst < 0.35, got %g (err=%v)", hMR, err)
	}
	hRW, err := HurstExponent(rw)
	if err != nil || hRW < 0.35 || hRW > 0.65 {
		t.Fatalf("expected random-walk Hurst near 0.5, got %g (err=%v)", hRW, err)
	}
	hTR, err := HurstExponent(tr)
	if err != nil || hTR <= 0.85 {
		t.Fatalf("expected trending Hurst > 0.85, got %g (err=%v)", hTR, err)
	}

	// 2. Wilder RSI bounds and exact cases.
	up := []float64{1, 2, 3, 4, 5, 6, 7, 8}
	if rsi, err := WilderRSI(up, 5); err != nil || rsi != 100 {
		t.Fatalf("expected RSI=100 on pure gains, got %g (%v)", rsi, err)
	}
	flat := []float64{5, 5, 5, 5, 5, 5}
	if rsi, err := WilderRSI(flat, 4); err != nil || rsi != 50 {
		t.Fatalf("expected RSI=50 on flat series, got %g (%v)", rsi, err)
	}
	mixed := []float64{10, 11, 10.5, 11.5, 10.8, 12.0, 13.5, 14.2}
	if rsi, err := WilderRSI(mixed, 5); err != nil || rsi <= 60 || rsi >= 100 {
		t.Fatalf("unexpected mixed Wilder RSI: %g (%v)", rsi, err)
	}

	// 3. RollingReturnCorr exactness and missing-bar ResetPrev isolation.
	rc, err := NewRollingReturnCorr(6) // 5 consecutive 1-bar returns
	if err != nil {
		t.Fatal(err)
	}
	lxs := []float64{1.00, 1.02, 1.01, 1.04, 1.02, 1.06}
	lys := []float64{2.00, 2.04, 2.02, 2.08, 2.04, 2.12} // dy = 2 * dx => corr == +1.0
	for i := range lxs {
		c, ok := rc.ObserveLog(lxs[i], lys[i])
		if i < 5 && ok {
			t.Fatalf("corr ready too early at %d", i)
		}
		if i == 5 && (!ok || math.Abs(c-1.0) > 1e-12) {
			t.Fatalf("expected corr=1.0 at bar 5, got %g ok=%v", c, ok)
		}
	}
	rc.ResetPrev()
	if _, ok := rc.Last(); ok {
		t.Fatal("ResetPrev must mark correlation unready until next consecutive return arrives")
	}
	// First bar after gap re-anchors prev without recording a jump return; second bar resumes!
	if _, ok := rc.ObserveLog(5.00, -3.00); ok {
		t.Fatal("anchor bar after ResetPrev must not emit correlation")
	}
	if c, ok := rc.ObserveLog(5.02, -2.96); !ok || math.Abs(c-1.0) > 1e-12 {
		t.Fatalf("expected clean resume without gap jump pollution, got %g ok=%v", c, ok)
	}
}
