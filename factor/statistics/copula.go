package statistics

import (
	"errors"
	"fmt"
	"math"
)

// CopulaFamily identifies a bivariate copula family for conditional probability pair trading.
type CopulaFamily string

const (
	CopulaEmpirical CopulaFamily = "empirical"
	CopulaGaussian  CopulaFamily = "gaussian"
	CopulaClayton   CopulaFamily = "clayton"
	CopulaGumbel    CopulaFamily = "gumbel"
	CopulaFrank     CopulaFamily = "frank"
	CopulaAuto      CopulaFamily = "auto"

	defaultCopulaLowerProb = 0.05
	defaultCopulaUpperProb = 0.95
	copulaProbEps          = 1e-15
	maxCopulaParam         = 200.0
)

// 16-point Gauss-Legendre quadrature nodes and weights on [0, 1] for Debye D_1(theta).
var (
	gl16Nodes = [16]float64{
		0.005299532504175034, 0.027712488463383712, 0.06718439880608412, 0.12229779582249848,
		0.19106187779867813, 0.2709916111713863, 0.35919822461037054, 0.4524937450811813,
		0.5475062549188187, 0.6408017753896295, 0.7290083888286137, 0.8089381222013219,
		0.8777022041775015, 0.9328156011939159, 0.9722875115366163, 0.994700467495825,
	}
	gl16Weights = [16]float64{
		0.013576229705877047, 0.031126761969323946, 0.04757925584124639, 0.062314485627766936,
		0.07479799440828837, 0.08457825969750127, 0.09130170752246179, 0.09472530522753425,
		0.09472530522753425, 0.09130170752246179, 0.08457825969750127, 0.07479799440828837,
		0.062314485627766936, 0.04757925584124639, 0.031126761969323946, 0.013576229705877047,
	}
)

func clampCopulaProb(p float64) float64 {
	if p < copulaProbEps {
		return copulaProbEps
	}
	if p > 1-copulaProbEps {
		return 1 - copulaProbEps
	}
	return p
}

// NormCDF returns the standard normal cumulative distribution function Phi(x).
func NormCDF(x float64) float64 {
	if math.IsNaN(x) {
		return math.NaN()
	}
	if math.IsInf(x, -1) {
		return 0
	}
	if math.IsInf(x, 1) {
		return 1
	}
	return 0.5 * math.Erfc(-x/math.Sqrt2)
}

// NormInv returns the standard normal quantile function Phi^-1(p) for p in (0, 1),
// refined with one Halley step on Erfc for full double-precision tail accuracy.
func NormInv(p float64) (float64, error) {
	if !finite(p) || p <= 0 || p >= 1 {
		return 0, errors.New("statistics: normal quantile requires probability in (0, 1)")
	}
	if p == 0.5 {
		return 0, nil
	}
	sign := 1.0
	q := p
	if p > 0.5 {
		sign = -1.0
		q = 1 - p
	}
	// q in (0, 0.5) => x < 0.
	x := math.Sqrt2 * math.Erfinv(2*q-1)
	cdf := 0.5 * math.Erfc(-x/math.Sqrt2)
	pdf := (1.0 / math.Sqrt(2*math.Pi)) * math.Exp(-0.5*x*x)
	if pdf > 0 && finite(pdf) {
		u := (cdf - q) / pdf
		denom := 1 + 0.5*x*u
		if denom > 0.1 {
			x -= u / denom
		}
	}
	if sign < 0 {
		return -x, nil
	}
	return x, nil
}

// RankUniforms maps values into mid-rank marginal uniforms (less + 0.5*eq) / N strictly in (0, 1).
// It returns ok=false if len(values) < 2, any value is non-finite, or all values are identical.
func RankUniforms(values []float64, dst []float64) ([]float64, bool) {
	n := len(values)
	if n < 2 {
		return dst[:0], false
	}
	for _, v := range values {
		if !finite(v) {
			return dst[:0], false
		}
	}
	if cap(dst) < n {
		dst = make([]float64, n)
	} else {
		dst = dst[:n]
	}
	invN := 1.0 / float64(n)
	allEqual := true
	first := values[0]
	for i, vi := range values {
		if vi != first {
			allEqual = false
		}
		less, eq := 0, 0
		for _, vj := range values {
			if vj < vi {
				less++
			} else if vj == vi {
				eq++
			}
		}
		dst[i] = clampCopulaProb((float64(less) + 0.5*float64(eq)) * invN)
	}
	if allEqual {
		return dst[:0], false
	}
	return dst, true
}

// KendallTau computes deterministic tie-adjusted Kendall's tau-b in [-1, 1].
func KendallTau(x, y []float64) (float64, error) {
	n := len(x)
	if n < 2 || len(y) != n {
		return 0, errors.New("statistics: Kendall tau requires aligned slices of length >= 2")
	}
	for i := 0; i < n; i++ {
		if !finite(x[i]) || !finite(y[i]) {
			return 0, errors.New("statistics: non-finite sample in Kendall tau")
		}
	}
	var conc, disc, tieX, tieY int64
	for i := 0; i < n-1; i++ {
		xi, yi := x[i], y[i]
		for j := i + 1; j < n; j++ {
			dx := x[j] - xi
			dy := y[j] - yi
			switch {
			case dx == 0 && dy == 0:
			case dx == 0:
				tieX++
			case dy == 0:
				tieY++
			case (dx > 0) == (dy > 0):
				conc++
			default:
				disc++
			}
		}
	}
	nx := conc + disc + tieX
	ny := conc + disc + tieY
	if nx == 0 || ny == 0 {
		return 0, errors.New("statistics: constant sample in Kendall tau")
	}
	tau := float64(conc-disc) / math.Sqrt(float64(nx)*float64(ny))
	if tau > 1 {
		tau = 1
	} else if tau < -1 {
		tau = -1
	}
	return tau, nil
}

func debyeIntegrand(t float64) float64 {
	if math.Abs(t) < 1e-6 {
		return 1 - 0.5*t + (t*t)/12.0
	}
	if t > 50 {
		return t * math.Exp(-t)
	}
	return t / math.Expm1(t)
}

// FrankDebye1 evaluates the first-order Debye function D_1(theta) = (1/theta) * int_0^theta t/(exp(t)-1) dt.
func FrankDebye1(theta float64) float64 {
	if !finite(theta) {
		return math.NaN()
	}
	if math.Abs(theta) < 1e-6 {
		return 1 - 0.25*theta + (theta*theta)/36.0
	}
	if theta < 0 {
		// D_1(-x) = D_1(x) + x/2 for x > 0.
		x := -theta
		return FrankDebye1(x) + 0.5*x
	}
	// For t > 45, t/(exp(t)-1) < 1.3e-18; split at min(theta, 45) for full precision at large theta.
	upper := math.Min(theta, 45.0)
	var integral float64
	// Split [0, upper] into two sub-intervals for 1e-15 accuracy across [0, 45].
	mid := 0.5 * upper
	for seg := 0; seg < 2; seg++ {
		a := float64(seg) * mid
		h := mid
		for k := 0; k < 16; k++ {
			t := a + h*gl16Nodes[k]
			integral += gl16Weights[k] * debyeIntegrand(t)
		}
	}
	integral *= mid
	return integral / theta
}

func frankTauFromPosTheta(theta float64) (tau, deriv float64) {
	if theta < 1e-5 {
		t2 := theta * theta
		return theta/9.0 - (theta*t2)/900.0, 1.0/9.0 - t2/300.0
	}
	d1 := FrankDebye1(theta)
	tau = 1.0 - (4.0/theta)*(1.0-d1)
	deriv = (4.0 / (theta * theta)) * (1.0 - 2.0*d1 + debyeIntegrand(theta))
	return tau, deriv
}

// FrankThetaFromTau deterministically inverts tau(theta) = 1 - 4*(1 - D_1(theta))/theta
// via bracketed Newton-bisection.
func FrankThetaFromTau(tau float64) (float64, error) {
	if !finite(tau) || tau <= -1 || tau >= 1 {
		return 0, errors.New("statistics: Frank Kendall tau must be in (-1, 1)")
	}
	if math.Abs(tau) < 1e-9 {
		return 9.0 * tau, nil
	}
	sign := 1.0
	target := tau
	if tau < 0 {
		sign = -1.0
		target = -tau
	}
	maxTau, _ := frankTauFromPosTheta(maxCopulaParam)
	if target >= maxTau {
		return sign * maxCopulaParam, nil
	}
	lo, hi := 0.0, maxCopulaParam
	x := math.Min(maxCopulaParam*0.5, 9.0*target/(1.0-target))
	if x <= lo || x >= hi {
		x = 0.5 * (lo + hi)
	}
	for iter := 0; iter < 40; iter++ {
		f, df := frankTauFromPosTheta(x)
		diff := f - target
		if math.Abs(diff) <= 1e-13 {
			break
		}
		if diff < 0 {
			lo = x
		} else {
			hi = x
		}
		next := x
		if df > 1e-15 {
			next = x - diff/df
		}
		if next <= lo || next >= hi {
			next = 0.5 * (lo + hi)
		}
		if math.Abs(next-x) <= 1e-13*(1+x) {
			x = next
			break
		}
		x = next
	}
	return sign * x, nil
}

// ValidateCopulaFamily checks whether family is one of the supported CopulaFamily names.
func ValidateCopulaFamily(family CopulaFamily) (CopulaFamily, error) {
	switch family {
	case "", CopulaAuto:
		return CopulaAuto, nil
	case CopulaEmpirical, CopulaGaussian, CopulaClayton, CopulaGumbel, CopulaFrank:
		return family, nil
	default:
		return family, fmt.Errorf("statistics: unknown copula family %q", family)
	}
}

// CopulaParameterFromTau converts Kendall's tau in (-1, 1) to the family parameter:
//   - "gaussian": rho = sin(pi/2 * tau) in (-1, 1)
//   - "clayton":  theta = 2*tau / (1 - tau) for tau > 0 (0 when tau == 0; error if tau < 0)
//   - "gumbel":   theta = 1 / (1 - tau) for tau >= 0 (error if tau < 0)
//   - "frank":    theta solved from Debye D_1(theta) for tau in (-1, 1)
func CopulaParameterFromTau(family CopulaFamily, tau float64) (float64, error) {
	if !finite(tau) || tau < -1 || tau > 1 {
		return 0, errors.New("statistics: Kendall tau must be in [-1, 1]")
	}
	switch family {
	case CopulaGaussian:
		rho := math.Sin(0.5 * math.Pi * tau)
		return math.Max(-0.999999, math.Min(0.999999, rho)), nil
	case CopulaClayton:
		if tau < 0 {
			return 0, errors.New("statistics: Clayton copula requires tau >= 0")
		}
		if tau == 0 {
			return 0, nil
		}
		if tau >= 1-1e-6 {
			return maxCopulaParam, nil
		}
		return math.Min(maxCopulaParam, 2*tau/(1-tau)), nil
	case CopulaGumbel:
		if tau < 0 {
			return 0, errors.New("statistics: Gumbel copula requires tau >= 0")
		}
		if tau >= 1-1e-6 {
			return maxCopulaParam, nil
		}
		return math.Min(maxCopulaParam, 1/(1-tau)), nil
	case CopulaFrank:
		tClamped := math.Max(-0.999, math.Min(0.999, tau))
		return FrankThetaFromTau(tClamped)
	default:
		return 0, fmt.Errorf("statistics: copula family %q has no scalar tau parameter", family)
	}
}

func claytonCondSingle(theta, u, v float64) float64 {
	if theta <= 1e-12 {
		return u
	}
	// C_{1|2}(u|v) = v^(-theta-1) * (u^(-theta) + v^(-theta) - 1)^(-1/theta - 1).
	// Compute log(u^(-theta) + v^(-theta) - 1) without cancellation or overflow.
	a := -theta * math.Log(u)
	b := -theta * math.Log(v)
	var logS float64
	if a >= b {
		logS = a + math.Log1p(math.Exp(-a)*math.Expm1(b))
	} else {
		logS = b + math.Log1p(math.Exp(-b)*math.Expm1(a))
	}
	logP := -(theta+1)*math.Log(v) - (1.0/theta+1.0)*logS
	return clampCopulaProb(math.Exp(logP))
}

func gumbelCondSingle(theta, u, v float64) float64 {
	if theta <= 1+1e-12 {
		return u
	}
	x := -math.Log(u)
	y := -math.Log(v)
	m := math.Max(x, y)
	if m <= 0 {
		return u
	}
	sumPow := math.Pow(x/m, theta) + math.Pow(y/m, theta)
	bigA := m * math.Pow(sumPow, 1.0/theta)
	logP := -bigA + y + (theta-1.0)*math.Log(y/bigA)
	return clampCopulaProb(math.Exp(logP))
}

func frankCondPosLowerHalf(alpha, u, v float64) float64 {
	// Precondition: alpha > 0 and u + v <= 1.
	if alpha <= 1e-9 {
		return u
	}
	if alpha <= 100 {
		eu := math.Expm1(alpha * u)
		ev := math.Expm1(alpha * v)
		euv := math.Expm1(alpha * (u + v - 1.0)) // <= 0 since u + v <= 1
		den := eu + ev - euv
		if den <= 0 {
			return u
		}
		return clampCopulaProb(eu / den)
	}
	m := alpha * math.Max(u, v)
	em := math.Exp(-m)
	num := math.Exp(alpha*u-m) - em
	den := num + (math.Exp(alpha*v-m) - em) + (em - math.Exp(alpha*(u+v-1.0)-m))
	if den <= 0 {
		return u
	}
	return clampCopulaProb(num / den)
}

func frankCondSingle(theta, u, v float64) float64 {
	if math.Abs(theta) <= 1e-9 {
		return u
	}
	if theta < 0 {
		// Exact identity: C_{1|2}(u | v; -alpha) == C_{1|2}(u | 1 - v; alpha).
		return frankCondSingle(-theta, u, 1.0-v)
	}
	if u+v <= 1.0 {
		return frankCondPosLowerHalf(theta, u, v)
	}
	// Exact radial symmetry: C_{1|2}(u | v; theta) == 1 - C_{1|2}(1 - u | 1 - v; theta).
	return clampCopulaProb(1.0 - frankCondPosLowerHalf(theta, 1.0-u, 1.0-v))
}

// CopulaConditionalCDF returns exact analytical conditional probabilities:
//   - probUGivenV = P(U <= u | V = v) = dC(u, v)/dv
//   - probVGivenU = P(V <= v | U = u) = dC(u, v)/du
//
// strictly within (0, 1) for parametric copula families ("gaussian", "clayton", "gumbel", "frank").
func CopulaConditionalCDF(family CopulaFamily, param, u, v float64) (probUGivenV, probVGivenU float64, err error) {
	if !finite(param) || !finite(u) || !finite(v) || u <= 0 || u >= 1 || v <= 0 || v >= 1 {
		return 0, 0, errors.New("statistics: copula conditional CDF requires finite parameter and u, v in (0, 1)")
	}
	switch family {
	case CopulaGaussian:
		if param <= -1 || param >= 1 {
			return 0, 0, errors.New("statistics: Gaussian copula correlation rho must be in (-1, 1)")
		}
		zu, errU := NormInv(u)
		zv, errV := NormInv(v)
		if errU != nil || errV != nil {
			return 0, 0, errors.New("statistics: invalid Gaussian copula marginals")
		}
		scale := math.Sqrt(1.0 - param*param)
		p1 := clampCopulaProb(NormCDF((zu - param*zv) / scale))
		p2 := clampCopulaProb(NormCDF((zv - param*zu) / scale))
		return p1, p2, nil

	case CopulaClayton:
		if param < 0 {
			return 0, 0, errors.New("statistics: Clayton copula parameter theta must be >= 0")
		}
		return claytonCondSingle(param, u, v), claytonCondSingle(param, v, u), nil

	case CopulaGumbel:
		if param < 1 {
			return 0, 0, errors.New("statistics: Gumbel copula parameter theta must be >= 1")
		}
		return gumbelCondSingle(param, u, v), gumbelCondSingle(param, v, u), nil

	case CopulaFrank:
		return frankCondSingle(param, u, v), frankCondSingle(param, v, u), nil

	default:
		return 0, 0, fmt.Errorf("statistics: unsupported parametric copula family %q", family)
	}
}

// CopulaLogDensity evaluates the analytical log-density ln c(u, v; param) for AIC / log-likelihood selection.
func CopulaLogDensity(family CopulaFamily, param, u, v float64) (float64, error) {
	if !finite(param) || !finite(u) || !finite(v) || u <= 0 || u >= 1 || v <= 0 || v >= 1 {
		return 0, errors.New("statistics: copula log-density requires finite parameter and u, v in (0, 1)")
	}
	switch family {
	case CopulaGaussian:
		if param <= -1 || param >= 1 {
			return 0, errors.New("statistics: Gaussian copula correlation rho must be in (-1, 1)")
		}
		if math.Abs(param) <= 1e-12 {
			return 0, nil
		}
		zu, errU := NormInv(u)
		zv, errV := NormInv(v)
		if errU != nil || errV != nil {
			return 0, errors.New("statistics: invalid Gaussian copula marginals")
		}
		omr2 := 1.0 - param*param
		quad := (param*param*(zu*zu+zv*zv) - 2*param*zu*zv) / (2.0 * omr2)
		return -0.5*math.Log(omr2) - quad, nil

	case CopulaClayton:
		if param < 0 {
			return 0, errors.New("statistics: Clayton copula parameter theta must be >= 0")
		}
		if param <= 1e-12 {
			return 0, nil
		}
		a := -param * math.Log(u)
		b := -param * math.Log(v)
		var logS float64
		if a >= b {
			logS = a + math.Log1p(math.Exp(-a)*math.Expm1(b))
		} else {
			logS = b + math.Log1p(math.Exp(-b)*math.Expm1(a))
		}
		return math.Log1p(param) - (1.0+param)*(math.Log(u)+math.Log(v)) - (1.0/param+2.0)*logS, nil

	case CopulaGumbel:
		if param < 1 {
			return 0, errors.New("statistics: Gumbel copula parameter theta must be >= 1")
		}
		if param <= 1+1e-12 {
			return 0, nil
		}
		x := -math.Log(u)
		y := -math.Log(v)
		m := math.Max(x, y)
		bigA := m * math.Pow(math.Pow(x/m, param)+math.Pow(y/m, param), 1.0/param)
		return -bigA + x + y + (param-1.0)*(math.Log(x)+math.Log(y)) + (1.0-2.0*param)*math.Log(bigA) + math.Log(bigA+param-1.0), nil

	case CopulaFrank:
		if math.Abs(param) <= 1e-9 {
			return 0, nil
		}
		alpha := param
		uu, vv := u, v
		if alpha < 0 {
			alpha = -alpha
			vv = 1.0 - vv
		}
		if uu+vv > 1.0 {
			uu, vv = 1.0-uu, 1.0-vv
		}
		var logExpm1Alpha, logDen float64
		if alpha <= 50 {
			logExpm1Alpha = math.Log(math.Expm1(alpha))
		} else {
			logExpm1Alpha = alpha + math.Log1p(-math.Exp(-alpha))
		}
		if alpha <= 100 {
			den := math.Expm1(alpha*uu) + math.Expm1(alpha*vv) - math.Expm1(alpha*(uu+vv-1.0))
			if den <= 0 {
				return 0, errors.New("statistics: Frank copula density underflow")
			}
			logDen = math.Log(den)
		} else {
			m := alpha * math.Max(uu, vv)
			em := math.Exp(-m)
			denScaled := (math.Exp(alpha*uu-m) - em) + (math.Exp(alpha*vv-m) - em) + (em - math.Exp(alpha*(uu+vv-1.0)-m))
			if denScaled <= 0 {
				return 0, errors.New("statistics: Frank copula density underflow")
			}
			logDen = m + math.Log(denScaled)
		}
		return math.Log(alpha) + alpha*(uu+vv-1.0) + logExpm1Alpha - 2.0*logDen, nil

	default:
		return 0, fmt.Errorf("statistics: unsupported parametric copula family %q", family)
	}
}

func empiricalCondSingle(uSample, vSample []float64, u, v, h float64) float64 {
	n := len(uSample)
	inv2h2 := 1.0 / (2.0 * h * h)
	maxLogW := math.Inf(-1)
	for i := 0; i < n; i++ {
		dv := v - vSample[i]
		lw := -dv * dv * inv2h2
		if lw > maxLogW {
			maxLogW = lw
		}
	}
	var num, den float64
	for i := 0; i < n; i++ {
		dv := v - vSample[i]
		w := math.Exp(-dv*dv*inv2h2 - maxLogW)
		num += w * NormCDF((u-uSample[i])/h)
		den += w
	}
	if den <= 0 {
		return u
	}
	return clampCopulaProb(num / den)
}

// EmpiricalCopulaConditionalCDF evaluates smooth Nadaraya-Watson Gaussian kernel conditional
// probabilities P(U <= u | V = v) and P(V <= v | U = u) strictly in (0, 1) from empirical
// uniform pairs (uSample, vSample). It is strictly monotonic in (u, v) and radially symmetric.
func EmpiricalCopulaConditionalCDF(uSample, vSample []float64, u, v float64) (probUGivenV, probVGivenU float64, err error) {
	n := len(uSample)
	if n < 2 || len(vSample) != n || !finite(u) || !finite(v) || u <= 0 || u >= 1 || v <= 0 || v >= 1 {
		return 0, 0, errors.New("statistics: empirical copula requires >= 2 uniform pairs and u, v in (0, 1)")
	}
	for i := 0; i < n; i++ {
		if !finite(uSample[i]) || !finite(vSample[i]) || uSample[i] <= 0 || uSample[i] >= 1 || vSample[i] <= 0 || vSample[i] >= 1 {
			return 0, 0, errors.New("statistics: empirical copula sample uniforms must lie in (0, 1)")
		}
	}
	// Silverman bandwidth on Uniform(0, 1) (std = 1/sqrt(12) ~ 0.2887): h = 1.06 * std * N^(-1/5).
	h := 0.306 * math.Pow(float64(n), -0.2)
	h = math.Max(0.05, math.Min(0.25, h))
	return empiricalCondSingle(uSample, vSample, u, v, h), empiricalCondSingle(vSample, uSample, v, u, h), nil
}

// CopulaProbitScore converts (P(U <= u | V = v), P(V <= v | U = u)) into a conservative
// signed probit mispricing score Z_cop matching BandTrader conventions:
//   - negative when Y is undervalued relative to X (both pYX < 0.5 and pXY > 0.5)
//   - positive when Y is overvalued relative to X (both pYX > 0.5 and pXY < 0.5)
//   - 0 when the two conditional probabilities disagree on mispricing sign
//
// It satisfies exact swap antisymmetry Z_cop(X, Y) == -Z_cop(Y, X).
func CopulaProbitScore(probYGivenX, probXGivenY float64) float64 {
	z1, err1 := NormInv(clampCopulaProb(probYGivenX))
	z2, err2 := NormInv(clampCopulaProb(1.0 - probXGivenY))
	if err1 != nil || err2 != nil || !finite(z1) || !finite(z2) {
		return 0
	}
	switch {
	case z1 < 0 && z2 < 0:
		return math.Max(z1, z2)
	case z1 > 0 && z2 > 0:
		return math.Min(z1, z2)
	default:
		return 0
	}
}

// CopulaConfig configures rolling copula estimation and conditional probability thresholds.
type CopulaConfig struct {
	Family    CopulaFamily // ""/"auto" (default), "empirical", "gaussian", "clayton", "gumbel", "frank"
	LowerProb float64      // entry P(U <= u | V = v) upper bound for long spread in (0, 0.5), default 0.05
	UpperProb float64      // entry P(U <= u | V = v) lower bound for short spread in (0.5, 1), default 0.95
	ExitBand  float64      // half-band around 0.5 in [0, 0.5) for exit [0.5-ExitBand, 0.5+ExitBand], default 0
	StopProb  float64      // optional extreme tail stop probability in (0, LowerProb), 0 disables
}

// Normalize fills defaults and validates CopulaConfig.
func (c CopulaConfig) Normalize() (CopulaConfig, error) {
	fam, err := ValidateCopulaFamily(c.Family)
	if err != nil {
		return c, err
	}
	c.Family = fam
	if !finite(c.LowerProb) || !finite(c.UpperProb) || !finite(c.ExitBand) || !finite(c.StopProb) {
		return c, errors.New("statistics: copula probabilities must be finite")
	}
	switch {
	case c.LowerProb == 0 && c.UpperProb == 0:
		c.LowerProb = defaultCopulaLowerProb
		c.UpperProb = defaultCopulaUpperProb
	case c.LowerProb > 0 && c.UpperProb == 0:
		if c.LowerProb >= 0.5 {
			return c, errors.New("statistics: copula lower probability must be in (0, 0.5)")
		}
		c.UpperProb = 1.0 - c.LowerProb
	case c.UpperProb > 0 && c.LowerProb == 0:
		if c.UpperProb <= 0.5 || c.UpperProb >= 1 {
			return c, errors.New("statistics: copula upper probability must be in (0.5, 1)")
		}
		c.LowerProb = 1.0 - c.UpperProb
	}
	if c.LowerProb <= 0 || c.LowerProb >= 0.5 {
		return c, errors.New("statistics: copula lower probability must be in (0, 0.5)")
	}
	if c.UpperProb <= 0.5 || c.UpperProb >= 1 {
		return c, errors.New("statistics: copula upper probability must be in (0.5, 1)")
	}
	if c.ExitBand < 0 || c.ExitBand >= 0.5 {
		return c, errors.New("statistics: copula exit band must be in [0, 0.5)")
	}
	if c.LowerProb >= 0.5-c.ExitBand || c.UpperProb <= 0.5+c.ExitBand {
		return c, errors.New("statistics: copula entry probabilities must lie strictly outside [0.5-ExitBand, 0.5+ExitBand]")
	}
	minTail := math.Min(c.LowerProb, 1.0-c.UpperProb)
	if c.StopProb < 0 || (c.StopProb > 0 && c.StopProb >= minTail) {
		return c, errors.New("statistics: copula stop probability must be 0 or in (0, min(LowerProb, 1-UpperProb))")
	}
	return c, nil
}

// CopulaPoint reports the causal copula estimate at the latest closed observation (x_t, y_t).
type CopulaPoint struct {
	U           float64      // marginal mid-rank uniform of Y in (0, 1)
	V           float64      // marginal mid-rank uniform of X in (0, 1)
	Tau         float64      // rolling Kendall's tau in [-1, 1]
	Parameter   float64      // fitted copula parameter (rho for gaussian, theta for Archimedean, 0 for empirical)
	Family      CopulaFamily // active family (selected family when configured with "auto")
	ProbYGivenX float64      // P(U <= u_t | V = v_t) in (0, 1)
	ProbXGivenY float64      // P(V <= v_t | U = u_t) in (0, 1)
	Mispricing  float64      // symmetric conditional mispricing score 0.5*(ProbYGivenX - ProbXGivenY) in (-0.5, 0.5)
	Z           float64      // conservative signed probit mispricing score
}

// RollingCopula maintains a sliding window of aligned (x, y) observations and evaluates
// zero-lookahead marginal mid-ranks and copula conditional probabilities at the window tail.
type RollingCopula struct {
	n      int
	pos    int
	count  int
	family CopulaFamily
	rx     []float64
	ry     []float64
	wx     []float64
	wy     []float64
	uBuf   []float64
	vBuf   []float64
}

// NewRollingCopula creates a causal rolling copula estimator for window >= 3.
func NewRollingCopula(window int, family CopulaFamily) (*RollingCopula, error) {
	if window < 3 {
		return nil, errors.New("statistics: rolling copula window must be at least 3")
	}
	fam, err := ValidateCopulaFamily(family)
	if err != nil {
		return nil, err
	}
	return &RollingCopula{
		n:      window,
		family: fam,
		rx:     make([]float64, window),
		ry:     make([]float64, window),
		wx:     make([]float64, window),
		wy:     make([]float64, window),
		uBuf:   make([]float64, window),
		vBuf:   make([]float64, window),
	}, nil
}

// Window returns the configured rolling window length.
func (r *RollingCopula) Window() int { return r.n }

// Observe consumes one aligned pair (x = leg X, y = leg Y) at step t.
// Returns ok=false until the window is full, on non-finite input, or when either leg is constant.
func (r *RollingCopula) Observe(x, y float64) (CopulaPoint, bool) {
	if !finite(x) || !finite(y) {
		return CopulaPoint{}, false
	}
	if r.count < r.n {
		r.count++
	}
	r.rx[r.pos], r.ry[r.pos] = x, y
	r.pos = (r.pos + 1) % r.n
	if r.count < r.n {
		return CopulaPoint{}, false
	}
	for k := 0; k < r.n; k++ {
		idx := (r.pos + k) % r.n
		r.wx[k] = r.rx[idx]
		r.wy[k] = r.ry[idx]
	}
	var okU, okV bool
	r.uBuf, okU = RankUniforms(r.wy, r.uBuf)
	r.vBuf, okV = RankUniforms(r.wx, r.vBuf)
	if !okU || !okV {
		return CopulaPoint{}, false
	}
	tau, err := KendallTau(r.wx, r.wy)
	if err != nil {
		return CopulaPoint{}, false
	}
	uCur, vCur := r.uBuf[r.n-1], r.vBuf[r.n-1]

	activeFam := r.family
	var param, pYX, pXY float64

	switch r.family {
	case CopulaEmpirical:
		pYX, pXY, err = EmpiricalCopulaConditionalCDF(r.uBuf[:r.n-1], r.vBuf[:r.n-1], uCur, vCur)
		if err != nil {
			return CopulaPoint{}, false
		}

	case CopulaGaussian:
		param, err = CopulaParameterFromTau(CopulaGaussian, tau)
		if err != nil {
			return CopulaPoint{}, false
		}
		pYX, pXY, err = CopulaConditionalCDF(CopulaGaussian, param, uCur, vCur)
		if err != nil {
			return CopulaPoint{}, false
		}

	case CopulaClayton:
		if tau <= 0 {
			param = 0
		} else {
			param, err = CopulaParameterFromTau(CopulaClayton, tau)
			if err != nil {
				return CopulaPoint{}, false
			}
		}
		pYX, pXY, err = CopulaConditionalCDF(CopulaClayton, param, uCur, vCur)
		if err != nil {
			return CopulaPoint{}, false
		}

	case CopulaGumbel:
		if tau <= 0 {
			param = 1
		} else {
			param, err = CopulaParameterFromTau(CopulaGumbel, tau)
			if err != nil {
				return CopulaPoint{}, false
			}
		}
		pYX, pXY, err = CopulaConditionalCDF(CopulaGumbel, param, uCur, vCur)
		if err != nil {
			return CopulaPoint{}, false
		}

	case CopulaFrank:
		param, err = CopulaParameterFromTau(CopulaFrank, tau)
		if err != nil {
			return CopulaPoint{}, false
		}
		pYX, pXY, err = CopulaConditionalCDF(CopulaFrank, param, uCur, vCur)
		if err != nil {
			return CopulaPoint{}, false
		}

	case CopulaAuto:
		activeFam, param = selectBestCopulaFamily(r.uBuf, r.vBuf, tau)
		pYX, pXY, err = CopulaConditionalCDF(activeFam, param, uCur, vCur)
		if err != nil {
			return CopulaPoint{}, false
		}
	}

	if !finite(pYX) || !finite(pXY) {
		return CopulaPoint{}, false
	}
	z := CopulaProbitScore(pYX, pXY)
	return CopulaPoint{
		U:           uCur,
		V:           vCur,
		Tau:         tau,
		Parameter:   param,
		Family:      activeFam,
		ProbYGivenX: pYX,
		ProbXGivenY: pXY,
		Mispricing:  0.5 * (pYX - pXY),
		Z:           z,
	}, true
}

// selectBestCopulaFamily chooses the 1-parameter copula maximizing window log-likelihood
// (equivalent to minimizing AIC = 2k - 2*logL since all candidates have k=1 parameter),
// falling back cleanly to Gaussian/Frank when tau <= 0.
func selectBestCopulaFamily(uVals, vVals []float64, tau float64) (CopulaFamily, float64) {
	candidates := []CopulaFamily{CopulaGaussian, CopulaFrank}
	if tau > 1e-6 {
		candidates = append(candidates, CopulaClayton, CopulaGumbel)
	}
	bestFam := CopulaGaussian
	bestParam, _ := CopulaParameterFromTau(CopulaGaussian, tau)
	bestLL := math.Inf(-1)

	for _, fam := range candidates {
		theta, err := CopulaParameterFromTau(fam, tau)
		if err != nil {
			continue
		}
		var ll float64
		ok := true
		for i := range uVals {
			ld, err := CopulaLogDensity(fam, theta, uVals[i], vVals[i])
			if err != nil || !finite(ld) {
				ok = false
				break
			}
			ll += ld
		}
		if ok && ll > bestLL+1e-12 {
			bestLL = ll
			bestFam = fam
			bestParam = theta
		}
	}
	return bestFam, bestParam
}

// CopulaTrader is a deterministic conditional-probability hysteresis state machine
// for copula pair trading. Each instance serves one pair.
type CopulaTrader struct {
	copula     CopulaConfig
	band       BandConfig
	stopProb   float64
	pos        int
	held       int
	blocked    bool
	blockedDir int
	lastReason string
}

// NewCopulaTrader validates CopulaConfig and directional/holding constraints from BandConfig.
func NewCopulaTrader(copulaCfg CopulaConfig, bandCfg BandConfig) (*CopulaTrader, error) {
	var err error
	copulaCfg, err = copulaCfg.Normalize()
	if err != nil {
		return nil, err
	}
	switch bandCfg.Mode {
	case "", BandModeReversion, BandModeMomentum:
	default:
		return nil, fmt.Errorf("statistics: unknown band mode %q", bandCfg.Mode)
	}
	if bandCfg.MaxHold < 0 {
		return nil, errors.New("statistics: band max hold must not be negative")
	}
	if bandCfg.LongOnly && bandCfg.ShortOnly {
		return nil, errors.New("statistics: long-only and short-only are exclusive")
	}
	if !finite(bandCfg.Stop) || bandCfg.Stop < 0 {
		return nil, errors.New("statistics: band stop must be finite and non-negative")
	}
	stopProb := copulaCfg.StopProb
	if stopProb == 0 && bandCfg.Stop > 0 {
		p := NormCDF(-bandCfg.Stop)
		minTail := math.Min(copulaCfg.LowerProb, 1.0-copulaCfg.UpperProb)
		if !bandCfg.isMomentum() && p >= minTail {
			return nil, errors.New("statistics: copula band stop must exceed entry tail threshold")
		}
		if bandCfg.isMomentum() && p >= 0.5-copulaCfg.ExitBand {
			return nil, errors.New("statistics: momentum copula band stop must exceed exit band")
		}
		stopProb = p
	}
	return &CopulaTrader{
		copula:   copulaCfg,
		band:     bandCfg,
		stopProb: stopProb,
	}, nil
}

// Position returns the current spread position: -1, 0, or +1.
func (t *CopulaTrader) Position() int { return t.pos }

// LastCloseReason returns the close reason ("exit", "stop", or "maxhold") of the latest flat transition.
func (t *CopulaTrader) LastCloseReason() string {
	if t.lastReason == "" {
		return "exit"
	}
	return t.lastReason
}

// Reset clears open position, hold counter, and re-entry block.
func (t *CopulaTrader) Reset() {
	t.pos, t.held, t.blocked, t.blockedDir, t.lastReason = 0, 0, false, 0, ""
}

func (t *CopulaTrader) clearsBlock(pYX, pXY float64) bool {
	eb := t.copula.ExitBand
	if math.Abs(pYX-0.5) <= eb && math.Abs(pXY-0.5) <= eb {
		return true
	}
	if t.blockedDir > 0 {
		if t.band.isMomentum() {
			return pYX <= 0.5+eb && pXY >= 0.5-eb
		}
		return pYX >= 0.5-eb && pXY <= 0.5+eb
	}
	if t.blockedDir < 0 {
		if t.band.isMomentum() {
			return pYX >= 0.5-eb && pXY <= 0.5+eb
		}
		return pYX <= 0.5+eb && pXY >= 0.5-eb
	}
	return false
}

// Preview evaluates what position Step(pYX, pXY) would return without mutating state.
func (t *CopulaTrader) Preview(pYX, pXY float64) int {
	cp := *t
	return cp.Step(pYX, pXY)
}

// Step consumes conditional probabilities (pYX = P(U <= u | V = v), pXY = P(V <= v | U = u))
// and returns the updated spread position in {-1, 0, +1}.
func (t *CopulaTrader) Step(pYX, pXY float64) int {
	if !finite(pYX) || !finite(pXY) || pYX <= 0 || pYX >= 1 || pXY <= 0 || pXY >= 1 {
		return t.pos
	}
	if t.blocked && t.clearsBlock(pYX, pXY) {
		t.blocked, t.blockedDir = false, 0
	}
	eb := t.copula.ExitBand
	if t.pos != 0 {
		prevDir := t.pos
		t.held++
		if t.band.isMomentum() {
			switch {
			case t.stopProb > 0 && ((prevDir > 0 && pYX <= t.stopProb && pXY >= 1.0-t.stopProb) || (prevDir < 0 && pYX >= 1.0-t.stopProb && pXY <= t.stopProb)):
				t.close(true, prevDir, "stop")
			case (prevDir > 0 && pYX <= 0.5+eb && pXY >= 0.5-eb) || (prevDir < 0 && pYX >= 0.5-eb && pXY <= 0.5+eb):
				t.close(false, 0, "exit")
			case t.band.MaxHold > 0 && t.held >= t.band.MaxHold:
				t.close(true, prevDir, "maxhold")
			}
			return t.pos
		}
		switch {
		case t.stopProb > 0 && ((prevDir > 0 && pYX <= t.stopProb && pXY >= 1.0-t.stopProb) || (prevDir < 0 && pYX >= 1.0-t.stopProb && pXY <= t.stopProb)):
			t.close(true, prevDir, "stop")
		case (prevDir > 0 && pYX >= 0.5-eb && pXY <= 0.5+eb) || (prevDir < 0 && pYX <= 0.5+eb && pXY >= 0.5-eb):
			t.close(false, 0, "exit")
		case t.band.MaxHold > 0 && t.held >= t.band.MaxHold:
			t.close(true, prevDir, "maxhold")
		}
		return t.pos
	}

	if t.blocked {
		return 0
	}
	if t.band.isMomentum() {
		switch {
		case pYX >= t.copula.UpperProb && pXY <= 1.0-t.copula.UpperProb && !t.band.ShortOnly:
			t.pos, t.held = 1, 0
		case pYX <= t.copula.LowerProb && pXY >= 1.0-t.copula.LowerProb && !t.band.LongOnly:
			t.pos, t.held = -1, 0
		}
		return t.pos
	}
	if t.stopProb > 0 {
		if (pYX <= t.stopProb && pXY >= 1.0-t.stopProb) || (pYX >= 1.0-t.stopProb && pXY <= t.stopProb) {
			return 0
		}
	}
	switch {
	case pYX <= t.copula.LowerProb && pXY >= 1.0-t.copula.LowerProb && !t.band.ShortOnly:
		t.pos, t.held = 1, 0
	case pYX >= t.copula.UpperProb && pXY <= 1.0-t.copula.UpperProb && !t.band.LongOnly:
		t.pos, t.held = -1, 0
	}
	return t.pos
}

func (t *CopulaTrader) close(block bool, dir int, reason string) {
	t.pos, t.held, t.blocked, t.blockedDir, t.lastReason = 0, 0, block, dir, reason
}
