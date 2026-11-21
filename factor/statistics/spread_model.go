package statistics

import (
	"errors"
	"fmt"
	"math"
)

const (
	SpreadModelLogOLS     = "log_ols"
	SpreadModelRawOLS     = "raw_ols"
	SpreadModelTLS        = "tls"
	SpreadModelKalman     = "kalman"
	SpreadModelRatio      = "ratio"
	SpreadModelReturnDiff = "return_diff"
	SpreadModelCopula     = "copula"

	defaultKalmanDelta = 1e-4
	defaultKalmanVe    = 1e-3
)

// SpreadModelConfig configures a two-leg online SpreadEstimator.
type SpreadModelConfig struct {
	Model       string       // ""/"log_ols" (default), "raw_ols", "tls", "kalman", "ratio", "return_diff", "copula"
	Window      int          // rolling window (or warmup bars for "kalman"), at least 3
	KalmanDelta float64      // state transition noise ratio in (0, 1) for "kalman"; 0 defaults to 1e-4
	KalmanVe    float64      // observation noise variance (> 0) for "kalman"; 0 defaults to 1e-3
	Copula      CopulaConfig // copula family and probability thresholds when Model == "copula"
}

// Normalize fills defaults and validates SpreadModelConfig.
func (c SpreadModelConfig) Normalize() (SpreadModelConfig, error) {
	switch c.Model {
	case "", SpreadModelLogOLS:
		c.Model = SpreadModelLogOLS
	case SpreadModelRawOLS, SpreadModelTLS, SpreadModelKalman, SpreadModelRatio, SpreadModelReturnDiff, SpreadModelCopula:
	default:
		return c, fmt.Errorf("statistics: unknown spread model %q", c.Model)
	}
	if c.Window < 3 {
		return c, errors.New("statistics: spread model window must be at least 3")
	}
	if !finite(c.KalmanDelta) {
		return c, errors.New("statistics: kalman delta must be finite")
	}
	if c.KalmanDelta == 0 {
		c.KalmanDelta = defaultKalmanDelta
	}
	if c.KalmanDelta <= 0 || c.KalmanDelta >= 1 {
		return c, errors.New("statistics: kalman delta must be in (0, 1)")
	}
	if !finite(c.KalmanVe) {
		return c, errors.New("statistics: kalman observation variance must be finite")
	}
	if c.KalmanVe == 0 {
		c.KalmanVe = defaultKalmanVe
	}
	if c.KalmanVe <= 0 {
		return c, errors.New("statistics: kalman observation variance must be positive")
	}
	if c.Model == SpreadModelCopula {
		copCfg, err := c.Copula.Normalize()
		if err != nil {
			return c, err
		}
		c.Copula = copCfg
	}
	return c, nil
}

// SpreadEstimate extends SpreadPoint with dollar-notional hedge, cross-pair scale,
// and copula conditional probability metadata.
type SpreadEstimate struct {
	SpreadPoint
	// NotionalBeta is the dollar-notional hedge ratio (-wX / wY) for portfolio construction:
	// - "log_ols", "tls", "kalman", "copula": Beta (log-price slope / elasticity)
	// - "raw_ols": Beta * (priceX / priceY), hedging 1 unit of Y with -Beta units of X
	// - "ratio", "return_diff": 1.0 (equal dollar-neutral legs)
	NotionalBeta float64
	// ScaleStd is the dimensionless spread volatility used for cross-pair inverse-vol sizing.
	ScaleStd float64
	// CopulaProbYGivenX is P(U <= u | V = v) in (0, 1) when Model == "copula" (NaN otherwise).
	CopulaProbYGivenX float64
	// CopulaProbXGivenY is P(V <= v | U = u) in (0, 1) when Model == "copula" (NaN otherwise).
	CopulaProbXGivenY float64
	// CopulaTau is rolling Kendall's tau in [-1, 1] when Model == "copula" (NaN otherwise).
	CopulaTau float64
	// CopulaFamily is the active copula family when Model == "copula".
	CopulaFamily string
}

// SpreadEstimator provides a unified causal online spread and hedge-ratio interface across
// log OLS, raw-price OLS, orthogonal TLS, Kalman state-space filtering, price ratio,
// normalized cumulative return difference, and bivariate copula conditional probability models.
type SpreadEstimator struct {
	cfg    SpreadModelConfig
	ols    *RollingSpread // used by "log_ols", "raw_ols", and "copula"
	copula *RollingCopula // used by "copula"
	n      int
	pos    int
	count  int
	xs     []float64
	ys     []float64
	ox     float64
	oy     float64
	start  bool

	// O(1) centered moments for "tls".
	sx, sy, sxx, sxy, syy float64

	// Kalman state-space filter on centered log prices.
	kalman KalmanPair

	// Latest fitted linear parameters in centered/model coordinates for WindowSeries & EvalFrozenZ.
	lastPoint SpreadPoint
	lastCInt  float64 // centered intercept for log/tls/kalman
	lastReady bool
}

// NewSpreadEstimator constructs an online SpreadEstimator for the given configuration.
func NewSpreadEstimator(cfg SpreadModelConfig) (*SpreadEstimator, error) {
	var err error
	cfg, err = cfg.Normalize()
	if err != nil {
		return nil, err
	}
	est := &SpreadEstimator{
		cfg: cfg,
		n:   cfg.Window,
	}
	switch cfg.Model {
	case SpreadModelLogOLS, SpreadModelRawOLS:
		rs, err := NewRollingSpread(cfg.Window)
		if err != nil {
			return nil, err
		}
		est.ols = rs
	case SpreadModelCopula:
		rs, err := NewRollingSpread(cfg.Window)
		if err != nil {
			return nil, err
		}
		rc, err := NewRollingCopula(cfg.Window, cfg.Copula.Family)
		if err != nil {
			return nil, err
		}
		est.ols = rs
		est.copula = rc
	case SpreadModelTLS, SpreadModelReturnDiff:
		est.xs = make([]float64, cfg.Window)
		est.ys = make([]float64, cfg.Window)
	case SpreadModelRatio:
		est.ys = make([]float64, cfg.Window)
	case SpreadModelKalman:
		est.xs = make([]float64, cfg.Window)
		est.ys = make([]float64, cfg.Window)
		est.kalman = KalmanPair{
			Covariance:          [2][2]float64{{10, 0}, {0, 10}},
			ProcessVariance:     cfg.KalmanDelta / (1 - cfg.KalmanDelta),
			ObservationVariance: cfg.KalmanVe,
		}
	}
	return est, nil
}

// Model returns the normalized spread model identifier.
func (e *SpreadEstimator) Model() string { return e.cfg.Model }

// Window returns the configured rolling/warmup window in bars.
func (e *SpreadEstimator) Window() int { return e.n }

func nonCopulaEstimate(p SpreadPoint, notionalBeta, scaleStd float64) SpreadEstimate {
	return SpreadEstimate{
		SpreadPoint:       p,
		NotionalBeta:      notionalBeta,
		ScaleStd:          scaleStd,
		CopulaProbYGivenX: math.NaN(),
		CopulaProbXGivenY: math.NaN(),
		CopulaTau:         math.NaN(),
	}
}

// ObserveLog consumes one aligned pair of log prices (lx = ln(X), ly = ln(Y)) at the closed decision bar.
func (e *SpreadEstimator) ObserveLog(lx, ly float64) (SpreadEstimate, bool) {
	if !finite(lx) || !finite(ly) {
		return SpreadEstimate{}, false
	}
	switch e.cfg.Model {
	case SpreadModelLogOLS:
		p, ok := e.ols.Observe(lx, ly)
		if !ok {
			e.lastReady = false
			return SpreadEstimate{}, false
		}
		e.lastPoint, e.lastReady = p, true
		return nonCopulaEstimate(p, p.Beta, p.Std), true

	case SpreadModelRawOLS:
		px, py := math.Exp(lx), math.Exp(ly)
		if !finite(px) || !finite(py) || px <= 0 || py <= 0 {
			return SpreadEstimate{}, false
		}
		p, ok := e.ols.Observe(px, py)
		if !ok {
			e.lastReady = false
			return SpreadEstimate{}, false
		}
		notionalBeta := p.Beta * (px / py)
		scaleStd := p.Std / py
		if !finite(notionalBeta) || !finite(scaleStd) || scaleStd <= 0 {
			e.lastReady = false
			return SpreadEstimate{}, false
		}
		e.lastPoint, e.lastReady = p, true
		return nonCopulaEstimate(p, notionalBeta, scaleStd), true

	case SpreadModelTLS:
		return e.observeTLS(lx, ly)

	case SpreadModelKalman:
		return e.observeKalman(lx, ly)

	case SpreadModelRatio:
		return e.observeRatio(lx, ly)

	case SpreadModelReturnDiff:
		return e.observeReturnDiff(lx, ly)

	case SpreadModelCopula:
		return e.observeCopula(lx, ly)
	}
	return SpreadEstimate{}, false
}

func (e *SpreadEstimator) observeCopula(lx, ly float64) (SpreadEstimate, bool) {
	olsP, olsOK := e.ols.Observe(lx, ly)
	cp, copOK := e.copula.Observe(lx, ly)
	if !copOK || !finite(olsP.Beta) {
		e.lastReady = false
		return SpreadEstimate{}, false
	}
	scaleStd := olsP.Std
	if !olsOK || !finite(scaleStd) || scaleStd <= 1e-12 {
		scaleStd = 1e-4
	}
	p := SpreadPoint{
		Alpha:    olsP.Alpha,
		Beta:     olsP.Beta,
		Residual: cp.Mispricing,
		Std:      scaleStd,
		Z:        cp.Z,
	}
	e.lastPoint, e.lastReady = p, true
	return SpreadEstimate{
		SpreadPoint:       p,
		NotionalBeta:      olsP.Beta,
		ScaleStd:          scaleStd,
		CopulaProbYGivenX: cp.ProbYGivenX,
		CopulaProbXGivenY: cp.ProbXGivenY,
		CopulaTau:         cp.Tau,
		CopulaFamily:      string(cp.Family),
	}, true
}

func (e *SpreadEstimator) observeTLS(lx, ly float64) (SpreadEstimate, bool) {
	if !e.start {
		e.ox, e.oy, e.start = lx, ly, true
	}
	cx, cy := lx-e.ox, ly-e.oy
	if e.count == e.n {
		oldX, oldY := e.xs[e.pos], e.ys[e.pos]
		e.sx -= oldX
		e.sy -= oldY
		e.sxx -= oldX * oldX
		e.sxy -= oldX * oldY
		e.syy -= oldY * oldY
	} else {
		e.count++
	}
	e.xs[e.pos], e.ys[e.pos] = cx, cy
	e.pos = (e.pos + 1) % e.n
	e.sx += cx
	e.sy += cy
	e.sxx += cx * cx
	e.sxy += cx * cy
	e.syy += cy * cy
	if e.count < e.n {
		e.lastReady = false
		return SpreadEstimate{}, false
	}
	nf := float64(e.n)
	mx, my := e.sx/nf, e.sy/nf
	vxx := e.sxx - nf*mx*mx
	vyy := e.syy - nf*my*my
	vxy := e.sxy - nf*mx*my
	if vxx <= 1e-12 || vyy <= 1e-12 || math.Abs(vxy) <= 1e-12 {
		e.lastReady = false
		return SpreadEstimate{}, false
	}
	diff := vyy - vxx
	disc := math.Hypot(diff, 2*vxy)
	var beta float64
	if diff >= 0 {
		beta = (diff + disc) / (2 * vxy)
	} else {
		beta = (2 * vxy) / (disc - diff)
	}
	if !finite(beta) {
		e.lastReady = false
		return SpreadEstimate{}, false
	}
	cAlpha := my - beta*mx
	var ssr float64
	for i := 0; i < e.n; i++ {
		r := e.ys[i] - cAlpha - beta*e.xs[i]
		ssr += r * r
	}
	std := math.Sqrt(math.Max(0, ssr) / (nf - 2))
	if std <= 1e-12 || !finite(std) {
		e.lastReady = false
		return SpreadEstimate{}, false
	}
	alpha := cAlpha + e.oy - beta*e.ox
	resid := cy - cAlpha - beta*cx
	z := resid / std
	scaleStd := std / math.Hypot(1, beta)
	if !finite(alpha) || !finite(resid) || !finite(z) || !finite(scaleStd) || scaleStd <= 0 {
		e.lastReady = false
		return SpreadEstimate{}, false
	}
	p := SpreadPoint{Alpha: alpha, Beta: beta, Residual: resid, Std: std, Z: z}
	e.lastPoint, e.lastCInt, e.lastReady = p, cAlpha, true
	return nonCopulaEstimate(p, beta, scaleStd), true
}

func (e *SpreadEstimator) observeKalman(lx, ly float64) (SpreadEstimate, bool) {
	if !e.start {
		e.ox, e.oy, e.start = lx, ly, true
	}
	cx, cy := lx-e.ox, ly-e.oy
	obs, err := e.kalman.Observe(cx, cy)
	if err != nil {
		e.lastReady = false
		return SpreadEstimate{}, false
	}
	if e.count < e.n {
		e.count++
	}
	e.xs[e.pos], e.ys[e.pos] = cx, cy
	e.pos = (e.pos + 1) % e.n
	if e.count < e.n {
		e.lastReady = false
		return SpreadEstimate{}, false
	}
	invStd := math.Sqrt(obs.Variance)
	if invStd <= 1e-12 || !finite(invStd) {
		e.lastReady = false
		return SpreadEstimate{}, false
	}
	z := obs.Error / invStd
	slope := obs.PosteriorBeta[0]
	cInt := obs.PosteriorBeta[1]
	alpha := cInt + e.oy - slope*e.ox
	if !finite(z) || !finite(slope) || !finite(alpha) {
		e.lastReady = false
		return SpreadEstimate{}, false
	}
	p := SpreadPoint{Alpha: alpha, Beta: slope, Residual: obs.Error, Std: invStd, Z: z}
	e.lastPoint, e.lastCInt, e.lastReady = p, cInt, true
	return nonCopulaEstimate(p, slope, invStd), true
}

func (e *SpreadEstimator) observeRatio(lx, ly float64) (SpreadEstimate, bool) {
	ratio := math.Exp(ly - lx)
	if !finite(ratio) || ratio <= 0 {
		return SpreadEstimate{}, false
	}
	if e.count < e.n {
		e.count++
	}
	e.ys[e.pos] = ratio
	e.pos = (e.pos + 1) % e.n
	if e.count < e.n {
		e.lastReady = false
		return SpreadEstimate{}, false
	}
	mean, std, ok := ringMeanStd(e.ys, e.pos, e.n)
	if !ok || std <= 1e-12 {
		e.lastReady = false
		return SpreadEstimate{}, false
	}
	resid := ratio - mean
	z := resid / std
	scaleStd := std / ratio
	if !finite(z) || !finite(scaleStd) || scaleStd <= 0 {
		e.lastReady = false
		return SpreadEstimate{}, false
	}
	p := SpreadPoint{Alpha: mean, Beta: 1.0, Residual: resid, Std: std, Z: z}
	e.lastPoint, e.lastReady = p, true
	return nonCopulaEstimate(p, 1.0, scaleStd), true
}

func (e *SpreadEstimator) observeReturnDiff(lx, ly float64) (SpreadEstimate, bool) {
	if e.count < e.n {
		e.count++
	}
	e.xs[e.pos], e.ys[e.pos] = lx, ly
	e.pos = (e.pos + 1) % e.n
	if e.count < e.n {
		e.lastReady = false
		return SpreadEstimate{}, false
	}
	lx0, ly0 := e.xs[e.pos], e.ys[e.pos]
	var mean, m2, last float64
	for k := 0; k < e.n; k++ {
		idx := (e.pos + k) % e.n
		d := math.Exp(e.ys[idx]-ly0) - math.Exp(e.xs[idx]-lx0)
		if !finite(d) {
			e.lastReady = false
			return SpreadEstimate{}, false
		}
		delta := d - mean
		mean += delta / float64(k+1)
		m2 += delta * (d - mean)
		last = d
	}
	std := math.Sqrt(math.Max(0, m2) / float64(e.n-1))
	if std <= 1e-12 || !finite(mean) || !finite(std) {
		e.lastReady = false
		return SpreadEstimate{}, false
	}
	resid := last - mean
	z := resid / std
	if !finite(z) {
		e.lastReady = false
		return SpreadEstimate{}, false
	}
	p := SpreadPoint{Alpha: mean, Beta: 1.0, Residual: resid, Std: std, Z: z}
	e.lastPoint, e.lastReady = p, true
	return nonCopulaEstimate(p, 1.0, std), true
}

// EvalFrozenZ evaluates (lx, ly) against the current window using an entry-frozen model slope
// frozenModelBeta (log slope for log_ols/tls, raw share slope for raw_ols). Models whose z-score
// is defined on innovation variance ("kalman"), a fixed 1:1 dollar spread ("ratio", "return_diff"),
// or copula conditional quantiles ("copula") return their latest model z-score directly.
func (e *SpreadEstimator) EvalFrozenZ(lx, ly, frozenModelBeta float64) (float64, bool) {
	if !finite(lx) || !finite(ly) || !finite(frozenModelBeta) {
		return 0, false
	}
	switch e.cfg.Model {
	case SpreadModelLogOLS:
		return e.ols.EvalWithBeta(lx, ly, frozenModelBeta)
	case SpreadModelRawOLS:
		px, py := math.Exp(lx), math.Exp(ly)
		if !finite(px) || !finite(py) || px <= 0 || py <= 0 {
			return 0, false
		}
		return e.ols.EvalWithBeta(px, py, frozenModelBeta)
	case SpreadModelTLS:
		if e.count < e.n {
			return 0, false
		}
		var mean, m2 float64
		for i := 0; i < e.n; i++ {
			v := e.ys[i] - frozenModelBeta*e.xs[i]
			if !finite(v) {
				return 0, false
			}
			delta := v - mean
			mean += delta / float64(i+1)
			m2 += delta * (v - mean)
		}
		std := math.Sqrt(math.Max(0, m2) / float64(e.n-1))
		if std <= 1e-12 || !finite(std) {
			return 0, false
		}
		resid := (ly - e.oy) - frozenModelBeta*(lx - e.ox) - mean
		z := resid / std
		if !finite(z) {
			return 0, false
		}
		return z, true
	case SpreadModelKalman, SpreadModelRatio, SpreadModelReturnDiff, SpreadModelCopula:
		if !e.lastReady {
			return 0, false
		}
		return e.lastPoint.Z, true
	}
	return 0, false
}

// WindowSeries appends the chronological window spread series (length Window) to dst for
// online stationarity / FitOU half-life, Hurst, and Wilder RSI checks. It does not mutate the estimator.
func (e *SpreadEstimator) WindowSeries(dst []float64) ([]float64, bool) {
	if !e.lastReady {
		return dst, false
	}
	switch e.cfg.Model {
	case SpreadModelLogOLS, SpreadModelRawOLS, SpreadModelCopula:
		return e.ols.windowResiduals(dst, e.lastPoint.Beta)
	case SpreadModelTLS, SpreadModelKalman:
		if e.count < e.n {
			return dst, false
		}
		slope, cInt := e.lastPoint.Beta, e.lastCInt
		for k := 0; k < e.n; k++ {
			idx := (e.pos + k) % e.n
			v := e.ys[idx] - cInt - slope*e.xs[idx]
			if !finite(v) {
				return dst, false
			}
			dst = append(dst, v)
		}
		return dst, true
	case SpreadModelRatio:
		if e.count < e.n {
			return dst, false
		}
		for k := 0; k < e.n; k++ {
			v := e.ys[(e.pos+k)%e.n]
			if !finite(v) {
				return dst, false
			}
			dst = append(dst, v)
		}
		return dst, true
	case SpreadModelReturnDiff:
		if e.count < e.n {
			return dst, false
		}
		lx0, ly0 := e.xs[e.pos], e.ys[e.pos]
		for k := 0; k < e.n; k++ {
			idx := (e.pos + k) % e.n
			d := math.Exp(e.ys[idx]-ly0) - math.Exp(e.xs[idx]-lx0)
			if !finite(d) {
				return dst, false
			}
			dst = append(dst, d)
		}
		return dst, true
	}
	return dst, false
}

func (r *RollingSpread) windowResiduals(dst []float64, beta float64) ([]float64, bool) {
	if r.count < r.n || !finite(beta) {
		return dst, false
	}
	nf := float64(r.n)
	cAlpha := r.sy/nf - beta*(r.sx/nf)
	for k := 0; k < r.n; k++ {
		idx := (r.pos + k) % r.n
		v := r.ys[idx] - cAlpha - beta*r.xs[idx]
		if !finite(v) {
			return dst, false
		}
		dst = append(dst, v)
	}
	return dst, true
}

func ringMeanStd(buf []float64, start, n int) (mean, std float64, ok bool) {
	if n < 2 || len(buf) < n {
		return 0, 0, false
	}
	var m2 float64
	for k := 0; k < n; k++ {
		v := buf[(start+k)%n]
		if !finite(v) {
			return 0, 0, false
		}
		delta := v - mean
		mean += delta / float64(k+1)
		m2 += delta * (v - mean)
	}
	std = math.Sqrt(math.Max(0, m2) / float64(n-1))
	if !finite(mean) || !finite(std) {
		return 0, 0, false
	}
	return mean, std, true
}

// HurstExponent estimates the generalized variance-scaling Hurst exponent H in [0, 1]
// of a spread or price series (len >= 8) by regressing 0.5 * ln(E[(x_{t+lag} - x_t)^2])
// on ln(lag) across lags 1..min(20, max(2, len/4)).
// Mean-reverting stationary spreads yield H < 0.5, random walks yield H ~ 0.5,
// and trending/persistent series yield H > 0.5.
func HurstExponent(values []float64) (float64, error) {
	n := len(values)
	if n < 8 {
		return 0, errors.New("statistics: Hurst exponent requires at least 8 observations")
	}
	for _, v := range values {
		if !finite(v) {
			return 0, errors.New("statistics: non-finite sample in Hurst exponent")
		}
	}
	maxLag := min(20, max(2, n/4))
	var k int
	var sumX, sumY, sxx, sxy float64
	for lag := 1; lag <= maxLag; lag++ {
		cnt := n - lag
		var sumSq float64
		for i := 0; i < cnt; i++ {
			d := values[i+lag] - values[i]
			sumSq += d * d
		}
		vLag := sumSq / float64(cnt)
		if vLag <= 1e-24 || !finite(vLag) {
			continue
		}
		lx := math.Log(float64(lag))
		ly := 0.5 * math.Log(vLag)
		k++
		dx := lx - sumX
		sumX += dx / float64(k)
		dy := ly - sumY
		sumY += dy / float64(k)
		sxx += dx * (lx - sumX)
		sxy += dx * (ly - sumY)
	}
	if k < 2 || sxx <= 1e-18 {
		return 0, errors.New("statistics: constant or degenerate sample in Hurst exponent")
	}
	h := sxy / sxx
	if !finite(h) {
		return 0, errors.New("statistics: non-finite Hurst exponent")
	}
	if h < 0 {
		return 0, nil
	}
	if h > 1 {
		return 1, nil
	}
	return h, nil
}

// WilderRSI computes Wilder's Relative Strength Index in [0, 100] over values (len >= period+1, period >= 2).
func WilderRSI(values []float64, period int) (float64, error) {
	if period < 2 || len(values) < period+1 {
		return 0, errors.New("statistics: Wilder RSI requires period >= 2 and at least period+1 observations")
	}
	for _, v := range values {
		if !finite(v) {
			return 0, errors.New("statistics: non-finite sample in Wilder RSI")
		}
	}
	var sumGain, sumLoss float64
	for i := 1; i <= period; i++ {
		d := values[i] - values[i-1]
		if d > 0 {
			sumGain += d
		} else if d < 0 {
			sumLoss -= d
		}
	}
	p := float64(period)
	avgGain := sumGain / p
	avgLoss := sumLoss / p
	for i := period + 1; i < len(values); i++ {
		d := values[i] - values[i-1]
		gain, loss := 0.0, 0.0
		if d > 0 {
			gain = d
		} else if d < 0 {
			loss = -d
		}
		avgGain = (avgGain*(p-1) + gain) / p
		avgLoss = (avgLoss*(p-1) + loss) / p
	}
	if avgGain == 0 && avgLoss == 0 {
		return 50.0, nil
	}
	if avgLoss == 0 {
		return 100.0, nil
	}
	rs := avgGain / avgLoss
	rsi := 100.0 - 100.0/(1.0+rs)
	if !finite(rsi) {
		return 0, errors.New("statistics: non-finite Wilder RSI")
	}
	return math.Max(0, math.Min(100, rsi)), nil
}

// RollingReturnCorr tracks Pearson correlation of 1-bar log returns over a window of
// windowBars >= 4 price observations (windowBars-1 consecutive 1-bar returns).
type RollingReturnCorr struct {
	cap             int
	pos             int
	count           int
	dx              []float64
	dy              []float64
	prevLX, prevLY  float64
	hasPrev         bool
	lastCorr        float64
	lastReady       bool
}

// NewRollingReturnCorr constructs a 1-bar log-return correlation tracker over windowBars >= 4 observations.
func NewRollingReturnCorr(windowBars int) (*RollingReturnCorr, error) {
	if windowBars < 4 {
		return nil, errors.New("statistics: rolling return correlation window must be at least 4 bars")
	}
	c := windowBars - 1
	return &RollingReturnCorr{
		cap: c,
		dx:  make([]float64, c),
		dy:  make([]float64, c),
	}, nil
}

// ResetPrev clears the previous log-price anchor when a bar is missing so gap jumps
// across missing periods are not recorded as 1-bar returns.
func (r *RollingReturnCorr) ResetPrev() {
	if r != nil {
		r.hasPrev = false
		r.lastReady = false
	}
}

// Last returns the latest computed Pearson correlation and true when the return window is full.
func (r *RollingReturnCorr) Last() (float64, bool) {
	if r == nil || !r.lastReady {
		return math.NaN(), false
	}
	return r.lastCorr, true
}

// ObserveLog consumes aligned log prices (lx = ln(X), ly = ln(Y)) at the closed decision bar.
func (r *RollingReturnCorr) ObserveLog(lx, ly float64) (float64, bool) {
	if r == nil || !finite(lx) || !finite(ly) {
		return math.NaN(), false
	}
	if !r.hasPrev {
		r.prevLX, r.prevLY, r.hasPrev = lx, ly, true
		r.lastReady = false
		return math.NaN(), false
	}
	rx := lx - r.prevLX
	ry := ly - r.prevLY
	r.prevLX, r.prevLY = lx, ly
	if r.count < r.cap {
		r.count++
	}
	r.dx[r.pos], r.dy[r.pos] = rx, ry
	r.pos = (r.pos + 1) % r.cap
	if r.count < r.cap {
		r.lastReady = false
		return math.NaN(), false
	}
	var mx, my, sxx, syy, sxy float64
	for k := 0; k < r.cap; k++ {
		vx, vy := r.dx[k], r.dy[k]
		ddx := vx - mx
		mx += ddx / float64(k+1)
		ddy := vy - my
		my += ddy / float64(k+1)
		sxx += ddx * (vx - mx)
		syy += ddy * (vy - my)
		sxy += ddx * (vy - my)
	}
	if sxx <= 1e-24 || syy <= 1e-24 {
		r.lastReady = false
		return math.NaN(), false
	}
	corr := sxy / math.Sqrt(sxx*syy)
	if !finite(corr) {
		r.lastReady = false
		return math.NaN(), false
	}
	corr = math.Max(-1, math.Min(1, corr))
	r.lastCorr, r.lastReady = corr, true
	return corr, true
}
