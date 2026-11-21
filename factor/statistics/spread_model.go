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

	defaultKalmanDelta = 1e-4
	defaultKalmanVe    = 1e-3
)

// SpreadModelConfig configures a two-leg online SpreadEstimator.
type SpreadModelConfig struct {
	Model       string  // ""/"log_ols" (default), "raw_ols", "tls", "kalman", "ratio", "return_diff"
	Window      int     // rolling window (or warmup bars for "kalman"), at least 3
	KalmanDelta float64 // state transition noise ratio in (0, 1) for "kalman"; 0 defaults to 1e-4
	KalmanVe    float64 // observation noise variance (> 0) for "kalman"; 0 defaults to 1e-3
}

// Normalize fills defaults and validates SpreadModelConfig.
func (c SpreadModelConfig) Normalize() (SpreadModelConfig, error) {
	switch c.Model {
	case "", SpreadModelLogOLS:
		c.Model = SpreadModelLogOLS
	case SpreadModelRawOLS, SpreadModelTLS, SpreadModelKalman, SpreadModelRatio, SpreadModelReturnDiff:
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
	return c, nil
}

// SpreadEstimate extends SpreadPoint with dollar-notional hedge and cross-pair scale metadata.
type SpreadEstimate struct {
	SpreadPoint
	// NotionalBeta is the dollar-notional hedge ratio (-wX / wY) for portfolio construction:
	// - "log_ols", "tls", "kalman": Beta (log-price slope / elasticity)
	// - "raw_ols": Beta * (priceX / priceY), hedging 1 unit of Y with -Beta units of X
	// - "ratio", "return_diff": 1.0 (equal dollar-neutral legs)
	NotionalBeta float64
	// ScaleStd is the dimensionless spread volatility used for cross-pair inverse-vol sizing.
	ScaleStd float64
}

// SpreadEstimator provides a unified causal online spread and hedge-ratio interface across
// log OLS, raw-price OLS, orthogonal TLS, Kalman state-space filtering, price ratio, and
// normalized cumulative return difference models.
type SpreadEstimator struct {
	cfg   SpreadModelConfig
	ols   *RollingSpread // used by "log_ols" and "raw_ols"
	n     int
	pos   int
	count int
	xs    []float64
	ys    []float64
	ox    float64
	oy    float64
	start bool

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
		return SpreadEstimate{SpreadPoint: p, NotionalBeta: p.Beta, ScaleStd: p.Std}, true

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
		return SpreadEstimate{SpreadPoint: p, NotionalBeta: notionalBeta, ScaleStd: scaleStd}, true

	case SpreadModelTLS:
		return e.observeTLS(lx, ly)

	case SpreadModelKalman:
		return e.observeKalman(lx, ly)

	case SpreadModelRatio:
		return e.observeRatio(lx, ly)

	case SpreadModelReturnDiff:
		return e.observeReturnDiff(lx, ly)
	}
	return SpreadEstimate{}, false
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
	return SpreadEstimate{SpreadPoint: p, NotionalBeta: beta, ScaleStd: scaleStd}, true
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
	return SpreadEstimate{SpreadPoint: p, NotionalBeta: slope, ScaleStd: invStd}, true
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
	return SpreadEstimate{SpreadPoint: p, NotionalBeta: 1.0, ScaleStd: scaleStd}, true
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
	return SpreadEstimate{SpreadPoint: p, NotionalBeta: 1.0, ScaleStd: std}, true
}

// EvalFrozenZ evaluates (lx, ly) against the current window using an entry-frozen model slope
// frozenModelBeta (log slope for log_ols/tls, raw share slope for raw_ols). Models whose z-score
// is defined on innovation variance ("kalman") or a fixed 1:1 dollar spread ("ratio", "return_diff")
// return their latest model z-score directly so entry and exit share the exact same scale.
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
	case SpreadModelKalman, SpreadModelRatio, SpreadModelReturnDiff:
		if !e.lastReady {
			return 0, false
		}
		return e.lastPoint.Z, true
	}
	return 0, false
}

// WindowSeries appends the chronological window spread series (length Window) to dst for
// online stationarity / FitOU half-life checks. It does not mutate the estimator.
func (e *SpreadEstimator) WindowSeries(dst []float64) ([]float64, bool) {
	if !e.lastReady {
		return dst, false
	}
	switch e.cfg.Model {
	case SpreadModelLogOLS, SpreadModelRawOLS:
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
