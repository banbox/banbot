package statistics

import (
	"errors"
	"fmt"
	"math"
)

const (
	BandModeReversion = "reversion"
	BandModeMomentum  = "momentum"
)

// BandConfig describes a z-score hysteresis rule shared by every engine.
//
// Mode selects directional semantics:
//   - "" or "reversion" (default): +1 is long the spread (z <= -Entry), -1 is short the
//     spread (z >= Entry). Exit closes when |z| <= Exit. Stop (optional, > Entry) closes
//     when |z| >= Stop and blocks re-entry until |z| falls back inside Exit, so a diverging
//     spread is never re-bought on the next bar.
//   - "momentum": breakout/relative-strength mode where z >= Entry goes long the spread (+1)
//     and z <= -Entry goes short the spread (-1). Positions take profit when reverting back
//     to z <= Exit (long) or z >= -Exit (short), and if Stop > 0 (> Exit) stop out with
//     re-entry block on reverse breakdown z <= -Stop (long) or z >= Stop (short).
//
// MaxHold (optional) closes after that many bars with the same re-entry block rule.
type BandConfig struct {
	Entry, Exit, Stop float64
	MaxHold           int
	LongOnly          bool   // never short the spread (-1)
	ShortOnly         bool   // never long the spread (+1)
	Mode              string // ""/"reversion" (default) or "momentum"
}

func (c BandConfig) isMomentum() bool { return c.Mode == BandModeMomentum }

// Validate rejects ambiguous thresholds instead of silently trading them.
func (c BandConfig) Validate() error {
	switch c.Mode {
	case "", BandModeReversion, BandModeMomentum:
	default:
		return fmt.Errorf("statistics: unknown band mode %q", c.Mode)
	}
	switch {
	case !finite(c.Entry) || !finite(c.Exit) || !finite(c.Stop):
		return errors.New("statistics: band thresholds must be finite")
	case c.Entry <= 0:
		return errors.New("statistics: band entry must be positive")
	case c.Exit < 0 || c.Exit >= c.Entry:
		return errors.New("statistics: band exit must be in [0, entry)")
	case !c.isMomentum() && c.Stop != 0 && c.Stop <= c.Entry:
		return errors.New("statistics: band stop must exceed entry (0 disables)")
	case c.isMomentum() && c.Stop != 0 && c.Stop <= c.Exit:
		return errors.New("statistics: momentum band stop must exceed exit (0 disables)")
	case c.MaxHold < 0:
		return errors.New("statistics: band max hold must not be negative")
	case c.LongOnly && c.ShortOnly:
		return errors.New("statistics: long-only and short-only are exclusive")
	}
	return nil
}

// CloseReason classifies why a previously open position prevPos (-1 or +1) closed at z.
func (c BandConfig) CloseReason(prevPos int, z float64) string {
	if c.isMomentum() {
		switch {
		case c.Stop > 0 && ((prevPos > 0 && z <= -c.Stop) || (prevPos < 0 && z >= c.Stop)):
			return "stop"
		case (prevPos > 0 && z <= c.Exit) || (prevPos < 0 && z >= -c.Exit):
			return "exit"
		default:
			return "maxhold"
		}
	}
	abs := math.Abs(z)
	switch {
	case c.Stop > 0 && abs >= c.Stop:
		return "stop"
	case abs <= c.Exit:
		return "exit"
	default:
		return "maxhold"
	}
}

// BandTrader is a tiny deterministic state machine; it owns no clock and no
// account, so time-series strategies and factor portfolio builders can share
// it. Each instance must serve exactly one spread.
type BandTrader struct {
	cfg     BandConfig
	pos     int
	held    int
	blocked bool
}

func NewBandTrader(cfg BandConfig) (*BandTrader, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &BandTrader{cfg: cfg}, nil
}

// Position returns the current spread position: -1, 0 or +1.
func (b *BandTrader) Position() int { return b.pos }

// Step consumes one z-score and returns the new position. A non-finite z
// leaves the state untouched: missing data must not trigger trades.
func (b *BandTrader) Step(z float64) int {
	if !finite(z) {
		return b.pos
	}
	abs := math.Abs(z)
	if b.blocked && abs <= b.cfg.Exit {
		b.blocked = false
	}
	if b.pos != 0 {
		b.held++
		if b.cfg.isMomentum() {
			switch {
			case b.cfg.Stop > 0 && ((b.pos > 0 && z <= -b.cfg.Stop) || (b.pos < 0 && z >= b.cfg.Stop)):
				b.close(true)
			case (b.pos > 0 && z <= b.cfg.Exit) || (b.pos < 0 && z >= -b.cfg.Exit):
				b.close(false)
			case b.cfg.MaxHold > 0 && b.held >= b.cfg.MaxHold:
				b.close(true)
			}
			return b.pos
		}
		switch {
		case b.cfg.Stop > 0 && abs >= b.cfg.Stop:
			b.close(true)
		case abs <= b.cfg.Exit:
			b.close(false)
		case b.cfg.MaxHold > 0 && b.held >= b.cfg.MaxHold:
			b.close(true)
		}
		return b.pos
	}
	if b.cfg.isMomentum() {
		if b.blocked {
			return 0
		}
		switch {
		case z >= b.cfg.Entry && !b.cfg.ShortOnly:
			b.pos, b.held = 1, 0
		case z <= -b.cfg.Entry && !b.cfg.LongOnly:
			b.pos, b.held = -1, 0
		}
		return b.pos
	}
	if b.blocked || (b.cfg.Stop > 0 && abs >= b.cfg.Stop) {
		return 0
	}
	switch {
	case z <= -b.cfg.Entry && !b.cfg.ShortOnly:
		b.pos, b.held = 1, 0
	case z >= b.cfg.Entry && !b.cfg.LongOnly:
		b.pos, b.held = -1, 0
	}
	return b.pos
}

// Reset flattens the trader without blocking re-entry (067: a leg lost its data; the pair is closed and paused,
// and may open again once both legs are back and the band says so).
func (b *BandTrader) Reset() { b.pos, b.held, b.blocked = 0, 0, false }

func (b *BandTrader) close(block bool) {
	b.pos, b.held, b.blocked = 0, 0, block
}

// SpreadPoint is the state of the latest window regression y = alpha + beta*x.
type SpreadPoint struct {
	Alpha, Beta float64
	Residual    float64 // latest y - alpha - beta*x
	Std         float64 // residual standard deviation over the window (ddof 2)
	Z           float64 // Residual / Std
}

// RollingSpread maintains a window-N OLS of y on x with O(1) updates and a
// ring buffer. It uses sums of centered inputs relative to the first sample,
// which keeps log-price inputs numerically stable for the sizes used in
// practice. The regression is re-fit every bar; callers wanting a frozen
// hedge ratio should fit statistics.Regress/EngleGranger instead.
type RollingSpread struct {
	n, pos, count         int
	xs, ys                []float64
	ox, oy                float64
	started               bool
	sx, sy, sxx, sxy, syy float64
}

func NewRollingSpread(window int) (*RollingSpread, error) {
	if window < 3 {
		return nil, errors.New("statistics: rolling spread window must be at least 3")
	}
	return &RollingSpread{n: window, xs: make([]float64, window), ys: make([]float64, window)}, nil
}

// Window returns the configured window length.
func (r *RollingSpread) Window() int { return r.n }

// Observe adds one (x, y) pair. ok is false until the window is full, when a
// non-finite input is supplied (the sample is skipped), or when x has no
// variance in the window (beta undefined).
func (r *RollingSpread) Observe(x, y float64) (SpreadPoint, bool) {
	if !finite(x) || !finite(y) {
		return SpreadPoint{}, false
	}
	if !r.started {
		r.ox, r.oy, r.started = x, y, true
	}
	cx, cy := x-r.ox, y-r.oy
	if r.count == r.n {
		ox, oy := r.xs[r.pos], r.ys[r.pos]
		r.sx -= ox
		r.sy -= oy
		r.sxx -= ox * ox
		r.sxy -= ox * oy
		r.syy -= oy * oy
	} else {
		r.count++
	}
	r.xs[r.pos], r.ys[r.pos] = cx, cy
	r.pos = (r.pos + 1) % r.n
	r.sx += cx
	r.sy += cy
	r.sxx += cx * cx
	r.sxy += cx * cy
	r.syy += cy * cy
	if r.count < r.n {
		return SpreadPoint{}, false
	}
	n := float64(r.n)
	mx, my := r.sx/n, r.sy/n
	vx := r.sxx - n*mx*mx
	if vx <= 1e-12 {
		return SpreadPoint{}, false
	}
	cov := r.sxy - n*mx*my
	beta := cov / vx
	ssr := (r.syy - n*my*my) - beta*cov
	if ssr < 0 {
		ssr = 0
	}
	std := math.Sqrt(ssr / (n - 2))
	alpha := (my - beta*mx) + r.oy - beta*r.ox
	resid := y - alpha - beta*x
	p := SpreadPoint{Alpha: alpha, Beta: beta, Residual: resid, Std: std}
	if std > 1e-12 {
		p.Z = resid / std
	} else {
		return p, false
	}
	if !finite(p.Z) || !finite(p.Beta) {
		return SpreadPoint{}, false
	}
	return p, true
}

// EvalWithBeta evaluates (x, y) against the current full window using a fixed
// hedge ratio frozenBeta instead of the refit OLS slope: it computes the sample
// mean and standard deviation (ddof 1) of r_i = y_i - frozenBeta*x_i across the
// window and returns the z-score of y - frozenBeta*x. It does not mutate r.
func (r *RollingSpread) EvalWithBeta(x, y, frozenBeta float64) (z float64, ok bool) {
	if r.count < r.n || !finite(x) || !finite(y) || !finite(frozenBeta) {
		return 0, false
	}
	var mean, m2 float64
	for i := 0; i < r.n; i++ {
		v := r.ys[i] - frozenBeta*r.xs[i]
		if !finite(v) {
			return 0, false
		}
		delta := v - mean
		mean += delta / float64(i+1)
		m2 += delta * (v - mean)
	}
	std := math.Sqrt(math.Max(0, m2) / float64(r.n-1))
	if std <= 1e-12 || !finite(std) {
		return 0, false
	}
	resid := (y - r.oy) - frozenBeta*(x - r.ox) - mean
	z = resid / std
	if !finite(z) {
		return 0, false
	}
	return z, true
}
