package statistics

import (
	"errors"
	"fmt"
	"math"
	"time"

	"gonum.org/v1/gonum/optimize"
)

// PINModel implements AFML19.5.1's three regimes: no information, bad news,
// good news. Rates are per declared observation period; buys/sells are integer
// arrival counts in that same unit. Converting arbitrary volume to counts is
// a caller assumption, not performed here.
type PINModel struct{ Alpha, Delta, Mu, Epsilon float64 }
type PINCounts struct{ Buys, Sells int }
type PINFitSpec struct {
	MaxEvaluations, MaxIterations int
	Runtime                       time.Duration
}
type PINFit struct {
	Model                        PINModel
	LogLikelihood                float64
	Evaluations, ConvergedStarts int
}

func (m PINModel) validate() error {
	if !afmlFinite(m.Alpha) || m.Alpha < 0 || m.Alpha > 1 || !afmlFinite(m.Delta) || m.Delta < 0 || m.Delta > 1 || !afmlFinite(m.Mu) || m.Mu < 0 || !afmlFinite(m.Epsilon) || m.Epsilon <= 0 || !afmlFinite(m.Mu+m.Epsilon) {
		return errors.New("statistics: invalid PIN parameters")
	}
	return nil
}

func logSumPIN(values [3]float64) float64 {
	maximum := math.Max(values[0], math.Max(values[1], values[2]))
	if math.IsInf(maximum, -1) {
		return maximum
	}
	sum := 0.
	for _, v := range values {
		sum += math.Exp(v - maximum)
	}
	return maximum + math.Log(sum)
}

func (m PINModel) regimeLogs(c PINCounts, elapsed float64) [3]float64 {
	poisson := func(count int, rate float64) float64 {
		mean := rate * elapsed
		if mean == 0 {
			if count == 0 {
				return 0
			}
			return math.Inf(-1)
		}
		factorial, _ := math.Lgamma(float64(count) + 1)
		return float64(count)*math.Log(mean) - mean - factorial
	}
	plainBuy, plainSell := poisson(c.Buys, m.Epsilon), poisson(c.Sells, m.Epsilon)
	return [3]float64{plainBuy + plainSell, plainBuy + poisson(c.Sells, m.Epsilon+m.Mu), poisson(c.Buys, m.Epsilon+m.Mu) + plainSell}
}

// LogLikelihood uses log-sum-exp to avoid underflow for large trade counts.
func (m PINModel) LogLikelihood(counts []PINCounts) (float64, error) {
	if err := m.validate(); err != nil {
		return 0, err
	}
	if len(counts) == 0 || len(counts) > 1000000 {
		return 0, errors.New("statistics: invalid PIN sample size")
	}
	weights := [3]float64{1 - m.Alpha, m.Alpha * m.Delta, m.Alpha * (1 - m.Delta)}
	ll := 0.
	for _, c := range counts {
		if c.Buys < 0 || c.Sells < 0 || c.Buys > 1000000000 || c.Sells > 1000000000 {
			return 0, errors.New("statistics: invalid PIN arrival counts")
		}
		v := m.regimeLogs(c, 1)
		for i := range v {
			v[i] += math.Log(weights[i])
		}
		ll += logSumPIN(v)
	}
	if !afmlFinite(ll) {
		return 0, errors.New("statistics: nonfinite PIN likelihood")
	}
	return ll, nil
}

func (m PINModel) Probability() (float64, error) {
	if err := m.validate(); err != nil {
		return 0, err
	}
	x := m.Alpha * m.Mu
	// Divide before adding to avoid overflowing a valid high-rate model.
	return (x / 2) / (x/2 + m.Epsilon), nil
}

// FitPIN fits the source likelihood with four deterministic Nelder-Mead starts.
// It is a local numerical MLE, not a proof of the global optimum. Total work,
// function evaluations and wall time are bounded; failed starts are excluded.
// Rates/logits use constrained transforms, so exact boundary maxima are limits.
func FitPIN(counts []PINCounts, spec PINFitSpec) (PINFit, error) {
	if spec.MaxEvaluations < 40 || spec.MaxEvaluations > 100000 || spec.MaxIterations < 1 || spec.Runtime <= 0 || len(counts) < 4 || len(counts) > 1000000 || uint64(len(counts))*uint64(spec.MaxEvaluations) > 50000000 {
		return PINFit{}, errors.New("statistics: invalid PIN fit budget")
	}
	mean, imbalance := 0., 0.
	for i, c := range counts {
		if c.Buys < 0 || c.Sells < 0 || c.Buys > 1000000000 || c.Sells > 1000000000 {
			return PINFit{}, errors.New("statistics: invalid PIN fit counts")
		}
		mean += (float64(c.Buys+c.Sells) - mean) / float64(i+1)
		imbalance += (math.Abs(float64(c.Buys-c.Sells)) - imbalance) / float64(i+1)
	}
	if mean == 0 {
		return PINFit{}, errors.New("statistics: empty PIN arrivals do not identify rates")
	}
	decode := func(x []float64) PINModel {
		logistic := func(v float64) float64 {
			if v >= 0 {
				return 1 / (1 + math.Exp(-v))
			}
			e := math.Exp(v)
			return e / (1 + e)
		}
		return PINModel{logistic(x[0]), logistic(x[1]), math.Exp(x[2]), math.Exp(x[3])}
	}
	result := PINFit{LogLikelihood: math.Inf(-1)}
	deadline := time.Now().Add(spec.Runtime)
	for start, alpha := range []float64{.2, .5, .8, .5} {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		mu := math.Max(.01, imbalance)
		epsilon := math.Max(.01, (mean-alpha*mu)/2)
		delta := .5
		if start == 3 {
			delta = .8
		}
		x := []float64{math.Log(alpha / (1 - alpha)), math.Log(delta / (1 - delta)), math.Log(mu), math.Log(epsilon)}
		objective := func(x []float64) float64 {
			result.Evaluations++
			ll, err := decode(x).LogLikelihood(counts)
			if err != nil {
				return 1e99
			}
			return -ll / float64(len(counts))
		}
		fit, err := optimize.Minimize(optimize.Problem{Func: objective}, x, &optimize.Settings{FuncEvaluations: spec.MaxEvaluations / 4, MajorIterations: spec.MaxIterations, Runtime: remaining, Converger: &optimize.FunctionConverge{Absolute: 1e-9, Relative: 1e-10, Iterations: 30}}, &optimize.NelderMead{})
		if err != nil || fit == nil || fit.Status.Early() || !afmlFinite(fit.F) || fit.F >= 1e99 {
			continue
		}
		result.ConvergedStarts++
		ll := -fit.F * float64(len(counts))
		if ll > result.LogLikelihood {
			result.Model = decode(fit.X)
			result.LogLikelihood = ll
		}
	}
	if result.ConvergedStarts == 0 {
		return PINFit{}, fmt.Errorf("statistics: no PIN start converged within budget")
	}
	return result, nil
}

// PINPosterior stores log probabilities of the three mutually exclusive news
// regimes. Update receives only NEW arrivals and elapsed time since the previous
// update. The survival likelihood captures information from no-trade intervals.
type PINPosterior struct {
	Model            PINModel
	LogProbabilities [3]float64
}

func (m PINModel) Posterior() (PINPosterior, error) {
	if err := m.validate(); err != nil {
		return PINPosterior{}, err
	}
	return PINPosterior{m, [3]float64{math.Log(1 - m.Alpha), math.Log(m.Alpha * m.Delta), math.Log(m.Alpha * (1 - m.Delta))}}, nil
}
func (p PINPosterior) Update(c PINCounts, elapsed float64) (PINPosterior, error) {
	if err := p.Model.validate(); err != nil {
		return PINPosterior{}, err
	}
	if c.Buys < 0 || c.Sells < 0 || c.Buys > 1000000000 || c.Sells > 1000000000 || !afmlFinite(elapsed) || elapsed <= 0 {
		return PINPosterior{}, errors.New("statistics: invalid PIN posterior observation")
	}
	for _, v := range p.LogProbabilities {
		if math.IsNaN(v) || math.IsInf(v, 1) {
			return PINPosterior{}, errors.New("statistics: invalid PIN posterior state")
		}
	}
	if z := logSumPIN(p.LogProbabilities); !afmlFinite(z) || math.Abs(z) > 1e-10 {
		return PINPosterior{}, errors.New("statistics: PIN posterior is not normalized")
	}
	likelihood := p.Model.regimeLogs(c, elapsed)
	for i := range likelihood {
		p.LogProbabilities[i] += likelihood[i]
	}
	z := logSumPIN(p.LogProbabilities)
	if !afmlFinite(z) {
		return PINPosterior{}, errors.New("statistics: impossible PIN posterior observation")
	}
	for i := range p.LogProbabilities {
		p.LogProbabilities[i] -= z
	}
	return p, nil
}

// Quotes are source Bayesian breakeven quotes, excluding fees/inventory utility.
func (p PINPosterior) Quotes(bad, neutral, good float64) (bid, ask, expected float64, err error) {
	if err = p.Model.validate(); err != nil {
		return
	}
	if !afmlFinite(bad) || !afmlFinite(neutral) || !afmlFinite(good) || bad > neutral || neutral > good {
		return 0, 0, 0, errors.New("statistics: invalid PIN security values")
	}
	z := logSumPIN(p.LogProbabilities)
	if !afmlFinite(z) || math.Abs(z) > 1e-10 {
		return 0, 0, 0, errors.New("statistics: invalid PIN quote posterior")
	}
	w := [3]float64{}
	for i, v := range p.LogProbabilities {
		if math.IsNaN(v) || math.IsInf(v, 1) {
			return 0, 0, 0, errors.New("statistics: invalid PIN quote posterior")
		}
		w[i] = math.Exp(v)
	}
	expected = w[0]*neutral + w[1]*bad + w[2]*good
	badRate, goodRate := w[1]*p.Model.Mu, w[2]*p.Model.Mu
	bid = expected - badRate/(p.Model.Epsilon+badRate)*(expected-bad)
	ask = expected + goodRate/(p.Model.Epsilon+goodRate)*(good-expected)
	if !afmlFinite(bid) || !afmlFinite(ask) || !afmlFinite(expected) {
		return 0, 0, 0, errors.New("statistics: PIN quote overflow")
	}
	return
}
