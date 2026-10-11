package statistics

import (
	"errors"
	"math"
	"math/rand"
)

func binaryPayoutValid(loss, gain, p float64) bool {
	return afmlFinite(loss) && loss < 0 && afmlFinite(gain) && gain > 0 && afmlFinite(p) && p > 0 && p < 1
}

// BinaryPayoutSharpe is chapter15's independent binary-bet annual Sharpe.
func BinaryPayoutSharpe(loss, gain, precision, frequency float64) (float64, error) {
	if !binaryPayoutValid(loss, gain, precision) || !afmlFinite(frequency) || frequency <= 0 {
		return 0, errors.New("statistics: invalid binary payout model")
	}
	// Scale payoffs before taking their difference or mean to avoid cancellation
	// and overflow; a common currency/payoff multiplier must not change Sharpe.
	scale := math.Max(-loss, gain)
	l, g := loss/scale, gain/scale
	sr := math.Sqrt(frequency) * (precision*g + (1-precision)*l) / ((g - l) * math.Sqrt(precision*(1-precision)))
	if !afmlFinite(sr) {
		return 0, errors.New("statistics: binary payout Sharpe overflow")
	}
	return sr, nil
}

// ImpliedBetPrecision selects the above-break-even root of snippet15.3.
func ImpliedBetPrecision(loss, gain, frequency, targetSharpe float64) (float64, error) {
	if !binaryPayoutValid(loss, gain, .5) || !afmlFinite(frequency) || frequency <= 0 || !afmlFinite(targetSharpe) || targetSharpe < 0 {
		return 0, errors.New("statistics: invalid implied precision model")
	}
	scale := math.Max(-loss, gain)
	l, g := loss/scale, gain/scale
	q := -l / (g - l)
	t := targetSharpe / math.Sqrt(frequency)
	// (p-q)^2=t^2*p*(1-p). Algebra cancels payoff scale.
	if !afmlFinite(t * t) {
		return 0, errors.New("statistics: implied precision overflow")
	}
	a, b, c := 1+t*t, -2*q-t*t, q*q
	disc := b*b - 4*a*c
	if disc < 0 || !afmlFinite(disc) {
		return 0, errors.New("statistics: implied precision discriminant overflow")
	}
	p := (-b + math.Sqrt(disc)) / (2 * a)
	if !afmlFinite(p) || p < q || p > 1 {
		return 0, errors.New("statistics: invalid implied precision root")
	}
	return p, nil
}

// ImpliedBetFrequency repairs snippet15.4's wrong-arity binHR verification by
// checking the unsquared Sharpe equation, rejecting negative-mean extraneous
// roots which a squared formula would otherwise admit.
func ImpliedBetFrequency(loss, gain, precision, targetSharpe float64) (float64, error) {
	if !binaryPayoutValid(loss, gain, precision) || !afmlFinite(targetSharpe) || targetSharpe <= 0 {
		return 0, errors.New("statistics: invalid implied frequency model")
	}
	perBet, err := BinaryPayoutSharpe(loss, gain, precision, 1)
	if err != nil || perBet <= 0 {
		return 0, errors.New("statistics: precision does not produce positive mean payout")
	}
	n := math.Pow(targetSharpe/perBet, 2)
	if !afmlFinite(n) || n <= 0 {
		return 0, errors.New("statistics: implied frequency overflow")
	}
	return n, nil
}

type StrategyFailureReport struct {
	Precision, RequiredPrecision, Probability, Bandwidth float64
	BootstrapPrecision                                   []float64
}

// BootstrapStrategyFailure follows chapter15.4's bootstrap precision/KDE-CDF
// recipe. HorizonBets declares k-year sampling length. A Gaussian kernel with
// Scott bandwidth is explicit; zero bootstrap variance uses the empirical CDF.
// This estimator and the snippet15.5 Normal heuristic are distinct diagnostics.
func BootstrapStrategyFailure(returns []float64, frequency, targetSharpe float64, horizonBets, trials int, seed, maxDraws int64) (StrategyFailureReport, error) {
	var out StrategyFailureReport
	if len(returns) < 2 || horizonBets < 1 || trials < 2 || trials > 1000000 || int64(horizonBets) > maxDraws/int64(trials) {
		return out, errors.New("statistics: invalid strategy failure bootstrap bounds")
	}
	positive, negative, npos := 0., 0., 0
	for _, value := range returns {
		if !afmlFinite(value) {
			return out, errors.New("statistics: nonfinite strategy return")
		}
		if value > 0 {
			positive += value
			npos++
		} else {
			negative += value
		}
	}
	if npos == 0 || npos == len(returns) || negative >= 0 || !afmlFinite(positive) || !afmlFinite(negative) {
		return out, errors.New("statistics: strategy failure requires positive and negative payouts")
	}
	out.Precision = float64(npos) / float64(len(returns))
	var err error
	out.RequiredPrecision, err = ImpliedBetPrecision(negative/float64(len(returns)-npos), positive/float64(npos), frequency, targetSharpe)
	if err != nil {
		return out, err
	}
	rng := rand.New(rand.NewSource(seed))
	out.BootstrapPrecision = make([]float64, trials)
	for i := range out.BootstrapPrecision {
		count := 0
		for j := 0; j < horizonBets; j++ {
			if returns[rng.Intn(len(returns))] > 0 {
				count++
			}
		}
		out.BootstrapPrecision[i] = float64(count) / float64(horizonBets)
	}
	_, sd, err := MeanStd(out.BootstrapPrecision, 1)
	if err != nil {
		return out, err
	}
	out.Bandwidth = sd * math.Pow(float64(trials), -.2)
	for _, p := range out.BootstrapPrecision {
		if out.Bandwidth == 0 {
			if p <= out.RequiredPrecision {
				out.Probability++
			}
		} else {
			out.Probability += .5 * math.Erfc((p-out.RequiredPrecision)/(out.Bandwidth*math.Sqrt2))
		}
	}
	out.Probability /= float64(trials)
	return out, nil
}

// NormalStrategyFailureHeuristic reproduces snippet15.5's literal CDF scale
// p*(1-p), which is neither sqrt(binomial variance/n) nor a bootstrap estimate.
func NormalStrategyFailureHeuristic(precision, requiredPrecision float64) (float64, error) {
	if !afmlFinite(precision) || precision <= 0 || precision >= 1 || !afmlFinite(requiredPrecision) || requiredPrecision < 0 || requiredPrecision > 1 {
		return 0, errors.New("statistics: invalid source failure heuristic")
	}
	return .5 * math.Erfc((precision-requiredPrecision)/(precision*(1-precision)*math.Sqrt2)), nil
}
