package statistics

import (
	"errors"
	"math"
	"math/rand"

	"gonum.org/v1/gonum/stat/distuv"
)

// BulkVolumeBuyFraction returns the BVC fraction Phi(delta/priorSigma), or a
// Student-t CDF with explicitly chosen positive degrees of freedom. It uses zero
// mean, not the empirical mean of prior price changes. Sigma must be historical.
func BulkVolumeBuyFraction(delta, priorSigma, degreesOfFreedom float64) (float64, error) {
	if !afmlFinite(delta) || !afmlFinite(priorSigma) || priorSigma <= 0 || !afmlFinite(degreesOfFreedom) || degreesOfFreedom < 0 {
		return 0, errors.New("statistics: invalid bulk-volume classification inputs")
	}
	z := delta / priorSigma
	if degreesOfFreedom == 0 {
		return .5 * math.Erfc(-z/math.Sqrt2), nil
	}
	return distuv.StudentsT{Mu: 0, Sigma: 1, Nu: degreesOfFreedom}.CDF(z), nil
}

// BulkVolumeFractions applies Chan ch6's previous-lookback sample STD to each
// current volume-bar price change. Missing history does not shorten the window;
// insufficient/degenerate calibration remains NaN. df0 selects his Normal CDF.
func BulkVolumeFractions(priceChanges []float64, lookback int, degreesOfFreedom float64) ([]float64, error) {
	if lookback < 2 || lookback > 100000 || len(priceChanges) > 10000000 || int64(lookback)*int64(len(priceChanges)) > 100000000 || !afmlFinite(degreesOfFreedom) || degreesOfFreedom < 0 {
		return nil, errors.New("statistics: invalid BVC history bounds")
	}
	out := make([]float64, len(priceChanges))
	for i, value := range priceChanges {
		out[i] = math.NaN()
		if math.IsInf(value, 0) {
			return nil, errors.New("statistics: infinite BVC price change")
		}
		if i < lookback || math.IsNaN(value) {
			continue
		}
		history := priceChanges[i-lookback : i]
		missing := false
		for _, v := range history {
			if math.IsNaN(v) {
				missing = true
				break
			}
		}
		if missing {
			continue
		}
		_, sigma, err := MeanStd(history, 1)
		if err != nil {
			return nil, err
		}
		if sigma <= 0 {
			continue
		}
		out[i], err = BulkVolumeBuyFraction(value, sigma, degreesOfFreedom)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// VolumeSynchronizedPIN computes mean absolute buy-sell imbalance on COMPLETE,
// equally sized volume buckets. Variable-volume candles cannot be passed as a
// substitute. Fractions may come from aggressor flags or explicit BVC calibration.
func VolumeSynchronizedPIN(buy, sell []float64, bucketVolume float64, window int) ([]float64, error) {
	if len(buy) != len(sell) || len(buy) == 0 || !afmlFinite(bucketVolume) || bucketVolume <= 0 || window < 1 {
		return nil, errors.New("statistics: invalid VPIN buckets")
	}
	out := make([]float64, len(buy))
	imbalance := make([]float64, len(buy))
	sum := 0.
	for i, b := range buy {
		s := sell[i]
		if !afmlFinite(b) || !afmlFinite(s) || b < 0 || s < 0 || math.Abs((b/bucketVolume+s/bucketVolume)-1) > 1e-10 {
			return nil, errors.New("statistics: VPIN requires equal complete bucket volumes")
		}
		imbalance[i] = math.Abs(b/bucketVolume - s/bucketVolume)
		sum += imbalance[i]
		if i >= window {
			sum -= imbalance[i-window]
		}
		out[i] = math.NaN()
		if i+1 >= window {
			out[i] = math.Max(0, math.Min(1, sum/float64(window)))
		}
	}
	return out, nil
}

type ImpactRegression struct {
	Lambda, StandardError, TStatistic float64
	Samples                           int
	TValid                            bool
}

func impactRegression(response, flow []float64) (ImpactRegression, error) {
	var out ImpactRegression
	if len(response) < 3 || len(response) != len(flow) {
		return out, errors.New("statistics: insufficient impact observations")
	}
	x := make([][]float64, len(flow))
	xx := 0.
	for i, v := range flow {
		if !afmlFinite(v) || !afmlFinite(response[i]) {
			return out, errors.New("statistics: nonfinite impact observations")
		}
		x[i] = []float64{v}
		xx += v * v
	}
	if xx <= 0 || !afmlFinite(xx) {
		return out, errors.New("statistics: unidentified/overflowed impact flow")
	}
	fit, err := Regress(response, x, false)
	if err != nil {
		return out, err
	}
	sse := 0.
	for _, r := range fit.Residuals {
		sse += r * r
	}
	if !afmlFinite(sse) {
		return out, errors.New("statistics: impact error overflow")
	}
	out.Lambda, out.Samples, out.TStatistic = fit.Beta[0], len(flow), math.NaN()
	out.StandardError = math.Sqrt(sse / float64(len(flow)-1) / xx)
	if out.StandardError > 0 {
		out.TStatistic = out.Lambda / out.StandardError
		out.TValid = afmlFinite(out.TStatistic)
	}
	return out, nil
}

// KyleLambda fits AFML19.4.1 deltaPrice=lambda*signedVolume+error, no intercept.
func KyleLambda(priceChanges, signedVolume []float64) (ImpactRegression, error) {
	return impactRegression(priceChanges, signedVolume)
}

// AmihudLambda follows the BOOK'S regression |delta log(close)|=lambda*dollar
// volume+error. It differs from the commonly used mean(|return|/dollarVolume).
func AmihudLambda(logReturns, dollarVolume []float64) (ImpactRegression, error) {
	response := make([]float64, len(logReturns))
	if len(dollarVolume) != len(logReturns) {
		return ImpactRegression{}, errors.New("statistics: Amihud dimensions")
	}
	for i, value := range logReturns {
		if !afmlFinite(dollarVolume[i]) || dollarVolume[i] <= 0 {
			return ImpactRegression{}, errors.New("statistics: Amihud needs positive dollar volume")
		}
		response[i] = math.Abs(value)
	}
	return impactRegression(response, dollarVolume)
}

type ImpactGibbsSpec struct {
	PriorMean, PriorVariance, VarianceShape, VarianceScale float64
	BurnIn, Draws                                          int
	Seed                                                   int64
}

type ImpactGibbsResult struct {
	LambdaMean, LambdaStd, VarianceMean float64
	LambdaDraws                         []float64
}

// HasbrouckImpactGibbs estimates AFML19.4.3 log-return on signed SQRT dollar
// flow with a real Gibbs sampler. The book supplies the regression but omits
// priors; callers therefore declare independent Normal lambda and inverse-Gamma
// noise priors. No OLS estimate is relabeled Bayesian, and no historical parameter
// replication is claimed. signedSqrtFlow sums b*sqrt(price*volume) within each bar.
func HasbrouckImpactGibbs(logReturns, signedSqrtFlow []float64, s ImpactGibbsSpec) (ImpactGibbsResult, error) {
	var out ImpactGibbsResult
	if len(logReturns) < 3 || len(logReturns) != len(signedSqrtFlow) || len(logReturns) > 100000 || !afmlFinite(s.PriorMean) || !afmlFinite(s.PriorVariance) || s.PriorVariance <= 0 || !afmlFinite(s.VarianceShape) || s.VarianceShape <= 0 || !afmlFinite(s.VarianceScale) || s.VarianceScale <= 0 || s.BurnIn < 0 || s.BurnIn > 100000 || s.Draws < 2 || s.Draws > 100000 || int64(s.BurnIn+s.Draws)*int64(len(logReturns)) > 100000000 {
		return out, errors.New("statistics: invalid Bayesian impact calibration/budget")
	}
	xx, xy := 0., 0.
	for i, x := range signedSqrtFlow {
		if !afmlFinite(x) || !afmlFinite(logReturns[i]) {
			return out, errors.New("statistics: nonfinite Bayesian impact data")
		}
		xx += x * x
		xy += x * logReturns[i]
	}
	if xx <= 0 || !afmlFinite(xx) || !afmlFinite(xy) {
		return out, errors.New("statistics: Bayesian impact design overflow/unidentified")
	}
	rng := rand.New(rand.NewSource(s.Seed))
	shape := s.VarianceShape + float64(len(logReturns))/2
	// shape>1 from at least three data rows, permitting bounded Marsaglia-Tsang.
	gamma := func() (float64, error) {
		d := shape - 1./3
		c := 1 / math.Sqrt(9*d)
		for attempt := 0; attempt < 1000; attempt++ {
			x := rng.NormFloat64()
			v := 1 + c*x
			if v <= 0 {
				continue
			}
			v = v * v * v
			u := rng.Float64()
			if u < 1-.0331*x*x*x*x || math.Log(u) < .5*x*x+d*(1-v+math.Log(v)) {
				return d * v, nil
			}
		}
		return 0, errors.New("statistics: gamma rejection budget exhausted")
	}
	variance := s.VarianceScale / s.VarianceShape
	out.LambdaDraws = make([]float64, 0, s.Draws)
	var variances []float64
	for i := 0; i < s.BurnIn+s.Draws; i++ {
		conditionalVariance := 1 / (xx/variance + 1/s.PriorVariance)
		mean := conditionalVariance * (xy/variance + s.PriorMean/s.PriorVariance)
		lambda := mean + math.Sqrt(conditionalVariance)*rng.NormFloat64()
		sse := 0.
		for j, x := range signedSqrtFlow {
			residual := logReturns[j] - lambda*x
			sse += residual * residual
		}
		draw, err := gamma()
		if err != nil {
			return ImpactGibbsResult{}, err
		}
		variance = (s.VarianceScale + .5*sse) / draw
		if !afmlFinite(lambda) || !afmlFinite(conditionalVariance) || conditionalVariance <= 0 || !afmlFinite(variance) || variance <= 0 {
			return ImpactGibbsResult{}, errors.New("statistics: Bayesian impact numerical overflow/underflow")
		}
		if i >= s.BurnIn {
			out.LambdaDraws = append(out.LambdaDraws, lambda)
			variances = append(variances, variance)
		}
	}
	var err error
	out.LambdaMean, out.LambdaStd, err = MeanStd(out.LambdaDraws, 1)
	if err != nil {
		return ImpactGibbsResult{}, err
	}
	out.VarianceMean, _, err = MeanStd(variances, 0)
	return out, err
}
