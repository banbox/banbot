package statistics

import (
	"errors"
	"math"

	"gonum.org/v1/gonum/mat"
)

type RiskEntropyReport struct {
	Eigenvalues, RiskShares                                   []float64
	Variance, ShannonNats, EffectiveComponents, Concentration float64
}

// PortfolioRiskEntropy implements18.8.3: theta_i=(W'w)_i^2*lambda_i/(w'Vw),
// H=1-exp(-sum(theta_i*log(theta_i)))/N. Source net allocations sum to1;
// shorts are allowed. PCA decomposition is nonunique for repeated eigenvalues,
// so these component entropy quantities depend on the chosen orthogonal basis
// in degenerate eigenspaces. This API uses Gonum's ascending eigenbasis and does
// not assert invariance under rotations in such spaces. Zero risk is undefined.
func PortfolioRiskEntropy(covariance [][]float64, weights []float64) (RiskEntropyReport, error) {
	n := len(weights)
	if n < 1 || n > 256 || len(covariance) != n {
		return RiskEntropyReport{}, errors.New("statistics: invalid risk entropy dimensions")
	}
	sym := mat.NewSymDense(n, nil)
	sum := 0.
	scale := 0.
	for i, w := range weights {
		if !afmlFinite(w) || len(covariance[i]) != n {
			return RiskEntropyReport{}, errors.New("statistics: invalid risk allocation/covariance")
		}
		sum += w
	}
	if !afmlFinite(sum) || math.Abs(sum-1) > 1e-10 {
		return RiskEntropyReport{}, errors.New("statistics: source risk entropy requires net allocation one")
	}
	for i := 0; i < n; i++ {
		for j := 0; j <= i; j++ {
			a, b := covariance[i][j], covariance[j][i]
			if !afmlFinite(a) || !afmlFinite(b) || math.Abs(a-b) > 1e-12*math.Max(1, math.Max(math.Abs(a), math.Abs(b))) {
				return RiskEntropyReport{}, errors.New("statistics: invalid risk covariance symmetry")
			}
			sym.SetSym(i, j, (a+b)/2)
			scale = math.Max(scale, math.Abs(a))
		}
	}
	var eig mat.EigenSym
	if !eig.Factorize(sym, true) {
		return RiskEntropyReport{}, errors.New("statistics: risk entropy decomposition failed")
	}
	var basis mat.Dense
	eig.VectorsTo(&basis)
	r := RiskEntropyReport{Eigenvalues: eig.Values(nil), RiskShares: make([]float64, n)}
	for i, v := range r.Eigenvalues {
		if v < -1e-12*scale {
			return RiskEntropyReport{}, errors.New("statistics: risk covariance is not PSD")
		}
		v = math.Max(0, v)
		r.Eigenvalues[i] = v
		loading := 0.
		for j, w := range weights {
			loading += basis.At(j, i) * w
		}
		r.RiskShares[i] = loading * loading * v
		r.Variance += r.RiskShares[i]
	}
	if r.Variance <= 0 || !afmlFinite(r.Variance) {
		return RiskEntropyReport{}, errors.New("statistics: undefined portfolio risk entropy")
	}
	for i, v := range r.RiskShares {
		p := v / r.Variance
		r.RiskShares[i] = p
		if p > 0 {
			r.ShannonNats -= p * math.Log(p)
		}
	}
	r.EffectiveComponents = math.Exp(r.ShannonNats)
	r.Concentration = math.Max(0, math.Min(1, 1-r.EffectiveComponents/float64(n)))
	return r, nil
}

type PITVolumeBar struct {
	EventMS, AvailableMS, ReceivedMS int64
	BuyVolume, SellVolume            float64
}

type EntropyCalibrationWindow struct {
	Bars       []PITVolumeBar
	DecisionMS int64
}
type BuyVolumeEntropyResult struct {
	EntropyBits, SourceRedundancy, EmpiricalCDF float64
	Symbols, Points                             int
}

// BuyVolumeEntropyModel freezes separately fitted quantile boundaries and
// empirical entropy calibration. Encoder training precedes entropy-calibration
// endpoints; holdout scoring cannot refit either. All bars have equal declared
// volume and observed aggressor classification; incomplete/unclassified volume
// is rejected, not silently treated as sell volume.
type BuyVolumeEntropyModel struct {
	encoder                          ReturnEncoder
	cdf                              *FrozenEmpiricalCDF
	volume                           float64
	encoderCutoff, calibrationCutoff int64
	window                           int
	comparisons                      int64
}

func entropyBuyFractions(bars []PITVolumeBar, volume float64, decisionMS int64) ([]float64, PITScalar, error) {
	if len(bars) < 2 || len(bars) > 100000 || !afmlFinite(volume) || volume <= 0 || decisionMS < 0 {
		return nil, PITScalar{}, errors.New("statistics: invalid buy-volume entropy window")
	}
	values := make([]float64, len(bars))
	meta := PITScalar{}
	for i, b := range bars {
		if b.EventMS < 0 || (i > 0 && b.EventMS <= bars[i-1].EventMS) || b.AvailableMS < b.EventMS || b.ReceivedMS < b.AvailableMS || b.ReceivedMS > decisionMS || !afmlFinite(b.BuyVolume) || !afmlFinite(b.SellVolume) || b.BuyVolume < 0 || b.SellVolume < 0 {
			return nil, meta, errors.New("statistics: invalid/unavailable classified volume bar")
		}
		total := b.BuyVolume + b.SellVolume
		if !afmlFinite(total) || math.Abs(total-volume) > 1e-10*volume {
			return nil, meta, errors.New("statistics: unequal or incomplete entropy volume buckets")
		}
		values[i] = b.BuyVolume / total
		meta.EventMS = b.EventMS
		meta.AvailableMS = max(meta.AvailableMS, b.AvailableMS)
		meta.ReceivedMS = max(meta.ReceivedMS, b.ReceivedMS)
	}
	return values, meta, nil
}

func encodedKontoyiannis(e ReturnEncoder, values []float64, window int, comparisons int64) (KontoyiannisResult, error) {
	codes, err := e.Encode(values)
	if err != nil {
		return KontoyiannisResult{}, err
	}
	symbols := make([]rune, len(codes))
	for i, k := range codes {
		if k < 0 || k > 255 {
			return KontoyiannisResult{}, errors.New("statistics: unsupported entropy alphabet")
		}
		symbols[i] = rune(0x100 + k)
	}
	return KontoyiannisEntropy(string(symbols), window, comparisons)
}

// FitBuyVolumeEntropy implements source18.8.4's quantile->Kontoyiannis->CDF
// feature with explicit train/calibration separation. Source does not specify
// rolling lengths or out-of-sample fitting; supplied windows/budgets declare
// those choices. CDF is an entropy rank feature, not calibrated adverse-selection
// probability. maxTotalComparisons budgets all calibration windows in advance.
func FitBuyVolumeEntropy(encoderTraining []PITVolumeBar, encoderCutoffMS int64, calibration []EntropyCalibrationWindow, calibrationCutoffMS int64, volume float64, letters, window int, maxComparisons, maxTotalComparisons int64) (*BuyVolumeEntropyModel, error) {
	if letters < 2 || letters > 256 || window < 0 || maxComparisons <= 0 || maxTotalComparisons <= 0 || len(calibration) == 0 || len(calibration) > 100000 || calibrationCutoffMS <= encoderCutoffMS || int64(len(calibration)) > maxTotalComparisons/maxComparisons {
		return nil, errors.New("statistics: invalid or over-budget entropy calibration")
	}
	train, _, err := entropyBuyFractions(encoderTraining, volume, encoderCutoffMS)
	if err != nil {
		return nil, err
	}
	encoder, err := FitQuantileEncoder(train, letters)
	if err != nil {
		return nil, err
	}
	entropies := make([]PITScalar, len(calibration))
	for i, c := range calibration {
		if c.DecisionMS <= encoderCutoffMS || c.DecisionMS > calibrationCutoffMS {
			return nil, errors.New("statistics: calibration overlaps encoder fitting/future cutoff")
		}
		values, meta, err := entropyBuyFractions(c.Bars, volume, c.DecisionMS)
		if err != nil {
			return nil, err
		}
		if meta.EventMS <= encoderCutoffMS {
			return nil, errors.New("statistics: entropy calibration endpoint is not after encoder training")
		}
		h, err := encodedKontoyiannis(encoder, values, window, maxComparisons)
		if err != nil {
			return nil, err
		}
		meta.Value = h.Entropy
		meta.AvailableMS = c.DecisionMS
		meta.ReceivedMS = c.DecisionMS
		entropies[i] = meta
	}
	cdf, err := FitEmpiricalCDF(entropies, calibrationCutoffMS)
	if err != nil {
		return nil, err
	}
	return &BuyVolumeEntropyModel{encoder, cdf, volume, encoderCutoffMS, calibrationCutoffMS, window, maxComparisons}, nil
}

func (m *BuyVolumeEntropyModel) Score(bars []PITVolumeBar, decisionMS int64) (BuyVolumeEntropyResult, error) {
	if m == nil || m.cdf == nil || decisionMS <= m.calibrationCutoff {
		return BuyVolumeEntropyResult{}, errors.New("statistics: invalid entropy holdout model/cutoff")
	}
	values, meta, err := entropyBuyFractions(bars, m.volume, decisionMS)
	if err != nil {
		return BuyVolumeEntropyResult{}, err
	}
	h, err := encodedKontoyiannis(m.encoder, values, m.window, m.comparisons)
	if err != nil {
		return BuyVolumeEntropyResult{}, err
	}
	meta.Value = h.Entropy
	meta.AvailableMS = decisionMS
	meta.ReceivedMS = decisionMS
	p, err := m.cdf.Score(meta, decisionMS)
	if err != nil {
		return BuyVolumeEntropyResult{}, err
	}
	return BuyVolumeEntropyResult{h.Entropy, h.Redundancy, p, len(values), h.Points}, nil
}

// EncodedMarketCompression is source18.8.1's retrospective symbolic-prefix
// entropy/redundancy diagnostic using a previously fitted encoder. Every input
// must already be visible. Low entropy alone proves neither exploitable alpha
// nor a future maximum-entropy outcome (18.8.2 is a research hypothesis).
func EncodedMarketCompression(encoder ReturnEncoder, observations []PITScalar, decisionMS int64, window int, maxComparisons int64) (KontoyiannisResult, error) {
	if len(observations) < 2 || len(observations) > 100000 {
		return KontoyiannisResult{}, errors.New("statistics: invalid market entropy observations")
	}
	values := make([]float64, len(observations))
	for i, p := range observations {
		if !validScalar(p) || !visibleScalar(p, decisionMS) || (i > 0 && p.EventMS <= observations[i-1].EventMS) {
			return KontoyiannisResult{}, errors.New("statistics: unordered/unavailable market entropy prefix")
		}
		values[i] = p.Value
	}
	return encodedKontoyiannis(encoder, values, window, maxComparisons)
}
