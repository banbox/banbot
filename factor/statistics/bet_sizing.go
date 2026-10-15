package statistics

import (
	"errors"
	"math"
)

// ProbabilityBetSize implements AFML snippet 10.1's one-versus-rest statistic.
// Prediction is the source label (-1, 0, or 1); side is 1 for ordinary labels
// and the primary -1/+1 direction for meta-labels. Probabilities must already
// be calibrated on training data. This transformation does not calibrate them.
func ProbabilityBetSize(probability, prediction float64, classes, side int) (float64, error) {
	if !finite(probability) || probability < 0 || probability > 1 || classes < 2 || (prediction != -1 && prediction != 0 && prediction != 1) || (side != -1 && side != 1) {
		return 0, errors.New("statistics: invalid probability bet inputs")
	}
	confidence := -1.0
	if probability == 1 {
		confidence = 1
	} else if probability != 0 {
		z := (probability - 1/float64(classes)) / math.Sqrt(probability*(1-probability))
		confidence = math.Erf(z / math.Sqrt2)
	}
	return prediction * float64(side) * confidence, nil
}

// ActiveBet's interval is [Start, End), unlike the inclusive intervals used
// for training-label uniqueness. Open means the end is not yet known. Callers
// doing historical inference must provide only end information known then;
// retrospectively resolved end times are research metadata, not live features.
type ActiveBet struct {
	Start, End int64
	Open       bool
	Size       float64
}

// AverageActiveBetSizes implements snippet 10.2 at explicitly requested times.
// It includes new bets at Start, excludes expired bets at End, and returns zero
// with no active bets. Times must increase; input order does not affect results.
func AverageActiveBetSizes(bets []ActiveBet, times []int64) ([]float64, error) {
	for _, bet := range bets {
		if !finite(bet.Size) || math.Abs(bet.Size) > 1 || (!bet.Open && bet.End < bet.Start) {
			return nil, errors.New("statistics: invalid active bet")
		}
	}
	result := make([]float64, len(times))
	for i, at := range times {
		if i > 0 && at <= times[i-1] {
			return nil, errors.New("statistics: active bet times must increase")
		}
		count := 0
		for _, bet := range bets {
			if bet.Start <= at && (bet.Open || at < bet.End) {
				// Incremental mean cannot overflow with bounded input signals.
				count++
				result[i] += (bet.Size - result[i]) / float64(count)
			}
		}
	}
	return result, nil
}

// DiscretizeBetSize rounds to nearest-even as NumPy/pandas do in snippet 10.3,
// then clips to [-1,1]. Rounding ties away from zero is a different policy.
func DiscretizeBetSize(size, step float64) (float64, error) {
	if !finite(size) || !finite(step) || step <= 0 || step > 1 || !finite(size/step) {
		return 0, errors.New("statistics: invalid bet discretization")
	}
	return max(-1, min(1, math.RoundToEven(size/step)*step)), nil
}

// SigmoidBetSize is x/sqrt(width+x²), snippet 10.4. Hypot avoids
// intermediate squaring overflow for valid large price divergences.
func SigmoidBetSize(width, divergence float64) (float64, error) {
	if !finite(width) || width <= 0 || !finite(divergence) {
		return 0, errors.New("statistics: invalid sigmoid bet inputs")
	}
	return divergence / math.Hypot(math.Sqrt(width), divergence), nil
}

func CalibrateBetWidth(divergence, size float64) (float64, error) {
	if !finite(divergence) || divergence == 0 || !finite(size) || size == 0 || math.Abs(size) >= 1 || math.Signbit(size) != math.Signbit(divergence) {
		return 0, errors.New("statistics: invalid bet width calibration")
	}
	width := divergence * divergence * (1/(size*size) - 1)
	if !finite(width) || width <= 0 {
		return 0, errors.New("statistics: bet width calibration overflow or underflow")
	}
	return width, nil
}

func DynamicBetPosition(width, forecast, market float64, maximum int) (int, error) {
	if !finite(forecast) || !finite(market) || maximum <= 0 || maximum > 1000000 {
		return 0, errors.New("statistics: invalid dynamic bet position")
	}
	size, err := SigmoidBetSize(width, forecast-market)
	if err != nil {
		return 0, err
	}
	// Python int truncates toward zero. A rounded endpoint of +/-1 must not
	// produce the inverse-price singularity which the source excludes.
	position := int(size * float64(maximum))
	return max(1-maximum, min(maximum-1, position)), nil
}

func InverseBetPrice(forecast, width, size float64) (float64, error) {
	if !finite(forecast) || !finite(width) || width <= 0 || !finite(size) || math.Abs(size) >= 1 {
		return 0, errors.New("statistics: invalid inverse bet price")
	}
	price := forecast - size*math.Sqrt(width)/math.Sqrt(1-size*size)
	if !finite(price) {
		return 0, errors.New("statistics: inverse bet price overflow")
	}
	return price, nil
}

// DynamicBetLimitPrice averages the inverse price for each signed quantity
// traversed from current to target (snippet 10.4's defining equation). The
// printed abs(range) loop mishandles shorts/zero-crossings; signed traversal
// preserves the inverse function in both directions. This is a mathematical
// price bound, not an exchange order or a promise of fills/profit.
func DynamicBetLimitPrice(target, current int, forecast, width float64, maximum int) (float64, error) {
	if maximum <= 0 || maximum > 1000000 || target == current || target <= -maximum || target >= maximum || current <= -maximum || current >= maximum {
		return 0, errors.New("statistics: invalid limit-price position range")
	}
	direction := 1
	if target < current {
		direction = -1
	}
	mean, count := 0.0, 0
	for quantity := current + direction; ; quantity += direction {
		price, err := InverseBetPrice(forecast, width, float64(quantity)/float64(maximum))
		if err != nil {
			return 0, err
		}
		count++
		mean += (price - mean) / float64(count)
		if !finite(mean) {
			return 0, errors.New("statistics: limit-price average overflow")
		}
		if quantity == target {
			return mean, nil
		}
	}
}
