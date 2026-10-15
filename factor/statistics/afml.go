package statistics

import (
	"context"
	"errors"
	"math"
	"math/rand"
)

func afmlFinite(x float64) bool { return !math.IsNaN(x) && !math.IsInf(x, 0) }

// CUSUMEvents implements AFML snippet 2.4 on levels (use log prices for log returns).
// A strict crossing resets only the triggered accumulator, as in the source.
func CUSUMEvents(values []float64, threshold float64) ([]int, error) {
	if !afmlFinite(threshold) || threshold <= 0 {
		return nil, errors.New("statistics: CUSUM threshold must be positive")
	}
	var out []int
	pos, neg := 0., 0.
	for i, x := range values {
		if !afmlFinite(x) {
			return nil, errors.New("statistics: CUSUM needs finite levels")
		}
		if i == 0 {
			continue
		}
		delta := x - values[i-1]
		if !afmlFinite(delta) {
			return nil, errors.New("statistics: CUSUM difference overflow")
		}
		pos, neg = math.Max(0, pos+delta), math.Min(0, neg+delta)
		if neg < -threshold {
			neg = 0
			out = append(out, i)
		} else if pos > threshold {
			pos = 0
			out = append(out, i)
		}
	}
	return out, nil
}

// EventSpan uses inclusive bar indices, matching the source indicator matrix.
// Spans and labels are research outcomes: never feed their future End into inference.
type EventSpan struct{ Start, End int }

type BarrierLabel struct {
	End                int
	Return, SideReturn float64
	Label              int
	Barrier            string
	Resolved           bool
}

// TripleBarrierLabel implements snippets 3.2/3.7 on close observations, not OHLC.
// vertical=-1 disables expiry; disabled horizontal factors are zero. An event
// with no observed touch remains unresolved rather than inventing an expiry.
// Threshold and side must have been available at Start; fitting uses End only
// after maturity. Meta labels are 1 for a positive side-adjusted return, else 0.
func TripleBarrierLabel(close []float64, start, vertical int, target, profitFactor, stopFactor float64, side int, meta bool) (BarrierLabel, error) {
	if start < 0 || start >= len(close) || vertical < -1 || (vertical >= 0 && (vertical < start || vertical >= len(close))) || (side != 1 && side != -1) || !afmlFinite(target) || target <= 0 || !afmlFinite(profitFactor) || profitFactor < 0 || !afmlFinite(stopFactor) || stopFactor < 0 {
		return BarrierLabel{}, errors.New("statistics: invalid triple barrier specification")
	}
	if !meta && profitFactor != stopFactor {
		return BarrierLabel{}, errors.New("statistics: learning side requires symmetric horizontal barriers")
	}
	last := len(close) - 1
	if vertical >= 0 {
		last = vertical
	}
	if !afmlFinite(target*profitFactor) || !afmlFinite(target*stopFactor) {
		return BarrierLabel{}, errors.New("statistics: barrier threshold overflow")
	}
	result := BarrierLabel{End: -1}
	for i := start; i <= last; i++ {
		if !afmlFinite(close[i]) || close[i] <= 0 {
			return BarrierLabel{}, errors.New("statistics: barrier path needs positive finite close prices")
		}
		r := close[i]/close[start] - 1
		sr := r * float64(side)
		if !afmlFinite(r) {
			return BarrierLabel{}, errors.New("statistics: barrier return overflow")
		}
		barrier := ""
		if stopFactor > 0 && sr < -target*stopFactor {
			barrier = "stop"
		} else if profitFactor > 0 && sr > target*profitFactor {
			barrier = "profit"
		} else if i == vertical {
			barrier = "vertical"
		}
		if barrier == "" {
			continue
		}
		label := 0
		if meta {
			if sr > 0 {
				label = 1
			}
		} else if r > 0 {
			label = 1
		} else if r < 0 {
			label = -1
		}
		return BarrierLabel{End: i, Return: r, SideReturn: sr, Label: label, Barrier: barrier, Resolved: true}, nil
	}
	return result, nil
}

func afmlValidateSpans(barCount int, spans []EventSpan) error {
	if barCount <= 0 || len(spans) == 0 {
		return errors.New("statistics: empty event sample")
	}
	for _, s := range spans {
		if s.Start < 0 || s.End < s.Start || s.End >= barCount {
			return errors.New("statistics: invalid event span")
		}
	}
	return nil
}

// EventUniqueness returns mean reciprocal concurrency over each event's lifespan
// (AFML 4.3/4.4). No dense bars-by-events indicator matrix is required.
func EventUniqueness(barCount int, spans []EventSpan) ([]float64, error) {
	if err := afmlValidateSpans(barCount, spans); err != nil {
		return nil, err
	}
	diff := make([]int, barCount+1)
	for _, s := range spans {
		diff[s.Start]++
		diff[s.End+1]--
	}
	concurrent := 0
	prefix := make([]float64, barCount+1)
	for i := 0; i < barCount; i++ {
		concurrent += diff[i]
		prefix[i+1] = prefix[i]
		if concurrent > 0 {
			prefix[i+1] += 1 / float64(concurrent)
		}
	}
	out := make([]float64, len(spans))
	for i, s := range spans {
		out[i] = (prefix[s.End+1] - prefix[s.Start]) / float64(s.End-s.Start+1)
	}
	return out, nil
}

// SequentialBootstrap samples event indices with replacement using marginal
// average uniqueness (snippet 4.5); a local seeded RNG makes runs reproducible.
func SequentialBootstrap(barCount int, spans []EventSpan, draws int, seed int64) ([]int, error) {
	return SequentialBootstrapContext(context.Background(), barCount, spans, draws, seed)
}

// SequentialBootstrapContext permits bounded cancellation inside the costly
// candidate/path scans; the ordinary wrapper retains its seeded output.
func SequentialBootstrapContext(ctx context.Context, barCount int, spans []EventSpan, draws int, seed int64) ([]int, error) {
	if ctx == nil {
		return nil, errors.New("statistics: nil bootstrap context")
	}
	if err := afmlValidateSpans(barCount, spans); err != nil {
		return nil, err
	}
	if draws < 1 {
		return nil, errors.New("statistics: bootstrap draws must be positive")
	}
	rng := rand.New(rand.NewSource(seed))
	counts := make([]int, barCount)
	weights := make([]float64, len(spans))
	out := make([]int, 0, draws)
	for len(out) < draws {
		if err := ctx.Err(); err != nil {
			return nil, context.Cause(ctx)
		}
		total := 0.
		for i, s := range spans {
			if err := ctx.Err(); err != nil {
				return nil, context.Cause(ctx)
			}
			w := 0.
			for t := s.Start; t <= s.End; t++ {
				if (t-s.Start)%1024 == 0 {
					if err := ctx.Err(); err != nil {
						return nil, context.Cause(ctx)
					}
				}
				w += 1 / float64(counts[t]+1)
			}
			weights[i] = w / float64(s.End-s.Start+1)
			total += weights[i]
		}
		draw := rng.Float64() * total
		selected := len(spans) - 1
		for i, w := range weights {
			draw -= w
			if draw < 0 {
				selected = i
				break
			}
		}
		out = append(out, selected)
		for t := spans[selected].Start; t <= spans[selected].End; t++ {
			counts[t]++
		}
	}
	return out, nil
}

// FractionalDiffWeights returns newest-to-oldest coefficients with w[0]=1 and
// w[k]=-w[k-1]*(d-k+1)/k. It stops BEFORE a coefficient below threshold (5.3).
// maxWeights is a resource bound; a truncated nonconverged kernel is an error.
func FractionalDiffWeights(d, threshold float64, maxWeights int) ([]float64, error) {
	if !afmlFinite(d) || d < 0 || !afmlFinite(threshold) || threshold <= 0 || threshold >= 1 || maxWeights < 1 {
		return nil, errors.New("statistics: invalid fractional difference parameters")
	}
	w := []float64{1}
	for k := 1; ; k++ {
		next := -w[k-1] * (d - float64(k) + 1) / float64(k)
		if !afmlFinite(next) {
			return nil, errors.New("statistics: fractional weight overflow")
		}
		if math.Abs(next) < threshold {
			return w, nil
		}
		if len(w) == maxWeights {
			return nil, errors.New("statistics: fractional kernel exceeds maxWeights")
		}
		w = append(w, next)
	}
}

// FractionalDifference applies a fixed-width kernel causally. NaN is missing;
// past NaNs are forward-filled as in 5.3, but a missing current bar stays NaN.
// Initial results without a complete history remain NaN. Inf is rejected.
func FractionalDifference(values, weights []float64) ([]float64, error) {
	if len(weights) == 0 || weights[0] != 1 {
		return nil, errors.New("statistics: invalid fractional kernel")
	}
	for _, w := range weights {
		if !afmlFinite(w) {
			return nil, errors.New("statistics: nonfinite fractional kernel")
		}
	}
	out := make([]float64, len(values))
	filled := make([]float64, len(values))
	last := math.NaN()
	for i, x := range values {
		out[i] = math.NaN()
		if math.IsInf(x, 0) {
			return nil, errors.New("statistics: infinite fractional input")
		}
		if !math.IsNaN(x) {
			last = x
		}
		filled[i] = last
		if i < len(weights)-1 || math.IsNaN(x) || math.IsNaN(filled[i-len(weights)+1]) {
			continue
		}
		sum := 0.
		for k, w := range weights {
			sum += w * filled[i-k]
		}
		if !afmlFinite(sum) {
			return nil, errors.New("statistics: fractional result overflow")
		}
		out[i] = sum
	}
	return out, nil
}
