package statistics

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"sort"

	"gonum.org/v1/gonum/mathext"
)

type BootstrapExperimentConfig struct {
	Observations, Bars, MaxHorizon, Trials, Workers int
	Seed                                            int64
	MaxWork                                         uint64
}

type BootstrapDistribution struct{ Mean, Median, SampleVariance float64 }
type BootstrapExperimentResult struct {
	Config                                    BootstrapExperimentConfig
	Standard, Sequential                      BootstrapDistribution
	PairedMeanDifference, PairedStandardError float64
	ANOVAF, ANOVAIndependentGroupP            float64
	MeanDistinctEvents                        float64
}

// RandomBootstrapEvents matches snippet4.7: repeated start keys overwrite,
// duration is uniform on [1,maxH), and event endpoints are inclusive. RNG
// identity is explicit; numpy and Go seeds do not generate identical samples.
func RandomBootstrapEvents(observations, bars, maxH int, rng *rand.Rand) ([]EventSpan, error) {
	if observations < 1 || observations > 10000 || bars < 1 || bars > 100000 || maxH < 2 || maxH > 100000 || rng == nil {
		return nil, errors.New("statistics: invalid bootstrap event dimensions")
	}
	ends := make(map[int]int, min(observations, bars))
	for range observations {
		start := rng.Intn(bars)
		ends[start] = start + 1 + rng.Intn(maxH-1)
	}
	starts := make([]int, 0, len(ends))
	for start := range ends {
		starts = append(starts, start)
	}
	sort.Ints(starts)
	spans := make([]EventSpan, len(starts))
	for i, start := range starts {
		spans[i] = EventSpan{Start: start, End: ends[start]}
	}
	return spans, nil
}

func sampledUniqueness(bars int, spans []EventSpan, indices []int) (float64, error) {
	sampled := make([]EventSpan, len(indices))
	for i, index := range indices {
		sampled[i] = spans[index]
	}
	u, err := EventUniqueness(bars, sampled)
	if err != nil {
		return 0, err
	}
	mean, _, err := MeanStd(u, 0)
	return mean, err
}

// RunBootstrapExperiment reproduces the standard/sequential uniqueness
// comparison. ANOVA is reported under the source independent-group null;
// paired standard error separately respects the shared event set per trial.
// Neither statistic establishes market alpha or general IID financial data.
func RunBootstrapExperiment(ctx context.Context, c BootstrapExperimentConfig) (BootstrapExperimentResult, error) {
	var empty BootstrapExperimentResult
	if ctx == nil || c.Trials < 2 || c.Trials > 1000000 || c.Workers < 1 || c.Workers > 256 || c.Observations < 1 || c.Observations > 10000 || c.Bars < 1 || c.Bars > 100000 || c.MaxHorizon < 2 || c.MaxHorizon > 100000 {
		return empty, errors.New("statistics: invalid bootstrap experiment configuration")
	}
	// Conservative candidate-path work plus bar scans, checked before allocation.
	per := uint64(c.Observations)*uint64(c.Observations)*uint64(c.MaxHorizon+1) + 2*uint64(c.Bars+c.MaxHorizon)
	if c.MaxWork == 0 || uint64(c.Trials) > c.MaxWork/per {
		return empty, errors.New("statistics: bootstrap experiment work budget exceeded")
	}
	type trial struct {
		ordinary, sequential float64
		distinct             int
	}
	chunks := (c.Trials + 999) / 1000
	values, err := DispatchReduce(ctx, chunks, c.Workers, make([]trial, 0, c.Trials), func(ctx context.Context, chunk int) ([]trial, error) {
		out := make([]trial, 0, min(1000, c.Trials-chunk*1000))
		for id := chunk * 1000; id < min(c.Trials, (chunk+1)*1000); id++ {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			// Unsigned wrap defines deterministic seed arithmetic at int64 limits.
			seed := int64(uint64(c.Seed) + uint64(id)*0x9e3779b97f4a7c15)
			rng := rand.New(rand.NewSource(seed))
			spans, err := RandomBootstrapEvents(c.Observations, c.Bars, c.MaxHorizon, rng)
			if err != nil {
				return nil, err
			}
			indices := make([]int, len(spans))
			for i := range indices {
				indices[i] = rng.Intn(len(spans))
			}
			a, err := sampledUniqueness(c.Bars+c.MaxHorizon, spans, indices)
			if err != nil {
				return nil, err
			}
			seq, err := SequentialBootstrapContext(ctx, c.Bars+c.MaxHorizon, spans, len(spans), rng.Int63())
			if err != nil {
				return nil, err
			}
			b, err := sampledUniqueness(c.Bars+c.MaxHorizon, spans, seq)
			if err != nil {
				return nil, err
			}
			out = append(out, trial{a, b, len(spans)})
		}
		return out, nil
	}, func(all, chunk []trial) ([]trial, error) { return append(all, chunk...), nil })
	if err != nil {
		return empty, err
	}
	a, b, d := make([]float64, c.Trials), make([]float64, c.Trials), make([]float64, c.Trials)
	distinct := 0.
	for i, v := range values {
		a[i], b[i], d[i] = v.ordinary, v.sequential, v.sequential-v.ordinary
		distinct += float64(v.distinct)
	}
	stats := func(x []float64) BootstrapDistribution {
		m, sd, _ := MeanStd(x, 1)
		sort.Float64s(x)
		median := x[len(x)/2]
		if len(x)%2 == 0 {
			median = (x[len(x)/2-1] + median) / 2
		}
		return BootstrapDistribution{m, median, sd * sd}
	}
	ma, sa, _ := MeanStd(a, 1)
	mb, sb, _ := MeanStd(b, 1)
	md, sd, _ := MeanStd(d, 1)
	within := (sa*sa + sb*sb) / 2
	f, p := 0., 1.
	if within > 0 {
		f = float64(c.Trials) * (ma - mb) * (ma - mb) / (2 * within)
		p = bootstrapANOVATail(f, float64(2*c.Trials-2))
	} else if ma != mb {
		return empty, errors.New("statistics: degenerate bootstrap ANOVA")
	}
	if math.IsNaN(f) || math.IsNaN(p) {
		return empty, errors.New("statistics: invalid bootstrap comparison")
	}
	return BootstrapExperimentResult{c, stats(a), stats(b), md, sd / math.Sqrt(float64(c.Trials)), f, p, distinct / float64(c.Trials)}, nil
}

// Complementary incomplete beta avoids the 1-CDF cancellation floor. Truly
// unrepresentable positive tails may still underflow to zero.
func bootstrapANOVATail(f, df2 float64) float64 {
	return mathext.RegIncBeta(df2/2, .5, df2/(df2+f))
}
