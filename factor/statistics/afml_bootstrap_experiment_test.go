package statistics

import (
	"context"
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func TestRandomBootstrapEventsSourceOverwrite(t *testing.T) {
	spans, err := RandomBootstrapEvents(10, 1, 5, rand.New(rand.NewSource(12)))
	if err != nil || len(spans) != 1 {
		t.Fatal(spans, err)
	}
	rng := rand.New(rand.NewSource(12))
	end := 0
	for range 10 {
		rng.Intn(1)
		end = 1 + rng.Intn(4)
	}
	if spans[0] != (EventSpan{0, end}) {
		t.Fatal(spans, end)
	}
	if _, err := RandomBootstrapEvents(1, 1, 1, rng); err == nil {
		t.Fatal("empty duration range accepted")
	}
}

func TestBootstrapANOVATailRepresentableSmallProbability(t *testing.T) {
	// Independent high-precision incomplete-beta reference values (df1=1).
	for _, tc := range []struct{ f, want float64 }{{75, 5.058577391843e-18}, {100, 1.730480096458e-23}} {
		got := bootstrapANOVATail(tc.f, 19998)
		if math.Abs(got/tc.want-1) > 1e-6 {
			t.Fatal(tc.f, got, tc.want)
		}
	}
	if got := bootstrapANOVATail(396365.6581442458, 1999998); got != 0 {
		t.Fatal("expected true underflow", got)
	}
}

type bootstrapCountingContext struct {
	context.Context
	calls int
}

func (c *bootstrapCountingContext) Err() error {
	c.calls++
	if c.calls >= 20 {
		return context.Canceled
	}
	return nil
}

func TestSequentialBootstrapMidPathCancellation(t *testing.T) {
	ctx := &bootstrapCountingContext{Context: context.Background()}
	spans := []EventSpan{{0, 99999}, {0, 99999}}
	out, err := SequentialBootstrapContext(ctx, 100000, spans, 10, 21)
	if err != context.Canceled || out != nil || ctx.calls > 22 {
		t.Fatal(out, err, ctx.calls)
	}
	plain, err := SequentialBootstrap(100000, spans, 3, 21)
	causal, e := SequentialBootstrapContext(context.Background(), 100000, spans, 3, 21)
	if err != nil || e != nil || !reflect.DeepEqual(plain, causal) {
		t.Fatal(plain, causal, err, e)
	}
	deadline, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = RunBootstrapExperiment(deadline, BootstrapExperimentConfig{1000, 10000, 1000, 2, 1, 21, 3000000000})
	if err == nil || time.Since(start) > 2*time.Second {
		t.Fatal("in-flight experiment cancellation", err, time.Since(start))
	}
}

func TestSampledUniquenessIndependentIndicator(t *testing.T) {
	spans := []EventSpan{{0, 2}, {1, 3}, {4, 4}}
	indices := []int{0, 0, 1, 2}
	got, err := sampledUniqueness(5, spans, indices)
	// Indicator rows have concurrency [2,3,3,1,1]. Two copies of
	// event0 each get(1/2+1/3+1/3)/3; event1 gets(1/3+1/3+1)/3.
	want := (2*(.5+2./3)/3 + (2./3+1)/3 + 1) / 4
	if err != nil || math.Abs(got-want) > 1e-14 {
		t.Fatal(got, want, err)
	}
}

func TestBootstrapExperimentDistributionParallelAndBudget(t *testing.T) {
	c := BootstrapExperimentConfig{10, 100, 5, 10000, 1, 21, 10000000}
	a, err := RunBootstrapExperiment(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	c.Workers = 4
	b, err := RunBootstrapExperiment(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	a.Config.Workers = 4
	if !reflect.DeepEqual(a, b) {
		t.Fatal("worker schedule changed experiment", a, b)
	}
	if b.Sequential.Mean <= b.Standard.Mean || b.PairedMeanDifference < 10*b.PairedStandardError || b.MeanDistinctEvents >= 10 || b.ANOVAIndependentGroupP > .001 {
		t.Fatal(b)
	}
	n := float64(c.Trials)
	f := n * math.Pow(b.Standard.Mean-b.Sequential.Mean, 2) / (b.Standard.SampleVariance + b.Sequential.SampleVariance)
	if math.Abs(f-b.ANOVAF) > 1e-8 {
		t.Fatal(f, b.ANOVAF)
	}
	c.MaxWork = 1
	if _, err := RunBootstrapExperiment(context.Background(), c); err == nil {
		t.Fatal("work budget ignored")
	}
	c.MaxWork = 10000000
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := RunBootstrapExperiment(ctx, c); err == nil {
		t.Fatal("cancellation ignored")
	}
	c.Observations = int(^uint(0) >> 1)
	if _, err := RunBootstrapExperiment(context.Background(), c); err == nil {
		t.Fatal("overflow dimensions accepted")
	}
}
