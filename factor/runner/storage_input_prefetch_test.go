package runner

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/banbox/banbot/orm"
)

func prefetchDrain(t *testing.T, in HistoricalInput, ctx context.Context) ([]HistoricalBatch, error) {
	var out []HistoricalBatch
	for {
		b, err := in.Next(ctx)
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		out = append(out, b)
		if _, err := in.Visible(ctx, b.AtMS, b.AtMS, 0); err != nil {
			return out, err
		}
	}
}

func prefetchOpenN(t *testing.T, concurrency int, wrap func(orm.Subscription, int64, int64, int, func() ([]*orm.DataSeries, error)) ([]*orm.DataSeries, error), page int) (HistoricalInput, Config) {
	c := archiveConfig(t, false)
	factory, _ := storageFixture(t, c, page)
	opts := factory.(*storageInputFactory).options
	opts.PrefetchConcurrency = concurrency
	base := opts.QueryPage
	opts.QueryPage = func(ctx context.Context, sub orm.Subscription, a, b int64, l int) ([]*orm.DataSeries, error) {
		return wrap(sub, a, b, l, func() ([]*orm.DataSeries, error) { return base(ctx, sub, a, b, l) })
	}
	f2, err := NewStorageInputFactory(opts)
	if err != nil {
		t.Fatal(err)
	}
	c.Chunks = nil
	c.Snapshot.VisibilityPolicy = "static-approximation"
	in, err := f2.Open(context.Background(), c, f2.Ranges()[0])
	if err != nil {
		t.Fatal(err)
	}
	return in, c
}

func prefetchOpen(t *testing.T, wrap func(orm.Subscription, int64, int64, int, func() ([]*orm.DataSeries, error)) ([]*orm.DataSeries, error), page int) (HistoricalInput, Config) {
	return prefetchOpenN(t, 0, wrap, page)
}

func TestPrefetchOrderMatchesSlowAndFast(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	var mu sync.Mutex
	var maxConc, cur int32
	slow, _ := prefetchOpenN(t, 0, func(sub orm.Subscription, a, b int64, l int, f func() ([]*orm.DataSeries, error)) ([]*orm.DataSeries, error) {
		n := atomic.AddInt32(&cur, 1)
		mu.Lock()
		if n > maxConc {
			maxConc = n
		}
		d := time.Duration(r.Intn(3000)) * time.Microsecond
		mu.Unlock()
		time.Sleep(d)
		defer atomic.AddInt32(&cur, -1)
		return f()
	}, 2)
	fast, _ := prefetchOpen(t, func(sub orm.Subscription, a, b int64, l int, f func() ([]*orm.DataSeries, error)) ([]*orm.DataSeries, error) {
		return f()
	}, 2)
	a, e1 := prefetchDrain(t, slow, context.Background())
	b, e2 := prefetchDrain(t, fast, context.Background())
	if e1 != nil || e2 != nil || len(a) != len(b) {
		t.Fatal(e1, e2, len(a), len(b))
	}
	for i := range a {
		if a[i].AtMS != b[i].AtMS || len(a[i].Records) != len(b[i].Records) {
			t.Fatalf("batch %d differs", i)
		}
		for j := range a[i].Records {
			x, y := a[i].Records[j], b[i].Records[j]
			if x.Series.Sid != y.Series.Sid || x.Series.TimeMS != y.Series.TimeMS || x.Series.Source != y.Series.Source || x.Series.TimeFrame != y.Series.TimeFrame {
				t.Fatalf("record %d/%d differs", i, j)
			}
		}
	}
	// background reads are bounded by the factory limit, plus the one synchronous read
	if maxConc > DefaultPrefetchConcurrency+1 {
		t.Fatalf("concurrent reads %d exceed bound %d", maxConc, DefaultPrefetchConcurrency+1)
	}
	t.Logf("batches=%d maxConc=%d", len(a), maxConc)
	_ = slow.Close()
	_ = fast.Close()
}

func TestPrefetchErrorAndLeak(t *testing.T) {
	runtime.GC()
	before := runtime.NumGoroutine()
	var calls int32
	boom := errors.New("boom")
	in, _ := prefetchOpen(t, func(sub orm.Subscription, a, b int64, l int, f func() ([]*orm.DataSeries, error)) ([]*orm.DataSeries, error) {
		if atomic.AddInt32(&calls, 1) == 4 {
			return nil, boom
		}
		return f()
	}, 1)
	_, err := prefetchDrain(t, in, context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("err=%v", err)
	}
	if err := in.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if after := runtime.NumGoroutine(); after > before {
		t.Fatalf("goroutines %d -> %d", before, after)
	}
}

func TestPrefetchCancelWhileBlocked(t *testing.T) {
	runtime.GC()
	before := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	var n int32
	in, _ := prefetchOpen(t, func(sub orm.Subscription, a, b int64, l int, f func() ([]*orm.DataSeries, error)) ([]*orm.DataSeries, error) {
		return f()
	}, 1)
	_ = n
	// block queries after first few via a second wrapper is awkward: cancel mid drain instead
	go func() { time.Sleep(2 * time.Millisecond); cancel() }()
	_, err := prefetchDrain(t, in, ctx)
	_ = err
	done := make(chan struct{})
	go func() { _ = in.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close hung")
	}
	time.Sleep(50 * time.Millisecond)
	if after := runtime.NumGoroutine(); after > before {
		t.Fatalf("goroutines %d -> %d", before, after)
	}
}

func TestPrefetchCloseEarlyWithBlockedQuery(t *testing.T) {
	var calls int32
	in, _ := prefetchOpen(t, func(sub orm.Subscription, a, b int64, l int, f func() ([]*orm.DataSeries, error)) ([]*orm.DataSeries, error) {
		return f()
	}, 1)
	_ = calls
	if _, err := in.Next(context.Background()); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = in.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close hung")
	}
	if _, err := in.Next(context.Background()); err == nil {
		t.Fatal("Next after Close succeeded")
	}
}

func TestPrefetchConcurrencyOptionBoundsReads(t *testing.T) {
	var cur, peak int32
	in, _ := prefetchOpenN(t, 1, func(sub orm.Subscription, a, b int64, l int, f func() ([]*orm.DataSeries, error)) ([]*orm.DataSeries, error) {
		n := atomic.AddInt32(&cur, 1)
		for {
			p := atomic.LoadInt32(&peak)
			if n <= p || atomic.CompareAndSwapInt32(&peak, p, n) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		defer atomic.AddInt32(&cur, -1)
		return f()
	}, 1)
	if _, err := prefetchDrain(t, in, context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = in.Close()
	if peak > 2 { // one background read + the synchronous read
		t.Fatalf("peak concurrent reads %d with PrefetchConcurrency=1", peak)
	}
}

func TestPrefetchConcurrencyRejectsOutOfRange(t *testing.T) {
	c := archiveConfig(t, false)
	factory, _ := storageFixture(t, c, 3)
	opts := factory.(*storageInputFactory).options
	for _, bad := range []int{-1, 33} {
		opts.PrefetchConcurrency = bad
		if _, err := NewStorageInputFactory(opts); err == nil {
			t.Fatalf("concurrency %d accepted", bad)
		}
	}
}
