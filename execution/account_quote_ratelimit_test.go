package execution

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/banbox/banexg/errs"
)

func rateLimitError(code int, data any) error {
	e := errs.NewMsg(code, "venue throttled")
	e.Data = data
	return error(e)
}

func TestAccountQuoteNotVisibleIsTransient(t *testing.T) {
	base := fmt.Errorf("%w: BTC", ErrAccountQuoteNotVisible)
	for name, err := range map[string]error{
		"bare": ErrAccountQuoteNotVisible, "wrapf": base,
		"nested": fmt.Errorf("pending: %w", errors.Join(errors.New("x"), base)),
	} {
		if !transientAccountReadError(err) {
			t.Errorf("%s must be transient", name)
		}
	}
	if transientAccountReadError(errors.New("execution: account depth not yet visible or stale")) {
		t.Error("an unrelated error with the same text must not be transient")
	}
}

func TestRateLimitPauseTable(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want time.Duration
	}{
		"rate-limit seconds":   {rateLimitError(errs.CodeRateLimit, int64(7)), 7 * time.Second},
		"banned seconds":       {rateLimitError(errs.CodeTemporarilyBanned, int64(120)), 120 * time.Second},
		"wrapped":              {fmt.Errorf("w: %w", rateLimitError(errs.CodeRateLimit, int64(3))), 3 * time.Second},
		"missing data":         {rateLimitError(errs.CodeRateLimit, nil), rateLimitDefaultPause},
		"banned missing data":  {rateLimitError(errs.CodeTemporarilyBanned, nil), rateLimitDefaultPause},
		"zero seconds":         {rateLimitError(errs.CodeRateLimit, int64(0)), rateLimitDefaultPause},
		"negative seconds":     {rateLimitError(errs.CodeRateLimit, int64(-5)), rateLimitDefaultPause},
		"non-int64 data":       {rateLimitError(errs.CodeRateLimit, "5"), rateLimitDefaultPause},
		"net fail":             {rateLimitError(errs.CodeNetFail, int64(9)), 0},
		"timeout":              {rateLimitError(errs.CodeTimeout, int64(9)), 0},
		"plain error":          {errors.New("x"), 0},
		"nil":                  {nil, 0},
		"quote not visible":    {ErrAccountQuoteNotVisible, 0},
		"deadline exceeded":    {context.DeadlineExceeded, 0},
		"server error + data":  {rateLimitError(errs.CodeServerError, int64(4)), 0},
		"execution unknown":    {rateLimitError(errs.CodeExecutionUnknown, int64(4)), 0},
		"system busy with int": {rateLimitError(errs.CodeSystemBusy, int64(4)), 0},
	} {
		if got := rateLimitPause(tc.err); got != tc.want {
			t.Errorf("%s: pause %v, want %v", name, got, tc.want)
		}
	}
}

func TestTransientOutageNoteRateLimitExtendsPause(t *testing.T) {
	var o transientOutage
	if o.paused() {
		t.Fatal("fresh outage must not be paused")
	}
	o.noteRateLimit("t", errors.New("plain"))
	if o.paused() {
		t.Fatal("non rate-limit error paused polling")
	}
	o.noteRateLimit("t", rateLimitError(errs.CodeRateLimit, int64(60)))
	if !o.paused() {
		t.Fatal("rate limit did not pause")
	}
	until := o.pauseUntilMS
	o.noteRateLimit("t", rateLimitError(errs.CodeRateLimit, int64(1))) // a shorter ask never shortens
	if o.pauseUntilMS != until {
		t.Fatal("shorter Retry-After shortened the pause")
	}
}

// swapQuote replaces the account quote provider of the fixture instrument.
func swapQuote(t *testing.T, b *SharedAccountBorrow, quote func(context.Context, string, int64) (VisibleQuote, error)) {
	t.Helper()
	err := b.use(func(s *SharedAccount) error {
		m := s.markets["BTC"]
		m.quote = quote
		s.markets["BTC"] = m
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// A pending-execution step that fails only because depth is not visible yet
// skips the tick: no freeze, worker alive, the next tick recovers.
func TestReportWorkerQuoteNotVisibleSkipsTickWithoutFreeze(t *testing.T) {
	rig := newReportRig(t)
	rig.wake()
	var hidden atomic.Bool
	var calls atomic.Int64
	swapQuote(t, rig.b, func(_ context.Context, id string, now int64) (VisibleQuote, error) {
		calls.Add(1)
		if hidden.Load() {
			return VisibleQuote{}, fmt.Errorf("%w: %s", ErrAccountQuoteNotVisible, id)
		}
		return VisibleQuote{Bid: intentPrice("100"), Ask: intentPrice("100"), AtMS: now, ReceivedMS: now, ValidUntilMS: now + 3_600_000, Bar: now}, nil
	})
	if _, err := rig.b.ApplyPendingCommand(context.Background(), rig.createCommand(), time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	rig.wake()
	rig.assertHealthy("baseline with a pending entry")
	if calls.Load() == 0 {
		t.Fatal("fixture: pending execution never asked for a quote")
	}
	hidden.Store(true)
	before := calls.Load()
	rig.wake()
	if calls.Load() == before {
		t.Fatal("pending execution did not reach the hidden quote")
	}
	rig.assertHealthy("quote not visible")
	rig.wake()
	rig.assertHealthy("quote still not visible")
	hidden.Store(false)
	rig.wake()
	rig.assertHealthy("quote visible again")
	if !rig.ready() {
		t.Fatal("account not ready after recovery")
	}
	rig.wantAccepted()
}

// A venue rate limit pauses account-changed reconciles for its Retry-After and
// never freezes; the first wake-up after the window reconciles again.
func TestReportWorkerRateLimitPausesAccountChangedWakeups(t *testing.T) {
	rig := newReportRig(t)
	rig.wake()
	rig.assertHealthy("baseline")
	rig.fake.set(func() { rig.fake.snapErr = rateLimitError(errs.CodeRateLimit, int64(2)) })
	rig.wake()
	rig.assertHealthy("after rate-limited snapshot")
	if rig.ready() {
		t.Fatal("account must be unready while the snapshot is rate limited")
	}
	_, limited := rig.fake.counts()
	// The venue recovers, but wake-ups inside the Retry-After window must not poll.
	rig.fake.set(func() { rig.fake.snapErr = nil })
	for i := 0; i < 3; i++ {
		rig.fake.reports <- BanexgStreamReport{AccountChanged: true}
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(rig.fake.reports) > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	if _, n := rig.fake.counts(); n != limited {
		t.Fatalf("Snapshot called %d times inside the rate-limit pause (was %d)", n, limited)
	}
	rig.assertHealthy("inside the pause")
	time.Sleep(2200 * time.Millisecond)
	rig.wake()
	if _, n := rig.fake.counts(); n <= limited {
		t.Fatalf("no reconcile after the pause: Snapshot calls %d, was %d", n, limited)
	}
	rig.assertHealthy("after the pause")
	if !rig.ready() {
		t.Fatal("account not ready after the pause")
	}
}
