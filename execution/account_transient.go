package execution

import (
	"context"
	"errors"
	"time"

	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

// ErrAccountQuoteNotVisible: the account market stream has no fresh depth for
// an instrument yet (just after registration) or it went stale. Pending
// execution cannot evaluate that instrument this tick; it is retried, never a
// reason to freeze the account.
var ErrAccountQuoteNotVisible = errors.New("execution: account depth not yet visible or stale")

// fundingRecoveryInterval bounds how long the funding watermark may lag while
// no reconciliation mismatch asks for a fetch. Venues settle funding every one
// to eight hours; the income endpoint is heavily rate limited.
const fundingRecoveryInterval = 5 * time.Minute

// fundingRetryBackoff delays the next scheduled funding fetch after a
// transient failure (an on-demand fetch after a cash mismatch is not delayed).
const fundingRetryBackoff = 30 * time.Second

// privateReconcileInterval spaces the periodic full venue snapshots of the
// private report worker (balance, positions, all open and conditional orders).
// Account-change wake-ups, order reports and transient outages reconcile
// sooner; ticks in between only advance pending execution.
var privateReconcileInterval = 30 * time.Second

// rateLimitDefaultPause is used when a rate-limit error carries no Retry-After.
const rateLimitDefaultPause = 30 * time.Second

// transientOutageAlarm is how long a transient read failure may persist before
// it is logged as an error (it keeps the account unready meanwhile).
const transientOutageAlarm = time.Minute

// transientAccountReadError reports venue read failures expected to clear by
// themselves: network failures, timeouts, rate limits, temporary bans, venue
// overload and indeterminate read results. The private report worker skips the
// tick for these instead of freezing the account and stopping: the account
// stays unready (new entries are refused with RejectAccountNotReady) until a
// later reconciliation succeeds. Any other error keeps the fail-closed freeze.
func transientAccountReadError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrAccountQuoteNotVisible) {
		return true
	}
	var e *errs.Error
	if errors.As(err, &e) && e != nil {
		switch e.Code {
		case errs.CodeNetFail, errs.CodeTimeout, errs.CodeSystemBusy, errs.CodeServerError,
			errs.CodeRateLimit, errs.CodeTemporarilyBanned, errs.CodeExecutionUnknown:
			return true
		}
	}
	return false
}

// TransientVenueReadError is transientAccountReadError for callers outside the
// account owner (for example live data sources polling venue endpoints).
func TransientVenueReadError(err error) bool { return transientAccountReadError(err) }

// transientOutage tracks one continuous run of transient read failures of the
// report worker for logging. It is used by the worker goroutine only.
type transientOutage struct {
	sinceMS int64
	alarmed bool
	// pauseUntilMS honours a venue rate limit: polling inside the venue's
	// Retry-After window can escalate a 429 into an IP ban.
	pauseUntilMS int64
}

// rateLimitPause is the back-off a rate-limit error asks for: the venue's
// Retry-After when banexg carries it, else rateLimitDefaultPause.
func rateLimitPause(err error) time.Duration {
	var e *errs.Error
	if !errors.As(err, &e) || e == nil || e.Code != errs.CodeRateLimit && e.Code != errs.CodeTemporarilyBanned {
		return 0
	}
	if seconds, ok := e.Data.(int64); ok && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return rateLimitDefaultPause
}

// noteRateLimit extends the polling pause when err is a venue rate limit.
func (o *transientOutage) noteRateLimit(stage string, err error) {
	now := time.Now().UnixMilli()
	if pause := rateLimitPause(err); pause > 0 && now+pause.Milliseconds() > o.pauseUntilMS {
		o.pauseUntilMS = now + pause.Milliseconds()
		log.Warn("venue rate limit; pausing account polling", zap.String("stage", stage), zap.Duration("pause", pause))
	}
}

// paused reports whether periodic venue polling must wait for a rate limit.
func (o *transientOutage) paused() bool { return time.Now().UnixMilli() < o.pauseUntilMS }

// transient classifies err and logs the start of an outage (warn) and its
// persistence beyond transientOutageAlarm (error, once).
func (o *transientOutage) transient(stage string, err error) bool {
	if !transientAccountReadError(err) {
		return false
	}
	o.noteRateLimit(stage, err)
	now := time.Now().UnixMilli()
	if o.sinceMS == 0 {
		o.sinceMS = now
		log.Warn("account venue read failed transiently; account unready, retrying", zap.String("stage", stage), zap.Error(err))
	} else if !o.alarmed && now-o.sinceMS >= transientOutageAlarm.Milliseconds() {
		o.alarmed = true
		log.Error("account venue reads keep failing; new entries are refused until they recover", zap.String("stage", stage), zap.Duration("for", time.Duration(now-o.sinceMS)*time.Millisecond), zap.Error(err))
	}
	return true
}

func (o *transientOutage) recovered() {
	if o.sinceMS != 0 {
		log.Info("account venue reads recovered", zap.Duration("after", time.Duration(time.Now().UnixMilli()-o.sinceMS)*time.Millisecond))
	}
	*o = transientOutage{}
}
