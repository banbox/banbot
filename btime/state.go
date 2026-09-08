package btime

import (
	"sync/atomic"
	"time"

	"github.com/banbox/bntp"
)

// ClockState is the per-Runtime clock. Its simulation timestamp is safe for
// concurrent reads and monotonic increments; callers still own higher-level
// time transitions such as moving to the next bar.
type ClockState struct {
	backtest  bool
	curTimeMS int64
	locShow   *time.Location
}

func NewClockState(backtest bool, loc *time.Location) *ClockState {
	if loc == nil {
		loc = UTCLocale
	}
	return &ClockState{backtest: backtest, locShow: loc}
}

func (c *ClockState) SetTimeMS(timeMS int64) {
	if c != nil {
		atomic.StoreInt64(&c.curTimeMS, timeMS)
	}
}

func (c *ClockState) TimeMS() int64 {
	if c == nil || !c.backtest {
		return UTCStamp()
	}
	if timeMS := atomic.LoadInt64(&c.curTimeMS); timeMS != 0 {
		return timeMS
	}
	now := UTCStamp()
	if atomic.CompareAndSwapInt64(&c.curTimeMS, 0, now) {
		return now
	}
	return atomic.LoadInt64(&c.curTimeMS)
}

// AdvanceMS atomically advances a backtest clock and returns its new value.
func (c *ClockState) AdvanceMS(delta int64) int64 {
	if c == nil || !c.backtest {
		return UTCStamp()
	}
	c.TimeMS()
	return atomic.AddInt64(&c.curTimeMS, delta)
}

func (c *ClockState) Time() float64 {
	return float64(c.TimeMS()) * 0.001
}

func (c *ClockState) Now() *time.Time {
	if c != nil && c.backtest {
		return MSToTime(c.TimeMS())
	}
	now := bntp.Now().In(c.location())
	return &now
}

func (c *ClockState) location() *time.Location {
	if c == nil || c.locShow == nil {
		return UTCLocale
	}
	return c.locShow
}

// SetTimeMS updates the legacy compatibility clock without a torn write.
func SetTimeMS(timeMS int64) {
	atomic.StoreInt64(&CurTimeMS, timeMS)
}

// AdvanceTimeMS atomically advances the legacy compatibility clock.
func AdvanceTimeMS(delta int64) int64 {
	return atomic.AddInt64(&CurTimeMS, delta)
}
