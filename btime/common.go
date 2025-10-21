package btime

import (
	"math"
	"sync"
)

type RetryWaits struct {
	fails       map[string]int
	retryAfters map[string]int64
	sleeps      []int64 // milliseconds
	lock        sync.Mutex
}

// NewRetryWaits waits is array of sleep milliseconds while fail
func NewRetryWaits(rate float64, waits []int64) *RetryWaits {
	if len(waits) == 0 {
		waits = []int64{3000, 6000, 12000, 30000}
	}
	res := &RetryWaits{
		fails:       make(map[string]int),
		retryAfters: make(map[string]int64),
	}
	for _, v := range waits {
		if v <= 0 {
			continue
		}
		if rate > 0 {
			v = int64(math.Round(float64(v) * rate))
		}
		res.sleeps = append(res.sleeps, v)
	}
	return res
}

func (r *RetryWaits) SetFail(key string) int64 {
	r.lock.Lock()
	num, _ := r.fails[key]
	r.fails[key] = num + 1
	if num >= len(r.sleeps) {
		num = len(r.sleeps) - 1
	}
	wait := r.sleeps[num]
	nextRetry := UTCStamp() + wait
	r.retryAfters[key] = nextRetry
	r.lock.Unlock()
	return nextRetry
}

func (r *RetryWaits) NextRetry(key string) int64 {
	r.lock.Lock()
	v, _ := r.retryAfters[key]
	r.lock.Unlock()
	return v
}

func (r *RetryWaits) Reset(key string) {
	r.lock.Lock()
	delete(r.fails, key)
	r.lock.Unlock()
}
