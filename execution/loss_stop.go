package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/shopspring/decimal"
)

// LossStopWindow pauses new entries when account equity fell by MaxLossRate or
// more over the trailing WindowMS.
type LossStopWindow struct {
	WindowMS    int64
	MaxLossRate decimal.Decimal
}

// LossStopPolicy is the account-level loss circuit breaker. Equity is settled
// cash plus the marked value of every open position, so realized results, fees,
// funding and floating losses all count. A breach pauses entries for PauseMS;
// reductions and protection are never paused. An empty policy disables it.
type LossStopPolicy struct {
	Windows []LossStopWindow
	PauseMS int64
}

func (p LossStopPolicy) enabled() bool { return len(p.Windows) > 0 }

func (p LossStopPolicy) validate() error {
	if !p.enabled() {
		if p.PauseMS != 0 {
			return errors.New("execution: loss stop pause requires a window")
		}
		return nil
	}
	if p.PauseMS <= 0 {
		return errors.New("execution: loss stop requires a positive pause")
	}
	seen := make(map[int64]bool, len(p.Windows))
	for _, w := range p.Windows {
		if w.WindowMS < lossStopMinBucketMS || seen[w.WindowMS] || !w.MaxLossRate.IsPositive() || w.MaxLossRate.GreaterThan(decimal.NewFromInt(1)) {
			return errors.New("execution: invalid loss stop window")
		}
		seen[w.WindowMS] = true
	}
	return nil
}

func (p LossStopPolicy) equal(other LossStopPolicy) bool {
	if p.PauseMS != other.PauseMS || len(p.Windows) != len(other.Windows) {
		return false
	}
	for i, w := range p.Windows {
		if w.WindowMS != other.Windows[i].WindowMS || !w.MaxLossRate.Equal(other.Windows[i].MaxLossRate) {
			return false
		}
	}
	return true
}

// LossStopStatus is the durable breaker state shared by every robot of the
// account. Entries are paused while now < PausedUntilMS.
type LossStopStatus struct {
	Policy          LossStopPolicy
	PausedUntilMS   int64
	TrippedAtMS     int64
	TrippedWindowMS int64
	TrippedLossRate decimal.Decimal
}

type lossStopSample struct {
	AtMS   int64
	Equity decimal.Decimal
}

type lossStopState struct {
	LossStopStatus
	Samples []lossStopSample
}

const (
	lossStopMinBucketMS = int64(60_000)
	lossStopMaxSamples  = 360
)

func (st *lossStopState) bucketMS() int64 {
	longest := int64(0)
	for _, w := range st.Policy.Windows {
		longest = max(longest, w.WindowMS)
	}
	return max(lossStopMinBucketMS, longest/lossStopMaxSamples)
}

// observe records one equity observation and trips the breaker when a window's
// loss rate reaches its limit. It reports whether the state changed.
//
// A window compares against the latest sample at or before its start. When
// that sample is too old to represent the start (the account was not observed
// for a while) or absent (the account is younger than the window), the oldest
// sample inside the window is used instead: the measured period shrinks rather
// than silently growing beyond the configured window.
func (st *lossStopState) observe(now int64, equity decimal.Decimal) bool {
	if !st.Policy.enabled() || now <= 0 {
		return false
	}
	bucket := st.bucketMS()
	longest := int64(0)
	for _, w := range st.Policy.Windows {
		longest = max(longest, w.WindowMS)
	}
	changed := false
	if n := len(st.Samples); n == 0 || now-st.Samples[n-1].AtMS >= bucket {
		st.Samples = append(st.Samples, lossStopSample{AtMS: now, Equity: equity})
		changed = true
	}
	for len(st.Samples) > 1 && st.Samples[1].AtMS <= now-longest {
		st.Samples = st.Samples[1:]
		changed = true
	}
	if now < st.PausedUntilMS {
		return changed
	}
	windows := append([]LossStopWindow(nil), st.Policy.Windows...)
	sort.Slice(windows, func(a, b int) bool { return windows[a].WindowMS < windows[b].WindowMS })
	for _, w := range windows {
		start := now - w.WindowMS
		var ref *lossStopSample
		for i := range st.Samples {
			if st.Samples[i].AtMS > start {
				break
			}
			ref = &st.Samples[i]
		}
		if ref != nil && start-ref.AtMS > max(2*bucket, w.WindowMS/2) {
			ref = nil
		}
		if ref == nil {
			for i := range st.Samples {
				if st.Samples[i].AtMS > start && st.Samples[i].AtMS < now {
					ref = &st.Samples[i]
					break
				}
			}
		}
		if ref == nil || !ref.Equity.IsPositive() {
			continue
		}
		rate := ref.Equity.Sub(equity).Div(ref.Equity)
		if rate.LessThan(w.MaxLossRate) {
			continue
		}
		st.PausedUntilMS = now + st.Policy.PauseMS
		st.TrippedAtMS, st.TrippedWindowMS, st.TrippedLossRate = now, w.WindowMS, rate
		return true
	}
	return changed
}

const lossStopCheckpoint = "loss-stop-v1"

func (s *Store) lossStopState(ctx context.Context) (lossStopState, error) {
	var state lossStopState
	body, err := s.StrategyCheckpoint(ctx, "account-risk", lossStopCheckpoint)
	if errors.Is(err, sql.ErrNoRows) {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	err = json.Unmarshal(body, &state)
	return state, err
}

func (s *Store) saveLossStopState(ctx context.Context, state lossStopState) error {
	body, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return s.SaveStrategyCheckpoint(ctx, "account-risk", lossStopCheckpoint, body)
}

// SetLossStopPolicy installs or replaces the account loss circuit breaker. An
// active pause survives a policy change; samples restart when windows change.
func (b *SharedAccountBorrow) SetLossStopPolicy(ctx context.Context, policy LossStopPolicy) error {
	if err := policy.validate(); err != nil {
		return err
	}
	return b.use(func(s *SharedAccount) error {
		return s.owner.DoLocal(s.owner.Token(), func(ownerCtx context.Context) error {
			ownerCtx, done := joinedOperationContext(ownerCtx, b.ctx)
			defer done()
			ownerCtx, doneCaller := joinedOperationContext(ownerCtx, ctx)
			defer doneCaller()
			state, err := s.store.lossStopState(ownerCtx)
			if err != nil {
				return err
			}
			if state.Policy.equal(policy) {
				return nil
			}
			state.Policy, state.Samples = policy, nil
			return s.store.saveLossStopState(ownerCtx, state)
		})
	})
}

// LossStopStatus observes current account equity against the breaker and
// returns its durable state. When equity cannot be valued right now (a mark is
// missing or stale) the observation is skipped and the stored state returned:
// an unavailable quote neither trips nor clears the breaker.
func (b *SharedAccountBorrow) LossStopStatus(ctx context.Context, now int64) (LossStopStatus, error) {
	var status LossStopStatus
	err := b.use(func(s *SharedAccount) error {
		return s.owner.DoLocal(s.owner.Token(), func(ownerCtx context.Context) error {
			ownerCtx, done := joinedOperationContext(ownerCtx, b.ctx)
			defer done()
			ownerCtx, doneCaller := joinedOperationContext(ownerCtx, ctx)
			defer doneCaller()
			state, err := s.store.lossStopState(ownerCtx)
			if err != nil {
				return err
			}
			status = state.LossStopStatus
			if !state.Policy.enabled() {
				return nil
			}
			equity, ok, err := s.markedEquity(ownerCtx, now)
			if err != nil || !ok {
				return err
			}
			if !state.observe(now, equity) {
				return nil
			}
			if err := s.store.saveLossStopState(ownerCtx, state); err != nil {
				return err
			}
			status = state.LossStopStatus
			return nil
		})
	})
	return status, err
}

// markedEquity values the account at visible midpoints. ok is false when any
// open position has no usable mark at now.
func (s *SharedAccount) markedEquity(ctx context.Context, now int64) (decimal.Decimal, bool, error) {
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return decimal.Zero, false, err
	}
	marks := make(map[string]decimal.Decimal)
	for _, position := range snapshot.ActualPositions {
		id := position.Instrument.ID
		if position.SignedSteps == 0 {
			continue
		}
		if _, seen := marks[id]; seen {
			continue
		}
		market, ok := s.markets[id]
		if !ok || !sameAccountInstrument(market.instrument, position.Instrument) {
			return decimal.Zero, false, nil
		}
		q, err := market.quote(ctx, id, now)
		if err != nil {
			return decimal.Zero, false, nil
		}
		at := now
		if market.clock != nil {
			at = max(at, market.clock())
		}
		if !q.Bid.IsPositive() || q.Ask.LessThan(q.Bid) || q.AtMS <= 0 || q.AtMS > q.ReceivedMS || q.ReceivedMS > at || q.ValidUntilMS <= at {
			return decimal.Zero, false, nil
		}
		marks[id] = q.Bid.Add(q.Ask).Div(decimal.NewFromInt(2))
	}
	equity, err := snapshot.Equity(marks)
	if err != nil {
		return decimal.Zero, false, nil
	}
	return equity, true, nil
}

// SetLossStopPolicy installs the loss circuit breaker of a registered account.
func (h *Hub) SetLossStopPolicy(ctx context.Context, key AccountKey, policy LossStopPolicy) error {
	return h.use(ctx, key, func(account *SharedAccountBorrow) error {
		return account.SetLossStopPolicy(ctx, policy)
	})
}

// LossStopStatus observes and returns the loss circuit breaker of an account
// at the owner's clock.
func (h *Hub) LossStopStatus(ctx context.Context, key AccountKey) (LossStopStatus, error) {
	var status LossStopStatus
	err := h.use(ctx, key, func(account *SharedAccountBorrow) error {
		var err error
		status, err = account.LossStopStatus(ctx, time.Now().UnixMilli())
		return err
	})
	return status, err
}
