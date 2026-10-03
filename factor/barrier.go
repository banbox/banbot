package factor

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrRoundStale   = errors.New("factor: stale/canceled round token")
	ErrRoundExpired = errors.New("factor: decision round expired")
	ErrRoundFrozen  = errors.New("factor: round already frozen")
)

type RoundKey struct {
	GridTime        int64
	PlanHash        string
	UniverseVersion string
	DecisionTime    int64
}
type RoundToken struct {
	Key        RoundKey
	Generation uint64
}
type decisionRound struct {
	token        RoundToken
	spec         SnapshotSpec
	requirements []Requirement
	deadline     int64
	rows         map[recordKey]VersionRecord
	snapshot     *Snapshot
	ctx          context.Context
	cancel       context.CancelFunc
	canceled     bool
	computing    bool
	published    bool
}

// RoundBarrier owns one currently admissible decision generation. Begin
// supersedes/cancels the old round. The runner bounds worker concurrency; late
// workers may finish, but cannot publish or mutate the live computation owner.
type RoundBarrier struct {
	mu         sync.Mutex
	generation uint64
	active     *decisionRound
	latest     Frame
	hasLatest  bool
	stopped    bool
	work       sync.WaitGroup
}

func (b *RoundBarrier) Begin(planHash string, spec SnapshotSpec, requirements []Requirement, deadlineMS int64) (RoundToken, error) {
	if planHash == "" || deadlineMS <= spec.DecisionTime {
		return RoundToken{}, errors.New("factor: round requires plan hash and deadline after decision")
	}
	if len(requirements) == 0 {
		return RoundToken{}, errors.New("factor: round requires expected streams")
	}
	// Validate the declaration now, while retaining its incomplete barrier.
	if _, err := Freeze(spec, nil, requirements); err != nil {
		return RoundToken{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.stopped {
		return RoundToken{}, ErrRoundStale
	}
	if b.hasLatest && spec.DecisionTime <= b.latest.DecisionTime {
		return RoundToken{}, errors.New("factor: decision time already published or older")
	}
	if b.active != nil {
		b.active.cancel()
	}
	b.generation++
	ctx, cancel := context.WithCancel(context.Background())
	grid := spec.GridTime
	if grid == 0 {
		grid = spec.DecisionTime
	}
	token := RoundToken{RoundKey{GridTime: grid, PlanHash: planHash, UniverseVersion: spec.Universe.Version, DecisionTime: spec.DecisionTime}, b.generation}
	b.active = &decisionRound{token: token, spec: cloneSpec(spec), requirements: append([]Requirement(nil), requirements...), deadline: deadlineMS, rows: make(map[recordKey]VersionRecord), ctx: ctx, cancel: cancel}
	return token, nil
}

func (b *RoundBarrier) current(token RoundToken, nowMS int64) (*decisionRound, error) {
	round := b.active
	if b.stopped || round == nil || round.token != token || round.canceled {
		return nil, ErrRoundStale
	}
	if nowMS >= round.deadline {
		round.canceled = true
		round.cancel()
		return nil, ErrRoundExpired
	}
	return round, nil
}

func (b *RoundBarrier) Context(token RoundToken) (context.Context, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active == nil || b.active.token != token || b.active.canceled {
		return nil, ErrRoundStale
	}
	return b.active.ctx, nil
}

func (b *RoundBarrier) Observe(token RoundToken, record VersionRecord, nowMS int64) error {
	copy, err := cloneRecord(record)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	round, err := b.current(token, nowMS)
	if err != nil {
		return err
	}
	if round.snapshot != nil {
		return ErrRoundFrozen
	}
	if _, exists := round.rows[copy.key()]; exists {
		oldHash, err := contentHash(round.rows[copy.key()])
		if err != nil {
			return err
		}
		newHash, err := contentHash(copy)
		if err != nil {
			return err
		}
		if oldHash != newHash {
			return errors.New("factor: conflicting round observation")
		}
		return nil
	}
	// Keep at most the currently visible event/revision per expected stream.
	// Unrequested/future streams do not expand an unbounded pending buffer.
	requested := false
	for _, need := range round.requirements {
		if need.SID == record.Series.Sid && need.Source == record.Series.Source && need.Frequency == record.Series.TimeFrame {
			requested = true
			break
		}
	}
	if !requested {
		return errors.New("factor: unrequested round stream")
	}
	if copy.EventTime > token.Key.GridTime || copy.AvailableAt > round.spec.DecisionTime || copy.AvailableAt > nowMS || copy.IngestedAt > nowMS || (round.spec.ReplayTime != 0 && copy.IngestedAt > round.spec.ReplayTime) {
		return nil
	}
	for key, old := range round.rows {
		if key.Source == copy.Series.Source && key.Frequency == copy.Series.TimeFrame && key.SID == copy.Series.Sid {
			if old.EventTime > copy.EventTime || (old.EventTime == copy.EventTime && old.Revision > copy.Revision) {
				return nil
			}
			delete(round.rows, key)
		}
	}
	round.rows[copy.key()] = copy
	return nil
}

func (b *RoundBarrier) Freeze(token RoundToken, nowMS int64) (*Snapshot, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	round, err := b.current(token, nowMS)
	if err != nil {
		return nil, err
	}
	if nowMS < round.spec.DecisionTime {
		return nil, errors.New("factor: cannot freeze before visibility cutoff")
	}
	if round.snapshot != nil {
		return round.snapshot, nil
	}
	rows := make([]VersionRecord, 0, len(round.rows))
	for _, row := range round.rows {
		rows = append(rows, row)
	}
	snapshot, err := Freeze(round.spec, rows, round.requirements)
	if err != nil {
		return nil, err
	}
	if !snapshot.status.Ready {
		return nil, ErrSnapshotIncomplete
	}
	round.snapshot = snapshot
	return snapshot, nil
}

func (b *RoundBarrier) Cancel(token RoundToken) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active == nil || b.active.token != token {
		return ErrRoundStale
	}
	b.active.canceled = true
	b.active.cancel()
	return nil
}

func (b *RoundBarrier) Latest() (Frame, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return cloneFrame(b.latest), b.hasLatest
}

func (b *RoundBarrier) Stop() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stopped = true
	if b.active != nil {
		b.active.canceled = true
		b.active.cancel()
	}
}

// Join follows Stop and waits for private workers to unwind. A callback must
// not join itself; the runtime/process owner is responsible for this boundary.
func (b *RoundBarrier) Join() { b.work.Wait() }

// Compute freezes the complete round, evaluates a private state fork, and
// atomically commits it only if its token/deadline are still current. now must
// return the caller's decision clock at both admission and completion.
func (b *RoundBarrier) Compute(token RoundToken, owner *Session, now func() int64) (Frame, error) {
	if owner == nil || now == nil {
		return Frame{}, errors.New("factor: compute requires session owner and clock")
	}
	if owner.plan.hash != token.Key.PlanHash {
		return Frame{}, ErrRoundStale
	}
	snapshot, err := b.Freeze(token, now())
	if err != nil {
		return Frame{}, err
	}
	b.mu.Lock()
	round, err := b.current(token, now())
	if err != nil {
		b.mu.Unlock()
		return Frame{}, err
	}
	if round.published {
		frame := cloneFrame(b.latest)
		b.mu.Unlock()
		return frame, nil
	}
	if round.computing {
		b.mu.Unlock()
		return Frame{}, errors.New("factor: round computation already running")
	}
	round.computing = true
	b.work.Add(1)
	b.mu.Unlock()
	defer b.work.Done()
	candidate, baseRevision, forkErr := owner.fork()
	var frame Frame
	if forkErr != nil {
		err = forkErr
	} else {
		frame, err = candidate.Evaluate(snapshot)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	current, currentErr := b.current(token, now())
	if currentErr != nil {
		return Frame{}, currentErr
	}
	current.computing = false
	if err != nil {
		return Frame{}, err
	}
	if err = owner.commit(candidate, baseRevision); err != nil {
		return Frame{}, err
	}
	current.published = true
	b.latest = cloneFrame(frame)
	b.hasLatest = true
	return cloneFrame(frame), nil
}
