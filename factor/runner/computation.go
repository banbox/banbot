package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"io"
	"os"
	"sync"
)

// ComputationGroup is explicitly owned by one compatible clock/data context.
// No accounts, targets, combiners or mutable callback frames are shared.
type ComputationGroup struct {
	mu       sync.Mutex
	sessions map[string]*sharedComputation
	expected map[string]int
}

// ComputationContext declares storage/source scope and the clock/sampling
// domain. Live groups require this explicit identity in every consumer.
type ComputationContext struct{ DataNamespace, ClockDomain, SamplingIdentity string }
type sharedComputation struct {
	group     *ComputationGroup
	key       string
	borrowers int // guarded by group.mu; released only after the driver's work joins
	mu        sync.Mutex
	session   *factor.Session
	expected  int
	snapshot  string
	grid      int64
	readers   map[*decisionEngine]bool
	changed   chan struct{}
}

func NewComputationGroup() *ComputationGroup { return &ComputationGroup{} }
func computationKey(c Config, plan *factor.Plan) (string, error) {
	spec := c.Snapshot
	spec.TrackedQuotesOnly = true
	spec.GridTime = 0
	spec.DecisionTime = 0
	inputIdentity := ""
	if c.HistoricalInput != nil {
		inputIdentity = c.HistoricalInput.Identity()
	}
	var chunks []struct {
		Digest   string
		From, To int64
	}
	for _, chunk := range c.Chunks {
		if c.HistoricalInput != nil {
			break
		}
		file, err := os.Open(chunk.Path)
		if err != nil {
			return "", err
		}
		digest := sha256.New()
		_, copyErr := io.Copy(digest, file)
		closeErr := file.Close()
		if err = errors.Join(copyErr, closeErr); err != nil {
			return "", err
		}
		chunks = append(chunks, struct {
			Digest   string
			From, To int64
		}{hex.EncodeToString(digest.Sum(nil)), chunk.From, chunk.To})
	}
	raw, err := json.Marshal(struct {
		Context         ComputationContext
		Chunks          any
		InputIdentity   string
		Plan            string
		Snapshot        factor.SnapshotSpec
		Interval, Delay int64
	}{c.ComputationContext, chunks, inputIdentity, plan.Hash(), spec, c.DecisionInterval, c.DecisionDelayMS})
	return string(raw), err
}
func (g *ComputationGroup) acquire(c Config, plan *factor.Plan) (*sharedComputation, error) {
	key, err := computationKey(c, plan)
	if err != nil {
		return nil, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.sessions == nil {
		g.sessions = map[string]*sharedComputation{}
	}
	if slot := g.sessions[key]; slot != nil {
		slot.borrowers++
		return slot, nil
	}
	session, err := factor.NewSession(plan)
	if err != nil {
		return nil, err
	}
	slot := &sharedComputation{group: g, key: key, borrowers: 1, session: session, expected: max(1, g.expected[key]), readers: map[*decisionEngine]bool{}, changed: make(chan struct{})}
	g.sessions[key] = slot
	return slot, nil
}
func (s *sharedComputation) signal() { close(s.changed); s.changed = make(chan struct{}) }
func (e *decisionEngine) evaluate(ctx context.Context, snapshot *factor.Snapshot) (factor.Frame, error) {
	if e.shared == nil {
		return e.session.Evaluate(snapshot)
	}
	s := e.shared
	for {
		s.mu.Lock()
		if s.snapshot != "" && s.snapshot != snapshot.ID() && snapshot.Spec().GridTime <= s.grid {
			s.mu.Unlock()
			return factor.Frame{}, errors.New("runner: incompatible shared snapshot at processed grid")
		}
		if s.snapshot == "" || s.snapshot == snapshot.ID() || len(s.readers) >= s.expected {
			if err := ctx.Err(); err != nil {
				s.mu.Unlock()
				return factor.Frame{}, err
			}
			frame, err := s.session.Evaluate(snapshot)
			if err == nil {
				if s.snapshot != snapshot.ID() {
					s.snapshot = snapshot.ID()
					s.grid = snapshot.Spec().GridTime
					s.readers = map[*decisionEngine]bool{}
				}
				s.readers[e] = true
				s.signal()
			}
			s.mu.Unlock()
			return frame, err
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return factor.Frame{}, ctx.Err()
		case <-changed:
		}
	}
}
func (e *decisionEngine) close() {
	if e.shared != nil {
		s := e.shared
		s.mu.Lock()
		s.expected = max(0, s.expected-1)
		delete(s.readers, e)
		s.signal()
		s.mu.Unlock()
		// A dynamic subscription generation may change the Universe/key many
		// times. The group owns active borrowers, not an unbounded session cache.
		s.group.mu.Lock()
		s.borrowers--
		if s.borrowers == 0 {
			delete(s.group.sessions, s.key)
		}
		s.group.mu.Unlock()
	}
}

// RunMany uses actual replay drivers with a bounded, lockstep shared Session.
// Each consumer retains its own research, budget, portfolio and sink state.
func RunMany(ctx context.Context, configs []Config, sinks []Sink, outputs []Output) ([]Result, error) {
	if len(sinks) != len(configs) || len(outputs) != len(configs) {
		return nil, errors.New("runner: mismatched replay consumers")
	}
	configs = append([]Config(nil), configs...)
	accounts := map[*execution.SharedAccount][]int{}
	for i, sink := range sinks {
		if account, ok := sink.(*AccountSink); ok {
			accounts[account.Account.Service()] = append(accounts[account.Account.Service()], i)
		}
	}
	for _, indexes := range accounts {
		if len(indexes) < 2 {
			continue
		}
		base := configs[indexes[0]]
		timeline := &replayTimeline{consumers: len(indexes), changed: make(chan struct{})}
		for _, index := range indexes {
			if configs[index].ObserveBatch != nil {
				timeline.allSourceEvents = true
			}
		}
		for index, i := range indexes {
			if !sameReplayTimeline(base, configs[i]) {
				return nil, errors.New("runner: shared account requires aligned historical timeline")
			}
			configs[i].timeline = timeline
			configs[i].timelineIndex = index
		}
	}
	group := NewComputationGroup()
	group.expected = map[string]int{}
	for _, c := range configs {
		plan, _, err := compileDecision(c)
		if err != nil {
			return nil, err
		}
		key, err := computationKey(c, plan)
		if err != nil {
			return nil, err
		}
		group.expected[key]++
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make([]Result, len(configs))
	errs := make([]error, len(configs))
	var work sync.WaitGroup
	for i, c := range configs {
		work.Add(1)
		go func(i int, c Config) {
			defer work.Done()
			c.ComputationGroup = group
			results[i], errs[i] = Run(ctx, c, sinks[i], outputs[i])
			if errs[i] != nil {
				cancel()
			}
		}(i, c)
	}
	work.Wait()
	return results, errors.Join(errs...)
}
