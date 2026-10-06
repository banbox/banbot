package runner

import (
	"context"
	"errors"
	"fmt"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
)

// Live consumes provider records and flushes completed decision barriers using
// a real decision-completion clock. It never replays archives into a venue.
// At most one newest row per declared stream and one pending target is kept.
type Live struct {
	mu                      sync.Mutex
	workMu                  sync.Mutex
	work                    sync.WaitGroup
	joinOnce                sync.Once
	joined                  chan struct{}
	ctx                     context.Context
	cancel                  context.CancelFunc
	funding                 []factor.VersionRecord
	quoteUpdates            map[int32]backtest.Quote
	c                       Config
	sink                    Sink
	out                     Output
	clock                   func() int64
	engine                  *decisionEngine
	policy                  *policyRun
	policyPending           *pendingProposal
	policyScope             atomic.Pointer[[]int32]
	barrier                 factor.RoundBarrier
	rows                    map[factor.StreamKey]factor.VersionRecord
	quotes                  map[int32]backtest.Quote
	pending, previous       *factor.TargetPortfolio
	sequence                uint64
	generation              uint64
	lastDecision, lastClock int64
	stopped                 bool
	warmRows                map[factor.StreamKey][]factor.VersionRecord
	lastWarmGrid            int64
	warmGridCount           int
	liveStarted             bool
}

func NewLive(c Config, sink Sink, clock func() int64, out Output) (*Live, error) {
	var err error
	c, err = CloneConfig(c)
	if err != nil {
		return nil, err
	}
	c.Snapshot = factor.CloneSnapshotSpec(c.Snapshot)
	c.Snapshot.TrackedQuotesOnly = true
	// Live lineage is supplied by the provider context, never archival paths.
	c.Chunks = nil
	if sink == nil || clock == nil || c.DecisionInterval <= 0 || c.LatencyMS <= 0 || c.ExpiryMS <= c.LatencyMS {
		return nil, errors.New("runner: live requires account sink, real clock and execution window")
	}
	if _, ok := sink.(BudgetSource); !ok {
		return nil, errors.New("runner: live requires reconciled strategy NAV")
	}
	plan, combo, err := compileLiveDecision(c)
	if err != nil {
		return nil, err
	}
	c.Manifest.ExecutionMode = "trade"
	c.Manifest.LatencyAssumption = fmt.Sprintf("live completion clock; observable event after decision+%dms", c.LatencyMS)
	engine, err := newDecisionEngine(c, plan, combo)
	if err != nil {
		return nil, err
	}
	policy, err := newPolicyRun(c)
	if err != nil {
		engine.close()
		return nil, err
	}
	if policy != nil {
		if _, ok := sink.(PolicySink); !ok {
			engine.close()
			return nil, errors.New("runner: live policy requires allocation-capable account sink")
		}
	}
	life, cancel := context.WithCancel(context.Background())
	return &Live{c: c, sink: sink, out: out, clock: clock, engine: engine, policy: policy, rows: map[factor.StreamKey]factor.VersionRecord{}, quotes: map[int32]backtest.Quote{}, ctx: life, cancel: cancel, joined: make(chan struct{})}, nil
}
func (l *Live) Inputs() []factor.InputSpec { return l.engine.plan.Inputs() }

// SharesComputation reports actual mutable session aliasing, rather than
// rejecting unrelated sessions that happen to use the same group container.
func (l *Live) SharesComputation(other *Live) bool {
	return l != nil && other != nil && l.engine.session == other.engine.session
}

// ValidateWarmup proves that startup history advanced complete decision
// snapshots through the plan's lookback and startup boundary. Successful
// individual Warmup calls may only have buffered an incomplete snapshot.
func (l *Live) ValidateWarmup(anchorMS int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped || l.liveStarted {
		return errors.New("runner: warmup readiness must precede live intake")
	}
	needed := l.engine.plan.WarmupLength()
	if needed == 0 {
		return nil
	}
	if anchorMS < l.c.DecisionDelayMS {
		return errors.New("runner: startup history has no eligible decision boundary")
	}
	through := (anchorMS - l.c.DecisionDelayMS) / l.c.DecisionInterval * l.c.DecisionInterval
	if l.warmGridCount < needed || l.lastWarmGrid != through {
		return fmt.Errorf("runner: startup history incomplete: completed %d decision grids (need %d), through %d (need %d)", l.warmGridCount, needed, l.lastWarmGrid, through)
	}
	return nil
}

// InheritAdmission carries monotonic target identity to a freshly warmed
// successor. The subscription owner must hold the old generation's callback
// boundary; the old engine remains usable if candidate preparation fails.
func (l *Live) InheritAdmission(previous *Live) error {
	if previous == nil || previous == l {
		return errors.New("runner: distinct live successor required")
	}
	previous.mu.Lock()
	defer previous.mu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	if previous.stopped || l.stopped || l.liveStarted || l.sequence != 0 || l.c.StrategyID != previous.c.StrategyID || l.c.AccountID != previous.c.AccountID {
		return errors.New("runner: incompatible or already admitted live successor")
	}
	l.sequence = previous.sequence
	l.lastDecision = previous.lastDecision
	if previous.policy != nil {
		if l.policy == nil || l.engine.manifest.StrategyHash() != previous.engine.manifest.StrategyHash() {
			return errors.New("runner: live policy upgrade requires explicit checkpoint migration")
		}
		l.policy.state = append([]byte(nil), previous.policy.state...)
		l.policy.version = previous.policy.version
		l.policy.previous = previous.policy.previous
		l.policy.sequence = previous.policy.sequence
	}
	if previous.previous != nil {
		targets := previous.previous.Targets()
		retained := l.ExecutionSIDs()
		removed := false
		for sid := range targets {
			if !slices.Contains(retained, sid) {
				delete(targets, sid)
				removed = true
			}
		}
		inherited, err := factor.NewTargetPortfolio(previous.previous.Spec(), targets)
		if err != nil {
			return err
		}
		if nextAccount, ok := l.sink.(*AccountSink); ok {
			if previousAccount, wasAccount := previous.sink.(*AccountSink); wasAccount && previousAccount == nextAccount {
				if removed {
					return errors.New("runner: execution scope removal requires a fresh AccountSink wrapper")
				}
			} else {
				nextAccount.previous = inherited
			}
		}
		l.previous = inherited
	}
	return nil
}
func (l *Live) HasSID(sid int32) bool { return l.c.Snapshot.SIDMap[sid] != "" }
func (l *Live) FundingSIDs() []int32 {
	seen := map[int32]bool{}
	for _, sid := range l.ExecutionSIDs() {
		seen[sid] = true
	}
	if account, ok := l.sink.(*AccountSink); ok {
		for sid := range account.FundingInstruments {
			seen[sid] = true
		}
	}
	sids := make([]int32, 0, len(seen))
	for sid := range seen {
		sids = append(sids, sid)
	}
	slices.Sort(sids)
	return sids
}
func (l *Live) HasFundingSID(sid int32) bool { return slices.Contains(l.FundingSIDs(), sid) }

// InstrumentForSID resolves the actual execution identity, which may differ
// from the market-data symbol. Account-level funding SIDs use their own map.
func (l *Live) InstrumentForSID(sid int32) (execution.Instrument, bool) {
	if account, ok := l.sink.(*AccountSink); ok {
		if instrument, exists := account.FundingInstruments[sid]; exists {
			return instrument, true
		}
		if instrument, exists := account.Instruments[sid]; exists {
			return instrument, true
		}
	}
	instrument, exists := l.c.Execution.Instruments[sid]
	return instrument, exists
}

// DataSIDs returns the inference inputs consumed by live. Evaluation-only
// members belong to the replay research consumer, which live does not run;
// subscribing them as required would make absent labels block real trading.
func (l *Live) DataSIDs() []int32 {
	u := l.c.Snapshot.Universe
	ids := append(append([]int32{}, u.Investable...), u.Reference...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return slices.Compact(ids)
}

func (l *Live) ExecutionSIDs() []int32 {
	u := l.c.Snapshot.Universe
	ids := append([]int32{}, u.Tracked...)
	if scope := l.policyScope.Load(); scope != nil {
		ids = append(ids, (*scope)...)
	}
	for _, sid := range u.Investable {
		if slices.Contains(u.Tradable, sid) {
			ids = append(ids, sid)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return slices.Compact(ids)
}

// Warmup ingests provider history without quoting, posting funding, budgeting,
// or admitting targets. SID-major startup history is buffered per declared
// stream, bounded by its DAG warmup length, then advanced at complete grids.
func (l *Live) Warmup(ctx context.Context, r factor.VersionRecord) error {
	l.workMu.Lock()
	defer l.workMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped || l.liveStarted {
		return errors.New("runner: historical warmup cannot follow live decisions")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	now := l.clock()
	if now < l.lastClock {
		return errors.New("runner: live clock moved backwards during warmup")
	}
	l.lastClock = now
	if r.IngestedAt > now || r.AvailableAt > now || r.EventTime > now || l.c.Snapshot.SIDMap[r.Series.Sid] == "" {
		return errors.New("runner: unavailable/undeclared warmup observation")
	}
	var input *factor.InputSpec
	for _, in := range l.engine.plan.Inputs() {
		if in.Source == r.Series.Source && in.TimeFrame == r.Series.TimeFrame {
			copy := in
			input = &copy
			break
		}
	}
	if input == nil {
		return nil
	}
	r, err := factor.CloneVersionRecord(r)
	if err != nil {
		return err
	}
	if l.warmRows == nil {
		l.warmRows = make(map[factor.StreamKey][]factor.VersionRecord)
	}
	key := factor.StreamKey{SID: r.Series.Sid, Source: r.Series.Source, TimeFrame: r.Series.TimeFrame}
	for _, old := range l.warmRows[key] {
		if old.EventTime == r.EventTime && old.Revision == r.Revision {
			return errors.New("runner: duplicate warmup revision requires explicit reconciliation")
		}
	}
	if len(l.warmRows[key]) >= max(1, input.WarmupLength)+1 {
		return errors.New("runner: declared warmup history bound exceeded")
	}
	l.warmRows[key] = append(l.warmRows[key], r)
	grids := make(map[int64]bool)
	all := make([]factor.VersionRecord, 0)
	for _, records := range l.warmRows {
		for _, row := range records {
			all = append(all, row)
			if row.Series.Closed && row.EventTime > l.lastWarmGrid && row.EventTime%l.c.DecisionInterval == 0 {
				grids[row.EventTime] = true
			}
		}
	}
	ordered := make([]int64, 0, len(grids))
	for grid := range grids {
		ordered = append(ordered, grid)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	for _, grid := range ordered {
		spec := l.c.Snapshot
		spec.GridTime = grid
		spec.DecisionTime = now
		spec.ReplayTime = now
		snapshot, err := factor.Freeze(spec, all, requirements(l.engine.plan, spec.Universe, grid))
		if err != nil {
			return err
		}
		if !snapshot.Status().Ready {
			break
		}
		l.mu.Unlock()
		if l.engine.shared != nil {
			l.engine.shared.mu.Lock()
		}
		err = l.engine.session.Warmup(snapshot)
		if l.engine.shared != nil {
			l.engine.shared.mu.Unlock()
		}
		l.mu.Lock()
		if l.stopped {
			return factor.ErrRoundStale
		}
		if l.liveStarted {
			return errors.New("runner: live intake started during historical warmup")
		}
		if err != nil {
			return err
		}
		l.lastWarmGrid = grid
		l.warmGridCount++
	}
	for key, records := range l.warmRows {
		var keep []factor.VersionRecord
		var latest *factor.VersionRecord
		for _, row := range records {
			if row.EventTime > l.lastWarmGrid {
				keep = append(keep, row)
			} else if latest == nil || row.EventTime > latest.EventTime || row.EventTime == latest.EventTime && row.Revision > latest.Revision {
				copy := row
				latest = &copy
			}
		}
		if latest != nil {
			keep = append(keep, *latest)
			l.rows[key] = *latest
		}
		l.warmRows[key] = keep
	}
	return nil
}
func (l *Live) Observe(ctx context.Context, r factor.VersionRecord) error {
	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		return errors.New("runner: live stopped")
	}
	now := l.clock()
	if now < l.lastClock {
		l.mu.Unlock()
		return errors.New("runner: live clock moved backwards")
	}
	if err := ctx.Err(); err != nil {
		l.mu.Unlock()
		return err
	}
	if r.AvailableAt > now || r.IngestedAt > now || r.EventTime > now {
		l.mu.Unlock()
		return errors.New("runner: live record not observable")
	}
	isFunding := r.Series.Source == l.c.FundingSource && l.c.Manifest.Costs.FundingPolicy == "required-stream" && l.HasFundingSID(r.Series.Sid)
	if !l.HasSID(r.Series.Sid) && !isFunding {
		l.mu.Unlock()
		return errors.New("runner: undeclared live SID")
	}
	copy, err := factor.CloneVersionRecord(r)
	if err != nil {
		l.mu.Unlock()
		return err
	}
	if isFunding {
		if len(l.funding) >= max(1, l.c.MaxPending) {
			l.mu.Unlock()
			return errors.New("runner: pending funding bound exceeded")
		}
		l.funding = append(l.funding, copy)
	}
	l.lastClock = now
	l.liveStarted = true
	l.warmRows = nil
	u := l.c.Snapshot.Universe
	executionSID := slices.Contains(u.Tracked, r.Series.Sid) || slices.Contains(u.Investable, r.Series.Sid) && slices.Contains(u.Tradable, r.Series.Sid)
	if l.policy != nil {
		executionSID = slices.Contains(l.ExecutionSIDs(), r.Series.Sid)
	}
	if r.Series.Source == l.c.Prices.Source && r.Series.TimeFrame == l.c.Prices.TimeFrame && executionSID {
		n := factor.Number(r.Series.Values, l.c.Prices.Field)
		if n.Validity == factor.Valid && n.Value > 0 {
			q := withSpread(backtest.Quote{AtMS: r.EventTime, AvailableAt: max(r.AvailableAt, r.IngestedAt), Price: n.Value}, r.Series.Values)
			if old, ok := l.quotes[r.Series.Sid]; !ok || old.AtMS <= q.AtMS {
				l.quotes[r.Series.Sid] = q
				if l.quoteUpdates == nil {
					l.quoteUpdates = map[int32]backtest.Quote{}
				}
				l.quoteUpdates[r.Series.Sid] = q
			}
		}
	}
	for _, in := range l.engine.plan.Inputs() {
		if l.HasSID(r.Series.Sid) && r.Series.Source == in.Source && r.Series.TimeFrame == in.TimeFrame {
			key := factor.StreamKey{SID: r.Series.Sid, Source: r.Series.Source, TimeFrame: r.Series.TimeFrame}
			old, ok := l.rows[key]
			if !ok || r.EventTime > old.EventTime || r.EventTime == old.EventTime && r.Revision > old.Revision {
				l.rows[key] = copy
			}
		}
	}
	// Intake finishes even while computation/output/account I/O is occupied.
	if !l.workMu.TryLock() {
		l.mu.Unlock()
		return nil
	}
	l.work.Add(1)
	l.mu.Unlock()
	defer l.workMu.Unlock()
	defer l.work.Done()
	ctx, cancel := l.workContext(ctx)
	defer cancel()
	return l.process(ctx)
}
func (l *Live) workContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(l.ctx, cancel)
	return ctx, func() { stop(); cancel() }
}
func (l *Live) process(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	quotes := copyQuotes(l.quoteUpdates)
	funding := append([]factor.VersionRecord(nil), l.funding...)
	now := l.clock()
	l.mu.Unlock()
	if observer, ok := l.sink.(QuoteObserver); ok {
		for sid, q := range quotes {
			if err := observer.ObserveQuote(ctx, sid, q, now); err != nil {
				return err
			}
			l.mu.Lock()
			if l.quoteUpdates[sid] == q {
				delete(l.quoteUpdates, sid)
			}
			l.mu.Unlock()
		}
	}
	for _, r := range funding {
		handled := false
		if observer, ok := l.sink.(interface {
			ObserveFundingRecord(context.Context, factor.VersionRecord, int64) error
		}); ok {
			if account, ok := l.sink.(*AccountSink); !ok || account.AuthoritativeFunding {
				if err := observer.ObserveFundingRecord(ctx, r, now); err != nil {
					return err
				}
				handled = true
			}
		}
		if !handled {
			observer, ok := l.sink.(FundingObserver)
			if !ok {
				return errors.New("runner: live sink cannot reconcile funding")
			}
			n := factor.Number(r.Series.Values, "rate")
			if n.Validity != factor.Valid {
				return errors.New("runner: invalid live funding rate")
			}
			if err := observer.ObserveFunding(ctx, backtest.Funding{ID: fmt.Sprintf("%s:%d:%d", r.Series.Source, r.Series.Sid, r.EventTime), SID: r.Series.Sid, AtMS: r.EventTime, AvailableAt: r.AvailableAt, Rate: n.Value}, now); err != nil {
				return err
			}
		}
		l.mu.Lock()
		l.funding = l.funding[1:]
		l.mu.Unlock()
	}
	return l.execute(ctx, l.clock())
}
func (l *Live) execute(ctx context.Context, now int64) error {
	if l.policy != nil {
		return l.executePolicy(ctx, now)
	}
	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		return factor.ErrRoundStale
	}
	if l.pending == nil {
		l.mu.Unlock()
		return nil
	}
	p := l.pending
	sp := p.Spec()
	if now >= sp.ExpireAt {
		l.pending = nil
		l.mu.Unlock()
		return nil
	}
	if now < sp.ExecutableAt {
		l.mu.Unlock()
		return nil
	}
	targets, err := p.EffectiveTargets(l.previous)
	if err != nil {
		l.mu.Unlock()
		return err
	}
	required := targets
	if sp.Mode == factor.Patch {
		required = p.Targets()
	}
	quotes := copyQuotes(l.quotes)
	for sid := range required {
		q, ok := quotes[sid]
		if !ok || q.AtMS < sp.ExecutableAt || q.AvailableAt > now {
			l.mu.Unlock()
			return nil
		}
	}
	l.mu.Unlock()
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = l.sink.ProcessSnapshot(ctx, p, quotes, now); err != nil {
		return err
	}
	previous, err := factor.NewTargetPortfolio(sp, targets)
	if err != nil {
		return err
	}
	l.mu.Lock()
	l.previous = previous
	if l.pending == p {
		l.pending = nil
	}
	stopped := l.stopped
	l.mu.Unlock()
	if stopped {
		return factor.ErrRoundStale
	}
	if l.out != nil {
		source, ok := l.sink.(StateSource)
		if !ok {
			return errors.New("runner: live output needs reconciled state")
		}
		state, err := source.StrategyState(ctx, now)
		if err != nil {
			return err
		}
		return emitTargetAccepted(l.out, p, state, now)
	}
	return nil
}

// Flush is called after the provider drains the timestamp. An incomplete
// barrier preserves the prior position and accepts a later flush of that round.
func (l *Live) prepareRound(decision, cutoff int64) (factor.SnapshotSpec, factor.RoundToken, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var token factor.RoundToken
	spec := l.c.Snapshot
	if l.stopped {
		return spec, token, factor.ErrRoundStale
	}
	now := l.clock()
	if now < l.lastClock {
		return spec, token, errors.New("runner: live clock moved backwards")
	}
	l.lastClock = now
	if decision <= l.lastDecision {
		return spec, token, nil
	}
	if decision > now || now >= decision+l.c.ExpiryMS {
		return spec, token, factor.ErrRoundExpired
	}
	if cutoff == 0 {
		cutoff = now
	}
	if cutoff < decision || cutoff > now {
		return spec, token, errors.New("runner: invalid live publication cutoff")
	}
	spec.GridTime = decision
	spec.DecisionTime = cutoff
	spec.ReplayTime = cutoff
	needs := requirements(l.engine.plan, spec.Universe, decision)
	var err error
	token, err = l.barrier.Begin(l.engine.plan.Hash(), spec, needs, decision+l.c.ExpiryMS)
	if err != nil {
		return spec, token, err
	}
	l.generation = token.Generation
	for _, need := range needs {
		if r, ok := l.rows[factor.StreamKey{SID: need.SID, Source: need.Source, TimeFrame: need.TimeFrame}]; ok {
			if err = l.barrier.Observe(token, r, now); err != nil {
				return spec, token, err
			}
		}
	}
	return spec, token, nil
}
func (l *Live) Flush(ctx context.Context, decision int64) error {
	return l.FlushAt(ctx, decision, 0)
}

// FlushAt fixes one provider batch's publication cutoff across compatible
// consumers. The real clock still controls deadline and stale-work checks.
func (l *Live) FlushAt(ctx context.Context, decision, cutoff int64) error {
	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		return factor.ErrRoundStale
	}
	l.work.Add(1)
	l.mu.Unlock()
	defer l.work.Done()
	spec, token, err := l.prepareRound(decision, cutoff)
	if err != nil {
		return err
	}
	if token.Generation == 0 {
		return nil
	}
	l.workMu.Lock()
	defer l.workMu.Unlock()
	ctx, cancel := l.workContext(ctx)
	defer cancel()
	if err := l.process(ctx); err != nil {
		return err
	}
	if l.engine.shared != nil {
		l.engine.shared.mu.Lock()
	}
	frame, err := l.barrier.Compute(token, l.engine.session, l.clock)
	if l.engine.shared != nil {
		l.engine.shared.mu.Unlock()
	}
	if errors.Is(err, factor.ErrSnapshotIncomplete) {
		if l.policy != nil {
			return l.monitorPolicy(ctx, spec.Universe, decision, token)
		}
		return nil
	}
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	frame, diag, err := l.engine.combine(frame, spec.Universe, nil)
	if err != nil {
		return err
	}
	completed := l.clock()
	if completed >= decision+l.c.ExpiryMS {
		return factor.ErrRoundExpired
	}
	nav, err := l.sink.(BudgetSource).StrategyNAV(ctx, completed)
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	l.sequence++
	sequence := l.sequence
	l.mu.Unlock()
	if l.policy != nil {
		return l.proposePolicyRound(ctx, frame, spec.Universe, sequence, nav, completed, decision, token, diag)
	}
	p, pdiag, err := l.engine.buildPortfolio(frame, spec.Universe, sequence, nav, completed+l.c.LatencyMS, decision+l.c.ExpiryMS)
	if err != nil {
		return err
	}
	diag = append(diag, pdiag...)
	if l.clock() >= decision+l.c.ExpiryMS {
		return factor.ErrRoundExpired
	}
	if l.out != nil {
		if err = l.out.Decision(factor.CloneFrame(frame), p, diag); err != nil {
			return err
		}
	}
	if _, err = l.barrier.Context(token); err != nil {
		return err
	}
	l.mu.Lock()
	if l.stopped || l.generation != token.Generation {
		l.mu.Unlock()
		return factor.ErrRoundStale
	}
	if l.clock() >= decision+l.c.ExpiryMS {
		l.mu.Unlock()
		return factor.ErrRoundExpired
	}
	l.lastDecision = decision
	if p != nil {
		l.pending = p
	}
	l.mu.Unlock()
	return l.process(ctx)
}

// Stop closes intake and cancels account I/O without waiting on callbacks.
func (l *Live) Stop() {
	l.mu.Lock()
	l.stopped = true
	l.pending = nil
	l.policyPending = nil
	l.cancel()
	l.barrier.Stop()
	l.mu.Unlock()
}

// Join follows Stop; callbacks that ignore cancellation may outlive its deadline.
func (l *Live) Join(ctx context.Context) error {
	l.mu.Lock()
	stopped := l.stopped
	l.mu.Unlock()
	if !stopped {
		return errors.New("runner: Join requires stopped intake")
	}
	l.joinOnce.Do(func() {
		go func() {
			l.work.Wait()
			l.workMu.Lock()
			l.workMu.Unlock()
			l.barrier.Join()
			l.engine.close()
			close(l.joined)
		}()
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.joined:
		return nil
	}
}
