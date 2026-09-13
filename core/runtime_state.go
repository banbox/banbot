package core

import (
	"context"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/sasha-s/go-deadlock"
	"go.uber.org/zap"
)

type admissionSnapshot struct {
	enabled map[string]bool
}

// legacyAdmissionMu protects the small compatibility facade below. Runtime
// code should use State's admission snapshot instead; the process-wide facade
// remains for callers that have not migrated to a Runtime yet.
var legacyAdmissionMu sync.RWMutex

// legacyFlagsMu protects the process-wide compatibility maps that predate
// Runtime-owned State. New code should use State accessors; these helpers keep
// the remaining legacy boundary safe when commands and workers overlap.
var legacyFlagsMu sync.RWMutex

// State owns the mutable core state for one Runtime. Process-scoped resources
// such as Cache are not owned by State. It is intentionally a concrete type so
// hot paths can use direct field access.
type State struct {
	// Logger is bound before execution and never installed globally.
	Logger       *zap.Logger
	RunMode      string
	RunEnv       string
	StartAt      int64
	EnvReal      bool
	LiveMode     bool
	BackTestMode bool

	tFSecs        map[string]int
	ExgName       string
	Market        string
	IsContract    bool
	CheckWallets  bool
	ContractType  string
	stgPairTfs    map[string]map[string]string
	Pairs         []string
	banPairsUntil map[string]int64
	noEnterUntil  map[string]int64
	tfPairHits    map[string]map[string]int
	jobPerfs      map[string]*JobPerf
	stratPerfSta  map[string]*PerfSta
	odBooks       map[string]*banexg.OrderBook
	NumTaCache    int
	orderMatchTfs map[string]bool

	CPUProfile    bool
	MemProfile    bool
	NetDisable    bool
	ParallelOnBar bool
	ConcurNum     int
	SysLang       string

	tfPairHitsLock deadlock.RWMutex
	lockOdMatch    sync.RWMutex
	StopAll        func()
	BotRunning     bool

	ctx       context.Context
	cancel    context.CancelFunc
	stopOnce  sync.Once
	exitLock  sync.Mutex
	exitCalls []func()
	stopped   bool

	admissionMu sync.RWMutex
	admission   atomic.Pointer[admissionSnapshot]
	// flagsMu protects the frequently-read admission override maps and runtime
	// execution flags. The maps/fields remain concrete compatibility fields;
	// explicit runtime code uses the typed accessors below instead of racing on
	// direct access.
	flagsMu sync.RWMutex

	simOrderMu    sync.Mutex
	simOrderMatch atomic.Bool
	newNumInSim   atomic.Int64
}

// BeginSimOrderMatch serializes one runtime's simulated matching pass. The
// counter belongs to that pass, so concurrent accounts must not reset it.
func (s *State) BeginSimOrderMatch() {
	if s == nil {
		return
	}
	s.simOrderMu.Lock()
	s.newNumInSim.Store(0)
	s.simOrderMatch.Store(true)
}

func (s *State) EndSimOrderMatch() int {
	if s == nil {
		return 0
	}
	count := int(s.newNumInSim.Load())
	s.simOrderMatch.Store(false)
	s.simOrderMu.Unlock()
	return count
}

func (s *State) AddSimOrder() {
	if s != nil && s.simOrderMatch.Load() {
		s.newNumInSim.Add(1)
	}
}

func (s *State) NewSimOrderCount() int {
	if s == nil {
		return 0
	}
	return int(s.newNumInSim.Load())
}

// NewState creates a self-contained core state and derives a private
// cancellation context from parent.
func NewState(parent context.Context) (*State, *errs.Error) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	state := &State{
		RunMode:       RunModeOther,
		RunEnv:        RunEnvDryRun,
		tFSecs:        make(map[string]int),
		stgPairTfs:    make(map[string]map[string]string),
		banPairsUntil: make(map[string]int64),
		noEnterUntil:  make(map[string]int64),
		tfPairHits:    make(map[string]map[string]int),
		jobPerfs:      make(map[string]*JobPerf),
		stratPerfSta:  make(map[string]*PerfSta),
		odBooks:       make(map[string]*banexg.OrderBook),
		orderMatchTfs: make(map[string]bool),
		NumTaCache:    1500,
		ConcurNum:     2,
		ctx:           ctx,
		cancel:        cancel,
		BotRunning:    true,
	}
	return state, nil
}

// EnsureRuntimeMaps initializes the mutable map fields used by strategy and
// order processing. NewState already returns a fully initialized value, but
// this keeps a zero-value State usable when a caller embeds it in a larger
// runtime object or constructs it in a test.
func (s *State) EnsureRuntimeMaps() {
	if s == nil {
		return
	}
	// Zero-value States can be initialized after construction. Keep each map
	// under the same lock used by its accessors so a late initialization cannot
	// race a refresh or callback that is already using the State.
	s.flagsMu.Lock()
	if s.tFSecs == nil {
		s.tFSecs = make(map[string]int)
	}
	if s.stgPairTfs == nil {
		s.stgPairTfs = make(map[string]map[string]string)
	}
	if s.banPairsUntil == nil {
		s.banPairsUntil = make(map[string]int64)
	}
	if s.noEnterUntil == nil {
		s.noEnterUntil = make(map[string]int64)
	}
	if s.jobPerfs == nil {
		s.jobPerfs = make(map[string]*JobPerf)
	}
	if s.stratPerfSta == nil {
		s.stratPerfSta = make(map[string]*PerfSta)
	}
	s.flagsMu.Unlock()
	s.tfPairHitsLock.Lock()
	if s.tfPairHits == nil {
		s.tfPairHits = make(map[string]map[string]int)
	}
	s.tfPairHitsLock.Unlock()
	s.lockOdMatch.Lock()
	if s.odBooks == nil {
		s.odBooks = make(map[string]*banexg.OrderBook)
	}
	if s.orderMatchTfs == nil {
		s.orderMatchTfs = make(map[string]bool)
	}
	s.lockOdMatch.Unlock()
}

// IsBotRunning returns the execution flag owned by this Runtime. The public
// field remains for legacy construction compatibility; Runtime code should
// use this accessor so reads synchronize with SetBotRunning and Stop.
func (s *State) IsBotRunning() bool {
	if s == nil {
		return false
	}
	s.flagsMu.RLock()
	running := s.BotRunning
	s.flagsMu.RUnlock()
	return running
}

// SetBotRunning updates the execution flag owned by this Runtime.
func (s *State) SetBotRunning(running bool) {
	if s == nil {
		return
	}
	s.flagsMu.Lock()
	s.BotRunning = running
	s.flagsMu.Unlock()
}

// ShouldCheckWallets returns whether this Runtime should refresh wallets.
func (s *State) ShouldCheckWallets() bool {
	if s == nil {
		return false
	}
	s.flagsMu.RLock()
	check := s.CheckWallets
	s.flagsMu.RUnlock()
	return check
}

// SetCheckWallets updates the wallet-refresh flag owned by this Runtime.
func (s *State) SetCheckWallets(check bool) {
	if s == nil {
		return
	}
	s.flagsMu.Lock()
	s.CheckWallets = check
	s.flagsMu.Unlock()
}

func (s *State) SetRunMode(mode string) {
	s.RunMode = mode
	s.LiveMode = mode == RunModeLive
	s.BackTestMode = mode == RunModeBackTest
}

// OnExit registers a callback for this Runtime. Registering after Stop runs
// the callback immediately so cleanup cannot be silently lost.
func (s *State) OnExit(call func()) {
	if s == nil || call == nil {
		return
	}
	s.exitLock.Lock()
	if s.stopped {
		s.exitLock.Unlock()
		call()
		return
	}
	s.exitCalls = append(s.exitCalls, call)
	s.exitLock.Unlock()
}

func (s *State) SetRunEnv(env string) {
	s.RunEnv = env
	if s.LiveMode {
		s.EnvReal = env != RunEnvDryRun
	} else {
		s.EnvReal = false
	}
}

// PairEnabled reports whether a symbol belongs to this Runtime's active pair
// set. The hot path reads an immutable snapshot and never touches the legacy
// compatibility map.
func (s *State) PairEnabled(pair string) bool {
	if s == nil {
		return false
	}
	snapshot := s.admission.Load()
	if snapshot == nil {
		s.admissionMu.Lock()
		snapshot = s.admission.Load()
		if snapshot == nil {
			snapshot = s.initAdmissionSnapshotLocked()
		}
		s.admissionMu.Unlock()
	}
	return snapshot.enabled[pair]
}

// SetPairs replaces the active pair snapshot and any additional symbols that
// remain eligible for order admission through explicit strategy policies.
func (s *State) SetPairs(pairs, additionalAllowed []string) {
	if s == nil {
		return
	}
	enabled := make(map[string]bool, len(pairs)+len(additionalAllowed))
	for _, pair := range pairs {
		enabled[pair] = true
	}
	for _, pair := range additionalAllowed {
		enabled[pair] = true
	}
	s.admissionMu.Lock()
	s.Pairs = slices.Clone(pairs)
	s.publishAdmissionLocked(enabled)
	s.flagsMu.Lock()
	for pair := range s.banPairsUntil {
		if !enabled[pair] {
			delete(s.banPairsUntil, pair)
		}
	}
	s.flagsMu.Unlock()
	s.admissionMu.Unlock()
}

// SetAdmissionPair updates one pair by publishing a replacement snapshot.
// Enabled pairs are also retained in the compatibility Pairs slice.
func (s *State) SetAdmissionPair(pair string, enabled bool) {
	if s == nil {
		return
	}
	s.admissionMu.Lock()
	current := s.admissionSnapshotLocked()
	next := maps.Clone(current.enabled)
	if next == nil {
		next = make(map[string]bool)
	}
	next[pair] = enabled
	if enabled && !slices.Contains(s.Pairs, pair) {
		s.Pairs = append(s.Pairs, pair)
	}
	s.publishAdmissionLocked(next)
	s.admissionMu.Unlock()
}

// SetAdmissionSnapshot updates the enabled state for the current compatibility
// pair set and publishes it atomically. Pairs absent from active are disabled.
func (s *State) SetAdmissionSnapshot(active map[string]bool) {
	if s == nil {
		return
	}
	s.admissionMu.Lock()
	current := s.admissionSnapshotLocked()
	next := maps.Clone(current.enabled)
	if next == nil {
		next = make(map[string]bool)
	}
	// Pairs is a public compatibility field. Include entries written there
	// before the first typed admission update instead of silently dropping
	// them when the initial snapshot was already published.
	for _, pair := range s.Pairs {
		if _, ok := next[pair]; !ok {
			next[pair] = active[pair]
		}
	}
	for pair := range next {
		next[pair] = active[pair]
	}
	// A refresh can discover a pair that was not present in the previous
	// snapshot. Publish it as part of the same replacement and retain it in
	// the compatibility pair list when it is enabled.
	for pair, enabled := range active {
		if _, ok := next[pair]; !ok {
			next[pair] = enabled
		}
		if enabled && !slices.Contains(s.Pairs, pair) {
			s.Pairs = append(s.Pairs, pair)
		}
	}
	s.publishAdmissionLocked(next)
	s.admissionMu.Unlock()
}

// AdmissionPairs returns a stable copy for low-frequency strategy updates.
func (s *State) AdmissionPairs() []string {
	if s == nil {
		return nil
	}
	s.admissionMu.RLock()
	pairs := slices.Clone(s.Pairs)
	s.admissionMu.RUnlock()
	return pairs
}

// IsPairBanned reports and lazily clears a runtime pair ban. The map field is
// retained for source compatibility, but explicit hot paths should use this
// method so readers and pair refresh writers are synchronized.
func (s *State) IsPairBanned(pair string, nowMS int64) bool {
	if s == nil {
		return false
	}
	s.flagsMu.Lock()
	until, ok := s.banPairsUntil[pair]
	if ok && nowMS >= until {
		delete(s.banPairsUntil, pair)
		ok = false
	}
	s.flagsMu.Unlock()
	return ok
}

// SetPairBanUntil updates one runtime pair ban. A non-positive value clears
// the current ban.
func (s *State) SetPairBanUntil(pair string, untilMS int64) {
	if s == nil {
		return
	}
	s.flagsMu.Lock()
	if s.banPairsUntil == nil {
		s.banPairsUntil = make(map[string]int64)
	}
	if untilMS <= 0 {
		delete(s.banPairsUntil, pair)
	} else {
		s.banPairsUntil[pair] = untilMS
	}
	s.flagsMu.Unlock()
}

// BannedPairs returns a stable list for low-frequency refresh and cleanup.
func (s *State) BannedPairs() []string {
	if s == nil {
		return nil
	}
	s.flagsMu.RLock()
	pairs := make([]string, 0, len(s.banPairsUntil))
	for pair := range s.banPairsUntil {
		pairs = append(pairs, pair)
	}
	s.flagsMu.RUnlock()
	return pairs
}

// NoEnterUntilFor reads the account trading switch owned by this runtime.
func (s *State) NoEnterUntilFor(account string) (int64, bool) {
	if s == nil {
		return 0, false
	}
	s.flagsMu.RLock()
	until, ok := s.noEnterUntil[account]
	s.flagsMu.RUnlock()
	return until, ok
}

// NoEnterUntilSnapshot returns a stable copy for low-frequency views.
func (s *State) NoEnterUntilSnapshot() map[string]int64 {
	if s == nil {
		return nil
	}
	s.flagsMu.RLock()
	result := maps.Clone(s.noEnterUntil)
	s.flagsMu.RUnlock()
	return result
}

// SetNoEnterUntil updates the account trading switch owned by this runtime.
// A non-positive value clears the switch.
func (s *State) SetNoEnterUntil(account string, untilMS int64) {
	if s == nil {
		return
	}
	s.flagsMu.Lock()
	if s.noEnterUntil == nil {
		s.noEnterUntil = make(map[string]int64)
	}
	if untilMS <= 0 {
		delete(s.noEnterUntil, account)
	} else {
		s.noEnterUntil[account] = untilMS
	}
	s.flagsMu.Unlock()
}

// StrategyTimeFrame returns the timeframe assigned to a strategy/pair in this
// Runtime. The nested map stays concrete for compatibility, while callers do
// not need to retain a mutable map reference across callbacks.
func (s *State) StrategyTimeFrame(strategy, pair string) (string, bool) {
	if s == nil {
		return "", false
	}
	s.flagsMu.RLock()
	pairs := s.stgPairTfs[strategy]
	tf, ok := pairs[pair]
	s.flagsMu.RUnlock()
	return tf, ok
}

// StrategyTimeFramesSnapshot returns a deep copy of the runtime strategy
// timeframe index for low-frequency reports and diagnostics.
func (s *State) StrategyTimeFramesSnapshot() map[string]map[string]string {
	if s == nil {
		return nil
	}
	s.flagsMu.RLock()
	result := make(map[string]map[string]string, len(s.stgPairTfs))
	for strategy, pairs := range s.stgPairTfs {
		result[strategy] = maps.Clone(pairs)
	}
	s.flagsMu.RUnlock()
	return result
}

// SetStrategyTimeFrame records one strategy/pair timeframe under the runtime
// state lock. It is intended for pair admission updates and setup boundaries.
func (s *State) SetStrategyTimeFrame(strategy, pair, timeframe string) {
	if s == nil {
		return
	}
	s.flagsMu.Lock()
	if s.stgPairTfs == nil {
		s.stgPairTfs = make(map[string]map[string]string)
	}
	pairs := s.stgPairTfs[strategy]
	if pairs == nil {
		pairs = make(map[string]string)
		s.stgPairTfs[strategy] = pairs
	}
	if timeframe == "" {
		delete(pairs, pair)
	} else {
		pairs[pair] = timeframe
	}
	s.flagsMu.Unlock()
}

// SetTimeFrameSeconds publishes one timeframe's duration in this Runtime.
func (s *State) SetTimeFrameSeconds(timeframe string, seconds int) {
	if s == nil {
		return
	}
	s.flagsMu.Lock()
	if s.tFSecs == nil {
		s.tFSecs = make(map[string]int)
	}
	if seconds <= 0 {
		delete(s.tFSecs, timeframe)
	} else {
		s.tFSecs[timeframe] = seconds
	}
	s.flagsMu.Unlock()
}

// TimeFrameSeconds reads one timeframe's duration from this Runtime.
func (s *State) TimeFrameSeconds(timeframe string) (int, bool) {
	if s == nil {
		return 0, false
	}
	s.flagsMu.RLock()
	seconds, ok := s.tFSecs[timeframe]
	s.flagsMu.RUnlock()
	return seconds, ok
}

// TimeFrameSecondsSnapshot returns a stable copy for setup/report consumers.
func (s *State) TimeFrameSecondsSnapshot() map[string]int {
	if s == nil {
		return nil
	}
	s.flagsMu.RLock()
	result := maps.Clone(s.tFSecs)
	s.flagsMu.RUnlock()
	return result
}

// ReplaceTimeFrameState publishes complete timeframe indexes after a strategy
// refresh has built them in private maps. The nested map is copied so callers
// cannot mutate Runtime state after this method returns.
func (s *State) ReplaceTimeFrameState(timeframes map[string]int, strategyPairs map[string]map[string]string) {
	if s == nil {
		return
	}
	s.flagsMu.Lock()
	if timeframes == nil {
		timeframes = make(map[string]int)
	}
	if strategyPairs == nil {
		strategyPairs = make(map[string]map[string]string)
	}
	s.tFSecs = maps.Clone(timeframes)
	s.stgPairTfs = make(map[string]map[string]string, len(strategyPairs))
	for strategy, pairs := range strategyPairs {
		s.stgPairTfs[strategy] = maps.Clone(pairs)
	}
	s.flagsMu.Unlock()
}

func cloneJobPerf(perf *JobPerf) *JobPerf {
	if perf == nil {
		return nil
	}
	cp := *perf
	return &cp
}

func clonePerfSta(sta *PerfSta) *PerfSta {
	if sta == nil {
		return nil
	}
	cp := *sta
	if sta.Splits != nil {
		splits := *sta.Splits
		cp.Splits = &splits
	}
	return &cp
}

// JobPerf returns a copy of one runtime performance multiplier. Returning a
// value copy prevents callers from mutating the state after the lock is
// released.
func (s *State) JobPerf(key string) *JobPerf {
	if s == nil {
		return nil
	}
	s.flagsMu.RLock()
	result := cloneJobPerf(s.jobPerfs[key])
	s.flagsMu.RUnlock()
	return result
}

// JobPerfValue reads one runtime performance multiplier without allocating.
// Hot paths that only need the numeric fields should prefer this value form;
// JobPerf remains available for callers that need a pointer-shaped copy.
func (s *State) JobPerfValue(key string) (JobPerf, bool) {
	if s == nil {
		return JobPerf{}, false
	}
	s.flagsMu.RLock()
	perf := s.jobPerfs[key]
	if perf == nil {
		s.flagsMu.RUnlock()
		return JobPerf{}, false
	}
	result := *perf
	s.flagsMu.RUnlock()
	return result, true
}

// SetJobPerf replaces one runtime performance multiplier.
func (s *State) SetJobPerf(key string, perf *JobPerf) {
	if s == nil {
		return
	}
	s.flagsMu.Lock()
	if s.jobPerfs == nil {
		s.jobPerfs = make(map[string]*JobPerf)
	}
	if perf == nil {
		delete(s.jobPerfs, key)
	} else {
		s.jobPerfs[key] = cloneJobPerf(perf)
	}
	s.flagsMu.Unlock()
}

// PerfSta returns a copy of one runtime strategy performance accumulator.
func (s *State) PerfSta(strategy string) *PerfSta {
	if s == nil {
		return nil
	}
	s.flagsMu.RLock()
	result := clonePerfSta(s.stratPerfSta[strategy])
	s.flagsMu.RUnlock()
	return result
}

// SetPerfSta replaces one runtime strategy performance accumulator.
func (s *State) SetPerfSta(strategy string, sta *PerfSta) {
	if s == nil {
		return
	}
	s.flagsMu.Lock()
	if s.stratPerfSta == nil {
		s.stratPerfSta = make(map[string]*PerfSta)
	}
	if sta == nil {
		delete(s.stratPerfSta, strategy)
	} else {
		s.stratPerfSta[strategy] = clonePerfSta(sta)
	}
	s.flagsMu.Unlock()
}

// JobPerfSnapshot returns copies of all multipliers whose key starts with
// prefix. It is used by score calculation while keeping map ownership inside
// the Runtime state.
func (s *State) JobPerfSnapshot(prefix string) []*JobPerf {
	if s == nil {
		return nil
	}
	s.flagsMu.RLock()
	result := make([]*JobPerf, 0, len(s.jobPerfs))
	for key, perf := range s.jobPerfs {
		if strings.HasPrefix(key, prefix) {
			result = append(result, cloneJobPerf(perf))
		}
	}
	s.flagsMu.RUnlock()
	return result
}

// PerformanceSnapshot returns owned copies of both runtime performance
// indexes for reports and persistence. It does not expose mutable map or
// pointer references after the state lock is released.
func (s *State) PerformanceSnapshot() (map[string]*JobPerf, map[string]*PerfSta) {
	if s == nil {
		return nil, nil
	}
	s.flagsMu.RLock()
	jobPerfs := make(map[string]*JobPerf, len(s.jobPerfs))
	for key, perf := range s.jobPerfs {
		jobPerfs[key] = cloneJobPerf(perf)
	}
	stratPerfSta := make(map[string]*PerfSta, len(s.stratPerfSta))
	for key, sta := range s.stratPerfSta {
		stratPerfSta[key] = clonePerfSta(sta)
	}
	s.flagsMu.RUnlock()
	return jobPerfs, stratPerfSta
}

// WithPerformance runs a short, synchronous update while holding the
// runtime performance lock. The maps are concrete and intended only for
// tightly-scoped domain calculations; callers must not retain them.
func (s *State) WithPerformance(fn func(map[string]*JobPerf, map[string]*PerfSta)) {
	if s == nil || fn == nil {
		return
	}
	s.flagsMu.Lock()
	if s.jobPerfs == nil {
		s.jobPerfs = make(map[string]*JobPerf)
	}
	if s.stratPerfSta == nil {
		s.stratPerfSta = make(map[string]*PerfSta)
	}
	fn(s.jobPerfs, s.stratPerfSta)
	s.flagsMu.Unlock()
}

// WithLegacyPerformance runs a compatibility performance update under the
// process-wide performance lock. The callback must not retain either map or
// its entries after it returns.
func WithLegacyPerformance(fn func(map[string]*JobPerf, map[string]*PerfSta)) {
	if fn == nil {
		return
	}
	legacyFlagsMu.Lock()
	if JobPerfs == nil {
		JobPerfs = make(map[string]*JobPerf)
	}
	if StratPerfSta == nil {
		StratPerfSta = make(map[string]*PerfSta)
	}
	fn(JobPerfs, StratPerfSta)
	legacyFlagsMu.Unlock()
}

// LegacyJobPerf returns a stable copy of one compatibility performance value.
func LegacyJobPerf(key string) *JobPerf {
	legacyFlagsMu.RLock()
	result := cloneJobPerf(JobPerfs[key])
	legacyFlagsMu.RUnlock()
	return result
}

// LegacyJobPerfValue reads one compatibility performance multiplier without
// allocating. It is intended for legacy hot paths that only inspect its value.
func LegacyJobPerfValue(key string) (JobPerf, bool) {
	legacyFlagsMu.RLock()
	perf := JobPerfs[key]
	if perf == nil {
		legacyFlagsMu.RUnlock()
		return JobPerf{}, false
	}
	result := *perf
	legacyFlagsMu.RUnlock()
	return result, true
}

// LegacyPerformanceSnapshot returns copies of the process-wide performance
// indexes for reports and compatibility backup/restore operations.
func LegacyPerformanceSnapshot() (map[string]*JobPerf, map[string]*PerfSta) {
	legacyFlagsMu.RLock()
	jobPerfs := make(map[string]*JobPerf, len(JobPerfs))
	for key, perf := range JobPerfs {
		jobPerfs[key] = cloneJobPerf(perf)
	}
	stratPerfSta := make(map[string]*PerfSta, len(StratPerfSta))
	for key, sta := range StratPerfSta {
		stratPerfSta[key] = clonePerfSta(sta)
	}
	legacyFlagsMu.RUnlock()
	return jobPerfs, stratPerfSta
}

// ReplaceLegacyPerformance publishes compatibility performance indexes using
// owned copies so a backup cannot be mutated after it is restored.
func ReplaceLegacyPerformance(jobPerfs map[string]*JobPerf, stratPerfSta map[string]*PerfSta) {
	legacyFlagsMu.Lock()
	JobPerfs = make(map[string]*JobPerf, len(jobPerfs))
	for key, perf := range jobPerfs {
		JobPerfs[key] = cloneJobPerf(perf)
	}
	StratPerfSta = make(map[string]*PerfSta, len(stratPerfSta))
	for key, sta := range stratPerfSta {
		StratPerfSta[key] = clonePerfSta(sta)
	}
	legacyFlagsMu.Unlock()
}

// ResetLegacyPerformance clears the compatibility performance indexes.
func ResetLegacyPerformance() {
	ReplaceLegacyPerformance(nil, nil)
}

func (s *State) admissionSnapshotLocked() *admissionSnapshot {
	if snapshot := s.admission.Load(); snapshot != nil {
		return snapshot
	}
	return s.initAdmissionSnapshotLocked()
}

func (s *State) initAdmissionSnapshotLocked() *admissionSnapshot {
	enabled := make(map[string]bool, len(s.Pairs))
	for _, pair := range s.Pairs {
		enabled[pair] = true
	}
	snapshot := &admissionSnapshot{enabled: enabled}
	s.admission.Store(snapshot)
	return snapshot
}

// publishAdmissionLocked takes ownership of a newly built map. Callers never
// mutate it after publication; admission reads remain a single atomic load.
func (s *State) publishAdmissionLocked(enabled map[string]bool) {
	s.admission.Store(&admissionSnapshot{enabled: enabled})
}

// LegacyPairEnabled is the compatibility facade for callers that have not
// been given a Runtime yet. New code should use State.PairEnabled.
func LegacyPairEnabled(pair string) bool {
	legacyAdmissionMu.RLock()
	enabled := PairsMap[pair]
	legacyAdmissionMu.RUnlock()
	return enabled
}

// LegacyAdmissionPairs returns a stable copy of the process-wide pair list.
// It is intentionally narrow so migrated callers do not need to touch the
// mutable global slice directly.
func LegacyAdmissionPairs() []string {
	legacyAdmissionMu.RLock()
	pairs := slices.Clone(Pairs)
	legacyAdmissionMu.RUnlock()
	return pairs
}

// SetLegacyAdmissionPair updates one pair in the process-wide compatibility
// facade. Enabled pairs are retained in Pairs to preserve the old API's
// re-add behavior after a pair is removed from a strategy.
func SetLegacyAdmissionPair(pair string, enabled bool) {
	legacyAdmissionMu.Lock()
	if PairsMap == nil {
		PairsMap = make(map[string]bool, len(Pairs))
	}
	PairsMap[pair] = enabled
	if enabled && !slices.Contains(Pairs, pair) {
		Pairs = append(Pairs, pair)
	}
	legacyAdmissionMu.Unlock()
}

// SetLegacyAdmissionSnapshot publishes the active state for the legacy pair
// set while retaining compatibility with callers that pass newly discovered
// pairs in active.
func SetLegacyAdmissionSnapshot(active map[string]bool) {
	legacyAdmissionMu.Lock()
	if PairsMap == nil {
		PairsMap = make(map[string]bool, len(Pairs)+len(active))
	}
	for _, pair := range Pairs {
		PairsMap[pair] = active[pair]
	}
	for pair, enabled := range active {
		PairsMap[pair] = enabled
		if enabled && !slices.Contains(Pairs, pair) {
			Pairs = append(Pairs, pair)
		}
	}
	legacyAdmissionMu.Unlock()
}

// LegacyPairIsBanned reports and lazily clears a process-wide pair ban.
func LegacyPairIsBanned(pair string, nowMS int64) bool {
	legacyFlagsMu.Lock()
	until, ok := BanPairsUntil[pair]
	if ok && nowMS >= until {
		delete(BanPairsUntil, pair)
		ok = false
	}
	legacyFlagsMu.Unlock()
	return ok
}

// SetLegacyPairBanUntil updates one process-wide pair ban.
func SetLegacyPairBanUntil(pair string, untilMS int64) {
	legacyFlagsMu.Lock()
	if BanPairsUntil == nil {
		BanPairsUntil = make(map[string]int64)
	}
	if untilMS <= 0 {
		delete(BanPairsUntil, pair)
	} else {
		BanPairsUntil[pair] = untilMS
	}
	legacyFlagsMu.Unlock()
}

// LegacyNoEnterUntilFor reads the process-wide account trading switch.
func LegacyNoEnterUntilFor(account string) (int64, bool) {
	legacyFlagsMu.RLock()
	until, ok := NoEnterUntil[account]
	legacyFlagsMu.RUnlock()
	return until, ok
}

// SetLegacyNoEnterUntil updates the process-wide account trading switch.
func SetLegacyNoEnterUntil(account string, untilMS int64) {
	legacyFlagsMu.Lock()
	if NoEnterUntil == nil {
		NoEnterUntil = make(map[string]int64)
	}
	if untilMS <= 0 {
		delete(NoEnterUntil, account)
	} else {
		NoEnterUntil[account] = untilMS
	}
	legacyFlagsMu.Unlock()
}

// LegacyNoEnterUntilSnapshot returns a stable copy for low-frequency views.
func LegacyNoEnterUntilSnapshot() map[string]int64 {
	legacyFlagsMu.RLock()
	result := maps.Clone(NoEnterUntil)
	legacyFlagsMu.RUnlock()
	return result
}

// ReplaceLegacyNoEnterUntil restores the process-wide account trading switch
// from an owned snapshot.
func ReplaceLegacyNoEnterUntil(values map[string]int64) {
	legacyFlagsMu.Lock()
	NoEnterUntil = maps.Clone(values)
	if NoEnterUntil == nil {
		NoEnterUntil = make(map[string]int64)
	}
	legacyFlagsMu.Unlock()
}

// ResetLegacyNoEnterUntil clears the compatibility account trading switch.
func ResetLegacyNoEnterUntil() {
	ReplaceLegacyNoEnterUntil(nil)
}

// LegacyStrategyTimeFramesSnapshot returns a deep copy of the compatibility
// strategy timeframe index for low-frequency consumers.
func LegacyStrategyTimeFramesSnapshot() map[string]map[string]string {
	legacyFlagsMu.RLock()
	result := make(map[string]map[string]string, len(StgPairTfs))
	for strategy, pairs := range StgPairTfs {
		result[strategy] = maps.Clone(pairs)
	}
	legacyFlagsMu.RUnlock()
	return result
}

// LegacyStrategyTimeFrame resolves one compatibility strategy/pair mapping.
func LegacyStrategyTimeFrame(strategy, pair string) (string, bool) {
	legacyFlagsMu.RLock()
	pairs := StgPairTfs[strategy]
	timeframe, ok := pairs[pair]
	legacyFlagsMu.RUnlock()
	return timeframe, ok
}

// LegacyTimeFrameStateSnapshot returns owned copies of both compatibility
// timeframe indexes. Pair refresh code mutates the copies and publishes them
// with ReplaceLegacyTimeFrameState after the refresh is complete.
func LegacyTimeFrameStateSnapshot() (map[string]int, map[string]map[string]string) {
	legacyFlagsMu.RLock()
	timeframes := maps.Clone(TFSecs)
	strategyPairs := make(map[string]map[string]string, len(StgPairTfs))
	for strategy, pairs := range StgPairTfs {
		strategyPairs[strategy] = maps.Clone(pairs)
	}
	legacyFlagsMu.RUnlock()
	return timeframes, strategyPairs
}

// ReplaceLegacyTimeFrameState publishes complete compatibility timeframe
// indexes. Inputs are cloned so callers retain ownership of their builders.
func ReplaceLegacyTimeFrameState(timeframes map[string]int, strategyPairs map[string]map[string]string) {
	legacyFlagsMu.Lock()
	TFSecs = maps.Clone(timeframes)
	if TFSecs == nil {
		TFSecs = make(map[string]int)
	}
	StgPairTfs = make(map[string]map[string]string, len(strategyPairs))
	for strategy, pairs := range strategyPairs {
		StgPairTfs[strategy] = maps.Clone(pairs)
	}
	legacyFlagsMu.Unlock()
}

// LegacyPairStateSnapshot returns an owned copy of the compatibility pair
// list and admission map.
func LegacyPairStateSnapshot() ([]string, map[string]bool) {
	legacyAdmissionMu.RLock()
	pairs := slices.Clone(Pairs)
	pairsMap := maps.Clone(PairsMap)
	legacyAdmissionMu.RUnlock()
	return pairs, pairsMap
}

// ReplaceLegacyPairState publishes the compatibility pair list and admission
// map together, then removes bans for pairs that are no longer eligible.
func ReplaceLegacyPairState(pairs []string, pairsMap map[string]bool) {
	active := maps.Clone(pairsMap)
	if active == nil {
		active = make(map[string]bool)
	}
	legacyAdmissionMu.Lock()
	Pairs = slices.Clone(pairs)
	PairsMap = active
	legacyFlagsMu.Lock()
	for pair := range BanPairsUntil {
		if !active[pair] {
			delete(BanPairsUntil, pair)
		}
	}
	legacyFlagsMu.Unlock()
	legacyAdmissionMu.Unlock()
}

// SetLegacyPairs replaces the compatibility pair list while preserving the
// legacy rule that policy-declared pairs remain eligible for admission.
func SetLegacyPairs(pairs, additional []string) {
	active := make(map[string]bool, len(pairs)+len(additional))
	for _, pair := range pairs {
		active[pair] = true
	}
	for _, pair := range additional {
		active[pair] = true
	}
	ReplaceLegacyPairState(pairs, active)
}

func (s *State) Context() context.Context {
	if s == nil {
		return nil
	}
	return s.ctx
}

func (s *State) Done() <-chan struct{} {
	if s == nil || s.ctx == nil {
		return nil
	}
	return s.ctx.Done()
}

func (s *State) Sleep(d time.Duration) bool {
	if s == nil || s.ctx == nil {
		time.Sleep(d)
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-s.ctx.Done():
		return false
	}
}

// GetOdBook reads this Runtime's order book cache without consulting the
// process-wide compatibility facade.
func (s *State) GetOdBook(pair string) (*banexg.OrderBook, bool) {
	if s == nil {
		return nil, false
	}
	s.lockOdMatch.RLock()
	book, ok := s.odBooks[pair]
	s.lockOdMatch.RUnlock()
	return book, ok
}

// SetOdBook updates this Runtime's order book cache.
func (s *State) SetOdBook(pair string, book *banexg.OrderBook) {
	if s == nil {
		return
	}
	s.lockOdMatch.Lock()
	if s.odBooks == nil {
		s.odBooks = make(map[string]*banexg.OrderBook)
	}
	s.odBooks[pair] = book
	s.lockOdMatch.Unlock()
}

// OrderMatchEnabled reports whether this Runtime should match orders for a
// timeframe. The concrete map remains for compatibility, while the accessor
// makes its lock contract explicit to hot-path callers.
func (s *State) OrderMatchEnabled(timeFrame string) bool {
	if s == nil {
		return false
	}
	s.lockOdMatch.RLock()
	enabled := s.orderMatchTfs[timeFrame]
	s.lockOdMatch.RUnlock()
	return enabled
}

// OrderMatchTfsSnapshot returns a stable copy of this Runtime's order-match
// policy. The returned map may be retained by reports without holding state
// locks.
func (s *State) OrderMatchTfsSnapshot() map[string]bool {
	if s == nil {
		return nil
	}
	s.lockOdMatch.RLock()
	result := maps.Clone(s.orderMatchTfs)
	s.lockOdMatch.RUnlock()
	return result
}

// SetOrderMatchEnabled updates one Runtime order-match flag.
func (s *State) SetOrderMatchEnabled(timeFrame string, enabled bool) {
	if s == nil {
		return
	}
	s.lockOdMatch.Lock()
	if s.orderMatchTfs == nil {
		s.orderMatchTfs = make(map[string]bool)
	}
	s.orderMatchTfs[timeFrame] = enabled
	s.lockOdMatch.Unlock()
}

// ReplaceOrderMatchTfs publishes a complete order-match policy while keeping
// the concrete map private to the Runtime state boundary.
func (s *State) ReplaceOrderMatchTfs(flags map[string]bool) {
	if s == nil {
		return
	}
	s.lockOdMatch.Lock()
	s.orderMatchTfs = maps.Clone(flags)
	if s.orderMatchTfs == nil {
		s.orderMatchTfs = make(map[string]bool)
	}
	s.lockOdMatch.Unlock()
}

// LegacyOrderMatchEnabled reads one process-wide order-match flag.
func LegacyOrderMatchEnabled(timeFrame string) bool {
	LockOdMatch.RLock()
	enabled := OrderMatchTfs[timeFrame]
	LockOdMatch.RUnlock()
	return enabled
}

// LegacyOrderMatchTfsSnapshot returns a stable copy of the compatibility
// policy for report and command consumers.
func LegacyOrderMatchTfsSnapshot() map[string]bool {
	LockOdMatch.RLock()
	result := maps.Clone(OrderMatchTfs)
	LockOdMatch.RUnlock()
	return result
}

// SetLegacyOrderMatchEnabled updates one compatibility order-match flag.
func SetLegacyOrderMatchEnabled(timeFrame string, enabled bool) {
	LockOdMatch.Lock()
	if OrderMatchTfs == nil {
		OrderMatchTfs = make(map[string]bool)
	}
	OrderMatchTfs[timeFrame] = enabled
	LockOdMatch.Unlock()
}

// ReplaceLegacyOrderMatchTfs publishes a complete compatibility order-match
// policy from an owned copy.
func ReplaceLegacyOrderMatchTfs(flags map[string]bool) {
	LockOdMatch.Lock()
	OrderMatchTfs = maps.Clone(flags)
	if OrderMatchTfs == nil {
		OrderMatchTfs = make(map[string]bool)
	}
	LockOdMatch.Unlock()
}

// DrainLegacyTfPairHits atomically returns and clears the compatibility hit
// counters used by the legacy summary scheduler.
func DrainLegacyTfPairHits() map[string]map[string]int {
	TfPairHitsLock.Lock()
	result := make(map[string]map[string]int, len(TfPairHits))
	for timeframe, pairs := range TfPairHits {
		result[timeframe] = maps.Clone(pairs)
	}
	TfPairHits = make(map[string]map[string]int)
	TfPairHitsLock.Unlock()
	return result
}

// AddLegacyTfPairHits records compatibility websocket bar counters under the
// same lock used by the legacy summary scheduler.
func AddLegacyTfPairHits(timeFrame, pair string, count int) {
	if count == 0 {
		return
	}
	TfPairHitsLock.Lock()
	if TfPairHits == nil {
		TfPairHits = make(map[string]map[string]int)
	}
	hits := TfPairHits[timeFrame]
	if hits == nil {
		hits = make(map[string]int)
		TfPairHits[timeFrame] = hits
	}
	hits[pair] += count
	TfPairHitsLock.Unlock()
}

// LegacyTfPairHitsSnapshot returns an owned copy for compatibility backup.
func LegacyTfPairHitsSnapshot() map[string]map[string]int {
	TfPairHitsLock.Lock()
	result := make(map[string]map[string]int, len(TfPairHits))
	for timeframe, pairs := range TfPairHits {
		result[timeframe] = maps.Clone(pairs)
	}
	TfPairHitsLock.Unlock()
	return result
}

// ReplaceLegacyTfPairHits restores compatibility counters from an owned copy.
func ReplaceLegacyTfPairHits(hits map[string]map[string]int) {
	TfPairHitsLock.Lock()
	TfPairHits = make(map[string]map[string]int, len(hits))
	for timeframe, pairs := range hits {
		TfPairHits[timeframe] = maps.Clone(pairs)
	}
	TfPairHitsLock.Unlock()
}

// AddTfPairHits records websocket bars in this Runtime's typed counters.
func (s *State) AddTfPairHits(timeFrame, pair string, count int) {
	if s == nil || count == 0 {
		return
	}
	s.tfPairHitsLock.Lock()
	if s.tfPairHits == nil {
		s.tfPairHits = make(map[string]map[string]int)
	}
	hits := s.tfPairHits[timeFrame]
	if hits == nil {
		hits = make(map[string]int)
		s.tfPairHits[timeFrame] = hits
	}
	hits[pair] += count
	s.tfPairHitsLock.Unlock()
}

// DrainTfPairHits atomically takes the current hit counters and starts a new
// set. This keeps summary/report code from retaining the mutable nested maps.
func (s *State) DrainTfPairHits() map[string]map[string]int {
	if s == nil {
		return nil
	}
	s.tfPairHitsLock.Lock()
	result := make(map[string]map[string]int, len(s.tfPairHits))
	for timeframe, pairs := range s.tfPairHits {
		result[timeframe] = maps.Clone(pairs)
	}
	s.tfPairHits = make(map[string]map[string]int)
	s.tfPairHitsLock.Unlock()
	return result
}

// Stop is idempotent and only affects this State.
func (s *State) Stop() {
	if s == nil {
		return
	}
	var stopAll func()
	var calls []func()
	s.stopOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		s.exitLock.Lock()
		s.stopped = true
		calls = s.exitCalls
		s.exitCalls = nil
		s.exitLock.Unlock()
		stopAll = s.StopAll
		s.SetBotRunning(false)
	})
	if stopAll != nil {
		stopAll()
	}
	for _, call := range calls {
		if call != nil {
			call()
		}
	}
}

func (s *State) Close() {
	if s == nil {
		return
	}
	s.Stop()
}
