package core

import (
	"context"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/sasha-s/go-deadlock"
)

type admissionSnapshot struct {
	enabled map[string]bool
}

// legacyAdmissionMu protects the small compatibility facade below. Runtime
// code should use State's admission snapshot instead; the process-wide facade
// remains for callers that have not migrated to a Runtime yet.
var legacyAdmissionMu sync.RWMutex

// State owns the mutable core state for one Runtime. Process-scoped resources
// such as Cache are not owned by State. It is intentionally a concrete type so
// hot paths can use direct field access.
type State struct {
	RunMode      string
	RunEnv       string
	StartAt      int64
	EnvReal      bool
	LiveMode     bool
	BackTestMode bool

	TFSecs       map[string]int
	ExgName      string
	Market       string
	IsContract   bool
	CheckWallets bool
	ContractType string
	StgPairTfs   map[string]map[string]string
	Pairs        []string
	// PairsMap is a legacy compatibility view. Runtime code must use
	// PairEnabled and the SetAdmission* methods instead of mutating it.
	PairsMap      map[string]bool
	BanPairsUntil map[string]int64
	NoEnterUntil  map[string]int64
	TfPairHits    map[string]map[string]int
	JobPerfs      map[string]*JobPerf
	StratPerfSta  map[string]*PerfSta
	OdBooks       map[string]*banexg.OrderBook
	NumTaCache    int
	OrderMatchTfs map[string]bool

	CPUProfile    bool
	MemProfile    bool
	NetDisable    bool
	SimOrderMatch bool
	NewNumInSim   int
	ParallelOnBar bool
	ConcurNum     int
	SysLang       string

	TfPairHitsLock deadlock.RWMutex
	LockOdMatch    sync.RWMutex
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
	// flagsMu protects the frequently-read admission override maps. The maps
	// remain concrete compatibility fields; explicit runtime code uses the
	// typed accessors below instead of racing on direct map access.
	flagsMu sync.RWMutex
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
		TFSecs:        make(map[string]int),
		StgPairTfs:    make(map[string]map[string]string),
		PairsMap:      make(map[string]bool),
		BanPairsUntil: make(map[string]int64),
		NoEnterUntil:  make(map[string]int64),
		TfPairHits:    make(map[string]map[string]int),
		JobPerfs:      make(map[string]*JobPerf),
		StratPerfSta:  make(map[string]*PerfSta),
		OdBooks:       make(map[string]*banexg.OrderBook),
		OrderMatchTfs: make(map[string]bool),
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
	if s.TFSecs == nil {
		s.TFSecs = make(map[string]int)
	}
	if s.StgPairTfs == nil {
		s.StgPairTfs = make(map[string]map[string]string)
	}
	if s.PairsMap == nil {
		s.PairsMap = make(map[string]bool)
	}
	if s.BanPairsUntil == nil {
		s.BanPairsUntil = make(map[string]int64)
	}
	if s.NoEnterUntil == nil {
		s.NoEnterUntil = make(map[string]int64)
	}
	if s.TfPairHits == nil {
		s.TfPairHits = make(map[string]map[string]int)
	}
	if s.JobPerfs == nil {
		s.JobPerfs = make(map[string]*JobPerf)
	}
	if s.StratPerfSta == nil {
		s.StratPerfSta = make(map[string]*PerfSta)
	}
	if s.OdBooks == nil {
		s.OdBooks = make(map[string]*banexg.OrderBook)
	}
	if s.OrderMatchTfs == nil {
		s.OrderMatchTfs = make(map[string]bool)
	}
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
	for pair := range s.BanPairsUntil {
		if !enabled[pair] {
			delete(s.BanPairsUntil, pair)
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
	until, ok := s.BanPairsUntil[pair]
	if ok && nowMS >= until {
		delete(s.BanPairsUntil, pair)
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
	if s.BanPairsUntil == nil {
		s.BanPairsUntil = make(map[string]int64)
	}
	if untilMS <= 0 {
		delete(s.BanPairsUntil, pair)
	} else {
		s.BanPairsUntil[pair] = untilMS
	}
	s.flagsMu.Unlock()
}

// BannedPairs returns a stable list for low-frequency refresh and cleanup.
func (s *State) BannedPairs() []string {
	if s == nil {
		return nil
	}
	s.flagsMu.RLock()
	pairs := make([]string, 0, len(s.BanPairsUntil))
	for pair := range s.BanPairsUntil {
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
	until, ok := s.NoEnterUntil[account]
	s.flagsMu.RUnlock()
	return until, ok
}

// SetNoEnterUntil updates the account trading switch owned by this runtime.
// A non-positive value clears the switch.
func (s *State) SetNoEnterUntil(account string, untilMS int64) {
	if s == nil {
		return
	}
	s.flagsMu.Lock()
	if s.NoEnterUntil == nil {
		s.NoEnterUntil = make(map[string]int64)
	}
	if untilMS <= 0 {
		delete(s.NoEnterUntil, account)
	} else {
		s.NoEnterUntil[account] = untilMS
	}
	s.flagsMu.Unlock()
}

func (s *State) admissionSnapshotLocked() *admissionSnapshot {
	if snapshot := s.admission.Load(); snapshot != nil {
		return snapshot
	}
	return s.initAdmissionSnapshotLocked()
}

func (s *State) initAdmissionSnapshotLocked() *admissionSnapshot {
	enabled := make(map[string]bool, len(s.Pairs)+len(s.PairsMap))
	for _, pair := range s.Pairs {
		enabled[pair] = true
	}
	for pair, allowed := range s.PairsMap {
		enabled[pair] = allowed
	}
	compat := maps.Clone(enabled)
	s.PairsMap = compat
	snapshot := &admissionSnapshot{enabled: enabled}
	s.admission.Store(snapshot)
	return snapshot
}

func (s *State) publishAdmissionLocked(enabled map[string]bool) {
	snapshotEnabled := maps.Clone(enabled)
	if snapshotEnabled == nil {
		snapshotEnabled = make(map[string]bool)
	}
	s.PairsMap = maps.Clone(snapshotEnabled)
	s.admission.Store(&admissionSnapshot{enabled: snapshotEnabled})
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
	s.LockOdMatch.RLock()
	book, ok := s.OdBooks[pair]
	s.LockOdMatch.RUnlock()
	return book, ok
}

// SetOdBook updates this Runtime's order book cache.
func (s *State) SetOdBook(pair string, book *banexg.OrderBook) {
	if s == nil {
		return
	}
	s.LockOdMatch.Lock()
	if s.OdBooks == nil {
		s.OdBooks = make(map[string]*banexg.OrderBook)
	}
	s.OdBooks[pair] = book
	s.LockOdMatch.Unlock()
}

// AddTfPairHits records websocket bars in this Runtime's typed counters.
func (s *State) AddTfPairHits(timeFrame, pair string, count int) {
	if s == nil || count == 0 {
		return
	}
	s.TfPairHitsLock.Lock()
	if s.TfPairHits == nil {
		s.TfPairHits = make(map[string]map[string]int)
	}
	hits := s.TfPairHits[timeFrame]
	if hits == nil {
		hits = make(map[string]int)
		s.TfPairHits[timeFrame] = hits
	}
	hits[pair] += count
	s.TfPairHitsLock.Unlock()
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
		s.BotRunning = false
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
