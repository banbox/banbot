package strat

import (
	"fmt"
	"maps"
	"strconv"
	"strings"
	"sync"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg/log"
	ta "github.com/banbox/banta"
	"go.uber.org/zap"
)

// State owns mutable strategy data for one runtime. The registry maps are
// intentionally concrete fields so hot-path callers do not pay for a lookup
// layer or an allocation. Callers own lifecycle synchronization for those
// registries; callback and cache helpers protect their own mutations.
type State struct {
	Versions    map[string]int
	Envs        map[string]*ta.BarEnv
	TmpEnvs     map[string]*ta.BarEnv
	AccJobs     map[string]map[string]map[string]*StratJob
	AccInfoJobs map[string]map[string]map[string]*StratJob
	PairStrats  map[string]map[string]*TradeStrat
	ForbidJobs  map[string]map[string]bool
	WsSubJobs   map[string]map[string]map[*StratJob]bool

	AccOdSubs    map[string][]FnOdChange
	AccFailOpens map[string]map[string]int
	WsSubUnWatch func(map[string][]string)
	cacheStrats  map[string]*TradeStrat
	pairHooks    PairUpdateHooks
	pairHooksMu  sync.RWMutex
	orderSubLock sync.Mutex
	failOpenLock sync.Mutex
	cacheMu      sync.Mutex
	tmpEnvLock   sync.Mutex
}

// NewState creates an isolated strategy state with every registry ready for
// use.
func NewState() *State {
	return &State{
		Versions:     make(map[string]int),
		Envs:         make(map[string]*ta.BarEnv),
		TmpEnvs:      make(map[string]*ta.BarEnv),
		AccJobs:      make(map[string]map[string]map[string]*StratJob),
		AccInfoJobs:  make(map[string]map[string]map[string]*StratJob),
		PairStrats:   make(map[string]map[string]*TradeStrat),
		ForbidJobs:   make(map[string]map[string]bool),
		WsSubJobs:    make(map[string]map[string]map[*StratJob]bool),
		AccOdSubs:    make(map[string][]FnOdChange),
		AccFailOpens: make(map[string]map[string]int),
		cacheStrats:  make(map[string]*TradeStrat),
	}
}

func (s *State) ensureMaps() {
	if s == nil {
		return
	}
	if s.Versions == nil {
		s.Versions = make(map[string]int)
	}
	if s.Envs == nil {
		s.Envs = make(map[string]*ta.BarEnv)
	}
	if s.TmpEnvs == nil {
		s.TmpEnvs = make(map[string]*ta.BarEnv)
	}
	if s.AccJobs == nil {
		s.AccJobs = make(map[string]map[string]map[string]*StratJob)
	}
	if s.AccInfoJobs == nil {
		s.AccInfoJobs = make(map[string]map[string]map[string]*StratJob)
	}
	if s.PairStrats == nil {
		s.PairStrats = make(map[string]map[string]*TradeStrat)
	}
	if s.ForbidJobs == nil {
		s.ForbidJobs = make(map[string]map[string]bool)
	}
	if s.WsSubJobs == nil {
		s.WsSubJobs = make(map[string]map[string]map[*StratJob]bool)
	}
	if s.AccOdSubs == nil {
		s.AccOdSubs = make(map[string][]FnOdChange)
	}
	if s.AccFailOpens == nil {
		s.AccFailOpens = make(map[string]map[string]int)
	}
	if s.cacheStrats == nil {
		s.cacheStrats = make(map[string]*TradeStrat)
	}
}

// legacyState is a typed view over the package globals. The globals remain
// the compatibility facade used by existing callers.
var legacyState = &State{
	Versions:     Versions,
	Envs:         Envs,
	TmpEnvs:      TmpEnvs,
	AccJobs:      AccJobs,
	AccInfoJobs:  AccInfoJobs,
	PairStrats:   PairStrats,
	ForbidJobs:   ForbidJobs,
	WsSubJobs:    WsSubJobs,
	AccOdSubs:    accOdSubs,
	AccFailOpens: accFailOpens,
	WsSubUnWatch: WsSubUnWatch,
	cacheStrats:  cacheStrats,
}

// LegacyState returns the typed view of the current package-level state.
// Refreshing the fields preserves compatibility with callers that rebind the
// legacy maps during a serial run or test.
func LegacyState() *State {
	legacyState.Versions = Versions
	legacyState.Envs = Envs
	legacyState.TmpEnvs = TmpEnvs
	legacyState.AccJobs = AccJobs
	legacyState.AccInfoJobs = AccInfoJobs
	legacyState.PairStrats = PairStrats
	legacyState.ForbidJobs = ForbidJobs
	legacyState.WsSubJobs = WsSubJobs
	legacyState.AccOdSubs = accOdSubs
	legacyState.AccFailOpens = accFailOpens
	legacyState.WsSubUnWatch = WsSubUnWatch
	legacyState.cacheMu.Lock()
	legacyState.cacheStrats = cacheStrats
	legacyState.cacheMu.Unlock()
	return legacyState
}

// Reset clears only the receiver's mutable strategy state.
func (s *State) Reset() {
	if s == nil {
		return
	}
	s.Versions = make(map[string]int)
	s.Envs = make(map[string]*ta.BarEnv)
	s.TmpEnvs = make(map[string]*ta.BarEnv)
	s.AccJobs = make(map[string]map[string]map[string]*StratJob)
	s.AccInfoJobs = make(map[string]map[string]map[string]*StratJob)
	s.PairStrats = make(map[string]map[string]*TradeStrat)
	s.ForbidJobs = make(map[string]map[string]bool)
	s.WsSubJobs = make(map[string]map[string]map[*StratJob]bool)

	s.orderSubLock.Lock()
	s.AccOdSubs = make(map[string][]FnOdChange)
	s.orderSubLock.Unlock()
	s.failOpenLock.Lock()
	s.AccFailOpens = make(map[string]map[string]int)
	s.failOpenLock.Unlock()
	s.cacheMu.Lock()
	s.cacheStrats = make(map[string]*TradeStrat)
	s.cacheMu.Unlock()
	s.WsSubUnWatch = nil
	s.pairHooksMu.Lock()
	s.pairHooks = PairUpdateHooks{}
	s.pairHooksMu.Unlock()

	if s == legacyState {
		Versions = s.Versions
		Envs = s.Envs
		TmpEnvs = s.TmpEnvs
		AccJobs = s.AccJobs
		AccInfoJobs = s.AccInfoJobs
		PairStrats = s.PairStrats
		ForbidJobs = s.ForbidJobs
		WsSubJobs = s.WsSubJobs
		accOdSubs = s.AccOdSubs
		accFailOpens = s.AccFailOpens
		WsSubUnWatch = s.WsSubUnWatch
		cacheMu.Lock()
		cacheStrats = s.cacheStrats
		cacheMu.Unlock()
	}
}

// SetPairUpdateHooks binds pair rotation callbacks to this strategy state.
// Runtime callers use this receiver-owned binding; SetPairUpdateHooks remains
// the serialized compatibility facade for legacy callers.
func (s *State) SetPairUpdateHooks(h PairUpdateHooks) {
	if s == nil {
		return
	}
	if h.LookupSymbol == nil {
		if h.SymbolState != nil {
			h.LookupSymbol = h.SymbolState.GetExSymbolCur
		} else if s == legacyState {
			h.LookupSymbol = orm.GetExSymbolCur
		}
	}
	s.pairHooksMu.Lock()
	s.pairHooks = h
	s.pairHooksMu.Unlock()
}

// PairUpdateHooks returns the callbacks bound to this state. A zero value
// means that the caller should use the legacy compatibility facade.
func (s *State) PairUpdateHooks() PairUpdateHooks {
	if s == nil {
		return PairUpdateHooks{}
	}
	s.pairHooksMu.RLock()
	h := s.pairHooks
	s.pairHooksMu.RUnlock()
	return h
}

// Get returns a strategy registered for pair and strategy ID without
// allocating or locking.
func (s *State) Get(pair, stratID string) *TradeStrat {
	if s == nil {
		return nil
	}
	return s.PairStrats[pair][stratID]
}

// OrderCallbacks returns the instance-owned order callback registry directly.
func (s *State) OrderCallbacks() map[string][]FnOdChange {
	if s == nil {
		return nil
	}
	return s.AccOdSubs
}

// FailureCounters returns the instance-owned failed-entry counters directly.
func (s *State) FailureCounters() map[string]map[string]int {
	if s == nil {
		return nil
	}
	return s.AccFailOpens
}

// AddOdSub adds an order-status callback to this state.
func (s *State) AddOdSub(acc string, cb FnOdChange) {
	if s == nil {
		return
	}
	s.orderSubLock.Lock()
	if s.AccOdSubs == nil {
		s.AccOdSubs = make(map[string][]FnOdChange)
	}
	s.AccOdSubs[acc] = append(s.AccOdSubs[acc], cb)
	s.orderSubLock.Unlock()
}

// AddAccFailOpen records one failed-entry reason for this state.
func (s *State) AddAccFailOpen(acc, tag string) {
	s.AddAccFailOpens(acc, tag, 1)
}

// AddAccFailOpens records failed-entry reasons for this state.
func (s *State) AddAccFailOpens(acc, tag string, num int) {
	if s == nil {
		return
	}
	s.failOpenLock.Lock()
	if s.AccFailOpens == nil {
		s.AccFailOpens = make(map[string]map[string]int)
	}
	tags := s.AccFailOpens[acc]
	if tags == nil {
		tags = make(map[string]int)
		s.AccFailOpens[acc] = tags
	}
	tags[tag] += num
	s.failOpenLock.Unlock()
}

// CachedStrategies returns the instance-owned strategy cache without a copy.
// Use GetCachedStrategy and SetCachedStrategy when concurrent cache access is
// possible.
func (s *State) CachedStrategies() map[string]*TradeStrat {
	if s == nil {
		return nil
	}
	return s.cacheStrats
}

// GetCachedStrategy looks up one entry in the instance-owned strategy cache.
func (s *State) GetCachedStrategy(key string) (*TradeStrat, bool) {
	if s == nil {
		return nil, false
	}
	s.cacheMu.Lock()
	stgy, ok := s.cacheStrats[key]
	s.cacheMu.Unlock()
	return stgy, ok
}

// SetCachedStrategy stores one entry in the instance-owned strategy cache.
func (s *State) SetCachedStrategy(key string, stgy *TradeStrat) {
	if s == nil {
		return
	}
	s.cacheMu.Lock()
	if s.cacheStrats == nil {
		s.cacheStrats = make(map[string]*TradeStrat)
	}
	s.cacheStrats[key] = stgy
	s.cacheMu.Unlock()
}

// newStrategyWithState keeps explicit runtimes out of the package-level
// strategy cache. The legacy facade continues to use New, while each typed
// state owns its strategy object and its mutable policy/slice fields.
func newStrategyWithState(state *State, pol *config.RunPolicyConfig) *TradeStrat {
	if state == nil || state == legacyState {
		return New(pol)
	}
	key := pol.ID() + "\n" + pol.ToYaml()
	if stgy, ok := state.GetCachedStrategy(key); ok {
		return stgy
	}

	localPol := pol.Clone()
	makeFn, ok := StratMake[localPol.Name]
	if !ok {
		panic("strategy not found: " + localPol.Name)
	}
	stgy := cloneTradeStrat(makeFn(localPol), localPol)
	stgy.Name = localPol.ID()
	validateDataCallbacks(stgy)
	if stgy.MinTfScore == 0 {
		stgy.MinTfScore = 0.75
	}
	stgy.Policy = localPol
	if localPol.StakeRate > 0 {
		stgy.StakeRate = localPol.StakeRate
	}
	if localPol.OrderBarMax > 0 {
		stgy.OdBarMax = localPol.OrderBarMax
	}
	if len(localPol.RunTimeframes) > 0 {
		stgy.RunTimeFrames = append([]string(nil), localPol.RunTimeframes...)
	} else if len(stgy.RunTimeFrames) == 0 && len(stgy.TimeFrames) > 0 {
		stgy.RunTimeFrames = config.SplitTimeFrames(stgy.TimeFrames)
	}
	if localPol.StopLoss != nil {
		slRate, isFloat := localPol.StopLoss.(float64)
		if isFloat {
			if slRate > 0 {
				stgy.StopLoss = slRate
			} else if slRate < 0 {
				log.Error("stop_loss should > 0", zap.String("policy", localPol.Name))
			}
		} else if slStr, ok := localPol.StopLoss.(string); ok {
			if strings.TrimSpace(slStr) != "" {
				var err error
				slStr = strings.TrimSpace(slStr)
				if strings.HasSuffix(slStr, "%") {
					slRate, err = strconv.ParseFloat(slStr[:len(slStr)-1], 64)
					slRate /= 100
				} else {
					slRate, err = strconv.ParseFloat(slStr, 64)
				}
				if err != nil {
					log.Error("invalid stop_loss", zap.String("policy", localPol.Name), zap.Error(err))
				} else if slRate > 0 {
					stgy.StopLoss = slRate
				}
			}
		} else if slInt, ok := localPol.StopLoss.(int); ok {
			if slInt != 0 {
				log.Error("stop_loss format error, expect to be 5% or 0.05", zap.String("policy", localPol.Name))
			}
		} else {
			log.Error("invalid stop_loss type, expect e.g.: 5% or 0.05", zap.String("policy", localPol.Name),
				zap.String("type", fmt.Sprintf("%T", localPol.StopLoss)))
		}
	}
	state.SetCachedStrategy(key, stgy)
	return stgy
}

func cloneTradeStrat(stgy *TradeStrat, pol *config.RunPolicyConfig) *TradeStrat {
	copy := *stgy
	copy.Policy = pol
	copy.WsSubs = maps.Clone(stgy.WsSubs)
	copy.RunTimeFrames = append([]string(nil), stgy.RunTimeFrames...)
	copy.Outputs = append([]string(nil), stgy.Outputs...)
	return &copy
}
