package strat

import (
	"sync/atomic"

	"github.com/banbox/banbot/orm"
	ta "github.com/banbox/banta"
	"github.com/sasha-s/go-deadlock"
)

type wsSubJobSnapshot map[string]map[string][]*StratJob

// WsSubJobRegistry is an instance-owned, immutable websocket subscription
// view. Refresh copies the compatible global registry; callbacks only load the
// published snapshot and never lock or copy maps.
type WsSubJobRegistry struct {
	state    *State
	symbols  *orm.SymbolState
	snapshot atomic.Pointer[wsSubJobSnapshot]
}

/*
下面变量中所有的stratName都是RunPolicy.ID()，不是原始策略名。后面添加了":l"或":s"后缀表示仅开多或仅开空
*/

var (
	Versions    = make(map[string]int)                             // stratName: int 策略版本号
	Envs        = make(map[string]*ta.BarEnv)                      // pair_tf: BarEnv
	TmpEnvs     = make(map[string]*ta.BarEnv)                      // pair_tf: BarEnv
	AccJobs     = make(map[string]map[string]map[string]*StratJob) // account: pair_tf: [stratID]StratJob
	AccInfoJobs = make(map[string]map[string]map[string]*StratJob) // account: pair_tf: [stratID_pair]StratJob 额外订阅
	PairStrats  = make(map[string]map[string]*TradeStrat)          // pair:[stratID]TradeStrat 所有的订阅策略，注意有些策略对象虽然Name相同但不是同一个实例
	ForbidJobs  = make(map[string]map[string]bool)                 // pair_tf: [stratID] occupy
	WsSubJobs   = make(map[string]map[string]map[*StratJob]bool)   // msgType: pair: job

	lockJobs     deadlock.RWMutex
	lockInfoJobs deadlock.Mutex
	lockTmpEnv   deadlock.Mutex

	accOdSubs = map[string][]FnOdChange{} // acc: listeners List of subscription order status change events 订阅订单状态变化事件列表
	lockOdSub deadlock.Mutex

	accFailOpens    = make(map[string]map[string]int) // Statistics of reasons for failed entry for accounts 各个账号开单失败原因统计
	lockAccFailOpen deadlock.Mutex

	WsSubUnWatch func(map[string][]string)
)

var legacyWsSubJobRegistry WsSubJobRegistry

func lockJobsReadForState(state *State) {
	if state == nil || state == legacyState {
		lockJobs.RLock()
		return
	}
	state.jobsMu.RLock()
}

func unlockJobsReadForState(state *State) {
	if state == nil || state == legacyState {
		lockJobs.RUnlock()
		return
	}
	state.jobsMu.RUnlock()
}

func lockJobsWriteForState(state *State) {
	if state == nil || state == legacyState {
		lockJobs.Lock()
		return
	}
	state.jobsMu.Lock()
	state.jobsSnapshotDirty.Store(true)
}

func unlockJobsWriteForState(state *State) {
	if state == nil || state == legacyState {
		lockJobs.Unlock()
		return
	}
	state.jobsMu.Unlock()
}

// NewWsSubJobRegistry creates a websocket subscription view for one runtime.
// A nil symbol state keeps the legacy unfiltered registry behavior.
func NewWsSubJobRegistry(symbols *orm.SymbolState) *WsSubJobRegistry {
	return NewWsSubJobRegistryWithState(nil, symbols)
}

// NewWsSubJobRegistryWithState creates a websocket subscription view backed by
// one strategy state. The state pointer is captured once at construction, so
// event dispatch remains a typed atomic snapshot load with no dynamic lookup.
func NewWsSubJobRegistryWithState(state *State, symbols *orm.SymbolState) *WsSubJobRegistry {
	registry := &WsSubJobRegistry{state: state, symbols: symbols}
	registry.Refresh()
	return registry
}

// LegacyWsSubJobRegistry returns the package-compatible subscription view for
// callers that do not bind an explicit symbol state.
func LegacyWsSubJobRegistry() *WsSubJobRegistry {
	return &legacyWsSubJobRegistry
}

// Refresh publishes the current compatible global websocket subscriptions.
func (r *WsSubJobRegistry) Refresh() {
	if r == nil {
		return
	}
	lockJobsReadForState(r.state)
	defer unlockJobsReadForState(r.state)
	var jobs map[string]map[string]map[*StratJob]bool
	if r.state != nil {
		r.state.ensureMaps()
		jobs = r.state.WsSubJobs
	} else {
		jobs = WsSubJobs
	}
	snapshot := make(wsSubJobSnapshot, len(jobs))
	for msgType, pairMap := range jobs {
		pairs := make(map[string][]*StratJob, len(pairMap))
		for pair, jobMap := range pairMap {
			jobs := make([]*StratJob, 0, len(jobMap))
			for job := range jobMap {
				if job == nil || r.symbols != nil && job.symbols != r.symbols {
					continue
				}
				jobs = append(jobs, job)
			}
			if len(jobs) > 0 {
				pairs[pair] = jobs
			}
		}
		if len(pairs) > 0 {
			snapshot[msgType] = pairs
		}
	}
	r.snapshot.Store(&snapshot)
}

func init() {
	legacyWsSubJobRegistry.Refresh()
}

// ForEach runs fn against the immutable websocket subscription view.
func (r *WsSubJobRegistry) ForEach(msgType, pair string, fn func(*StratJob)) {
	if fn == nil {
		return
	}
	if r == nil {
		return
	}
	snapshot := r.snapshot.Load()
	if snapshot == nil {
		return
	}
	for _, job := range (*snapshot)[msgType][pair] {
		fn(job)
	}
}

// Pairs returns the currently subscribed pairs for one websocket message type.
func (r *WsSubJobRegistry) Pairs(msgType string) []string {
	if r == nil {
		return nil
	}
	snapshot := r.snapshot.Load()
	if snapshot == nil {
		return nil
	}
	pairMap := (*snapshot)[msgType]
	pairs := make([]string, 0, len(pairMap))
	for pair := range pairMap {
		pairs = append(pairs, pair)
	}
	return pairs
}

// Types returns the websocket message types with registered pairs.
func (r *WsSubJobRegistry) Types() []string {
	if r == nil {
		return nil
	}
	snapshot := r.snapshot.Load()
	if snapshot == nil {
		return nil
	}
	types := make([]string, 0, len(*snapshot))
	for msgType := range *snapshot {
		types = append(types, msgType)
	}
	return types
}

// RefreshWsSubJobsSnapshot publishes the legacy unfiltered websocket view.
func RefreshWsSubJobsSnapshot() {
	legacyWsSubJobRegistry.Refresh()
}

// ForEachWsSubJob runs fn against the legacy immutable websocket view.
func ForEachWsSubJob(msgType, pair string, fn func(*StratJob)) {
	legacyWsSubJobRegistry.ForEach(msgType, pair, fn)
}

// WsSubJobPairs returns pairs from the legacy immutable websocket view.
func WsSubJobPairs(msgType string) []string {
	return legacyWsSubJobRegistry.Pairs(msgType)
}

// WsSubJobTypes returns message types from the legacy immutable websocket view.
func WsSubJobTypes() []string {
	return legacyWsSubJobRegistry.Types()
}

var (
	FailOpenCostTooLess    = "CostTooLess"
	FailOpenBadDirtOrLimit = "BadDirtOrLimit"
	FailOpenNanNum         = "NanNum"
	FailOpenBadStopLoss    = "BadStopLoss"
	FailOpenBadTakeProfit  = "BadTakeProfit"
	FailOpenBarTooLate     = "BarTooLate"
	FailOpenNoEntry        = "NoEntry"
	FailOpenNumLimit       = "NumLimit"
	FailOpenNumLimitPol    = "NumLimitPol"
)
