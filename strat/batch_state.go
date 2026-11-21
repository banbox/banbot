package strat

import (
	"cmp"
	"encoding/binary"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/banbox/banbot/config"
)

// BatchState owns the mutable batch queue and its backtest timestamp for one trader.
type BatchState struct {
	lock    sync.Mutex
	tasks   map[string]*BatchMap
	lastMS  atomic.Int64
	seed    atomic.Uint64
	hasSeed atomic.Bool
}

// BarPriority returns the neutral, reproducible priority of a symbol within one bar time:
// 64-bit FNV-1a over (seed, barMS, symbol) with a final avalanche mix. Lower value runs first.
func BarPriority(barMS int64, symbol string, seed uint64) uint64 {
	const offset, prime = 14695981039346656037, 1099511628211
	h := uint64(offset)
	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[:8], seed)
	binary.LittleEndian.PutUint64(buf[8:], uint64(barMS))
	for _, c := range buf {
		h ^= uint64(c)
		h *= prime
	}
	for i := 0; i < len(symbol); i++ {
		h ^= uint64(symbol[i])
		h *= prime
	}
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	return h
}

// SetBarOrderSeed pins an explicit neutral bar-order hash seed on this BatchState.
func (s *BatchState) SetBarOrderSeed(seed uint64) {
	if s == nil {
		return
	}
	s.seed.Store(seed)
	s.hasSeed.Store(true)
}

// BarOrderSeed returns any explicit bar-order hash seed set on this BatchState.
func (s *BatchState) BarOrderSeed() (uint64, bool) {
	if s == nil || !s.hasSeed.Load() {
		return 0, false
	}
	return s.seed.Load(), true
}

type BatchReady struct {
	TimeFrame string
	Account   string
	MainJobs  []*StratJob
	InfoJobs  map[string]*JobEnv
	Strategy  *TradeStrat
}

var legacyBatchState = NewBatchState()

func NewBatchState() *BatchState {
	return &BatchState{tasks: make(map[string]*BatchMap)}
}

// LegacyBatchState is only for package-level compatibility facades.
func LegacyBatchState() *BatchState {
	return legacyBatchState
}

func (s *BatchState) LastBatchMS() int64 {
	if s == nil {
		return 0
	}
	return s.lastMS.Load()
}

func (s *BatchState) SetLastBatchMS(value int64) {
	if s != nil {
		s.lastMS.Store(value)
	}
}

func (s *BatchState) AddTask(key, pairKey string, task *JobEnv, tfMSecs, execMS int64) {
	if s == nil || key == "" || pairKey == "" || task == nil || task.Job == nil || task.Job.Strat == nil {
		return
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.tasks == nil {
		s.tasks = make(map[string]*BatchMap)
	}
	tasks := s.tasks[key]
	if tasks == nil {
		tasks = &BatchMap{Map: make(map[string]*JobEnv), TFMSecs: tfMSecs}
		s.tasks[key] = tasks
	}
	tasks.ExecMS = execMS
	if pending := tasks.Map[pairKey]; pending != nil {
		pending.Job = task.Job
		pending.Env = task.Env
		pending.Symbol = task.Symbol
		return
	}
	tasks.Map[pairKey] = task
}

// TakeReady removes ready batches while locked. Callers execute BatchReady callbacks after it returns.
func (s *BatchState) TakeReady(currMS int64, deterministic bool) ([]BatchReady, int) {
	if s == nil {
		return nil, 0
	}
	s.lock.Lock()
	defer s.lock.Unlock()

	var ready []BatchReady
	waitNum := 0
	if deterministic {
		hasReady := false
		for key, tasks := range s.tasks {
			isReady, shouldWait := batchReadyStatus(key, tasks, currMS)
			if shouldWait {
				waitNum++
			}
			if isReady && hasValidBatchTask(tasks) {
				hasReady = true
			}
		}
		if !hasReady {
			return nil, waitNum
		}
		for _, key := range sortedBatchKeys(s.tasks) {
			tasks := s.tasks[key]
			item, isReady, _ := s.takeBatchReady(key, tasks, currMS, deterministic)
			if !isReady {
				continue
			}
			delete(s.tasks, key)
			ready = append(ready, item)
		}
	} else {
		for key, tasks := range s.tasks {
			item, isReady, shouldWait := s.takeBatchReady(key, tasks, currMS, deterministic)
			if shouldWait {
				waitNum++
			}
			if !isReady {
				continue
			}
			delete(s.tasks, key)
			ready = append(ready, item)
		}
	}
	return ready, waitNum
}

func batchReadyStatus(key string, tasks *BatchMap, currMS int64) (bool, bool) {
	if tasks == nil {
		return false, false
	}
	if _, _, ok := batchKeyParts(key); !ok {
		return false, false
	}
	if currMS < tasks.ExecMS {
		return false, tasks.ExecMS-currMS < tasks.TFMSecs/2
	}
	return true, false
}

func batchKeyParts(key string) (int, int, bool) {
	first := strings.IndexByte(key, '_')
	if first < 0 {
		return 0, 0, false
	}
	rest := key[first+1:]
	second := strings.IndexByte(rest, '_')
	if second < 0 {
		return 0, 0, false
	}
	return first, first + 1 + second, true
}

func hasValidBatchTask(tasks *BatchMap) bool {
	if tasks == nil {
		return false
	}
	for _, task := range tasks.Map {
		if isValidBatchTask(task) {
			return true
		}
	}
	return false
}

func isValidBatchTask(task *JobEnv) bool {
	return task != nil && task.Job != nil && task.Job.Strat != nil
}

func (s *BatchState) takeBatchReady(key string, tasks *BatchMap, currMS int64, deterministic bool) (BatchReady, bool, bool) {
	var item BatchReady
	isReady, shouldWait := batchReadyStatus(key, tasks, currMS)
	if !isReady {
		return item, false, shouldWait
	}
	if deterministic && !hasValidBatchTask(tasks) {
		return item, false, false
	}
	first, second, _ := batchKeyParts(key)
	if deterministic {
		seed := s.resolveBarOrderSeed(tasks)
		for _, taskKey := range sortedBatchJobKeysWithSeed(tasks, seed) {
			addBatchTask(&item, tasks.Map[taskKey])
		}
	} else {
		for _, task := range tasks.Map {
			addBatchTask(&item, task)
		}
	}
	if item.Strategy == nil {
		return item, false, false
	}
	item.TimeFrame, item.Account = key[:first], key[first+1:second]
	if item.InfoJobs == nil {
		item.InfoJobs = make(map[string]*JobEnv)
	}
	return item, true, false
}

func addBatchTask(item *BatchReady, task *JobEnv) {
	if !isValidBatchTask(task) {
		return
	}
	item.Strategy = task.Job.Strat
	if task.Env == nil {
		item.MainJobs = append(item.MainJobs, task.Job)
		return
	}
	if item.InfoJobs == nil {
		item.InfoJobs = make(map[string]*JobEnv)
	}
	item.InfoJobs[task.Symbol] = task
}

func (s *BatchState) PendingCount() int {
	if s == nil {
		return 0
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	return len(s.tasks)
}

func (s *BatchState) restore(tasks map[string]*BatchMap, lastMS int64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if tasks == nil {
		tasks = make(map[string]*BatchMap)
	}
	s.tasks = tasks
	s.lastMS.Store(lastMS)
}

func (s *BatchState) Reset() {
	if s == nil {
		return
	}
	s.restore(make(map[string]*BatchMap), 0)
	s.seed.Store(0)
	s.hasSeed.Store(false)
}

func (s *BatchState) resolveBarOrderSeed(tasks *BatchMap) uint64 {
	if s != nil && s.hasSeed.Load() {
		return s.seed.Load()
	}
	if tasks == nil {
		return 0
	}
	var fallbackLegacy bool
	for _, task := range tasks.Map {
		if !isValidBatchTask(task) {
			continue
		}
		job := task.Job
		if cfg := job.Strat.runtimeConfigFor(job); cfg != nil {
			return cfg.BTBarOrderSeed
		}
		if !job.isExplicitRuntimeJob() {
			fallbackLegacy = true
		}
	}
	if fallbackLegacy {
		return config.Data.BTBarOrderSeed
	}
	return 0
}

func sortedBatchKeys(tasks map[string]*BatchMap) []string {
	keys := make([]string, 0, len(tasks))
	for key := range tasks {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func batchTaskBarMS(task *JobEnv) int64 {
	if task == nil {
		return 0
	}
	if task.Env != nil {
		if task.Env.TimeStop > 0 {
			return task.Env.TimeStop
		}
		if task.Env.TimeStart > 0 {
			return task.Env.TimeStart
		}
	}
	if task.Job != nil {
		if task.Job.Env != nil {
			if task.Job.Env.TimeStop > 0 {
				return task.Job.Env.TimeStop
			}
			if task.Job.Env.TimeStart > 0 {
				return task.Job.Env.TimeStart
			}
		}
		if task.Job.LastBarMS > 0 {
			return task.Job.LastBarMS
		}
	}
	return 0
}

func batchBarMS(tasks *BatchMap) int64 {
	if tasks == nil {
		return 0
	}
	var barMS int64
	for _, task := range tasks.Map {
		if !isValidBatchTask(task) {
			continue
		}
		if ms := batchTaskBarMS(task); ms > barMS {
			barMS = ms
		}
	}
	return barMS
}

func batchTaskSymbol(taskKey string, task *JobEnv) string {
	if task != nil {
		if task.Symbol != "" {
			return task.Symbol
		}
		if task.Job != nil && task.Job.Symbol != nil && task.Job.Symbol.Symbol != "" {
			return task.Job.Symbol.Symbol
		}
	}
	if strings.HasSuffix(taskKey, "_main") {
		return strings.TrimSuffix(taskKey, "_main")
	}
	if strings.HasSuffix(taskKey, "_info") {
		return strings.TrimSuffix(taskKey, "_info")
	}
	return taskKey
}

type batchKeyedPriority struct {
	key  string
	prio uint64
}

func sortedBatchJobKeysWithSeed(tasks *BatchMap, seed uint64) []string {
	if tasks == nil {
		return nil
	}
	barMS := batchBarMS(tasks)
	if barMS <= 0 {
		return sortedBatchJobKeys(tasks.Map)
	}
	ordered := make([]batchKeyedPriority, 0, len(tasks.Map))
	for key, task := range tasks.Map {
		ordered = append(ordered, batchKeyedPriority{
			key:  key,
			prio: BarPriority(barMS, batchTaskSymbol(key, task), seed),
		})
	}
	slices.SortFunc(ordered, func(a, b batchKeyedPriority) int {
		if c := cmp.Compare(a.prio, b.prio); c != 0 {
			return c
		}
		return cmp.Compare(a.key, b.key)
	})
	keys := make([]string, len(ordered))
	for i, item := range ordered {
		keys[i] = item.key
	}
	return keys
}

func sortedBatchJobKeys(tasks map[string]*JobEnv) []string {
	keys := make([]string, 0, len(tasks))
	for key := range tasks {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
