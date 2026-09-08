package strat

import (
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// BatchState owns the mutable batch queue and its backtest timestamp for one trader.
type BatchState struct {
	lock   sync.Mutex
	tasks  map[string]*BatchMap
	lastMS atomic.Int64
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
			item, isReady, _ := takeBatchReady(key, s.tasks[key], currMS, deterministic)
			if !isReady {
				continue
			}
			delete(s.tasks, key)
			ready = append(ready, item)
		}
	} else {
		for key, tasks := range s.tasks {
			item, isReady, shouldWait := takeBatchReady(key, tasks, currMS, deterministic)
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

func takeBatchReady(key string, tasks *BatchMap, currMS int64, deterministic bool) (BatchReady, bool, bool) {
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
		for _, taskKey := range sortedBatchJobKeys(tasks.Map) {
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
	s.restore(make(map[string]*BatchMap), 0)
}

// BackupLegacyBatchState keeps the old shallow backup semantics for BackupVars only.
func BackupLegacyBatchState() (map[string]*BatchMap, int64) {
	legacyBatchState.lock.Lock()
	defer legacyBatchState.lock.Unlock()
	return legacyBatchState.tasks, legacyBatchState.lastMS.Load()
}

// RestoreLegacyBatchState restores a snapshot produced by BackupLegacyBatchState.
func RestoreLegacyBatchState(tasks map[string]*BatchMap, lastMS int64) {
	legacyBatchState.restore(tasks, lastMS)
}

func sortedBatchKeys(tasks map[string]*BatchMap) []string {
	keys := make([]string, 0, len(tasks))
	for key := range tasks {
		keys = append(keys, key)
	}
	sort.Strings(keys)
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
