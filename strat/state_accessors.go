package strat

import (
	"maps"
	"slices"
)

import ta "github.com/banbox/banta"

// jobRegistrySnapshot contains maps that are never mutated after publication.
// StratJob and TradeStrat pointers are intentionally borrowed: their own
// execution state follows the existing strategy lifecycle contract.
type jobRegistrySnapshot struct {
	jobs       map[string]map[string]map[string]*StratJob
	pairStrats map[string]map[string]*TradeStrat
}

type infoJobRegistrySnapshot map[string]map[string]map[string]*StratJob

func cloneJobRegistrySnapshot(s *State) *jobRegistrySnapshot {
	result := &jobRegistrySnapshot{
		jobs:       make(map[string]map[string]map[string]*StratJob, len(s.AccJobs)),
		pairStrats: make(map[string]map[string]*TradeStrat, len(s.PairStrats)),
	}
	for account, accountJobs := range s.AccJobs {
		byEnv := make(map[string]map[string]*StratJob, len(accountJobs))
		for envKey, jobs := range accountJobs {
			byEnv[envKey] = maps.Clone(jobs)
		}
		result.jobs[account] = byEnv
	}
	for pair, strategies := range s.PairStrats {
		result.pairStrats[pair] = maps.Clone(strategies)
	}
	return result
}

func cloneInfoJobRegistrySnapshot(s *State) infoJobRegistrySnapshot {
	result := make(infoJobRegistrySnapshot, len(s.AccInfoJobs))
	for account, accountJobs := range s.AccInfoJobs {
		bySubKey := make(map[string]map[string]*StratJob, len(accountJobs))
		for subKey, jobs := range accountJobs {
			bySubKey[subKey] = maps.Clone(jobs)
		}
		result[account] = bySubKey
	}
	return result
}

// registrySnapshot returns a stable primary-job view. A write marks the view
// dirty, and the first reader after that write performs the one required copy
// while holding the write lock. Normal bar reads only take a read lock and
// return the published maps without allocation.
func (s *State) registrySnapshot() *jobRegistrySnapshot {
	if s == nil {
		return nil
	}
	if s == legacyState {
		// The compatibility facade still has callers that mutate package maps
		// under the legacy lock. Keep that path isolated from the explicit
		// snapshot cache; it is not a Runtime hot path.
		lockJobsReadForState(s)
		snapshot := cloneJobRegistrySnapshot(s)
		unlockJobsReadForState(s)
		return snapshot
	}
	s.jobsMu.RLock()
	snapshot := s.jobsSnapshot
	dirty := s.jobsSnapshotDirty.Load()
	s.jobsMu.RUnlock()
	if snapshot != nil && !dirty {
		return snapshot
	}
	s.jobsMu.Lock()
	if s.jobsSnapshot == nil || s.jobsSnapshotDirty.Load() {
		s.jobsSnapshot = cloneJobRegistrySnapshot(s)
		s.jobsSnapshotDirty.Store(false)
	}
	snapshot = s.jobsSnapshot
	s.jobsMu.Unlock()
	return snapshot
}

// infoRegistrySnapshot is the side-input counterpart to registrySnapshot.
// Side-input registration has its own lock because it is rebuilt independently
// from primary jobs during data subscription refresh.
func (s *State) infoRegistrySnapshot() infoJobRegistrySnapshot {
	if s == nil {
		return nil
	}
	if s == legacyState {
		lockInfoJobsReadForState(s)
		snapshot := cloneInfoJobRegistrySnapshot(s)
		unlockInfoJobsReadForState(s)
		return snapshot
	}
	s.infoJobsMu.RLock()
	snapshot := s.infoSnapshot
	dirty := s.infoSnapshotDirty.Load()
	s.infoJobsMu.RUnlock()
	if snapshot != nil && !dirty {
		return *snapshot
	}
	s.infoJobsMu.Lock()
	if s.infoSnapshot == nil || s.infoSnapshotDirty.Load() {
		snapshot := cloneInfoJobRegistrySnapshot(s)
		s.infoSnapshot = &snapshot
		s.infoSnapshotDirty.Store(false)
	}
	snapshot = s.infoSnapshot
	s.infoJobsMu.Unlock()
	return *snapshot
}

// CollectJobs returns a low-frequency snapshot of all jobs owned by this
// strategy state. Callers use the returned slice for lifecycle dispatch; the
// job objects remain state-owned and are not copied.
func (s *State) CollectJobs() []*StratJob {
	if s == nil {
		return nil
	}
	snapshot := s.registrySnapshot()
	if snapshot == nil {
		return nil
	}
	accounts := sortedMapKeys(snapshot.jobs)
	jobs := make([]*StratJob, 0)
	seen := make(map[*StratJob]struct{})
	for _, account := range accounts {
		accountJobs := snapshot.jobs[account]
		envKeys := sortedMapKeys(accountJobs)
		for _, envKey := range envKeys {
			envJobs := accountJobs[envKey]
			jobKeys := sortedMapKeys(envJobs)
			for _, jobKey := range jobKeys {
				job := envJobs[jobKey]
				if job == nil {
					continue
				}
				if _, ok := seen[job]; ok {
					continue
				}
				seen[job] = struct{}{}
				jobs = append(jobs, job)
			}
		}
	}
	return jobs
}

func mapKeys[V any](items map[string]V) []string {
	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	return keys
}

func sortedMapKeys[V any](items map[string]V) []string {
	keys := mapKeys(items)
	slices.Sort(keys)
	return keys
}

// Jobs returns the strategy jobs owned by this state. The returned map is the
// state-owned map, so callers on the hot path can index it directly without a
// dynamic lookup or a copy.
func (s *State) Jobs(account string) map[string]map[string]*StratJob {
	if s == nil {
		return nil
	}
	jobs := s.AccJobs[account]
	if jobs == nil {
		jobs = make(map[string]map[string]*StratJob)
		s.AccJobs[account] = jobs
	}
	if s != legacyState {
		// The returned map is retained for legacy construction helpers that
		// populate registries directly. Marking it dirty keeps the next
		// published snapshot correct without forcing those callers through a
		// generic mutation API.
		s.jobsSnapshotDirty.Store(true)
	}
	return jobs
}

// LookupJob returns a job from the state registry under the same read lock
// used by pair rotation. The returned job remains owned by the state; callers
// must not retain it past the operation that already coordinates its lifetime.
func (s *State) LookupJob(account, pairTF, strategy string) *StratJob {
	if s == nil {
		return nil
	}
	lockJobsReadForState(s)
	defer unlockJobsReadForState(s)
	accountJobs := s.AccJobs[account]
	if accountJobs == nil {
		return nil
	}
	strategyJobs := accountJobs[pairTF]
	if strategyJobs == nil {
		return nil
	}
	return strategyJobs[strategy]
}

// JobMap returns a copy of the jobs for one environment. Use JobMapView on
// the event path when a read-only borrowed view is sufficient.
func (s *State) JobMap(account, pairTF string) map[string]*StratJob {
	jobs := s.JobMapView(account, pairTF)
	return maps.Clone(jobs)
}

// JobMapView returns the published immutable jobs for one environment. The
// returned map must be treated as read-only and only retained for the current
// operation; a later pair rotation publishes a different map.
func (s *State) JobMapView(account, pairTF string) map[string]*StratJob {
	if s == nil {
		return nil
	}
	snapshot := s.registrySnapshot()
	accountJobs := snapshot.jobs[account]
	if accountJobs == nil {
		return nil
	}
	jobs := accountJobs[pairTF]
	return jobs
}

// JobMaps returns copied primary job maps for one account. Use JobMapsView for
// low-allocation read-only traversal.
func (s *State) JobMaps(account string) map[string]map[string]*StratJob {
	view := s.JobMapsView(account)
	if view == nil {
		return nil
	}
	result := make(map[string]map[string]*StratJob, len(view))
	for pairTF, jobs := range view {
		result[pairTF] = maps.Clone(jobs)
	}
	return result
}

// JobMapsView returns the published immutable primary job maps for one
// account. Callers must treat both map levels as read-only.
func (s *State) JobMapsView(account string) map[string]map[string]*StratJob {
	if s == nil {
		return nil
	}
	snapshot := s.registrySnapshot()
	accountJobs := snapshot.jobs[account]
	if accountJobs == nil {
		return nil
	}
	return accountJobs
}

// InfoJobMap returns copied side-input job maps for one account. Use
// InfoJobMapView on the event path when a read-only borrowed view is enough.
func (s *State) InfoJobMap(account string) map[string]map[string]*StratJob {
	view := s.InfoJobMapView(account)
	if view == nil {
		return nil
	}
	result := make(map[string]map[string]*StratJob, len(view))
	for key, jobs := range view {
		result[key] = maps.Clone(jobs)
	}
	return result
}

// InfoJobMapView returns the published immutable side-input job maps for one
// account. Side-input registration has its own snapshot publication lock;
// callers must treat both map levels as read-only.
func (s *State) InfoJobMapView(account string) map[string]map[string]*StratJob {
	if s == nil {
		return nil
	}
	return s.infoRegistrySnapshot()[account]
}

// Accounts returns a stable snapshot of account names in the primary job
// registry. It is intended for callback registration and other low-frequency
// lifecycle work.
func (s *State) Accounts() []string {
	if s == nil {
		return nil
	}
	return sortedMapKeys(s.registrySnapshot().jobs)
}

// PairStrategies returns copied strategy maps indexed by pair. Use
// PairStrategiesView for low-allocation read-only traversal.
func (s *State) PairStrategies() map[string]map[string]*TradeStrat {
	view := s.PairStrategiesView()
	if view == nil {
		return nil
	}
	result := make(map[string]map[string]*TradeStrat, len(view))
	for pair, strategies := range view {
		result[pair] = maps.Clone(strategies)
	}
	return result
}

// PairStrategiesView returns the published immutable strategy maps indexed by
// pair. Callers must treat both map levels as read-only.
func (s *State) PairStrategiesView() map[string]map[string]*TradeStrat {
	if s == nil {
		return nil
	}
	return s.registrySnapshot().pairStrats
}

// InfoJobs returns the side-input jobs owned by this state.
func (s *State) InfoJobs(account string) map[string]map[string]*StratJob {
	if s == nil {
		return nil
	}
	jobs := s.AccInfoJobs[account]
	if jobs == nil {
		jobs = make(map[string]map[string]*StratJob)
		s.AccInfoJobs[account] = jobs
	}
	if s != legacyState {
		s.infoSnapshotDirty.Store(true)
	}
	return jobs
}

// Env returns one primary strategy environment without consulting the legacy
// package registry.
func (s *State) Env(key string) (*ta.BarEnv, bool) {
	if s == nil {
		return nil, false
	}
	s.envMu.RLock()
	env, ok := s.Envs[key]
	s.envMu.RUnlock()
	return env, ok
}

// SetEnv publishes one primary strategy environment. The map field remains
// exported for legacy construction, while runtime code uses this setter at
// registry boundaries.
func (s *State) SetEnv(key string, env *ta.BarEnv) {
	if s == nil {
		return
	}
	s.envMu.Lock()
	if s.Envs == nil {
		s.Envs = make(map[string]*ta.BarEnv)
	}
	s.Envs[key] = env
	s.envMu.Unlock()
}

// DeleteEnv removes one primary strategy environment.
func (s *State) DeleteEnv(key string) {
	if s == nil {
		return
	}
	s.envMu.Lock()
	delete(s.Envs, key)
	s.envMu.Unlock()
}

// EnvKeys returns a stable list for low-frequency registry cleanup.
func (s *State) EnvKeys() []string {
	if s == nil {
		return nil
	}
	s.envMu.RLock()
	keys := make([]string, 0, len(s.Envs))
	for key := range s.Envs {
		keys = append(keys, key)
	}
	s.envMu.RUnlock()
	return keys
}

// Version returns a strategy version from this runtime's registry.
func (s *State) Version(name string) (int, bool) {
	if s == nil {
		return 0, false
	}
	// Strategy reload publishes Versions under jobsMu. Use the same read lock
	// here so an order callback cannot race a reload while resolving the
	// version to persist on a newly created order.
	lockJobsReadForState(s)
	defer unlockJobsReadForState(s)
	version, ok := s.Versions[name]
	return version, ok
}

// JobKeys returns a snapshot of the job keys for one account. It is intended
// for low-frequency refresh logic; bar processing should index Jobs directly.
func (s *State) JobKeys(account string) map[string]map[string]bool {
	if s == nil {
		return nil
	}
	jobs := s.JobMaps(account)
	result := make(map[string]map[string]bool, len(jobs))
	for pairTF, strategyJobs := range jobs {
		ids := make(map[string]bool, len(strategyJobs))
		for strategyID := range strategyJobs {
			ids[strategyID] = true
		}
		result[pairTF] = ids
	}
	return result
}

// JobKeysAll returns one low-frequency snapshot across every account. It is
// used by pair-rotation bookkeeping; bar processing should keep using Jobs.
func (s *State) JobKeysAll() map[string]map[string]bool {
	if s == nil {
		return nil
	}
	snapshot := s.registrySnapshot()
	result := make(map[string]map[string]bool)
	for _, jobs := range snapshot.jobs {
		for pairTF, strategyJobs := range jobs {
			ids := result[pairTF]
			if ids == nil {
				ids = make(map[string]bool, len(strategyJobs))
				result[pairTF] = ids
			}
			for strategyID := range strategyJobs {
				ids[strategyID] = true
			}
		}
	}
	return result
}
