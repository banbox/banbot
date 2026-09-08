package strat

import ta "github.com/banbox/banta"

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
	return jobs
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
	return jobs
}

// Env returns one primary strategy environment without consulting the legacy
// package registry.
func (s *State) Env(key string) (*ta.BarEnv, bool) {
	if s == nil {
		return nil, false
	}
	env, ok := s.Envs[key]
	return env, ok
}

// Version returns a strategy version from this runtime's registry.
func (s *State) Version(name string) (int, bool) {
	if s == nil {
		return 0, false
	}
	version, ok := s.Versions[name]
	return version, ok
}

// JobKeys returns a snapshot of the job keys for one account. It is intended
// for low-frequency refresh logic; bar processing should index Jobs directly.
func (s *State) JobKeys(account string) map[string]map[string]bool {
	if s == nil {
		return nil
	}
	jobs := s.AccJobs[account]
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
	result := make(map[string]map[string]bool)
	for _, jobs := range s.AccJobs {
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
