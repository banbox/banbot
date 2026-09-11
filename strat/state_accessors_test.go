package strat

import (
	"sync"
	"testing"

	ta "github.com/banbox/banta"
)

func TestStateLookupJobUsesStateRegistry(t *testing.T) {
	state := NewState()
	job := &StratJob{Strat: &TradeStrat{Name: "strategy"}, Env: &ta.BarEnv{}}
	state.Jobs("account")["BTC/USDT_1m"] = map[string]*StratJob{"strategy": job}
	if got := state.LookupJob("account", "BTC/USDT_1m", "strategy"); got != job {
		t.Fatalf("LookupJob() = %p, want %p", got, job)
	}
	if got := state.LookupJob("account", "missing", "strategy"); got != nil {
		t.Fatalf("LookupJob(missing) = %p, want nil", got)
	}
}

func TestStateJobSnapshotsDoNotExposeMutableRegistries(t *testing.T) {
	state := NewState()
	job := &StratJob{Strat: &TradeStrat{Name: "strategy"}, Env: &ta.BarEnv{}}
	state.Jobs("account")["BTC/USDT_1m"] = map[string]*StratJob{"strategy": job}
	state.InfoJobs("account")["custom"] = map[string]*StratJob{"strategy": job}

	jobs := state.JobMap("account", "BTC/USDT_1m")
	info := state.InfoJobMap("account")
	delete(jobs, "strategy")
	delete(info["custom"], "strategy")
	if state.LookupJob("account", "BTC/USDT_1m", "strategy") != job {
		t.Fatal("JobMap returned the state-owned registry")
	}
	if got := state.InfoJobMap("account")["custom"]["strategy"]; got != job {
		t.Fatal("InfoJobMap returned a state-owned registry")
	}
}

func TestStateJobSnapshotCanRaceWithRotation(t *testing.T) {
	state := NewState()
	state.Jobs("account")["BTC/USDT_1m"] = map[string]*StratJob{"strategy": {}}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				_ = state.JobMap("account", "BTC/USDT_1m")
			}
		}()
	}
	for range 100 {
		lockJobsWriteForState(state)
		if state.AccJobs["account"] == nil {
			state.AccJobs["account"] = make(map[string]map[string]*StratJob)
		}
		if state.AccJobs["account"]["BTC/USDT_1m"] == nil {
			state.AccJobs["account"]["BTC/USDT_1m"] = make(map[string]*StratJob)
		}
		state.AccJobs["account"]["BTC/USDT_1m"]["strategy"] = &StratJob{}
		unlockJobsWriteForState(state)
	}
	wg.Wait()
}
