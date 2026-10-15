package strat

import (
	"sync"
	"testing"

	ta "github.com/banbox/banta"
)

func TestStateLookupJobUsesStateRegistry(t *testing.T) {
	state := NewState()
	job := &StratJob{Strat: &TradeStrat{Name: "strategy"}, Env: &ta.BarEnv{}}
	state.SetJobMap("account", "BTC/USDT_1m", map[string]*StratJob{"strategy": job})
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
	state.SetJobMap("account", "BTC/USDT_1m", map[string]*StratJob{"strategy": job})
	state.SetInfoJobMap("account", "custom", map[string]*StratJob{"strategy": job})

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
	state.SetJobMap("account", "BTC/USDT_1m", map[string]*StratJob{"strategy": {}})
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
		if state.accJobs["account"] == nil {
			state.accJobs["account"] = make(map[string]map[string]*StratJob)
		}
		if state.accJobs["account"]["BTC/USDT_1m"] == nil {
			state.accJobs["account"]["BTC/USDT_1m"] = make(map[string]*StratJob)
		}
		state.accJobs["account"]["BTC/USDT_1m"]["strategy"] = &StratJob{}
		unlockJobsWriteForState(state)
	}
	wg.Wait()
}

func TestVersionsSnapshotDoesNotRaceReload(t *testing.T) {
	state := NewState()
	const rounds = 200
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range rounds {
			lockJobsWriteForState(state)
			state.versions = map[string]int{"strategy": i}
			unlockJobsWriteForState(state)
		}
	}()
	for range rounds {
		versions := state.VersionsSnapshot()
		versions["caller"] = 1
		if _, ok := state.Version("caller"); ok {
			t.Fatal("VersionsSnapshot exposed the state-owned registry")
		}
	}
	wg.Wait()
}

func TestLegacyVersionsSnapshotDoesNotExposeRegistry(t *testing.T) {
	lockJobsWriteForState(nil)
	old := Versions
	Versions = map[string]int{"strategy": 1}
	unlockJobsWriteForState(nil)
	t.Cleanup(func() {
		lockJobsWriteForState(nil)
		Versions = old
		unlockJobsWriteForState(nil)
	})

	versions := LegacyVersionsSnapshot()
	versions["caller"] = 1
	lockJobsReadForState(nil)
	_, exposed := Versions["caller"]
	unlockJobsReadForState(nil)
	if exposed {
		t.Fatal("LegacyVersionsSnapshot exposed the package registry")
	}
}

func TestLegacyVersionAndEnvAccessorsUseCompatibilityRegistries(t *testing.T) {
	lockJobsWriteForState(nil)
	oldVersions := Versions
	Versions = map[string]int{"alpha": 7}
	unlockJobsWriteForState(nil)
	oldEnvs := Envs
	env := &ta.BarEnv{}
	Envs = map[string]*ta.BarEnv{"BTC_USDT_1m": env}
	t.Cleanup(func() {
		lockJobsWriteForState(nil)
		Versions = oldVersions
		unlockJobsWriteForState(nil)
		Envs = oldEnvs
	})

	if version, ok := GetVersion("alpha"); !ok || version != 7 {
		t.Fatalf("legacy version = %d/%v, want 7/true", version, ok)
	}
	if got, ok := GetEnv("BTC_USDT_1m"); !ok || got != env {
		t.Fatalf("legacy env = %p/%v, want %p/true", got, ok, env)
	}
}

func TestStateRegistrySettersOwnMembershipAndInvalidateViews(t *testing.T) {
	state := NewState()
	state.EnsureAccount("account")
	before := state.JobMapsView("account")
	infoBefore := state.InfoJobMapView("account")
	job := &StratJob{}
	members := map[string]*StratJob{"strategy": job}
	state.SetJobMap("account", "environment", members)
	state.SetInfoJobMap("account", "subscription", members)
	delete(members, "strategy")
	if state.JobMapView("account", "environment")["strategy"] != job || state.InfoJobMapView("account")["subscription"]["strategy"] != job {
		t.Fatal("registry retained caller-owned membership")
	}
	if len(before) != 0 || len(infoBefore) != 0 {
		t.Fatal("registry write mutated an already published view")
	}
}

func TestPublishedRegistryReadsDoNotAllocate(t *testing.T) {
	state := NewState()
	state.SetJobMap("account", "environment", map[string]*StratJob{"job": {}})
	state.SetInfoJobMap("account", "subscription", map[string]*StratJob{"job": {}})
	_ = state.JobMapView("account", "environment")
	_ = state.InfoJobMapView("account")
	allocations := testing.AllocsPerRun(1000, func() {
		if len(state.JobMapView("account", "environment")) != 1 || len(state.InfoJobMapView("account")) != 1 {
			panic("missing published registry")
		}
	})
	if allocations != 0 {
		t.Fatalf("published registry reads allocated %v times", allocations)
	}
}
