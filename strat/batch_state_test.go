package strat

import "testing"

func TestBatchStateRejectsInvalidTasksAndHandlesNilReceiver(t *testing.T) {
	var nilState *BatchState
	nilState.AddTask("1m_default_demo", "pair_main", nil, 60_000, 1)
	if ready, wait := nilState.TakeReady(1, false); ready != nil || wait != 0 || nilState.PendingCount() != 0 {
		t.Fatalf("nil state returned ready=%v wait=%d pending=%d", ready, wait, nilState.PendingCount())
	}

	state := NewBatchState()
	state.AddTask("1m_default_demo", "pair_main", nil, 60_000, 1)
	state.AddTask("1m_default_demo", "pair_main", &JobEnv{}, 60_000, 1)
	if state.PendingCount() != 0 {
		t.Fatalf("invalid tasks were added: pending=%d", state.PendingCount())
	}
}

func TestBatchStateTakeReadyToleratesMalformedLegacySnapshot(t *testing.T) {
	state := NewBatchState()
	state.restore(map[string]*BatchMap{
		"malformed":               {Map: map[string]*JobEnv{}},
		"1m_default_nil-batch":    nil,
		"1m_default_invalid-task": {Map: map[string]*JobEnv{"pair": nil}},
	}, 0)
	ready, wait := state.TakeReady(1, true)
	if len(ready) != 0 || wait != 0 || state.PendingCount() != 3 {
		t.Fatalf("malformed snapshot ready=%v wait=%d pending=%d", ready, wait, state.PendingCount())
	}
}

func TestBatchStateTakeReadyNoReadyPathAllocations(t *testing.T) {
	maxInt64 := int64(^uint64(0) >> 1)
	for _, tc := range []struct {
		name          string
		deterministic bool
		pending       bool
	}{
		{name: "empty nondeterministic"},
		{name: "empty deterministic", deterministic: true},
		{name: "pending nondeterministic", pending: true},
		{name: "pending deterministic", deterministic: true, pending: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := NewBatchState()
			if tc.pending {
				state.AddTask("1m_default_alloc", "pair_main", &JobEnv{
					Job: &StratJob{Strat: &TradeStrat{Name: "alloc"}},
				}, 60_000, maxInt64)
			}
			allocs := testing.AllocsPerRun(100, func() {
				ready, wait := state.TakeReady(0, tc.deterministic)
				if ready != nil || wait != 0 {
					t.Fatalf("ready=%v wait=%d", ready, wait)
				}
			})
			if allocs != 0 {
				t.Fatalf("TakeReady allocations = %v, want 0", allocs)
			}
		})
	}
}

func BenchmarkBatchStateTakeReadyNonDeterministic(b *testing.B) {
	state := NewBatchState()
	state.AddTask("1m_default_bench", "pair_main", &JobEnv{Job: &StratJob{Strat: &TradeStrat{Name: "bench"}}}, 60_000, int64(^uint64(0)>>1))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state.TakeReady(0, false)
	}
}

func BenchmarkBatchStateTakeReadyDeterministic(b *testing.B) {
	state := NewBatchState()
	state.AddTask("1m_default_bench", "pair_main", &JobEnv{Job: &StratJob{Strat: &TradeStrat{Name: "bench"}}}, 60_000, int64(^uint64(0)>>1))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state.TakeReady(0, true)
	}
}
