package core

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStateExitCallbacksAreScopedAndIdempotent(t *testing.T) {
	state, err := NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(state.Close)

	var calls atomic.Int32
	state.OnExit(func() { calls.Add(1) })
	state.Stop()
	state.Stop()
	state.OnExit(func() { calls.Add(1) })

	if got := calls.Load(); got != 2 {
		t.Fatalf("exit callbacks = %d, want 2", got)
	}
	if state.BotRunning {
		t.Fatal("state remains running after Stop")
	}
}

func TestRuntimeFlagsUseTypedAccessorsConcurrently(t *testing.T) {
	state, err := NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(state.Close)

	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for n := 0; n < 1000; n++ {
				state.SetBotRunning((n+worker)%2 == 0)
				_ = state.IsBotRunning()
				state.SetCheckWallets((n+worker)%2 == 0)
				_ = state.ShouldCheckWallets()
			}
		}(worker)
	}
	wg.Wait()
}

func TestSymbolParserConcurrentCacheMisses(t *testing.T) {
	parser := NewSymbolParser("binance")
	const workers = 8
	const pairsPerWorker = 16

	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < pairsPerWorker; i++ {
				pair := fmt.Sprintf("COIN%d%d/USDT:USDT", worker, i)
				base, quote, settle, ident := parser.Split(pair)
				if base == "" || quote != "USDT" || settle != "USDT" || ident != "" {
					t.Errorf("unexpected split for %s: %q %q %q %q", pair, base, quote, settle, ident)
				}
			}
		}(worker)
	}
	wg.Wait()
}

func TestSymbolParserCacheIsolatedByExchange(t *testing.T) {
	parser := NewSymbolParser("binance")
	if base, quote, settle, ident := parser.Split("BTC/USDT:USDT"); base != "BTC" || quote != "USDT" || settle != "USDT" || ident != "" {
		t.Fatalf("binance split = %q %q %q %q", base, quote, settle, ident)
	}

	parser.SetStrategy("china", func(pair string) [4]string {
		return [4]string{"IF", "CNY", "CNY", "2409"}
	})
	if base, quote, settle, ident := parser.Split("IF2409"); base != "IF" || quote != "CNY" || settle != "CNY" || ident != "2409" {
		t.Fatalf("china split = %q %q %q %q", base, quote, settle, ident)
	}

	parser.SetExchangeName("binance")
	if base, quote, settle, ident := parser.Split("BTC/USDT:USDT"); base != "BTC" || quote != "USDT" || settle != "USDT" || ident != "" {
		t.Fatalf("restored binance split = %q %q %q %q", base, quote, settle, ident)
	}
}

func TestStateCallbacksCanReenterStopAndClose(t *testing.T) {
	state, err := NewState(nil)
	if err != nil {
		t.Fatal(err)
	}

	var stopAllCalls atomic.Int32
	var exitCalls atomic.Int32
	state.StopAll = func() {
		stopAllCalls.Add(1)
		state.Stop()
		state.Close()
	}
	state.OnExit(func() {
		exitCalls.Add(1)
		state.Stop()
		state.Close()
	})

	done := make(chan struct{})
	go func() {
		state.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("State.Close deadlocked while callbacks reentered Stop/Close")
	}

	if got := stopAllCalls.Load(); got != 1 {
		t.Fatalf("StopAll calls = %d, want 1", got)
	}
	if got := exitCalls.Load(); got != 1 {
		t.Fatalf("OnExit calls = %d, want 1", got)
	}
}

func TestStateAdmissionIsIsolatedAcrossInstances(t *testing.T) {
	first, err := NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewState(nil)
	if err != nil {
		first.Close()
		t.Fatal(err)
	}
	t.Cleanup(first.Close)
	t.Cleanup(second.Close)

	first.SetPairs([]string{"FIRST/USDT"}, []string{"FIRST-POLICY/USDT"})
	second.SetPairs([]string{"SECOND/USDT"}, nil)
	first.SetAdmissionPair("FIRST-ADDED/USDT", true)
	first.SetAdmissionPair("FIRST/USDT", false)

	if first.PairEnabled("FIRST/USDT") || !first.PairEnabled("FIRST-POLICY/USDT") ||
		!first.PairEnabled("FIRST-ADDED/USDT") {
		t.Fatalf("first admission snapshot = %#v", first.PairsMap)
	}
	if !second.PairEnabled("SECOND/USDT") || second.PairEnabled("FIRST-ADDED/USDT") {
		t.Fatalf("second admission snapshot = %#v", second.PairsMap)
	}
	if first.PairsMap["FIRST/USDT"] || !first.PairsMap["FIRST-ADDED/USDT"] {
		t.Fatalf("legacy compatibility map was not published: %#v", first.PairsMap)
	}
}

func TestStateAdmissionSnapshotIncludesDiscoveredPairs(t *testing.T) {
	state, err := NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(state.Close)
	state.SetPairs([]string{"BASE/USDT"}, nil)

	active := map[string]bool{
		"BASE/USDT":       true,
		"DISCOVERED/USDT": true,
	}
	state.SetAdmissionSnapshot(active)
	active["DISCOVERED/USDT"] = false

	if !state.PairEnabled("DISCOVERED/USDT") {
		t.Fatal("snapshot lost a newly discovered enabled pair")
	}
	if !slices.Contains(state.AdmissionPairs(), "DISCOVERED/USDT") {
		t.Fatalf("new pair was not retained in compatibility list: %v", state.AdmissionPairs())
	}
	if !state.PairsMap["DISCOVERED/USDT"] {
		t.Fatalf("compatibility map does not contain discovered pair: %#v", state.PairsMap)
	}
}

func TestStateAdmissionConcurrentPairReservations(t *testing.T) {
	state, err := NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(state.Close)

	const workers = 8
	const pairsPerWorker = 32
	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < pairsPerWorker; i++ {
				state.SetAdmissionPair(fmt.Sprintf("PAIR-%d-%d/USDT", worker, i), true)
			}
		}(worker)
	}
	wg.Wait()

	pairs := state.AdmissionPairs()
	seen := make(map[string]bool, len(pairs))
	for _, pair := range pairs {
		if seen[pair] {
			t.Fatalf("duplicate pair in admission list: %s/%v", pair, pairs)
		}
		seen[pair] = true
		if !state.PairEnabled(pair) {
			t.Fatalf("reserved pair is not enabled: %s", pair)
		}
	}
	if len(pairs) != workers*pairsPerWorker {
		t.Fatalf("reserved pair count = %d, want %d", len(pairs), workers*pairsPerWorker)
	}
}

func TestStateAdmissionConcurrentSnapshotReads(t *testing.T) {
	state, err := NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(state.Close)
	state.SetPairs([]string{"BASE/USDT"}, nil)

	const iterations = 1000
	var wg sync.WaitGroup
	wg.Add(5)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			state.SetPairs([]string{"BASE/USDT", fmt.Sprintf("SET-%d/USDT", i%8)}, nil)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			state.SetAdmissionPair(fmt.Sprintf("PAIR-%d/USDT", i%8), i%2 == 0)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			state.SetAdmissionSnapshot(map[string]bool{
				"BASE/USDT":                      true,
				fmt.Sprintf("PAIR-%d/USDT", i%8): i%2 == 0,
			})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iterations*2; i++ {
			_ = state.PairEnabled("BASE/USDT")
			_ = state.PairEnabled(fmt.Sprintf("PAIR-%d/USDT", i%8))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			pairs := state.AdmissionPairs()
			if len(pairs) > 0 {
				_ = pairs[0]
			}
		}
	}()
	wg.Wait()

	state.SetPairs([]string{"FINAL/USDT"}, nil)
	if !state.PairEnabled("FINAL/USDT") || state.PairEnabled("BASE/USDT") {
		t.Fatalf("final admission snapshot = %#v", state.PairsMap)
	}
}

func TestStateAdmissionOverridesAreSynchronized(t *testing.T) {
	state, err := NewState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(state.Close)
	state.SetPairs([]string{"BTC/USDT"}, nil)

	const iterations = 1000
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			state.SetPairBanUntil("BTC/USDT", int64(i+1))
			_ = state.IsPairBanned("BTC/USDT", int64(i))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			state.SetNoEnterUntil("account", int64(i+1))
			_, _ = state.NoEnterUntilFor("account")
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			state.SetPairs([]string{"BTC/USDT", fmt.Sprintf("PAIR-%d/USDT", i%8)}, nil)
			for _, pair := range state.BannedPairs() {
				if !state.PairEnabled(pair) {
					state.SetPairBanUntil(pair, 0)
				}
			}
		}
	}()
	wg.Wait()
}

func BenchmarkSymbolParserColdMiss(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		NewSymbolParser("binance").Split("COIN/USDT:USDT")
	}
}
