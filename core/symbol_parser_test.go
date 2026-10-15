package core

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestSplitSymbolConcurrentCalls(t *testing.T) {
	const workers = 16
	const pairsPerWorker = 32
	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func() {
			defer wg.Done()
			for i := 0; i < pairsPerWorker; i++ {
				base, quote, settle, ident := SplitSymbol("IF2409")
				if base != "IF2409" || quote != "" || settle != "" || ident != "" {
					t.Errorf("binance split = %q %q %q %q", base, quote, settle, ident)
				}
			}
		}()
	}
	wg.Wait()
}

func TestSplitSymbolPreservesDefaultBehavior(t *testing.T) {
	oldExgName := ExgName
	ExgName = "china"
	t.Cleanup(func() { ExgName = oldExgName })

	if base, quote, settle, ident := SplitSymbol("IF2409"); base != "IF2409" || quote != "" || settle != "" || ident != "" {
		t.Fatalf("undelimited split = %q %q %q %q", base, quote, settle, ident)
	}
	if base, quote, settle, ident := SplitSymbol("BTC/USDT:USDT"); base != "BTC" || quote != "USDT" || settle != "USDT" || ident != "" {
		t.Fatalf("delimited split = %q %q %q %q", base, quote, settle, ident)
	}
}

func TestSplitSymbolIgnoresGlobalExchangeState(t *testing.T) {
	oldExgName := ExgName
	t.Cleanup(func() { ExgName = oldExgName })

	for _, exgName := range []string{"china", "binance", ""} {
		ExgName = exgName
		base, quote, settle, ident := SplitSymbol("IF2409")
		got := [4]string{base, quote, settle, ident}
		if got != [4]string{"IF2409", "", "", ""} {
			t.Fatalf("%q split = %q, want IF2409 with empty suffixes", exgName, got)
		}
	}
}

func TestSplitSymbolIsExchangeAgnosticBeforeCacheWarmup(t *testing.T) {
	oldExgName := ExgName
	t.Cleanup(func() { ExgName = oldExgName })

	ExgName = "china"
	base, quote, settle, ident := SplitSymbol("ZZ2409")
	if got := [4]string{base, quote, settle, ident}; got != [4]string{"ZZ2409", "", "", ""} {
		t.Fatalf("china split = %q, want generic delimiter parsing", got)
	}

	ExgName = "binance"
	base, quote, settle, ident = SplitSymbol("ZZ2409")
	if got := [4]string{base, quote, settle, ident}; got != [4]string{"ZZ2409", "", "", ""} {
		t.Fatalf("binance split after china = %q, want generic delimiter parsing", got)
	}
}

func TestSymbolParserExchangeNameDoesNotSelectDefaultSemantics(t *testing.T) {
	oldExgName := ExgName
	ExgName = "china"
	t.Cleanup(func() { ExgName = oldExgName })

	for _, exgName := range []string{"", "china", "binance"} {
		parser := NewSymbolParser(exgName)
		if base, quote, settle, ident := parser.Split("IF2409"); base != "IF2409" || quote != "" || settle != "" || ident != "" {
			t.Fatalf("%q default split = %q/%q/%q/%q", exgName, base, quote, settle, ident)
		}
	}
}

func TestSymbolParserUsesExplicitExchangeStrategy(t *testing.T) {
	china := NewSymbolParserWithStrategy("china", func(pair string) [4]string {
		if pair == "IF2409" {
			return [4]string{"IF", "CNY", "CNY", "2409"}
		}
		return splitSymbolParts(pair)
	})
	if base, quote, settle, ident := china.Split("IF2409"); base != "IF" || quote != "CNY" || settle != "CNY" || ident != "2409" {
		t.Fatalf("china split = %q %q %q %q", base, quote, settle, ident)
	}

	china.SetExchangeName("binance")
	if base, quote, settle, ident := china.Split("IF2409"); base != "IF2409" || quote != "" || settle != "" || ident != "" {
		t.Fatalf("default split = %q %q %q %q", base, quote, settle, ident)
	}
}

func TestSymbolParserCachesStrategyResult(t *testing.T) {
	var calls atomic.Int32
	parser := NewSymbolParserWithStrategy("china", func(string) [4]string {
		calls.Add(1)
		return [4]string{"IF", "CNY", "CNY", "2409"}
	})

	parser.Split("IF2409")
	parser.Split("IF2409")
	if got := calls.Load(); got != 1 {
		t.Fatalf("strategy calls = %d, want 1", got)
	}
}

func TestSymbolParserErrorStrategyDoesNotCacheRejectedResult(t *testing.T) {
	var calls atomic.Int32
	parser := NewSymbolParserWithErrorStrategy("adapter", func(string) ([4]string, error) {
		if calls.Add(1) == 1 {
			return [4]string{}, errors.New("metadata is not ready")
		}
		return [4]string{"BASE", "QUOTE", "SETTLE", "ID"}, nil
	})

	if base, quote, settle, ident := parser.Split("retry-symbol"); base != "" || quote != "" || settle != "" || ident != "" {
		t.Fatalf("rejected parser result = %q/%q/%q/%q, want empty tuple", base, quote, settle, ident)
	}
	if base, quote, settle, ident := parser.Split("retry-symbol"); base != "BASE" || quote != "QUOTE" || settle != "SETTLE" || ident != "ID" {
		t.Fatalf("retried parser result = %q/%q/%q/%q", base, quote, settle, ident)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("error strategy calls = %d, want 2", got)
	}
}

func TestSymbolParserConcurrentMissCallsStrategyOnce(t *testing.T) {
	const workers = 32
	var calls atomic.Int32
	parser := NewSymbolParserWithStrategy("adapter-a", func(string) [4]string {
		calls.Add(1)
		return [4]string{"BASE", "QUOTE", "SETTLE", "ID"}
	})

	start := make(chan struct{})
	results := make(chan [4]string, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			<-start
			base, quote, settle, ident := parser.Split("shared-symbol")
			results <- [4]string{base, quote, settle, ident}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	for got := range results {
		if want := [4]string{"BASE", "QUOTE", "SETTLE", "ID"}; got != want {
			t.Fatalf("concurrent split = %q, want %q", got, want)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("strategy calls = %d, want 1", got)
	}
}

func TestSymbolParserStrategiesDoNotSharePairCache(t *testing.T) {
	first := NewSymbolParserWithStrategy("exchange-a", func(string) [4]string {
		return [4]string{"FIRST", "USD", "USD", ""}
	})
	second := NewSymbolParserWithStrategy("exchange-b", func(string) [4]string {
		return [4]string{"SECOND", "CNY", "CNY", ""}
	})

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		base, _, _, _ := first.Split("same-symbol")
		if base != "FIRST" {
			t.Errorf("first parser base = %q, want FIRST", base)
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		base, _, _, _ := second.Split("same-symbol")
		if base != "SECOND" {
			t.Errorf("second parser base = %q, want SECOND", base)
		}
	}()
	close(start)
	wg.Wait()
}

func BenchmarkSplitSymbolDelimited(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		SplitSymbol("BTC/USDT:USDT")
	}
}

func BenchmarkSymbolParserCacheHit(b *testing.B) {
	parser := NewSymbolParser("binance")
	parser.Split("BTC/USDT:USDT")
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		parser.Split("BTC/USDT:USDT")
	}
}

func BenchmarkSymbolParserCacheHitAlternating(b *testing.B) {
	parser := NewSymbolParser("binance")
	pairs := [2]string{"BTC/USDT:USDT", "ETH/USDT:USDT"}
	for _, pair := range pairs {
		parser.Split(pair)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parser.Split(pairs[i&1])
	}
}
