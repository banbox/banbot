package runtime

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

type trackedConstructionParent struct {
	context.Context
	active atomic.Int32
}

func (p *trackedConstructionParent) Value(any) any { return nil }

func (p *trackedConstructionParent) AfterFunc(callback func()) func() bool {
	p.active.Add(1)
	stop := context.AfterFunc(p.Context, callback)
	return func() bool {
		p.active.Add(-1)
		return stop()
	}
}

type invalidConstructionExchange struct{ banexg.BanExchange }

func (*invalidConstructionExchange) Info() *banexg.ExgInfo {
	return &banexg.ExgInfo{ID: "test", MarketType: banexg.MarketLinear}
}

func (*invalidConstructionExchange) PriceSymbolParts(string) ([4]string, *errs.Error) {
	return [4]string{}, errs.NewMsg(errs.CodeParamInvalid, "invalid configured symbol")
}

type nilInfoExchange struct{ banexg.BanExchange }

func (*nilInfoExchange) Info() *banexg.ExgInfo { return nil }

type emptyInfoExchange struct{ banexg.BanExchange }

func (*emptyInfoExchange) Info() *banexg.ExgInfo { return &banexg.ExgInfo{} }

type panicInfoExchange struct{ banexg.BanExchange }

func (*panicInfoExchange) Info() *banexg.ExgInfo { panic("metadata unavailable") }

func TestRuntimeRejectsUnavailableAdapterIdentity(t *testing.T) {
	process := NewProcess()
	defer process.Close()
	if _, err := process.NewRuntime(Options{
		Exchange: &nilInfoExchange{}, ExchangeName: "test", Market: banexg.MarketSpot,
	}); err == nil || !strings.Contains(err.Error(), "adapter identity") {
		t.Fatalf("unavailable adapter identity error = %v", err)
	}
	process.runtimeMu.Lock()
	tracked := len(process.runtimes)
	process.runtimeMu.Unlock()
	if tracked != 0 {
		t.Fatalf("unavailable adapter identity registered a runtime: %d", tracked)
	}
}

func TestRuntimeRejectsIncompleteAdapterIdentityMetadata(t *testing.T) {
	for name, exchange := range map[string]banexg.BanExchange{
		"empty": &emptyInfoExchange{},
		"panic": &panicInfoExchange{},
	} {
		t.Run(name, func(t *testing.T) {
			process := NewProcess()
			defer process.Close()
			if _, err := process.NewRuntime(Options{
				Exchange: exchange, ExchangeName: "test", Market: banexg.MarketSpot,
			}); err == nil || !strings.Contains(err.Error(), "adapter identity") {
				t.Fatalf("incomplete adapter identity error = %v", err)
			}
		})
	}
}

func TestRuntimeFailedConstructionReleasesSIDRegistry(t *testing.T) {
	process := NewProcess()
	defer process.Close()
	const registryURL = "postgresql://registry.example/failed-runtime"
	_, err := process.NewRuntime(Options{
		Config: &config.Config{
			Database: &config.DatabaseConfig{SIDRegistryURL: registryURL},
			Pairs:    []string{"invalid"},
		},
		Exchange:     &invalidConstructionExchange{},
		ExchangeName: "test",
		Market:       banexg.MarketLinear,
	})
	if err == nil {
		t.Fatal("invalid runtime construction unexpectedly succeeded")
	}
	process.sidRegistryMu.Lock()
	remaining := len(process.sidRegistries)
	process.sidRegistryMu.Unlock()
	if remaining != 0 {
		t.Fatalf("failed construction retained %d SID registries", remaining)
	}
}

func TestRuntimeFailedConstructionReleasesSIDAllocator(t *testing.T) {
	process := NewProcess()
	defer process.Close()
	const registryURL = "postgresql://registry.example/failed-runtime-allocator"
	_, err := process.NewRuntime(Options{
		Config: &config.Config{
			Database: &config.DatabaseConfig{SIDRegistryURL: registryURL},
		},
		// The recovery binding runs after allocator creation and fails for '.'.
		DataDir: ".",
	})
	if err == nil {
		t.Fatal("runtime construction unexpectedly succeeded with invalid recovery directory")
	}
	process.sidRegistryMu.Lock()
	remainingRegistries := len(process.sidRegistries)
	process.sidRegistryMu.Unlock()
	process.symbolAllocatorMu.Lock()
	remainingAllocators := len(process.symbolAllocators)
	process.symbolAllocatorMu.Unlock()
	if remainingRegistries != 0 || remainingAllocators != 0 {
		t.Fatalf("failed construction retained process state: registries=%d allocators=%d", remainingRegistries, remainingAllocators)
	}
}

func TestConcurrentFailedConstructionsReleaseAllocatedSIDState(t *testing.T) {
	process := NewProcess()
	defer process.Close()
	const registryURL = "postgresql://registry.example/concurrent-failed-allocator"
	const workers = 8
	start := make(chan struct{})
	results := make(chan error, workers)
	var group sync.WaitGroup
	for i := 0; i < workers; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			_, err := process.NewRuntime(Options{
				Config:  &config.Config{Database: &config.DatabaseConfig{SIDRegistryURL: registryURL}},
				DataDir: ".",
			})
			results <- err
		}()
	}
	close(start)
	group.Wait()
	close(results)
	for err := range results {
		if err == nil {
			t.Fatal("failed construction unexpectedly succeeded")
		}
	}
	process.sidRegistryMu.Lock()
	remainingRegistries := len(process.sidRegistries)
	process.sidRegistryMu.Unlock()
	process.symbolAllocatorMu.Lock()
	remainingAllocators := len(process.symbolAllocators)
	process.symbolAllocatorMu.Unlock()
	if remainingRegistries != 0 || remainingAllocators != 0 {
		t.Fatalf("concurrent failed constructions retained process state: registries=%d allocators=%d", remainingRegistries, remainingAllocators)
	}
}

func TestConcurrentFailedConstructionsReleaseSharedSIDState(t *testing.T) {
	process := NewProcess()
	defer process.Close()
	const registryURL = "postgresql://registry.example/concurrent-failed-runtime"
	start := make(chan struct{})
	exchanges := []*processBlockingExchange{
		{entered: make(chan struct{}), release: make(chan struct{})},
		{entered: make(chan struct{}), release: make(chan struct{})},
	}
	results := make(chan error, len(exchanges))
	var group sync.WaitGroup
	for _, exchange := range exchanges {
		group.Add(1)
		go func(exchange *processBlockingExchange) {
			defer group.Done()
			<-start
			_, err := process.NewRuntime(Options{
				Config: &config.Config{
					Database: &config.DatabaseConfig{SIDRegistryURL: registryURL},
				},
				DataDir:      ".",
				Exchange:     exchange,
				ExchangeName: "test",
				Market:       banexg.MarketSpot,
				Pairs:        []string{"BTC/USDT"},
			})
			results <- err
		}(exchange)
	}
	close(start)
	for _, exchange := range exchanges {
		select {
		case <-exchange.entered:
		case <-time.After(time.Second):
			t.Fatal("concurrent construction did not reach the shared validation barrier")
		}
	}
	for _, exchange := range exchanges {
		close(exchange.release)
	}
	group.Wait()
	close(results)
	for err := range results {
		if err == nil {
			t.Fatal("failed construction unexpectedly succeeded")
		}
	}
	process.sidRegistryMu.Lock()
	remainingRegistries := len(process.sidRegistries)
	process.sidRegistryMu.Unlock()
	process.symbolAllocatorMu.Lock()
	remainingAllocators := len(process.symbolAllocators)
	process.symbolAllocatorMu.Unlock()
	if remainingRegistries != 0 || remainingAllocators != 0 {
		t.Fatalf("failed constructions retained process state: registries=%d allocators=%d", remainingRegistries, remainingAllocators)
	}
}

func TestRuntimeInvalidSymbolsReleaseParentRegistration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	parent := &trackedConstructionParent{Context: ctx}
	process := NewProcess()
	defer process.Close()
	for attempt := 0; attempt < 3; attempt++ {
		_, err := process.NewRuntime(Options{
			Context: parent, Exchange: &invalidConstructionExchange{},
			ExchangeName: "test", Market: "linear", Pairs: []string{"invalid"},
		})
		if err == nil {
			t.Fatal("invalid symbol accepted")
		}
		if parent.active.Load() != 0 {
			t.Fatal("failed construction retained a child cancellation context")
		}
	}
}
