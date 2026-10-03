package execution

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func ownerTestKey() AccountKey {
	return AccountKey{"paper-session", "trader", "USDT"}
}

func TestAccountOwnerIdentityReleaseAndGeneration(t *testing.T) {
	var registry AccountRegistry
	defer registry.Close()
	first, err := registry.Acquire(ownerTestKey())
	if err != nil {
		t.Fatal(err)
	}
	second, err := registry.Acquire(ownerTestKey())
	if err != nil {
		t.Fatal(err)
	}
	if first.owner != second.owner {
		t.Fatal("same account has two writers")
	}
	key := ownerTestKey()
	key.SettlementDomain = "BTC"
	other, err := registry.Acquire(key)
	if err != nil {
		t.Fatal(err)
	}
	if first.owner == other.owner {
		t.Fatal("settlement domains share writer")
	}
	var independent AccountRegistry
	defer independent.Close()
	foreign, err := independent.Acquire(ownerTestKey())
	if err != nil {
		t.Fatal(err)
	}
	called := 0
	call := func(context.Context) error { called++; return nil }
	if err := first.DoLocal(foreign.Token(), call); !errors.Is(err, ErrOwnerToken) {
		t.Fatalf("foreign registry generation: %v", err)
	}
	stale := first.Token()
	stale.Generation++
	if err := first.DoLocal(stale, call); !errors.Is(err, ErrOwnerToken) {
		t.Fatalf("stale token: %v", err)
	}
	wrong := first.Token()
	wrong.Key.Account = "another-account"
	if err := first.DoLocal(wrong, call); !errors.Is(err, ErrOwnerToken) {
		t.Fatalf("wrong account: %v", err)
	}
	first.Release()
	first.Release()
	if err := first.DoLocal(first.Token(), call); !errors.Is(err, ErrReleased) {
		t.Fatalf("released: %v", err)
	}
	if err := second.DoLocal(second.Token(), call); err != nil {
		t.Fatal(err)
	}
	if called != 1 {
		t.Fatalf("callback count = %d", called)
	}
	registry.Stop()
	if err := second.DoLocal(second.Token(), call); !errors.Is(err, ErrOwnerStopped) {
		t.Fatalf("stopped: %v", err)
	}
	if _, err := registry.Acquire(ownerTestKey()); !errors.Is(err, ErrOwnerStopped) {
		t.Fatalf("takeover: %v", err)
	}
}

func TestAccountOwnerSerializesConcurrentBorrowers(t *testing.T) {
	var registry AccountRegistry
	defer registry.Close()
	var tasks sync.WaitGroup
	count := 0 // intentionally non-atomic: race verification proves one writer.
	for i := 0; i < 40; i++ {
		handle, err := registry.Acquire(ownerTestKey())
		if err != nil {
			t.Fatal(err)
		}
		tasks.Add(1)
		go func() {
			defer tasks.Done()
			defer handle.Release()
			if err := handle.DoLocal(handle.Token(), func(context.Context) error { count++; return nil }); err != nil {
				t.Error(err)
			}
		}()
	}
	tasks.Wait()
	if count != 40 {
		t.Fatalf("writes = %d", count)
	}
}

func TestAccountKeyRequiresCanonicalIdentity(t *testing.T) {
	var registry AccountRegistry
	defer registry.Close()
	for _, key := range []AccountKey{{}, {"session", "", "USDT"}, {"session", "account", " USDT"}} {
		if _, err := registry.Acquire(key); err == nil {
			t.Fatalf("accepted invalid identity: %#v", key)
		}
	}
}
