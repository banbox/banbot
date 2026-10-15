package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banexg"
)

func TestRuntimeBorrowsSameAccountOwner(t *testing.T) {
	p := NewProcess()
	defer p.Close()
	key := execution.AccountKey{VenueSessionIdentity: "paper-session", Account: "default", SettlementDomain: "USDT"}
	first, err := p.NewRuntime(Options{AccountOwnerKey: &key})
	if err != nil {
		t.Fatal(err)
	}
	second, err := p.NewRuntime(Options{AccountOwnerKey: &key})
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == second.ID || first.AccountOwner().Token() != second.AccountOwner().Token() {
		t.Fatal("runtime identity split account owner")
	}
	otherKey := key
	otherKey.Account = "another-account"
	other, err := p.NewRuntime(Options{AccountOwnerKey: &otherKey})
	if err != nil {
		t.Fatal(err)
	}
	if first.AccountOwner().Token() == other.AccountOwner().Token() {
		t.Fatal("different accounts share owner")
	}
	other.Close()
	first.Close()
	if err := second.AccountOwner().DoLocal(second.AccountOwner().Token(), func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := first.AccountOwner().DoLocal(first.AccountOwner().Token(), func(context.Context) error { return nil }); !errors.Is(err, execution.ErrReleased) {
		t.Fatalf("closed borrower: %v", err)
	}
	second.Close()
	// A new borrower must find the retained Process-owned service even with no
	// runtime borrowers; signals must still find its Process lifecycle owner.
	activeProcesses.Lock()
	_, registered := activeProcesses.items[p]
	activeProcesses.Unlock()
	if !registered {
		t.Fatal("process owner disappeared after borrowers closed")
	}
	third, err := p.NewRuntime(Options{AccountOwnerKey: &key})
	if err != nil {
		t.Fatal(err)
	}
	if third.AccountOwner().Token() != second.AccountOwner().Token() {
		t.Fatal("release recreated owner")
	}
}

func TestProcessAccountOwnerStopJoinsSlowCallback(t *testing.T) {
	p := NewProcess()
	defer p.Close()
	key := execution.AccountKey{VenueSessionIdentity: "paper-session", Account: "default", SettlementDomain: "USDT"}
	rt, err := p.NewRuntime(Options{AccountOwnerKey: &key})
	if err != nil {
		t.Fatal(err)
	}
	started, canceled, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan error, 1)
	handle := rt.AccountOwner()
	go func() {
		returned <- handle.DoLocal(handle.Token(), func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			close(canceled)
			<-release
			return nil
		})
	}()
	<-started
	closed := make(chan struct{})
	go func() { p.Close(); close(closed) }()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("owner cancellation missing")
	}
	select {
	case <-closed:
		t.Fatal("Process.Close returned before callback joined")
	default:
	}
	close(release)
	if err := <-returned; err != nil {
		t.Fatal(err)
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Process.Close did not join")
	}
}

func TestRuntimeAccountOwnerFoundationRejectsTrading(t *testing.T) {
	p := NewProcess()
	defer p.Close()
	key := execution.AccountKey{VenueSessionIdentity: "paper-session", Account: "default", SettlementDomain: "USDT"}
	for _, opts := range []Options{
		{Mode: core.RunModeLive}, {Mode: core.RunModeBackTest}, {Env: core.RunEnvProd}, {Exchange: &banexg.Exchange{}},
	} {
		opts.AccountOwnerKey = &key
		if rt, err := p.NewRuntime(opts); err == nil {
			rt.Close()
			t.Fatal("unbridged trading admitted")
		}
	}
	legacy, err := p.NewRuntime(Options{})
	if err != nil {
		t.Fatal(err)
	}
	if legacy.AccountOwner() != nil {
		t.Fatal("legacy default changed")
	}
}
