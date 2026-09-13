package biz

import (
	"strconv"
	"sync"
	"testing"
)

func TestTradingStateRegistryAccessIsSafeDuringReset(t *testing.T) {
	state := NewTradingState()
	start := make(chan struct{})
	var wg sync.WaitGroup

	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 500; i++ {
				account := "account-" + strconv.Itoa(i%4)
				state.SetOrderManager(account, nil)
				state.SetLiveManager(account, nil)
				_ = state.OrderManager(account)
				_ = state.LiveManager(account)
				_ = state.Wallet(account)
				_ = state.OrderManagersSnapshot()
				_ = state.LiveManagersSnapshot()
				_ = state.WalletsSnapshot()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 500; i++ {
			state.Reset()
		}
	}()

	close(start)
	wg.Wait()
	state.Reset()
	if len(state.OrderManagersSnapshot()) != 0 || len(state.LiveManagersSnapshot()) != 0 || len(state.WalletsSnapshot()) != 0 {
		t.Fatalf("trading state was not reset: managers=%d live=%d wallets=%d",
			len(state.OrderManagersSnapshot()), len(state.LiveManagersSnapshot()), len(state.WalletsSnapshot()))
	}
}

func TestTradingStateWalletSnapshotDoesNotExposeRegistry(t *testing.T) {
	state := NewTradingState()
	wallet := state.Wallet("isolated")
	snapshot := state.WalletsSnapshot()
	delete(snapshot, "isolated")
	if got := state.Wallet("isolated"); got != wallet {
		t.Fatal("mutating wallet snapshot changed the runtime registry")
	}
}
