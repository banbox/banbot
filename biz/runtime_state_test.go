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
	if len(state.OrderManagers) != 0 || len(state.LiveManagers) != 0 || len(state.Wallets) != 0 {
		t.Fatalf("trading state was not reset: managers=%d live=%d wallets=%d",
			len(state.OrderManagers), len(state.LiveManagers), len(state.Wallets))
	}
}
