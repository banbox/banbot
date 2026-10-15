package biz

import (
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
)

func TestRemoteCommandRuntimeIsolation(t *testing.T) {
	first := &core.State{}
	second := &core.State{}
	clock := btime.NewClockState(true, nil)
	clock.SetTimeMS(123000)
	makeService := func(state *core.State) *RemoteCommandService {
		return NewRemoteCommandServiceWithRuntimeDeps(RuntimeDeps{
			Core: state, Clock: clock,
			Accounts: map[string]*config.AccountConfig{"owned": {}},
		})
	}
	command := RemoteCommand{Source: RemoteSourceTelegram, Account: "owned", Action: RemoteActionTradingSwitch, DisableHours: 1}
	result, err := makeService(first).Run(command)
	if err != nil || result.UntilMS != 3723000 {
		t.Fatalf("runtime command failed: %+v %v", result, err)
	}
	if first.NoEnterUntilSnapshot()["owned"] != result.UntilMS || len(second.NoEnterUntilSnapshot()) != 0 {
		t.Fatal("trading switch escaped its runtime")
	}
	if _, err := makeService(second).Run(command); err != nil {
		t.Fatalf("runtime rate limits are shared: %v", err)
	}
	command.Account = "unknown"
	if _, err := makeService(first).Run(command); err == nil {
		t.Fatal("unknown runtime account accepted")
	}
}
