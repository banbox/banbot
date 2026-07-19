package biz

import (
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
)

func TestInitLocalOrderMgrUsesScopedBacktestStop(t *testing.T) {
	originalBiz := BackupVars()
	originalAccounts := config.Accounts
	originalStopAll := core.StopAll
	t.Cleanup(func() {
		RestoreVars(originalBiz)
		config.Accounts = originalAccounts
		core.StopAll = originalStopAll
	})

	config.Accounts = map[string]*config.AccountConfig{config.DefAcc: {}}
	ResetVars()
	liveStopCalls := 0
	localStopCalls := 0
	core.StopAll = func() { liveStopCalls++ }
	InitLocalOrderMgr(nil, false, func() { localStopCalls++ })

	odMgr, ok := GetOdMgr(config.DefAcc).(*LocalOrderMgr)
	if !ok {
		t.Fatalf("order manager type = %T, want *LocalOrderMgr", GetOdMgr(config.DefAcc))
	}
	odMgr.stopBacktest()
	if localStopCalls != 1 || liveStopCalls != 0 {
		t.Fatalf("stop calls local=%d live=%d, want 1/0", localStopCalls, liveStopCalls)
	}
}
