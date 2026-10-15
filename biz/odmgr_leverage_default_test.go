package biz

import "testing"

func TestAccountLeverageOmittedDefaultsToOne(t *testing.T) {
	mgr := &OrderMgr{runtimeDeps: true}
	if got := mgr.accountLeverage(); got != 1 {
		t.Fatalf("omitted leverage = %v, want 1 to avoid infinite margin", got)
	}
	mgr.runtimeCfg.accountLeverage = 3
	if got := mgr.accountLeverage(); got != 3 {
		t.Fatalf("explicit leverage changed: %v", got)
	}
}
