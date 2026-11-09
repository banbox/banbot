package runtime

import (
	"testing"

	"github.com/banbox/banbot/config"
)

// A prod-env backtest config that only carries a no_trade account (historical site configs) must still get a default account.
func TestRuntimeDefaultAccountBacktestNeverProd(t *testing.T) {
	accounts := map[string]*config.AccountConfig{"six": {NoTrade: true}}
	if got := runtimeDefaultAccount(true, accounts); got != "" {
		t.Fatalf("live prod without tradable account should stay empty, got %q", got)
	}
	if got := runtimeDefaultAccount(false, accounts); got != "default" {
		t.Fatalf("non-prod default account = %q", got)
	}
	got := runtimeExecutionAccounts(false, accounts, "default")
	if len(got) != 1 || got["default"] == nil || got["default"].NoTrade {
		t.Fatalf("execution accounts must be one tradable blank default account: %+v", got)
	}
}
