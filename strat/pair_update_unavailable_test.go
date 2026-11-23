package strat

import (
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
)

func TestUpdatePairsMissingHooksExplainsUnavailable(t *testing.T) {
	resetStratGlobals()
	stg := &TradeStrat{Name: "test", Policy: &config.RunPolicyConfig{}}
	_, err := stg.UpdatePairs(PairUpdateReq{Add: []string{"BTC/USDT"}})
	if err == nil {
		t.Fatal("expected error when hooks are not set")
	}
	if err.Code != core.ErrRunTime {
		t.Fatalf("error code = %d, want %d", err.Code, core.ErrRunTime)
	}
	if !strings.Contains(err.Short(), "not available") {
		t.Fatalf("message should explain unavailability: %q", err.Short())
	}
}
