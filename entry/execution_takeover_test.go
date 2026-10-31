package entry

import (
	"testing"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
)

func TestExecutionTakeoverOwnerRequiresDeclaredAccountStrategy(t *testing.T) {
	bridge := &biz.SharedOrderBridgeConfig{Strategies: map[string]biz.SharedStrategyBinding{"ma:takeover": {ID: "declared-owner"}}}
	if owner, err := executionTakeoverOwner(&config.Config{}, bridge); err != nil || owner != "" {
		t.Fatal("omitted takeover changed default", owner, err)
	}
	if owner, err := executionTakeoverOwner(&config.Config{TakeOverStrat: "ma:takeover"}, bridge); err != nil || owner != "declared-owner" {
		t.Fatal("configured owner not resolved", owner, err)
	}
	for _, configured := range []*biz.SharedOrderBridgeConfig{nil, {Strategies: map[string]biz.SharedStrategyBinding{"other": {ID: "other"}}}} {
		if owner, err := executionTakeoverOwner(&config.Config{TakeOverStrat: "ma:takeover"}, configured); err == nil || owner != "" {
			t.Fatal("unknown takeover owner accepted", owner, err)
		}
	}
}
