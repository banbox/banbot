package runtimeplan

import (
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/strat"
)

func TestInspectDistinguishesBadConfigFromRuntimeErrorInConstructor(t *testing.T) {
	t.Run("Require failure reports BadConfig in constructor_panic", func(t *testing.T) {
		const strategyName = "runtime_plan_require_bad_config"
		strat.RegisterStrategy(strategyName, func(pol *config.RunPolicyConfig) *strat.TradeStrat {
			fast := pol.DefInt("fast", 30, nil)
			slow := pol.DefInt("slow", 10, nil)
			pol.Require(fast < slow, "fast must be less than slow")
			return &strat.TradeStrat{RunTimeFrames: []string{"1h"}}
		})
		t.Cleanup(func() { strat.UnregisterStrategy(strategyName) })

		output, err := Inspect(validRequest(t, strategyName), t.TempDir())
		if err == nil || output == nil {
			t.Fatalf("Inspect should fail closed with output: output=%+v err=%v", output, err)
		}
		if len(output.Unsupported) != 1 {
			t.Fatalf("unsupported = %+v, want 1 item", output.Unsupported)
		}
		got := output.Unsupported[0]
		if got.Code != "constructor_panic" || got.Message != "[BadConfig] fast must be less than slow" {
			t.Fatalf("unsupported[0] = %+v, want constructor_panic with [BadConfig] message", got)
		}
	})

	t.Run("genuine runtime.Error is not tagged as BadConfig", func(t *testing.T) {
		const strategyName = "runtime_plan_constructor_nil_deref"
		strat.RegisterStrategy(strategyName, func(*config.RunPolicyConfig) *strat.TradeStrat {
			var p *int
			_ = *p
			return &strat.TradeStrat{RunTimeFrames: []string{"1h"}}
		})
		t.Cleanup(func() { strat.UnregisterStrategy(strategyName) })

		output, err := Inspect(validRequest(t, strategyName), t.TempDir())
		if err == nil || output == nil || len(output.Unsupported) != 1 {
			t.Fatalf("Inspect output=%+v err=%v", output, err)
		}
		got := output.Unsupported[0]
		if got.Code != "constructor_panic" {
			t.Fatalf("code = %q, want constructor_panic", got.Code)
		}
		if strings.Contains(got.Message, "[BadConfig]") || !strings.Contains(got.Message, "runtime error:") {
			t.Fatalf("runtime bug message = %q, must contain runtime error and not [BadConfig]", got.Message)
		}
	})
}
