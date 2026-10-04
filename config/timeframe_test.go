package config

import "testing"

func TestAdvancedTimeFrameKeys(t *testing.T) {
	for _, path := range []string{
		"run_policy[0].factor.expressions",
		"run_policy[0].factor.expressions.bindings.kline",
		"run_policy[0].factor.prices",
	} {
		t.Run(path, func(t *testing.T) {
			if err := validateAdvanced(path, map[string]any{"timeframe": "1h"}); err != nil {
				t.Fatalf("timeframe rejected: %v", err)
			}
			if err := validateAdvanced(path, map[string]any{"frequency": "1h"}); err == nil {
				t.Fatal("legacy frequency key accepted")
			}
			for _, value := range []any{nil, 3600, ""} {
				if err := validateAdvanced(path, map[string]any{"timeframe": value}); err == nil {
					t.Fatalf("invalid timeframe %v accepted", value)
				}
			}
		})
	}
}
