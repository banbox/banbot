package config

import "testing"

func TestAdvancedCostsFundingApr081(t *testing.T) {
	path := "run_policy[0].manifest.costs"
	if err := validateAdvanced(path, map[string]any{"funding_apr": 10.0}); err != nil {
		t.Fatal(err)
	}
	if err := validateAdvanced(path, map[string]any{"funding_apr": -1.0}); err == nil {
		t.Fatal("negative apr must be rejected")
	}
}
