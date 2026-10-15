package config

import "testing"

func TestStratRefineTFLookupAndEnsureShareCacheSemantics(t *testing.T) {
	oldMap := refineTfMap
	oldPolicies := RunPolicy
	t.Cleanup(func() {
		refineLock.Lock()
		refineTfMap = oldMap
		refineLock.Unlock()
		RunPolicy = oldPolicies
	})
	RunPolicy = nil
	ClearRefineMap()

	if got, ok := GetStratRefineTF("missing", "5m"); got != "5m" || ok {
		t.Fatalf("missing lookup = %q, %v; want original timeframe and false", got, ok)
	}
	if got := EnsureStratRefineTF("missing", "5m"); got != "5m" {
		t.Fatalf("missing ensure = %q; want original timeframe", got)
	}
	if got, ok := GetStratRefineTF("missing", "5m"); got != "5m" || !ok {
		t.Fatalf("cached identity lookup = %q, %v; want cached value and true", got, ok)
	}

	refineLock.Lock()
	refineTfMap["strategy"] = map[string]string{"1h": "15m"}
	refineLock.Unlock()
	if got, ok := GetStratRefineTF("strategy", "1h"); got != "15m" || !ok {
		t.Fatalf("cached lookup = %q, %v; want 15m and true", got, ok)
	}
	if got := EnsureStratRefineTF("strategy", "1h"); got != "15m" {
		t.Fatalf("cached ensure = %q; want 15m", got)
	}
}
