package orm

import "testing"

func TestNormalizeDatabaseURLLegacyBracketedIPv4(t *testing.T) {
	got := normalizeDatabaseURL("postgresql://user:pass@[127.0.0.1]:5432/ban")
	if got != "postgresql://user:pass@127.0.0.1:5432/ban" {
		t.Fatalf("normalized URL = %q", got)
	}
}
