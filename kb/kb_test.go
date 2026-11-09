package kb

import (
	"strings"
	"testing"
)

func TestGetKB(t *testing.T) {
	for _, lang := range []string{LangCN, LangEN} {
		for _, name := range []string{"ts", "factor", "statarb"} {
			s, err := GetKB(lang, name)
			if err != nil || len(s) < 1000 {
				t.Fatalf("GetKB(%s,%s) err=%v len=%d", lang, name, err, len(s))
			}
		}
		all, err := GetKB(lang, "ts", "factor", "statarb")
		if err != nil || strings.Count(all, Separator) != 2 {
			t.Fatalf("multi GetKB(%s) err=%v", lang, err)
		}
	}
	if _, err := GetKB("fr", "ts"); err == nil {
		t.Fatal("expected lang error")
	}
	if _, err := GetKB("cn"); err == nil {
		t.Fatal("expected empty names error")
	}
	if _, err := GetKB("cn", "nope"); err == nil {
		t.Fatal("expected unknown name error")
	}
}
