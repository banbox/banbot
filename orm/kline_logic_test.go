package orm

import (
	"testing"

	"github.com/banbox/banbot/core"
)

func TestParseDownArgsAlignStartAndInferEndByLimit(t *testing.T) {
	tfMSecs := int64(60_000) // 1m
	base := int64((1_700_000_000_000 / tfMSecs) * tfMSecs)
	start, end := parseDownArgs(tfMSecs, base+1_000, 0, 3, false)
	if start != base+tfMSecs {
		t.Fatalf("start mismatch, want=%d got=%d", base+tfMSecs, start)
	}
	if end != base+4*tfMSecs {
		t.Fatalf("end mismatch, want=%d got=%d", base+4*tfMSecs, end)
	}
}

func TestParseDownArgsWithUnfinishAlignEndUp(t *testing.T) {
	tfMSecs := int64(300_000) // 5m
	base := int64((1_700_000_000_000 / tfMSecs) * tfMSecs)
	_, end := parseDownArgs(tfMSecs, 0, base+1_000, 0, true)
	if end != base+tfMSecs {
		t.Fatalf("end mismatch, want=%d got=%d", base+tfMSecs, end)
	}
}

func TestParseDownArgsWithLimitOnlyBuildsStart(t *testing.T) {
	tfMSecs := int64(60_000)
	base := int64((1_700_000_000_000 / tfMSecs) * tfMSecs)
	start, end := parseDownArgs(tfMSecs, 0, base+5*tfMSecs, 2, false)
	if start != base+3*tfMSecs || end != base+5*tfMSecs {
		t.Fatalf("unexpected start/end: %d/%d", start, end)
	}
}

func TestParseDownArgsMSMinStampBypassStartAdjust(t *testing.T) {
	tfMSecs := int64(60_000)
	base := int64((1_700_000_000_000 / tfMSecs) * tfMSecs)
	start, _ := parseDownArgs(tfMSecs, core.MSMinStamp, base+5*tfMSecs, 0, false)
	if start != core.MSMinStamp {
		t.Fatalf("start should preserve MSMinStamp, got=%d", start)
	}
}

func TestGetDownTFMapping(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{in: "1m", want: "1m"},
		{in: "5m", want: "1m"},
		{in: "15m", want: "15m"},
		{in: "45m", want: "15m"},
		{in: "1h", want: "1h"},
		{in: "2h", want: "1h"},
		{in: "1d", want: "1d"},
		{in: "3d", want: "1d"},
	}
	for _, tt := range tests {
		got, err := GetDownTF(tt.in)
		if err != nil {
			t.Fatalf("GetDownTF(%s) unexpected err: %v", tt.in, err)
		}
		if got != tt.want {
			t.Fatalf("GetDownTF(%s) want=%s got=%s", tt.in, tt.want, got)
		}
	}
}

func TestGetDownTFInvalid(t *testing.T) {
	if _, err := GetDownTF("59s"); err == nil {
		t.Fatal("GetDownTF(59s) should fail")
	}
	if _, err := GetDownTF("17m"); err == nil {
		t.Fatal("GetDownTF(17m) should fail")
	}
	if _, err := GetDownTF("25h"); err == nil {
		t.Fatal("GetDownTF(25h) should fail")
	}
}

func TestUnfinishChain(t *testing.T) {
	if got := unfinishChain("1m"); len(got) != 0 {
		t.Fatalf("1m chain should be empty, got=%v", got)
	}
	got5m := unfinishChain("5m")
	if len(got5m) != 1 || got5m[0] != "1m" {
		t.Fatalf("5m chain mismatch, got=%v", got5m)
	}
	got1h := unfinishChain("1h")
	want1h := []string{"15m", "5m", "1m"}
	if len(got1h) != len(want1h) {
		t.Fatalf("1h chain len mismatch, got=%v", got1h)
	}
	for i := range want1h {
		if got1h[i] != want1h[i] {
			t.Fatalf("1h chain mismatch, want=%v got=%v", want1h, got1h)
		}
	}
}
