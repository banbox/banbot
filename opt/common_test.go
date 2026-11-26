package opt

import "testing"

func TestDescGroupsSortsFractionalScores(t *testing.T) {
	items := []*OptInfo{
		{Score: -0.4},
		{Score: 0.2},
		{Score: -0.9},
		{Score: 0.7},
		{Score: 12.5},
		{Score: 12.8},
	}
	goods, bads := DescGroups(items)
	check := func(name string, got []*OptInfo, want ...float64) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("%s: got %d items, want %d", name, len(got), len(want))
		}
		for i, it := range got {
			if it.Score != want[i] {
				t.Fatalf("%s[%d]: got score %v, want %v", name, i, it.Score, want[i])
			}
		}
	}
	check("goods", goods, 12.8, 12.5, 0.7, 0.2)
	check("bads", bads, -0.4, -0.9)
}

func TestOptGoodPosUsesBestFractionalScore(t *testing.T) {
	items := []*OptInfo{{Score: -0.5}, {Score: 0.1}, {Score: 0.9}}
	if best := optGoodPos(items, 0); best.Score != 0.9 {
		t.Fatalf("got score %v, want 0.9", best.Score)
	}
}
