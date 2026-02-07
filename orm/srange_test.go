package orm

import "testing"

func TestMergeMSRanges(t *testing.T) {
	in := []MSRange{
		{Start: 10, Stop: 20},
		{Start: 0, Stop: 10},
		{Start: 30, Stop: 40},
		{Start: 20, Stop: 30}, // touching
	}
	out := mergeMSRanges(in)
	if len(out) != 1 || out[0].Start != 0 || out[0].Stop != 40 {
		t.Fatalf("mergeMSRanges got %#v", out)
	}
}

func TestSubtractMSRanges(t *testing.T) {
	target := MSRange{Start: 0, Stop: 40}
	covered := []MSRange{
		{Start: 5, Stop: 10},
		{Start: 0, Stop: 5},
		{Start: 20, Stop: 30},
	}
	out := subtractMSRanges(target, covered)
	if len(out) != 2 {
		t.Fatalf("subtractMSRanges got %#v", out)
	}
	if out[0] != (MSRange{Start: 10, Stop: 20}) {
		t.Fatalf("subtractMSRanges[0] got %#v", out[0])
	}
	if out[1] != (MSRange{Start: 30, Stop: 40}) {
		t.Fatalf("subtractMSRanges[1] got %#v", out[1])
	}
}

