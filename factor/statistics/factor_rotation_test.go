package statistics

import (
	"math"
	"testing"
)

func TestVarimaxRecoversSimpleStructureAndPreservesCovariance(t *testing.T) {
	angle := .4
	rows := [][]float64{{math.Cos(angle), math.Sin(angle)}, {1.2 * math.Cos(angle), 1.2 * math.Sin(angle)}, {-math.Sin(angle), math.Cos(angle)}, {-1.2 * math.Sin(angle), 1.2 * math.Cos(angle)}}
	for _, normalized := range []bool{false, true} {
		out, err := VarimaxLoadings(rows, normalized, 1000, 1e-12)
		if err != nil {
			t.Fatal(err)
		}
		for i := range rows {
			if math.Min(math.Abs(out[i][0]), math.Abs(out[i][1])) > 1e-5 {
				t.Fatalf("simple structure not recovered %v", out)
			}
			for j := range rows {
				before, after := 0.0, 0.0
				for k := range 2 {
					before += rows[i][k] * rows[j][k]
					after += out[i][k] * out[j][k]
				}
				closeTo(t, before, after)
			}
		}
	}
	if _, err := VarimaxLoadings(rows, false, 1, 1e-12); err == nil {
		t.Fatal("unconverged varimax returned success")
	}
	if _, err := VarimaxLoadings([][]float64{{0, 0}, {0, 0}}, true, 100, 1e-7); err == nil {
		t.Fatal("unidentified zero rotation accepted")
	}
	if _, err := VarimaxLoadings([][]float64{{math.MaxFloat64, 0}, {1, 2}}, false, 100, 1e-7); err == nil {
		t.Fatal("gradient overflow accepted")
	}
}
