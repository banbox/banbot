package factor

import (
	"math"
	"testing"
)

func TestRobustCrossSectionAndMultiExposurePIT(t *testing.T) {
	x := Field("prices", "close", "1h")
	exposure := Field("prices", "x", "1h")
	second := Field("prices", "z", "1h")
	plan, err := New().Add("robust", RobustZScore(x)).Add("clip", MADWinsorize(x, 2)).Add("ols", MultiResidual(x, exposure, second)).Add("wls", WeightedResidual(x, Field("prices", "weight", "1h"), exposure, second)).Compile()
	if err != nil {
		t.Fatal(err)
	}
	rows := map[int32]map[string]any{1: {"close": 1.0, "x": 0.0, "z": 0.0, "weight": 1.0}, 2: {"close": 2.0, "x": 1.0, "z": 0.0, "weight": 2.0}, 3: {"close": 3.0, "x": 2.0, "z": 0.0, "weight": 3.0}, 4: {"close": 100.0, "x": 0.0, "z": 1.0, "weight": 4.0}}
	session, _ := NewSession(plan)
	frame, err := session.Evaluate(testSnapshot(t, 1000, rows))
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(frame.Values["robust"][4].Value-97.5/1.4826) > 1e-10 {
		t.Fatal(frame.Values["robust"])
	}
	if math.Abs(frame.Values["clip"][4].Value-(2.5+2*1.4826)) > 1e-10 {
		t.Fatal(frame.Values["clip"])
	}
	for _, name := range []string{"ols", "wls"} {
		for sid := int32(1); sid <= 4; sid++ {
			if got := frame.Values[name][sid]; got.Validity != Valid || math.Abs(got.Value) > 1e-10 {
				t.Fatalf("%s sid %d: %+v", name, sid, got)
			}
		}
	}
	rows[4]["close"] = nil
	session, _ = NewSession(plan)
	frame, err = session.Evaluate(testSnapshot(t, 1000, rows))
	if err != nil {
		t.Fatal(err)
	}
	if frame.Values["robust"][4].Validity != Null {
		t.Fatal("NULL semantics lost")
	}
	if _, err := New().Add("bad", MADWinsorize(x, 0)).Compile(); err == nil {
		t.Fatal("nonpositive MAD bound accepted")
	}
}
func TestLeastSquaresDependentExposureAndWeights(t *testing.T) {
	x := [][]float64{{1, 1, 2}, {1, 2, 4}, {1, 3, 6}, {1, 4, 8}}
	y := []float64{5, 8, 11, 14}
	coef, rank, err := LeastSquares(x, y, []float64{1, 2, 3, 4})
	if err != nil || rank != 2 || math.Abs(coef[0]-2) > 1e-10 || math.Abs(coef[1]-3) > 1e-10 || coef[2] != 0 {
		t.Fatalf("coef=%v rank=%d err=%v", coef, rank, err)
	}
	if _, _, err := LeastSquares(x, y, []float64{1, 0, 1, 1}); err == nil {
		t.Fatal("nonpositive WLS weights accepted")
	}
	if _, _, err := LeastSquares([][]float64{{1e300}}, []float64{1e300}, []float64{1e300}); err == nil {
		t.Fatal("weighted numerical overflow accepted")
	}
}
