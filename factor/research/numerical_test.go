package research

import (
	"math"
	"strconv"
	"testing"

	"github.com/banbox/banbot/factor"
)

func TestCorrelationNumericalScales(t *testing.T) {
	for _, scale := range []float64{1, 1e100, 1e200, 1e-200, math.SmallestNonzeroFloat64} {
		t.Run("scale="+strconv.FormatFloat(scale, 'g', -1, 64), func(t *testing.T) {
			points := []pair{{x: scale, y: 3 * scale}, {x: 2 * scale, y: 2 * scale}, {x: 3 * scale, y: scale}}
			for _, rank := range []bool{false, true} {
				got := correlation(points, rank)
				if got.Validity != factor.Valid || !finiteValues(got.Value) || math.Abs(got.Value+1) > 1e-14 {
					t.Fatalf("rank=%v correlation=%+v, want -1", rank, got)
				}
			}
		})
	}
	for _, points := range [][]pair{
		{{x: -math.MaxFloat64, y: -math.MaxFloat64}, {x: math.MaxFloat64, y: math.MaxFloat64}},
		{{x: 1e308, y: 1}, {x: math.Nextafter(1e308, math.Inf(1)), y: 2}, {x: math.Nextafter(math.Nextafter(1e308, math.Inf(1)), math.Inf(1)), y: 3}},
	} {
		got := correlation(points, false)
		if got.Validity != factor.Valid || !finiteValues(got.Value) || math.Abs(got.Value-1) > 1e-14 {
			t.Fatalf("extreme correlation=%+v, want 1", got)
		}
	}
}

func TestCorrelationOrdinaryAndTranslation(t *testing.T) {
	for _, shift := range []float64{0, 1e12} {
		got := correlation([]pair{{x: 1 + shift, y: 1}, {x: 2 + shift, y: 3}, {x: 3 + shift, y: 2}}, false)
		if got.Validity != factor.Valid || math.Abs(got.Value-.5) > 1e-14 || !finiteValues(got.Value) || math.Abs(got.Value) > 1 {
			t.Fatalf("translated correlation=%+v, want .5", got)
		}
	}
	for _, points := range [][]pair{nil, {{x: 1, y: 2}}, {{x: 1, y: 1}, {x: 1, y: 2}}, {{x: math.NaN(), y: 1}, {x: 2, y: 2}}} {
		for _, rank := range []bool{false, true} {
			if got := correlation(points, rank); got.Validity == factor.Valid {
				t.Fatalf("undefined correlation marked valid: %+v", got)
			}
		}
	}
}

func TestCovarianceNumericalRepresentation(t *testing.T) {
	for _, rows := range [][][]float64{{{-1e200}, {1e200}}, {{-math.MaxFloat64}, {math.MaxFloat64}}, {{-1e-200}, {1e-200}}} {
		if got, err := EstimateCovariance(rows, 0, false); err == nil {
			t.Fatalf("unrepresentable covariance accepted: %+v", got)
		}
	}
	for _, scale := range []float64{1e150, 1e-150} {
		got, err := EstimateCovariance([][]float64{{-scale}, {scale}}, 0, false)
		want := 2 * scale * scale
		if err != nil || !finiteValues(got.Matrix[0][0]) || math.Abs(got.Matrix[0][0]/want-1) > 1e-14 {
			t.Fatalf("representable covariance=%+v error=%v, want %g", got, err, want)
		}
	}
	got, err := EstimateCovariance([][]float64{{math.MaxFloat64}, {math.MaxFloat64}}, 0, false)
	if err != nil || got.Matrix[0][0] != 0 {
		t.Fatalf("constant covariance=%+v error=%v", got, err)
	}
	// A large common offset must not destroy the representable small variance.
	x := 1e154
	y := math.Nextafter(x, math.Inf(1))
	got, err = EstimateCovariance([][]float64{{x}, {y}}, 0, false)
	want := (y - x) * (y - x) / 2
	if err != nil || math.Abs(got.Matrix[0][0]/want-1) > 1e-14 {
		t.Fatalf("offset covariance=%+v error=%v, want %g", got, err, want)
	}
}

func TestCovarianceHandCalculationAndShrinkage(t *testing.T) {
	for _, diagonal := range []bool{false, true} {
		for _, shrink := range []float64{0, .5, 1} {
			got, err := EstimateCovariance([][]float64{{1, 2}, {2, 4}, {3, 6}}, shrink, diagonal)
			if err != nil {
				t.Fatal(err)
			}
			wantOff := 2 * (1 - shrink)
			wantShrink := shrink
			if diagonal {
				wantOff, wantShrink = 0, 1
			}
			m := got.Matrix
			if got.Samples != 3 || got.Shrinkage != wantShrink || m[0][0] != 1 || m[1][1] != 4 || m[0][1] != wantOff || m[1][0] != wantOff || m[0][0]*m[1][1]-m[0][1]*m[1][0] < 0 {
				t.Fatalf("wrong covariance/shrinkage or non-PSD: %+v", got)
			}
		}
	}
}
