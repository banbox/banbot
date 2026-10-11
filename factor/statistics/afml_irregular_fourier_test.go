package statistics

import (
	"math"
	"testing"
)

func scalar(ms int64, v float64) PITScalar { return PITScalar{ms, ms, ms, v} }

func TestNonuniformDFTAnalyticAndVisibility(t *testing.T) {
	// Irregular timestamps carry one complex harmonic's real projection. This
	// hand oracle uses phases 0,pi/2,3pi/2: sum [1,0,0]/3 =1/3.
	s := []PITScalar{scalar(0, 1), scalar(250, 0), scalar(750, 0), {900, 1000, 2000, 999}}
	r, err := NonuniformDFT(s, []float64{0, 1}, 0, 1000, false, 8)
	if err != nil {
		t.Fatal(err)
	}
	if r[1].Samples != 3 || math.Abs(r[1].Real-1./3) > 1e-12 || math.Abs(r[1].Imaginary) > 1e-12 {
		t.Fatal(r)
	}
	centered, err := NonuniformDFT(s, []float64{0}, 0, 1000, true, 4)
	if err != nil || math.Abs(centered[0].Amplitude) > 1e-12 {
		t.Fatalf("centered DC %+v %v", centered, err)
	}
	if _, err = NonuniformDFT(s, []float64{0, 1}, 0, 1000, false, 7); err == nil {
		t.Fatal("work budget bypassed")
	}
	if _, err = NonuniformDFT(s, []float64{math.NaN()}, 0, 1000, false, 4); err == nil {
		t.Fatal("NaN accepted")
	}
}

func TestNonuniformDFTMinuteFootprint(t *testing.T) {
	// One-second offset varying across minutes makes the grid nonuniform while
	// retaining a sharply phase-coherent once/minute component.
	s := make([]PITScalar, 120)
	for i := range s {
		s[i] = scalar(int64(i)*60000+int64(i%3)*100, 1)
	}
	r, err := NonuniformDFT(s, []float64{1. / 60, 1. / 59}, 0, 7200000, false, 240)
	if err != nil {
		t.Fatal(err)
	}
	if r[0].Amplitude < .999 || r[1].Amplitude > .03 {
		t.Fatal(r)
	}
}

func TestFrozenEmpiricalCDFHoldout(t *testing.T) {
	s := []PITScalar{scalar(1, 1), scalar(2, 2), scalar(3, 2), scalar(4, 4)}
	f, err := FitEmpiricalCDF(s, 5)
	if err != nil {
		t.Fatal(err)
	}
	s[0].Value = 100
	v, err := f.Score(scalar(6, 2), 6)
	if err != nil || v != .75 {
		t.Fatalf("inclusive tied CDF %v %v", v, err)
	}
	if _, err = f.Score(scalar(4, 2), 6); err == nil {
		t.Fatal("training overlap accepted")
	}
	if _, err = f.Score(PITScalar{6, 7, 8, 2}, 7); err == nil {
		t.Fatal("unreceived score accepted")
	}
	if _, err = FitEmpiricalCDF([]PITScalar{{1, 2, 8, 1}}, 5); err == nil {
		t.Fatal("future training accepted")
	}
}
