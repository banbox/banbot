package statistics

import (
	"math"
	"reflect"
	"testing"
	"time"
)

func calmarDate(year int) int64 { return time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli() }

func calmarFixture() ([]PITScalar, GrowthDrawdownSpec) {
	var rows []PITScalar
	for i, v := range []float64{200, 100, 125, 100, 110} {
		at := calmarDate(2020 + i)
		rows = append(rows, PITScalar{at, at, at, v})
	}
	end := calmarDate(2024)
	return rows, GrowthDrawdownSpec{CAGR: PITScalar{end, end, end, .1}, CAGRStartMS: calmarDate(2023), EndMS: end, CutoffMS: end}
}

func TestCalmarMARSourceDifferentHorizons(t *testing.T) {
	rows, s := calmarFixture()
	r, err := CalmarMAR(rows, s)
	if err != nil || !r.MARValid || !r.CalmarValid || r.MAR != .2 || math.Abs(r.Calmar-.5) > 1e-14 || r.WholeMaximumDrawdown != .5 || math.Abs(r.ThreeYearMaximumDrawdown-.2) > 1e-14 {
		t.Fatalf("source ratios: %+v %v", r, err)
	}
	// Numerator period is retained, not silently changed to match either DD window.
	if r.CAGRStartMS != calmarDate(2023) || r.CalmarStartMS != calmarDate(2021) {
		t.Fatal("period identity lost")
	}
	rows = append(rows, PITScalar{calmarDate(2025), calmarDate(2025), calmarDate(2025), math.NaN()})
	p, err := CalmarMAR(rows, s)
	if err != nil || !reflect.DeepEqual(r, p) {
		t.Fatal("future malformed payload changed cutoff")
	}
}

func TestCalmarMARUndefinedAndReceipt(t *testing.T) {
	rows, s := calmarFixture()
	r, err := CalmarMAR(rows[2:], s)
	if err != nil || r.CalmarValid || r.CalmarUndefined == "" || !r.MARValid {
		t.Fatalf("short history: %+v %v", r, err)
	}
	missing := append([]PITScalar{rows[0]}, rows[2:]...)
	r, err = CalmarMAR(missing, s)
	if err != nil || r.CalmarValid || r.CalmarUndefined != "missing exact three-year boundary valuation" {
		t.Fatalf("boundary: %+v %v", r, err)
	}
	for i := range rows {
		rows[i].Value = 100 + float64(i)
	}
	r, err = CalmarMAR(rows, s)
	if err != nil || r.MARValid || r.CalmarValid || r.MARUndefined == "" || r.CalmarUndefined == "" {
		t.Fatalf("zero DD: %+v %v", r, err)
	}
	s.CAGR.ReceivedMS++
	if _, err := CalmarMAR(rows, s); err == nil {
		t.Fatal("unreceived CAGR accepted")
	}
	rows, s = calmarFixture()
	rows[2].ReceivedMS = s.CutoffMS + 1
	if _, err := CalmarMAR(rows, s); err == nil {
		t.Fatal("unreceived historical peak silently omitted")
	}
	rows, s = calmarFixture()
	rows[2].Value = -1
	if _, err := CalmarMAR(rows, s); err == nil {
		t.Fatal("negative visible NAV accepted")
	}
}
