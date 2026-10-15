package statistics

import (
	"math"
	"reflect"
	"testing"
)

func TestCalendarBarrierMaturityMissingTargetAndPathIsolation(t *testing.T) {
	times := []int64{100, 120, 210, 300}
	close := []float64{100, 101, 99, math.NaN()}
	r := CalendarBarrierRequest{Start: 0, HorizonMS: 50, Target: .05, TargetAvailableMS: 100, TargetReceivedMS: 100, SideAvailableMS: 99, SideReceivedMS: 99, ProfitFactor: 1, StopFactor: 1, Side: 1}
	out, err := CalendarBarrierLabels(times, close, []CalendarBarrierRequest{r}, .01, 3)
	if err != nil || out[0].Vertical != 2 || !out[0].Resolved || out[0].EndMS != 210 || out[0].Barrier != "vertical" || out[0].Label != -1 {
		t.Fatal(out, err)
	}
	// Malformed future close lies beyond the maturity endpoint and is ignored.
	if _, err := CalendarBarrierLabels(times, close, []CalendarBarrierRequest{r}, .01, 2); err == nil {
		t.Fatal("budget ignored")
	}
	r.TargetAvailableMS = 101
	if _, err := CalendarBarrierLabels(times, close, []CalendarBarrierRequest{r}, .01, 4); err == nil {
		t.Fatal("future volatility leaked")
	}
	r.TargetAvailableMS = 100
	r.TargetReceivedMS = 101
	if _, err := CalendarBarrierLabels(times, close, []CalendarBarrierRequest{r}, .01, 4); err == nil {
		t.Fatal("delayed local target receipt leaked")
	}
	r.TargetReceivedMS = 100
	r.SideReceivedMS = 101
	if _, err := CalendarBarrierLabels(times, close, []CalendarBarrierRequest{r}, .01, 4); err == nil {
		t.Fatal("delayed local side receipt leaked")
	}
	r.SideReceivedMS = 99
	r.Target = .01
	r.Side = 0
	r.SideAvailableMS = 0
	out, err = CalendarBarrierLabels(times, close, []CalendarBarrierRequest{r}, .01, 0)
	if err != nil || out[0].Included {
		t.Fatal("minRet equality must drop", out, err)
	}
	r.Target = math.NaN()
	if out, err := CalendarBarrierLabels(times, close, []CalendarBarrierRequest{r}, .01, 0); err != nil || out[0].Included {
		t.Fatal(out, err)
	}
	for _, target := range []float64{math.Inf(-1), math.Inf(1)} {
		r.Target = target
		if _, err := CalendarBarrierLabels(times, close, []CalendarBarrierRequest{r}, .01, 0); err == nil {
			t.Fatal("infinite target silently excluded", target)
		}
	}
	r.Target = .02
	r.Side = -1
	r.SideAvailableMS = 100
	r.SideReceivedMS = 100
	r.Meta = true
	r.HorizonMS = 1000
	r.ProfitFactor = 1
	r.StopFactor = 2
	out, err = CalendarBarrierLabels(times, []float64{100, 95, 96, 97}, []CalendarBarrierRequest{r}, .01, 4)
	if err != nil || out[0].Vertical != -1 || out[0].EndMS != 120 || out[0].Label != 1 {
		t.Fatal(out, err)
	}
	r.ProfitFactor = 0
	r.StopFactor = 0
	out, err = CalendarBarrierLabels(times, []float64{100, 95, 96, 97}, []CalendarBarrierRequest{r}, .01, 4)
	if err != nil || out[0].Resolved || out[0].EndMS != 0 {
		t.Fatal("unobserved expiry invented", out, err)
	}
}

func TestExponentialUniquenessDecayDeclaredHalfLife(t *testing.T) {
	x := []float64{1, 1, 1}
	out, err := ExponentialUniquenessDecay(x, 1)
	if err != nil || !reflect.DeepEqual(out, []float64{.25, .5, 1}) || !reflect.DeepEqual(x, []float64{1, 1, 1}) {
		t.Fatal(out, err)
	}
	if _, err := ExponentialUniquenessDecay([]float64{0, 0}, 1); err == nil {
		t.Fatal("zero training mass accepted")
	}
}
