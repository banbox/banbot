package utils

import (
	"encoding/json"
	"math"
	"testing"
)

func TestUlcerPopulationAndAnnualExcessReturn(t *testing.T) {
	ui, upi, err := CalcUlcerPerformance([]float64{100, 90, 95, 105}, 1, .01)
	wantUI := math.Sqrt(125.0 / 4)
	if err != nil || ui == nil || upi == nil || math.Abs(*ui-wantUI) > 1e-10 || math.Abs(*upi-4/wantUI) > 1e-10 {
		t.Fatalf("UI=%v UPI=%v error=%v", ui, upi, err)
	}
	_, negative, err := CalcUlcerPerformance([]float64{100, 90}, 2, 0)
	want := (math.Sqrt(.9) - 1) * 100 / math.Sqrt(50)
	if err != nil || negative == nil || math.Abs(*negative-want) > 1e-10 {
		t.Fatalf("negative UPI=%v error=%v", negative, err)
	}
}

func TestUlcerUndefinedAndInvalid(t *testing.T) {
	for _, nav := range [][]float64{{100}, {100, 100}, {100, 101}} {
		ui, upi, err := CalcUlcerPerformance(nav, 1, 0)
		if err != nil || ui == nil || *ui != 0 || upi != nil {
			t.Fatalf("NAV=%v UI=%v UPI=%v error=%v", nav, ui, upi, err)
		}
		if _, err := json.Marshal([]*float64{ui, upi}); err != nil {
			t.Fatal(err)
		}
	}
	for _, nav := range [][]float64{nil, {0, 1}, {100, -1}, {100, math.NaN()}, {math.Inf(1), 100}} {
		ui, upi, err := CalcUlcerPerformance(nav, 1, 0)
		if err == nil || ui != nil || upi != nil {
			t.Fatalf("invalid NAV=%v accepted", nav)
		}
	}
	ui, upi, err := CalcUlcerPerformance([]float64{100, 90}, 0, 0)
	if err == nil || ui == nil || upi != nil {
		t.Fatal("unknown annualization should retain UI and omit UPI")
	}
}

func TestUlcerSequenceAndSamplingMatter(t *testing.T) {
	a, _, _ := CalcUlcerPerformance([]float64{100, 90, 110, 105}, 1, 0)
	b, _, _ := CalcUlcerPerformance([]float64{100, 110, 90, 105}, 1, 0)
	if *a == *b {
		t.Fatal("UI ignored order")
	}
	compressed, _, _ := CalcUlcerPerformance([]float64{100, 110}, 1, 0)
	if *compressed != 0 || *a <= 0 {
		t.Fatal("test must expose decimation losing drawdowns")
	}
}

func TestUlcerAccumulatorReplaceLast(t *testing.T) {
	var a UlcerAccumulator
	a.Add(100)
	a.Add(120)
	a.ReplaceLast(90)
	a.Add(math.NaN())
	a.ReplaceLast(95)
	a.Add(105)
	ui, upi, err := a.Performance(1, 0)
	wui, wupi, _ := CalcUlcerPerformance([]float64{100, 90, 95, 105}, 1, 0)
	if err != nil || a.Count() != 4 || math.Abs(*ui-*wui) > 1e-10 || math.Abs(*upi-*wupi) > 1e-10 {
		t.Fatalf("replace changed metrics %v %v %v", ui, upi, err)
	}
}

func TestUlcerReturnsUseUnobservedBaseline(t *testing.T) {
	ui, upi, err := CalcUlcerPerformanceByReturns([]float64{-.1, 95.0/90 - 1}, 1, 0)
	want := math.Sqrt(125.0 / 2)
	if err != nil || ui == nil || upi == nil || math.Abs(*ui-want) > 1e-10 || math.Abs(*upi-(-5/want)) > 1e-10 {
		t.Fatalf("returns UI=%v UPI=%v err=%v", ui, upi, err)
	}
	var a UlcerAccumulator
	a.Add(100)
	a.ReplaceLast(90)
	a.Add(99)
	_, ratio, _ := a.Performance(1, 0)
	if ratio != nil {
		t.Fatal("replaced first price should become baseline and no drawdown")
	}
}
