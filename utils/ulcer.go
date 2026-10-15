package utils

import (
	"fmt"
	"math"
)

const UlcerYearDays = 365.2425

// UlcerAccumulator measures full-assessment drawdowns without storing the NAV.
// Values are positive equity, including the initial capital observation.
type UlcerAccumulator struct {
	count                  int
	first, last, peak, sum float64
	priorPeak, priorSum    float64
	priorFirst             float64
	invalid                bool
	priorInvalid           bool
}

func (a *UlcerAccumulator) Add(value float64) {
	a.priorPeak, a.priorSum = a.peak, a.sum
	a.priorFirst = a.first
	a.priorInvalid = a.invalid
	if a.count == 0 && a.first == 0 {
		a.first = value
	}
	a.count++
	a.last = value
	if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		a.invalid = true
		return
	}
	a.peak = math.Max(a.peak, value)
	dd := 100 * (value/a.peak - 1)
	a.sum += dd * dd
}

// ReplaceLast avoids overweighting multiple wallet updates at one timestamp.
func (a *UlcerAccumulator) ReplaceLast(value float64) {
	if a.count == 0 {
		a.Add(value)
		return
	}
	a.count--
	a.peak, a.sum = a.priorPeak, a.priorSum
	a.first = a.priorFirst
	a.invalid = a.priorInvalid
	a.Add(value)
}

func (a *UlcerAccumulator) Count() int { return a.count }

// Performance returns UI in percentage points and UPI as a dimensionless ratio.
// UPI uses compound annual growth less an annual risk-free rate (a fraction).
// A zero UI has an undefined UPI, represented by nil for JSON-safe reporting.
func (a *UlcerAccumulator) Performance(elapsedYears, annualRiskFree float64) (*float64, *float64, error) {
	if a.count == 0 || a.invalid {
		return nil, nil, fmt.Errorf("ulcer metrics require finite positive equity")
	}
	ui := math.Sqrt(a.sum / float64(a.count))
	if elapsedYears <= 0 || math.IsNaN(elapsedYears) || math.IsInf(elapsedYears, 0) || math.IsNaN(annualRiskFree) || math.IsInf(annualRiskFree, 0) {
		return &ui, nil, fmt.Errorf("ulcer performance requires finite positive elapsed years and finite annual risk-free rate")
	}
	if ui == 0 {
		return &ui, nil, nil
	}
	cagr := math.Expm1((math.Log(a.last) - math.Log(a.first)) / elapsedYears)
	upi := (cagr - annualRiskFree) * 100 / ui
	if math.IsNaN(upi) || math.IsInf(upi, 0) {
		return &ui, nil, fmt.Errorf("ulcer annual return is not finite")
	}
	return &ui, &upi, nil
}

func CalcUlcerPerformance(nav []float64, elapsedYears, annualRiskFree float64) (*float64, *float64, error) {
	var a UlcerAccumulator
	for _, v := range nav {
		a.Add(v)
	}
	return a.Performance(elapsedYears, annualRiskFree)
}

// CalcUlcerPerformanceByReturns uses an unobserved initial NAV of one as the
// drawdown reference. Its UI denominator counts only the supplied returns.
func CalcUlcerPerformanceByReturns(returns []float64, elapsedYears, annualRiskFree float64) (*float64, *float64, error) {
	a := UlcerAccumulator{first: 1, last: 1, peak: 1}
	for _, r := range returns {
		a.Add(a.last * (1 + r))
	}
	return a.Performance(elapsedYears, annualRiskFree)
}
