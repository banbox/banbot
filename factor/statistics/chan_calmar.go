package statistics

import (
	"errors"
	"time"
)

// GrowthDrawdownSpec supplies a received CAGR and its declared measurement
// interval. The book fixes drawdown horizons but leaves numerator/boundary
// conventions unspecified; this API never silently selects a CAGR period.
type GrowthDrawdownSpec struct {
	CAGR                         PITScalar
	CAGRStartMS, EndMS, CutoffMS int64
}

type GrowthDrawdownReport struct {
	CAGR                                              float64
	CAGRStartMS, EndMS, HistoryStartMS, CalmarStartMS int64
	AvailableMS, ReceivedMS                           int64
	WholeMaximumDrawdown, ThreeYearMaximumDrawdown    float64
	MAR, Calmar                                       float64
	MARValid, CalmarValid                             bool
	MARUndefined, CalmarUndefined                     string
}

// CalmarMAR implements Machine Trading pp11–12: CAGR divided by absolute
// maximum drawdown, with a recent three-calendar-year denominator for Calmar
// and complete received history for MAR. UTC AddDate defines the declared
// anniversary convention (including Go's leap-day normalization). An exact
// received boundary valuation is required: no interpolated or carried NAV is
// manufactured. Short history/missing boundary and zero drawdown are explicit
// undefined results; malformed visible NAVs are errors. NAVs must already be
// adjusted for external flows and include all relevant valuation observations.
func CalmarMAR(equity []PITScalar, s GrowthDrawdownSpec) (GrowthDrawdownReport, error) {
	var out GrowthDrawdownReport
	if len(equity) < 2 || len(equity) > 1000000 || s.CutoffMS < 0 || s.EndMS < 0 || s.EndMS > s.CutoffMS || s.CAGRStartMS < 0 || s.CAGRStartMS >= s.EndMS || !validScalar(s.CAGR) || s.CAGR.EventMS != s.EndMS || !visibleScalar(s.CAGR, s.CutoffMS) || s.CAGR.Value < -1 {
		return out, errors.New("statistics: invalid CAGR/drawdown research bounds")
	}
	out.CAGR = s.CAGR.Value
	out.CAGRStartMS = s.CAGRStartMS
	out.EndMS = s.EndMS
	out.CalmarStartMS = time.UnixMilli(s.EndMS).UTC().AddDate(-3, 0, 0).UnixMilli()
	out.AvailableMS = s.CAGR.AvailableMS
	out.ReceivedMS = s.CAGR.ReceivedMS
	var visible []PITScalar
	for i, p := range equity {
		if p.EventMS < 0 || p.AvailableMS < p.EventMS || p.ReceivedMS < p.AvailableMS || (i > 0 && p.EventMS <= equity[i-1].EventMS) {
			return GrowthDrawdownReport{}, errors.New("statistics: invalid NAV chronology")
		}
		if p.EventMS > s.EndMS {
			continue
		}
		if !visibleScalar(p, s.CutoffMS) {
			return GrowthDrawdownReport{}, errors.New("statistics: incomplete received NAV history")
		}
		if !finite(p.Value) || p.Value < 0 {
			return GrowthDrawdownReport{}, errors.New("statistics: malformed visible NAV")
		}
		visible = append(visible, p)
		out.AvailableMS = max(out.AvailableMS, p.AvailableMS)
		out.ReceivedMS = max(out.ReceivedMS, p.ReceivedMS)
	}
	if len(visible) < 2 || visible[0].Value <= 0 || visible[len(visible)-1].EventMS != s.EndMS {
		return GrowthDrawdownReport{}, errors.New("statistics: complete received NAV endpoints required")
	}
	out.HistoryStartMS = visible[0].EventMS
	drawdown := func(rows []PITScalar) float64 {
		peak := rows[0].Value
		var dd float64
		for _, p := range rows {
			peak = max(peak, p.Value)
			if peak > 0 {
				dd = max(dd, 1-p.Value/peak)
			}
		}
		return dd
	}
	out.WholeMaximumDrawdown = drawdown(visible)
	if out.WholeMaximumDrawdown > 0 {
		out.MAR = out.CAGR / out.WholeMaximumDrawdown
		out.MARValid = true
	} else {
		out.MARUndefined = "zero whole-history drawdown"
	}
	boundary := -1
	for i, p := range visible {
		if p.EventMS == out.CalmarStartMS {
			boundary = i
			break
		}
	}
	if out.HistoryStartMS > out.CalmarStartMS {
		out.CalmarUndefined = "less than three calendar years of history"
	} else if boundary < 0 {
		out.CalmarUndefined = "missing exact three-year boundary valuation"
	} else if visible[boundary].Value <= 0 {
		out.CalmarUndefined = "nonpositive boundary NAV"
	} else {
		out.ThreeYearMaximumDrawdown = drawdown(visible[boundary:])
		if out.ThreeYearMaximumDrawdown > 0 {
			out.Calmar = out.CAGR / out.ThreeYearMaximumDrawdown
			out.CalmarValid = true
		} else {
			out.CalmarUndefined = "zero three-year drawdown"
		}
	}
	if !finite(out.MAR) || !finite(out.Calmar) {
		return GrowthDrawdownReport{}, errors.New("statistics: growth/drawdown ratio overflow")
	}
	return out, nil
}
