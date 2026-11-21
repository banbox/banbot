package btime

// Time unit constants in milliseconds and minutes for UTC session calculations.
const (
	MinuteMS      int64 = 60_000
	HourMS        int64 = 3_600_000
	DayMS         int64 = 86_400_000
	MinutesPerDay int   = 1440
)

// Predefined UTC session anchor offsets expressed in minutes from 00:00 UTC.
const (
	UTC0Minutes           = 0            // 00:00 UTC standard crypto daily rollover
	AsiaOpenMinutes       = 0            // 00:00 UTC Asian session open (08:00 UTC+8 / 09:00 JST)
	AsiaCloseMinutes      = 8 * 60       // 08:00 UTC Asian session close
	LondonOpenMinutes     = 7 * 60       // 07:00 UTC European / ICT London Killzone open
	LondonCashOpenMinutes = 8 * 60       // 08:00 UTC London cash open
	LondonCloseMinutes    = 16 * 60      // 16:00 UTC London session close
	NYKillzoneOpenMinutes = 12 * 60      // 12:00 UTC ICT New York Killzone open
	NewYorkOpenMinutes    = 13*60 + 30   // 13:30 UTC US equity cash / NYSE open
	USCashCloseMinutes    = 20 * 60      // 20:00 UTC US equity cash close
	NewYorkCloseMinutes   = 21 * 60      // 21:00 UTC New York session close
	CMEOpenMinutes        = 23 * 60      // 23:00 UTC CME Globex daily open
	CMECloseMinutes       = 22 * 60      // 22:00 UTC CME Globex daily close
)

// UTCSessionWindow defines a recurring daily UTC window [StartMin, EndMin) in
// minutes since 00:00 UTC.
//   - StartMin < EndMin: intra-day window [StartMin, EndMin)
//   - StartMin > EndMin: cross-midnight window [StartMin, 1440) U [0, EndMin)
//   - EndMin - StartMin >= 1440: full 24-hour window
type UTCSessionWindow struct {
	Name     string
	StartMin int
	EndMin   int
}

// Contains reports whether UTC millisecond timestamp ms falls inside w.
func (w UTCSessionWindow) Contains(ms int64) bool {
	return InUTCWindow(ms, w.StartMin, w.EndMin)
}

// DurationMinutes returns the window length in minutes in [0, 1440].
func (w UTCSessionWindow) DurationMinutes() int {
	if w.EndMin-w.StartMin >= MinutesPerDay {
		return MinutesPerDay
	}
	s := NormalizeDayMinutes(w.StartMin)
	e := NormalizeDayMinutes(w.EndMin)
	if s == e {
		return 0
	}
	if s < e {
		return e - s
	}
	return (MinutesPerDay - s) + e
}

// Predefined crypto & macro UTC trading sessions and ICT Killzones.
var (
	SessionUTC0        = UTCSessionWindow{Name: "UTC0", StartMin: 0, EndMin: MinutesPerDay}
	SessionAsia        = UTCSessionWindow{Name: "Asia", StartMin: AsiaOpenMinutes, EndMin: AsiaCloseMinutes}             // 00:00-08:00 UTC
	SessionLondon      = UTCSessionWindow{Name: "London", StartMin: LondonOpenMinutes, EndMin: LondonCloseMinutes}       // 07:00-16:00 UTC
	SessionNewYork     = UTCSessionWindow{Name: "NewYork", StartMin: NewYorkOpenMinutes, EndMin: NewYorkCloseMinutes}    // 13:30-21:00 UTC
	SessionUSCash      = UTCSessionWindow{Name: "USCash", StartMin: NewYorkOpenMinutes, EndMin: USCashCloseMinutes}      // 13:30-20:00 UTC
	SessionCME         = UTCSessionWindow{Name: "CME", StartMin: CMEOpenMinutes, EndMin: CMECloseMinutes}                // 23:00-22:00 UTC (wraps midnight)
	KillzoneAsia       = UTCSessionWindow{Name: "KillzoneAsia", StartMin: 0, EndMin: 4 * 60}                             // 00:00-04:00 UTC
	KillzoneLondon     = UTCSessionWindow{Name: "KillzoneLondon", StartMin: 7 * 60, EndMin: 10 * 60}                     // 07:00-10:00 UTC
	KillzoneNewYork    = UTCSessionWindow{Name: "KillzoneNewYork", StartMin: 12 * 60, EndMin: 15 * 60}                   // 12:00-15:00 UTC
	KillzoneLondonExit = UTCSessionWindow{Name: "KillzoneLondonExit", StartMin: 15 * 60, EndMin: 17 * 60}                // 15:00-17:00 UTC
)

func euclidDivInt64(a, b int64) int64 {
	q := a / b
	r := a % b
	if r < 0 {
		q--
	}
	return q
}

func euclidModInt64(a, b int64) int64 {
	r := a % b
	if r < 0 {
		r += b
	}
	return r
}

// NormalizeDayMinutes maps any integer minute offset into [0, 1439] via
// Euclidean modulo 1440.
func NormalizeDayMinutes(minutes int) int {
	m := minutes % MinutesPerDay
	if m < 0 {
		m += MinutesPerDay
	}
	return m
}

// InUTCWindow reports whether UTC millisecond timestamp ms lies in the daily
// half-open window [startMin, endMin) (in minutes from 00:00 UTC).
//   - When endMin - startMin >= 1440 (e.g. [0, 1440)), returns true for all ms.
//   - When normalized startMin < endMin, checks [startMin, endMin).
//   - When normalized startMin > endMin (e.g. 22:00-02:00 UTC or 20:00-24:00 UTC),
//     wraps across 00:00 UTC as [startMin, 1440) U [0, endMin).
//   - When startMin == endMin (zero-width window), returns false.
func InUTCWindow(ms int64, startMin, endMin int) bool {
	if endMin-startMin >= MinutesPerDay {
		return true
	}
	if startMin == endMin {
		return false
	}
	s := NormalizeDayMinutes(startMin)
	e := NormalizeDayMinutes(endMin)
	if s == e {
		return false
	}
	msOfDay := euclidModInt64(ms, DayMS)
	startMS := int64(s) * MinuteMS
	endMS := int64(e) * MinuteMS
	if s < e {
		return msOfDay >= startMS && msOfDay < endMS
	}
	return msOfDay >= startMS || msOfDay < endMS
}

// SessionDayIndex returns the anchor-aligned trading day index using Euclidean
// floor division: floor((ms - anchorMS) / 86400000), where anchorMS is the
// normalized anchor offset in [0, 86400000).
func SessionDayIndex(ms int64, anchorMinutesUTC int) int64 {
	anchorMS := int64(NormalizeDayMinutes(anchorMinutesUTC)) * MinuteMS
	return euclidDivInt64(ms-anchorMS, DayMS)
}

// SessionStartMS returns the UTC millisecond timestamp of the start of the
// anchor-aligned session day containing ms.
func SessionStartMS(ms int64, anchorMinutesUTC int) int64 {
	anchorMS := int64(NormalizeDayMinutes(anchorMinutesUTC)) * MinuteMS
	return euclidDivInt64(ms-anchorMS, DayMS)*DayMS + anchorMS
}

// MinutesIntoSession returns the elapsed whole minutes in [0, 1439] from the
// current session day's anchor timestamp to ms.
func MinutesIntoSession(ms int64, anchorMinutesUTC int) int {
	elapsedMS := ms - SessionStartMS(ms, anchorMinutesUTC)
	return int(elapsedMS / MinuteMS)
}

// MinutesOfDayUTC returns the UTC minute-of-day in [0, 1439] for ms.
func MinutesOfDayUTC(ms int64) int {
	return MinutesIntoSession(ms, 0)
}

// IsNearSessionEnd reports whether the bar [barStartMS, barStopMS] reaches or
// crosses the daily session close timestamp closeMinutesUTC (in minutes from
// 00:00 UTC; negative or >=1440 values wrap modulo 1440, e.g. -15 = 23:45 UTC).
//
// For intraday bars (0 < barStopMS - barStartMS < 86400000), it checks whether
// (barStartMS, barStopMS] contains an occurrence of closeMinutesUTC, so 5m, 15m,
// 1h, or 4h bars that step across the target minute still trigger reliably once
// per session day without triggering again on the next day's opening bar.
// Daily or coarser bars (barStopMS - barStartMS >= 86400000) return false.
func IsNearSessionEnd(barStartMS, barStopMS int64, closeMinutesUTC int) bool {
	if barStopMS < barStartMS {
		return false
	}
	span := barStopMS - barStartMS
	if span >= DayMS {
		return false
	}
	if span == 0 {
		closeMS := int64(NormalizeDayMinutes(closeMinutesUTC)) * MinuteMS
		return euclidModInt64(barStopMS-closeMS, DayMS) == 0
	}
	return SessionDayIndex(barStartMS, closeMinutesUTC) < SessionDayIndex(barStopMS, closeMinutesUTC)
}
