package btime

import "time"

// civilFromDays converts a UTC day count since 1970-01-01 (may be negative) into
// proleptic Gregorian (year, month, day) using Howard Hinnant's algorithm.
func civilFromDays(z int64) (year int, month time.Month, day int) {
	z += 719468
	era := euclidDivInt64(z, 146097)
	doe := uint64(z - era*146097)                           // [0, 146096]
	yoe := (doe - doe/1460 + doe/36524 - doe/146096) / 365 // [0, 399]
	y := int64(yoe) + era*400
	doy := doe - (365*yoe + yoe/4 - yoe/100) // [0, 365]
	mp := (5*doy + 2) / 153                  // [0, 11]
	day = int(doy - (153*mp+2)/5 + 1)        // [1, 31]
	var m int
	if mp < 10 {
		m = int(mp) + 3
	} else {
		m = int(mp) - 9
	}
	if m <= 2 {
		y++
	}
	return int(y), time.Month(m), day
}

// daysFromCivil converts proleptic Gregorian (year, month, day) into days since
// 1970-01-01 (1970-01-01 == 0). Assumes month in [1, 12] and day in [1, 31].
func daysFromCivil(year int, month time.Month, day int) int64 {
	y := int64(year)
	m := int64(month)
	if m <= 2 {
		y--
	}
	era := euclidDivInt64(y, 400)
	yoe := uint64(y - era*400) // [0, 399]
	var mp uint64
	if m > 2 {
		mp = uint64(m - 3)
	} else {
		mp = uint64(m + 9)
	}
	doy := (153*mp+2)/5 + uint64(day) - 1      // [0, 365]
	doe := yoe*365 + yoe/4 - yoe/100 + doy     // [0, 146096]
	return era*146097 + int64(doe) - 719468
}

func weekdayFromDayIndex(dayIdx int64) time.Weekday {
	// 1970-01-01 (dayIdx == 0) was Thursday (time.Weekday(4)).
	return time.Weekday(euclidModInt64(dayIdx+4, 7))
}

// UTCCalendarParts decomposes a UTC millisecond timestamp ms into proleptic
// Gregorian calendar components without heap allocations:
//   - year: full year (e.g. 2024)
//   - month: time.January (1) .. time.December (12)
//   - day: 1 .. 31
//   - weekday: time.Sunday (0) .. time.Saturday (6)
//   - hour: 0 .. 23
//   - minute: 0 .. 59
func UTCCalendarParts(ms int64) (year int, month time.Month, day int, weekday time.Weekday, hour, minute int) {
	dayIdx := euclidDivInt64(ms, DayMS)
	msOfDay := euclidModInt64(ms, DayMS)
	year, month, day = civilFromDays(dayIdx)
	weekday = weekdayFromDayIndex(dayIdx)
	hour = int(msOfDay / HourMS)
	minute = int((msOfDay % HourMS) / MinuteMS)
	return
}

// IsLeapYearUTC reports whether year is a leap year in the proleptic Gregorian
// calendar (divisible by 4, except centuries not divisible by 400).
func IsLeapYearUTC(year int) bool {
	return year%4 == 0 && (year%100 != 0 || year%400 == 0)
}

// DaysInMonthUTC returns the exact number of days (28..31) in (year, month) in
// the proleptic Gregorian calendar. Returns 0 if month is outside [1, 12].
func DaysInMonthUTC(year int, month time.Month) int {
	switch month {
	case time.January, time.March, time.May, time.July, time.August, time.October, time.December:
		return 31
	case time.April, time.June, time.September, time.November:
		return 30
	case time.February:
		if IsLeapYearUTC(year) {
			return 29
		}
		return 28
	default:
		return 0
	}
}

// DaysFromMonthStartUTC returns the 0-based day offset from the start of the
// UTC month containing ms: 0 on the 1st, 1 on the 2nd, ..., day-1.
func DaysFromMonthStartUTC(ms int64) int {
	_, _, day := civilFromDays(euclidDivInt64(ms, DayMS))
	return day - 1
}

// DaysBeforeMonthEndUTC returns the 0-based number of remaining calendar days
// after the UTC date of ms until the end of its month: 0 on the last day of the
// month (e.g. Feb 28/29, Apr 30, Mar 31), 1 on the second-to-last day, etc.
func DaysBeforeMonthEndUTC(ms int64) int {
	year, month, day := civilFromDays(euclidDivInt64(ms, DayMS))
	return DaysInMonthUTC(year, month) - day
}

// IsTurnOfMonthUTC reports whether ms falls within the Turn-of-Month (TOM)
// calendar window defined as the last lastNDays calendar days of the UTC month
// OR the first firstMDays calendar days of the UTC month.
//   - Non-positive lastNDays disables the month-end half of the window.
//   - Non-positive firstMDays disables the month-start half of the window.
func IsTurnOfMonthUTC(ms int64, lastNDays, firstMDays int) bool {
	if lastNDays <= 0 && firstMDays <= 0 {
		return false
	}
	year, month, day := civilFromDays(euclidDivInt64(ms, DayMS))
	if firstMDays > 0 && (day-1) < firstMDays {
		return true
	}
	if lastNDays > 0 && (DaysInMonthUTC(year, month)-day) < lastNDays {
		return true
	}
	return false
}

// DayOfWeekUTC returns the standard Go UTC day-of-week in [0, 6] where
// 0 = Sunday, 1 = Monday, ..., 5 = Friday, 6 = Saturday.
func DayOfWeekUTC(ms int64) int {
	return int(weekdayFromDayIndex(euclidDivInt64(ms, DayMS)))
}

// ISODayOfWeekUTC returns the ISO-8601 UTC day-of-week in [1, 7] where
// 1 = Monday, 2 = Tuesday, ..., 5 = Friday, 6 = Saturday, 7 = Sunday.
func ISODayOfWeekUTC(ms int64) int {
	wd := DayOfWeekUTC(ms)
	if wd == 0 {
		return 7
	}
	return wd
}

// NthWeekdayOfMonthUTC returns the calendar day-of-month (1..31) of the n-th
// occurrence (n >= 1) of weekday in (year, month), such as n=3, weekday=time.Friday
// for the 3rd Friday US equity / CME monthly expiry. Returns 0 if month/weekday
// is invalid, n <= 0, or the month has fewer than n occurrences of weekday.
func NthWeekdayOfMonthUTC(year int, month time.Month, weekday time.Weekday, n int) int {
	dim := DaysInMonthUTC(year, month)
	if dim == 0 || weekday < time.Sunday || weekday > time.Saturday || n <= 0 {
		return 0
	}
	firstWD := weekdayFromDayIndex(daysFromCivil(year, month, 1))
	offset := (int(weekday) - int(firstWD) + 7) % 7
	day := 1 + offset + (n-1)*7
	if day > dim {
		return 0
	}
	return day
}

// LastWeekdayOfMonthUTC returns the calendar day-of-month (1..31) of the last
// occurrence of weekday in (year, month), such as weekday=time.Friday for
// Deribit/Binance monthly & quarterly options/futures settlement day.
// Returns 0 if month or weekday is outside its valid range.
func LastWeekdayOfMonthUTC(year int, month time.Month, weekday time.Weekday) int {
	dim := DaysInMonthUTC(year, month)
	if dim == 0 || weekday < time.Sunday || weekday > time.Saturday {
		return 0
	}
	lastWD := weekdayFromDayIndex(daysFromCivil(year, month, dim))
	back := (int(lastWD) - int(weekday) + 7) % 7
	return dim - back
}

// IsNthWeekdayOfMonthUTC reports whether the UTC date of ms is the n-th
// occurrence of weekday in its month (e.g. IsNthWeekdayOfMonthUTC(ms, time.Friday, 3)).
func IsNthWeekdayOfMonthUTC(ms int64, weekday time.Weekday, n int) bool {
	if weekday < time.Sunday || weekday > time.Saturday || n <= 0 {
		return false
	}
	dayIdx := euclidDivInt64(ms, DayMS)
	if weekdayFromDayIndex(dayIdx) != weekday {
		return false
	}
	_, _, day := civilFromDays(dayIdx)
	return (day-1)/7+1 == n
}

// IsMonthlyExpiryDayUTC reports whether the UTC date of ms is the last
// occurrence of weekday in its month (e.g. IsMonthlyExpiryDayUTC(ms, time.Friday)
// for crypto monthly/quarterly options and futures settlement day).
func IsMonthlyExpiryDayUTC(ms int64, weekday time.Weekday) bool {
	if weekday < time.Sunday || weekday > time.Saturday {
		return false
	}
	dayIdx := euclidDivInt64(ms, DayMS)
	if weekdayFromDayIndex(dayIdx) != weekday {
		return false
	}
	year, month, day := civilFromDays(dayIdx)
	return day+7 > DaysInMonthUTC(year, month)
}

// WeekOfMonthUTC returns the 1-based intra-month week bucket in [1, 5] for ms:
// days 1..7 -> 1, 8..14 -> 2, 15..21 -> 3, 22..28 -> 4, 29..31 -> 5.
// Equivalently, this equals the ordinal occurrence (1..5) of that day's weekday
// within the current UTC month.
func WeekOfMonthUTC(ms int64) int {
	_, _, day := civilFromDays(euclidDivInt64(ms, DayMS))
	return (day-1)/7 + 1
}

// InMonthRangeUTC reports whether the UTC month of ms falls inside the inclusive
// month window [startMonth, endMonth] (1 = January .. 12 = December).
//   - When startMonth <= endMonth, checks startMonth <= month <= endMonth.
//   - When startMonth > endMonth, wraps across year-end as
//     month >= startMonth || month <= endMonth (e.g. InMonthRangeUTC(ms, 11, 4)
//     for the November–April "Halloween / Sell in May" seasonal window).
//   - Returns false if either bound is outside [1, 12].
func InMonthRangeUTC(ms int64, startMonth, endMonth int) bool {
	if startMonth < 1 || startMonth > 12 || endMonth < 1 || endMonth > 12 {
		return false
	}
	_, month, _ := civilFromDays(euclidDivInt64(ms, DayMS))
	m := int(month)
	if startMonth <= endMonth {
		return m >= startMonth && m <= endMonth
	}
	return m >= startMonth || m <= endMonth
}

func periodIndexUTC(ms int64, period string) (int64, bool) {
	switch period {
	case "day", "Day", "DAY", "1d", "d":
		return euclidDivInt64(ms, DayMS), true
	case "week", "Week", "WEEK", "1w", "w":
		// 1969-12-29 (dayIdx == -3) was Monday 00:00 UTC, so (dayIdx + 3) / 7
		// partitions time into contiguous [Monday 00:00 UTC, next Monday 00:00 UTC) weeks.
		dayIdx := euclidDivInt64(ms, DayMS)
		return euclidDivInt64(dayIdx+3, 7), true
	case "month", "Month", "MONTH", "1M", "M":
		year, month, _ := civilFromDays(euclidDivInt64(ms, DayMS))
		return int64(year)*12 + int64(month-1), true
	case "quarter", "Quarter", "QUARTER", "1q", "q":
		year, month, _ := civilFromDays(euclidDivInt64(ms, DayMS))
		return int64(year)*4 + int64((month-1)/3), true
	case "year", "Year", "YEAR", "1y", "y":
		year, _, _ := civilFromDays(euclidDivInt64(ms, DayMS))
		return int64(year), true
	default:
		return 0, false
	}
}

// IsFirstBarOfPeriodUTC reports whether curBarMS has crossed into a newer UTC
// calendar period relative to prevBarMS, enabling timeframe-agnostic scheduled
// DCA / periodic rebalancing across 5m, 15m, 1h, 4h, 1d, or 3d bars even when
// bar timestamps do not land on day == 1 or 00:00 UTC.
//
// Supported period values:
//   - "day" (or "1d"): UTC 00:00 daily boundary
//   - "week" (or "1w"): ISO week boundary at Monday 00:00 UTC
//   - "month" (or "1M"): UTC 1st-of-month 00:00 boundary
//   - "quarter" (or "1q"): UTC calendar quarter boundary (Jan/Apr/Jul/Oct 1)
//   - "year" (or "1y"): UTC calendar year boundary (Jan 1)
//
// Safety rules:
//   - Returns false when prevBarMS <= 0 (first bar with no prior timestamp),
//     curBarMS <= 0, curBarMS <= prevBarMS (duplicate or out-of-order bar), or
//     period is unrecognized.
func IsFirstBarOfPeriodUTC(prevBarMS, curBarMS int64, period string) bool {
	if prevBarMS <= 0 || curBarMS <= 0 || curBarMS <= prevBarMS {
		return false
	}
	prevIdx, ok := periodIndexUTC(prevBarMS, period)
	if !ok {
		return false
	}
	curIdx, _ := periodIndexUTC(curBarMS, period)
	return curIdx > prevIdx
}
