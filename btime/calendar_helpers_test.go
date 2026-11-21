package btime

import (
	"testing"
	"time"
)

func TestUTCCalendarPartsExhaustiveAndZeroAlloc(t *testing.T) {
	// Cross-check every calendar date from 1960-01-01 through 2110-12-31
	// (covering pre-1970 negative timestamps, 2000 leap century, 2020/2024 leap
	// years, 2021 common year, and 2100 non-leap century) against time.UTC.
	start := time.Date(1960, time.January, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2110, time.December, 31, 0, 0, 0, 0, time.UTC)
	for cur := start; !cur.After(end); cur = cur.AddDate(0, 0, 1) {
		sample := cur.Add(13*time.Hour + 47*time.Minute + 29*time.Second + 999*time.Millisecond)
		ms := sample.UnixMilli()
		y, m, d, wd, hr, min := UTCCalendarParts(ms)
		if y != sample.Year() || m != sample.Month() || d != sample.Day() || wd != sample.Weekday() || hr != 13 || min != 47 {
			t.Fatalf("UTCCalendarParts(%v) = (%d,%v,%d,%v,%d,%d), want (%d,%v,%d,%v,13,47)",
				sample, y, m, d, wd, hr, min,
				sample.Year(), sample.Month(), sample.Day(), sample.Weekday())
		}
		if got := DayOfWeekUTC(ms); got != int(sample.Weekday()) {
			t.Fatalf("DayOfWeekUTC(%v) = %d, want %d", sample, got, int(sample.Weekday()))
		}
		wantISO := int(sample.Weekday())
		if wantISO == 0 {
			wantISO = 7
		}
		if got := ISODayOfWeekUTC(ms); got != wantISO {
			t.Fatalf("ISODayOfWeekUTC(%v) = %d, want %d", sample, got, wantISO)
		}
	}

	ms := utcMS(2024, time.February, 29, 8, 15, 0, 0)
	allocs := testing.AllocsPerRun(100, func() {
		_, _, _, _, _, _ = UTCCalendarParts(ms)
		_ = IsTurnOfMonthUTC(ms, 2, 3)
		_ = IsMonthlyExpiryDayUTC(ms, time.Friday)
		_ = InMonthRangeUTC(ms, 11, 4)
		_ = IsFirstBarOfPeriodUTC(ms-HourMS, ms, "month")
	})
	if allocs != 0 {
		t.Fatalf("expected 0 heap allocations, got %v", allocs)
	}
}

func TestLeapYearsDaysInMonthAndTurnOfMonth(t *testing.T) {
	febCases := []struct {
		year     int
		wantLeap bool
		wantDays int
	}{
		{2000, true, 29},
		{2020, true, 29},
		{2021, false, 28},
		{2024, true, 29},
		{2100, false, 28},
	}
	for _, tc := range febCases {
		if got := IsLeapYearUTC(tc.year); got != tc.wantLeap {
			t.Fatalf("IsLeapYearUTC(%d) = %v, want %v", tc.year, got, tc.wantLeap)
		}
		if got := DaysInMonthUTC(tc.year, time.February); got != tc.wantDays {
			t.Fatalf("DaysInMonthUTC(%d, Feb) = %d, want %d", tc.year, got, tc.wantDays)
		}
		lastDayMS := utcMS(tc.year, time.February, tc.wantDays, 23, 59, 0, 0)
		if rem := DaysBeforeMonthEndUTC(lastDayMS); rem != 0 {
			t.Fatalf("%d-02-%d DaysBeforeMonthEndUTC = %d, want 0", tc.year, tc.wantDays, rem)
		}
		prevDayMS := utcMS(tc.year, time.February, tc.wantDays-1, 12, 0, 0, 0)
		if rem := DaysBeforeMonthEndUTC(prevDayMS); rem != 1 {
			t.Fatalf("%d-02-%d DaysBeforeMonthEndUTC = %d, want 1", tc.year, tc.wantDays-1, rem)
		}
	}

	if DaysInMonthUTC(2024, time.April) != 30 || DaysInMonthUTC(2024, time.March) != 31 {
		t.Fatal("unexpected April/March day counts")
	}
	if DaysInMonthUTC(2024, 0) != 0 || DaysInMonthUTC(2024, 13) != 0 {
		t.Fatal("invalid months must return 0 days")
	}

	// DaysFromMonthStartUTC & IsTurnOfMonthUTC (last 2 days of month + first 3 days of month)
	if got := DaysFromMonthStartUTC(utcMS(2024, time.March, 1, 0, 0, 0, 0)); got != 0 {
		t.Fatalf("Mar 1 DaysFromMonthStartUTC = %d, want 0", got)
	}
	if got := DaysFromMonthStartUTC(utcMS(2024, time.March, 3, 18, 0, 0, 0)); got != 2 {
		t.Fatalf("Mar 3 DaysFromMonthStartUTC = %d, want 2", got)
	}

	// Leap Feb 2024 (29 days): last 2 days are Feb 28 & Feb 29; Feb 27 is outside.
	if IsTurnOfMonthUTC(utcMS(2024, time.February, 27, 12, 0, 0, 0), 2, 3) {
		t.Fatal("2024-02-27 must NOT be in TOM(last=2, first=3)")
	}
	if !IsTurnOfMonthUTC(utcMS(2024, time.February, 28, 0, 0, 0, 0), 2, 3) {
		t.Fatal("2024-02-28 MUST be in TOM(last=2, first=3)")
	}
	if !IsTurnOfMonthUTC(utcMS(2024, time.February, 29, 23, 59, 0, 0), 2, 3) {
		t.Fatal("2024-02-29 MUST be in TOM(last=2, first=3)")
	}
	// Non-leap Feb 2100 (28 days): last 2 days are Feb 27 & Feb 28; Feb 26 is outside.
	if IsTurnOfMonthUTC(utcMS(2100, time.February, 26, 12, 0, 0, 0), 2, 3) {
		t.Fatal("2100-02-26 must NOT be in TOM(last=2, first=3)")
	}
	if !IsTurnOfMonthUTC(utcMS(2100, time.February, 27, 12, 0, 0, 0), 2, 3) {
		t.Fatal("2100-02-27 MUST be in TOM(last=2, first=3)")
	}
	if !IsTurnOfMonthUTC(utcMS(2100, time.February, 28, 12, 0, 0, 0), 2, 3) {
		t.Fatal("2100-02-28 MUST be in TOM(last=2, first=3)")
	}
	// First 3 days of March: Mar 1, 2, 3 inside; Mar 4 outside.
	for d := 1; d <= 3; d++ {
		if !IsTurnOfMonthUTC(utcMS(2024, time.March, d, 6, 0, 0, 0), 2, 3) {
			t.Fatalf("2024-03-%02d MUST be in TOM(last=2, first=3)", d)
		}
	}
	if IsTurnOfMonthUTC(utcMS(2024, time.March, 4, 0, 0, 0, 0), 2, 3) {
		t.Fatal("2024-03-04 must NOT be in TOM(last=2, first=3)")
	}
	// Disabled halves
	if IsTurnOfMonthUTC(utcMS(2024, time.March, 1, 0, 0, 0, 0), 0, 0) {
		t.Fatal("TOM(0, 0) must return false")
	}
	if IsTurnOfMonthUTC(utcMS(2024, time.March, 31, 0, 0, 0, 0), 0, 3) {
		t.Fatal("TOM(0, 3) must not match month end")
	}
	if IsTurnOfMonthUTC(utcMS(2024, time.March, 1, 0, 0, 0, 0), 2, 0) {
		t.Fatal("TOM(2, 0) must not match month start")
	}
}

func TestNthAndLastWeekdayMonthlyExpiryAndWeekOfMonth(t *testing.T) {
	// Verified 2024 calendar dates for 3rd Friday (CME/US OPEX) and Last Friday (Deribit/Binance expiry):
	want2024Fridays := []struct {
		month      time.Month
		thirdFri   int
		lastFri    int
		fifthFri   int // 0 if month has only 4 Fridays
	}{
		{time.January, 19, 26, 0},
		{time.February, 16, 23, 0}, // 2024-02-29 is Thursday (5th Thursday!)
		{time.March, 15, 29, 29},
		{time.April, 19, 26, 0},
		{time.May, 17, 31, 31},
		{time.June, 21, 28, 0},
		{time.July, 19, 26, 0},
		{time.August, 16, 30, 30},
		{time.September, 20, 27, 0},
		{time.October, 18, 25, 0},
		{time.November, 15, 29, 29},
		{time.December, 20, 27, 0},
	}

	for _, tc := range want2024Fridays {
		if got := NthWeekdayOfMonthUTC(2024, tc.month, time.Friday, 3); got != tc.thirdFri {
			t.Fatalf("2024-%02d 3rd Friday = %d, want %d", tc.month, got, tc.thirdFri)
		}
		if got := LastWeekdayOfMonthUTC(2024, tc.month, time.Friday); got != tc.lastFri {
			t.Fatalf("2024-%02d Last Friday = %d, want %d", tc.month, got, tc.lastFri)
		}
		if got := NthWeekdayOfMonthUTC(2024, tc.month, time.Friday, 5); got != tc.fifthFri {
			t.Fatalf("2024-%02d 5th Friday = %d, want %d", tc.month, got, tc.fifthFri)
		}

		// Verify IsMonthlyExpiryDayUTC & IsNthWeekdayOfMonthUTC across every day of the month
		dim := DaysInMonthUTC(2024, tc.month)
		expiryHits := 0
		opexHits := 0
		for d := 1; d <= dim; d++ {
			ms := utcMS(2024, tc.month, d, 8, 0, 0, 0)
			if IsMonthlyExpiryDayUTC(ms, time.Friday) {
				expiryHits++
				if d != tc.lastFri {
					t.Fatalf("2024-%02d-%02d unexpectedly matched IsMonthlyExpiryDayUTC", tc.month, d)
				}
			}
			if IsNthWeekdayOfMonthUTC(ms, time.Friday, 3) {
				opexHits++
				if d != tc.thirdFri {
					t.Fatalf("2024-%02d-%02d unexpectedly matched IsNthWeekdayOfMonthUTC(Fri, 3)", tc.month, d)
				}
			}
			wom := WeekOfMonthUTC(ms)
			if wom < 1 || wom > 5 || wom != (d-1)/7+1 {
				t.Fatalf("2024-%02d-%02d WeekOfMonthUTC = %d, want %d", tc.month, d, wom, (d-1)/7+1)
			}
		}
		if expiryHits != 1 || opexHits != 1 {
			t.Fatalf("2024-%02d expiryHits=%d opexHits=%d, want 1 and 1", tc.month, expiryHits, opexHits)
		}
	}

	// Leap day 2024-02-29 is the 5th Thursday of Feb 2024, whereas Feb 2021 has no 5th Thursday
	if got := NthWeekdayOfMonthUTC(2024, time.February, time.Thursday, 5); got != 29 {
		t.Fatalf("2024-02 5th Thursday = %d, want 29", got)
	}
	if got := NthWeekdayOfMonthUTC(2021, time.February, time.Thursday, 5); got != 0 {
		t.Fatalf("2021-02 5th Thursday = %d, want 0", got)
	}
	if NthWeekdayOfMonthUTC(2024, time.March, time.Friday, 0) != 0 ||
		NthWeekdayOfMonthUTC(2024, time.March, time.Friday, 6) != 0 ||
		NthWeekdayOfMonthUTC(2024, 13, time.Friday, 1) != 0 ||
		LastWeekdayOfMonthUTC(2024, 0, time.Friday) != 0 ||
		IsMonthlyExpiryDayUTC(utcMS(2024, time.March, 29, 8, 0, 0, 0), time.Weekday(9)) {
		t.Fatal("invalid weekday/month/n arguments must safely return 0 or false")
	}
}

func TestInMonthRangeUTCStraightAndCrossYear(t *testing.T) {
	// Halloween Effect / "Sell in May and Go Away": InMonthRangeUTC(ms, 11, 4)
	// Months 11, 12, 1, 2, 3, 4 -> true; Months 5, 6, 7, 8, 9, 10 -> false.
	wantHalloween := map[time.Month]bool{
		time.January:   true,
		time.February:  true,
		time.March:     true,
		time.April:     true,
		time.May:       false,
		time.June:      false,
		time.July:      false,
		time.August:    false,
		time.September: false,
		time.October:   false,
		time.November:  true,
		time.December:  true,
	}
	for m := time.January; m <= time.December; m++ {
		ms := utcMS(2025, m, 15, 12, 0, 0, 0)
		if got := InMonthRangeUTC(ms, 11, 4); got != wantHalloween[m] {
			t.Fatalf("InMonthRangeUTC(2025-%02d, 11, 4) = %v, want %v", m, got, wantHalloween[m])
		}
		wantSummer := m >= time.May && m <= time.October
		if got := InMonthRangeUTC(ms, 5, 10); got != wantSummer {
			t.Fatalf("InMonthRangeUTC(2025-%02d, 5, 10) = %v, want %v", m, got, wantSummer)
		}
	}
	// Single-month range & out-of-bounds guards
	if !InMonthRangeUTC(utcMS(2025, time.January, 31, 23, 0, 0, 0), 1, 1) {
		t.Fatal("Jan 31 should match [1, 1]")
	}
	if InMonthRangeUTC(utcMS(2025, time.January, 15, 0, 0, 0, 0), 0, 4) ||
		InMonthRangeUTC(utcMS(2025, time.January, 15, 0, 0, 0, 0), 11, 13) {
		t.Fatal("invalid month bounds must return false")
	}
}

func TestIsFirstBarOfPeriodUTCMultiTimeframeAndSafety(t *testing.T) {
	// 1) First-bar / no-previous-value / duplicate / out-of-order / invalid period safety
	midMonth := utcMS(2024, time.May, 15, 14, 30, 0, 0)
	firstOfMonth := utcMS(2024, time.May, 1, 0, 0, 0, 0)
	for _, p := range []string{"day", "week", "month", "quarter", "year"} {
		if IsFirstBarOfPeriodUTC(0, midMonth, p) {
			t.Fatalf("prevBarMS=0 (no prior bar) must return false for %s", p)
		}
		if IsFirstBarOfPeriodUTC(0, firstOfMonth, p) {
			t.Fatalf("prevBarMS=0 on period boundary must still return false for %s", p)
		}
		if IsFirstBarOfPeriodUTC(-100, midMonth, p) {
			t.Fatalf("negative prevBarMS must return false for %s", p)
		}
		if IsFirstBarOfPeriodUTC(midMonth, midMonth, p) {
			t.Fatalf("duplicate timestamp must return false for %s", p)
		}
		if IsFirstBarOfPeriodUTC(midMonth, midMonth-HourMS, p) {
			t.Fatalf("out-of-order timestamp must return false for %s", p)
		}
	}
	if IsFirstBarOfPeriodUTC(firstOfMonth-HourMS, firstOfMonth, "unknown") ||
		IsFirstBarOfPeriodUTC(firstOfMonth-HourMS, firstOfMonth, "1m") {
		t.Fatal("unsupported or minute period must return false")
	}

	// 2) Full-year 2024 simulation preceded by warmup in late Dec 2023 across
	// 15m, 4h, 1d, and 3d timeframes (plus non-midnight shifted 4h/3d grids!).
	//   - [2024-01-01, 2025-01-01): exact 366 UTC days, 12 UTC months, 4 UTC quarters, 1 UTC year
	//   - [2024-01-01 Mon, 2025-01-06 Mon): exact 53 full UTC Monday-to-Sunday weeks (371 days)
	warmupStart := utcMS(2023, time.December, 20, 0, 0, 0, 0)
	winStart := utcMS(2024, time.January, 1, 0, 0, 0, 0)
	yearEnd := utcMS(2025, time.January, 1, 0, 0, 0, 0)
	week53End := utcMS(2025, time.January, 6, 0, 0, 0, 0)

	type tfSpec struct {
		name     string
		stepMS   int64
		offsetMS int64 // non-zero offset tests bars that NEVER land on 00:00 UTC or day==1!
		checkDay bool
	}
	specs := []tfSpec{
		{name: "15m", stepMS: 15 * MinuteMS, offsetMS: 0, checkDay: true},
		{name: "4h", stepMS: 4 * HourMS, offsetMS: 0, checkDay: true},
		{name: "4h_shifted_1h", stepMS: 4 * HourMS, offsetMS: 1 * HourMS, checkDay: true},
		{name: "1d", stepMS: DayMS, offsetMS: 0, checkDay: true},
		{name: "3d", stepMS: 3 * DayMS, offsetMS: 0, checkDay: false}, // steps over many day==1 & Monday boundaries!
		{name: "3d_shifted_8h", stepMS: 3 * DayMS, offsetMS: 8 * HourMS, checkDay: false},
	}

	for _, sp := range specs {
		var prev int64
		var dayHits, weekHits, monthHits, quarterHits, yearHits int
		seenMonthIdx := make(map[int64]int)
		seenWeekIdx := make(map[int64]int)
		firstIter := true
		for ts := warmupStart + sp.offsetMS; ts < week53End; ts += sp.stepMS {
			dTrig := IsFirstBarOfPeriodUTC(prev, ts, "day")
			wTrig := IsFirstBarOfPeriodUTC(prev, ts, "week")
			mTrig := IsFirstBarOfPeriodUTC(prev, ts, "month")
			qTrig := IsFirstBarOfPeriodUTC(prev, ts, "quarter")
			yTrig := IsFirstBarOfPeriodUTC(prev, ts, "year")
			if firstIter {
				if dTrig || wTrig || mTrig || qTrig || yTrig {
					t.Fatalf("[%s] very first bar (prev=0) must never trigger", sp.name)
				}
				firstIter = false
			}
			prev = ts
			if ts < winStart {
				continue
			}
			wIdx, _ := periodIndexUTC(ts, "week")
			if wTrig {
				weekHits++
				seenWeekIdx[wIdx]++
				if seenWeekIdx[wIdx] > 1 {
					t.Fatalf("[%s] duplicate week trigger in weekIdx=%d", sp.name, wIdx)
				}
			}
			if ts < yearEnd {
				if dTrig {
					dayHits++
				}
				mIdx, _ := periodIndexUTC(ts, "month")
				if mTrig {
					monthHits++
					seenMonthIdx[mIdx]++
					if seenMonthIdx[mIdx] > 1 {
						t.Fatalf("[%s] duplicate month trigger in monthIdx=%d", sp.name, mIdx)
					}
				}
				if qTrig {
					quarterHits++
				}
				if yTrig {
					yearHits++
				}
			}
		}

		if sp.checkDay && dayHits != 366 {
			t.Fatalf("[%s] 2024 day triggers = %d, want 366", sp.name, dayHits)
		}
		if weekHits != 53 {
			t.Fatalf("[%s] 53-week window triggers = %d, want 53", sp.name, weekHits)
		}
		if monthHits != 12 {
			t.Fatalf("[%s] 2024 month triggers = %d, want 12", sp.name, monthHits)
		}
		if quarterHits != 4 {
			t.Fatalf("[%s] 2024 quarter triggers = %d, want 4", sp.name, quarterHits)
		}
		if yearHits != 1 {
			t.Fatalf("[%s] 2024 year triggers = %d, want 1", sp.name, yearHits)
		}
	}

	// Verify exact Monday 00:00 UTC boundary for "week" (Sunday 23:59 -> Monday 00:00 triggers; Saturday -> Sunday does not)
	sat := utcMS(2024, time.May, 11, 12, 0, 0, 0)
	sun := utcMS(2024, time.May, 12, 23, 59, 0, 0)
	mon := utcMS(2024, time.May, 13, 0, 0, 0, 0)
	if IsFirstBarOfPeriodUTC(sat, sun, "week") {
		t.Fatal("Saturday -> Sunday must NOT trigger UTC Monday week rollover")
	}
	if !IsFirstBarOfPeriodUTC(sun, mon, "week") {
		t.Fatal("Sunday 23:59 -> Monday 00:00 MUST trigger UTC Monday week rollover")
	}
}
