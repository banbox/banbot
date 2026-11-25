package btime

import (
	"testing"
	"time"
)

func utcMS(year int, month time.Month, day, hour, min, sec, milli int) int64 {
	return time.Date(year, month, day, hour, min, sec, milli*1_000_000, time.UTC).UnixMilli()
}

func TestNormalizeDayMinutesAndSessions(t *testing.T) {
	cases := []struct {
		in   int
		want int
	}{
		{0, 0},
		{480, 480},
		{1439, 1439},
		{1440, 0},
		{1500, 60},
		{-15, 1425},
		{-1440, 0},
		{-1455, 1425},
	}
	for _, tc := range cases {
		if got := NormalizeDayMinutes(tc.in); got != tc.want {
			t.Fatalf("NormalizeDayMinutes(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}

	if SessionUTC0.DurationMinutes() != 1440 {
		t.Fatalf("SessionUTC0 duration = %d, want 1440", SessionUTC0.DurationMinutes())
	}
	if SessionAsia.DurationMinutes() != 480 {
		t.Fatalf("SessionAsia duration = %d, want 480", SessionAsia.DurationMinutes())
	}
	if SessionLondon.DurationMinutes() != 540 {
		t.Fatalf("SessionLondon duration = %d, want 540", SessionLondon.DurationMinutes())
	}
	if SessionNewYork.DurationMinutes() != 450 {
		t.Fatalf("SessionNewYork duration = %d, want 450", SessionNewYork.DurationMinutes())
	}
	if SessionCME.DurationMinutes() != 1380 {
		t.Fatalf("SessionCME (23:00-22:00) duration = %d, want 1380", SessionCME.DurationMinutes())
	}
}

func TestInUTCWindowIntraDayAndCrossMidnight(t *testing.T) {
	// Intra-day Asia window [00:00, 08:00)
	if !SessionAsia.Contains(utcMS(2025, 3, 10, 0, 0, 0, 0)) {
		t.Fatal("00:00:00.000 should be in SessionAsia")
	}
	if !SessionAsia.Contains(utcMS(2025, 3, 10, 7, 59, 59, 999)) {
		t.Fatal("07:59:59.999 should be in SessionAsia")
	}
	if SessionAsia.Contains(utcMS(2025, 3, 10, 8, 0, 0, 0)) {
		t.Fatal("08:00:00.000 should NOT be in SessionAsia (half-open end)")
	}

	// New York [13:30, 21:00)
	if SessionNewYork.Contains(utcMS(2025, 3, 10, 13, 29, 59, 999)) {
		t.Fatal("13:29:59.999 should NOT be in SessionNewYork")
	}
	if !SessionNewYork.Contains(utcMS(2025, 3, 10, 13, 30, 0, 0)) {
		t.Fatal("13:30:00.000 should be in SessionNewYork")
	}
	if !SessionNewYork.Contains(utcMS(2025, 3, 10, 20, 59, 0, 0)) {
		t.Fatal("20:59:00.000 should be in SessionNewYork")
	}
	if SessionNewYork.Contains(utcMS(2025, 3, 10, 21, 0, 0, 0)) {
		t.Fatal("21:00:00.000 should NOT be in SessionNewYork")
	}

	// Cross-midnight window [22:00, 02:00) -> startMin=1320, endMin=120
	if InUTCWindow(utcMS(2025, 3, 10, 21, 59, 59, 999), 22*60, 2*60) {
		t.Fatal("21:59:59.999 should NOT be in [22:00, 02:00)")
	}
	if !InUTCWindow(utcMS(2025, 3, 10, 22, 0, 0, 0), 22*60, 2*60) {
		t.Fatal("22:00:00.000 should be in [22:00, 02:00)")
	}
	if !InUTCWindow(utcMS(2025, 3, 11, 0, 0, 0, 0), 22*60, 2*60) {
		t.Fatal("00:00:00.000 should be in [22:00, 02:00)")
	}
	if !InUTCWindow(utcMS(2025, 3, 11, 1, 59, 59, 999), 22*60, 2*60) {
		t.Fatal("01:59:59.999 should be in [22:00, 02:00)")
	}
	if InUTCWindow(utcMS(2025, 3, 11, 2, 0, 0, 0), 22*60, 2*60) {
		t.Fatal("02:00:00.000 should NOT be in [22:00, 02:00)")
	}

	// CME [23:00, 22:00) -> only [22:00, 23:00) is maintenance break
	if SessionCME.Contains(utcMS(2025, 3, 10, 22, 30, 0, 0)) {
		t.Fatal("22:30 should be outside SessionCME (maintenance hour)")
	}
	if !SessionCME.Contains(utcMS(2025, 3, 10, 23, 0, 0, 0)) {
		t.Fatal("23:00 should be inside SessionCME")
	}
	if !SessionCME.Contains(utcMS(2025, 3, 11, 15, 0, 0, 0)) {
		t.Fatal("15:00 should be inside SessionCME")
	}

	// [20:00, 24:00) expressed as endMin=1440
	if !InUTCWindow(utcMS(2025, 3, 10, 23, 30, 0, 0), 20*60, 1440) {
		t.Fatal("23:30 should be inside [20:00, 24:00)")
	}
	if InUTCWindow(utcMS(2025, 3, 11, 0, 0, 0, 0), 20*60, 1440) {
		t.Fatal("00:00 should NOT be inside [20:00, 24:00)")
	}

	// Full day [0, 1440) vs zero-width [480, 480)
	if !SessionUTC0.Contains(utcMS(2025, 3, 10, 12, 34, 56, 0)) {
		t.Fatal("SessionUTC0 should contain all timestamps")
	}
	if InUTCWindow(utcMS(2025, 3, 10, 8, 0, 0, 0), 480, 480) {
		t.Fatal("zero-width window [480, 480) must return false")
	}

	// Pre-1970 negative timestamp
	preEpoch := utcMS(1969, 12, 31, 7, 30, 0, 0)
	if !SessionAsia.Contains(preEpoch) {
		t.Fatal("1969-12-31 07:30 UTC should be in SessionAsia")
	}
}

func TestSessionDayIndexAndMinutesIntoSession(t *testing.T) {
	// Epoch boundary with 08:00 UTC anchor:
	// 1970-01-01 07:59:59.999 UTC -> day -1, 1439 minutes into session
	// 1970-01-01 08:00:00.000 UTC -> day 0, 0 minutes into session
	beforeAnchor := utcMS(1970, 1, 1, 7, 59, 59, 999)
	atAnchor := utcMS(1970, 1, 1, 8, 0, 0, 0)
	if got := SessionDayIndex(beforeAnchor, 480); got != -1 {
		t.Fatalf("SessionDayIndex(1970-01-01 07:59:59.999, 480) = %d, want -1", got)
	}
	if got := MinutesIntoSession(beforeAnchor, 480); got != 1439 {
		t.Fatalf("MinutesIntoSession(beforeAnchor, 480) = %d, want 1439", got)
	}
	if got := SessionDayIndex(atAnchor, 480); got != 0 {
		t.Fatalf("SessionDayIndex(1970-01-01 08:00:00.000, 480) = %d, want 0", got)
	}
	if got := MinutesIntoSession(atAnchor, 480); got != 0 {
		t.Fatalf("MinutesIntoSession(atAnchor, 480) = %d, want 0", got)
	}

	// 13:30 UTC anchor across two calendar dates
	d1Pre := utcMS(2025, 6, 15, 13, 29, 0, 0)
	d1Open := utcMS(2025, 6, 15, 13, 30, 0, 0)
	d1Midnight := utcMS(2025, 6, 16, 0, 15, 0, 0)
	d1End := utcMS(2025, 6, 16, 13, 29, 59, 999)
	d2Open := utcMS(2025, 6, 16, 13, 30, 0, 0)

	idx1 := SessionDayIndex(d1Open, NewYorkOpenMinutes)
	if SessionDayIndex(d1Pre, NewYorkOpenMinutes) != idx1-1 {
		t.Fatal("13:29 must belong to previous NY session day")
	}
	if SessionDayIndex(d1Midnight, NewYorkOpenMinutes) != idx1 {
		t.Fatal("next calendar midnight 00:15 must still belong to same 13:30 NY session day")
	}
	if SessionDayIndex(d1End, NewYorkOpenMinutes) != idx1 {
		t.Fatal("next day 13:29:59.999 must still belong to same 13:30 NY session day")
	}
	if SessionDayIndex(d2Open, NewYorkOpenMinutes) != idx1+1 {
		t.Fatal("next day 13:30:00.000 must start NY session day idx1+1")
	}
	if got := MinutesIntoSession(d1Midnight, NewYorkOpenMinutes); got != (10*60 + 45) {
		t.Fatalf("MinutesIntoSession(00:15, 13:30) = %d, want %d", got, 10*60+45)
	}
}

func TestIsNearSessionEndMultiTimeframe(t *testing.T) {
	day := utcMS(2025, 5, 1, 0, 0, 0, 0)

	// 15m bars around 00:00 UTC rollover (closeMinutesUTC = 0 or 1440)
	for _, closeMin := range []int{0, 1440} {
		if IsNearSessionEnd(day+23*HourMS+30*MinuteMS, day+23*HourMS+45*MinuteMS, closeMin) {
			t.Fatalf("23:30-23:45 should NOT trigger midnight close (closeMin=%d)", closeMin)
		}
		if !IsNearSessionEnd(day+23*HourMS+45*MinuteMS, day+24*HourMS, closeMin) {
			t.Fatalf("23:45-24:00 MUST trigger midnight close (closeMin=%d)", closeMin)
		}
		if IsNearSessionEnd(day+24*HourMS, day+24*HourMS+15*MinuteMS, closeMin) {
			t.Fatalf("next day's 00:00-00:15 MUST NOT re-trigger midnight close (closeMin=%d)", closeMin)
		}
	}

	// 1h bar stepping across 13:30 UTC (810m) or 23:45 UTC (-15m): must NOT miss!
	if IsNearSessionEnd(day+12*HourMS, day+13*HourMS, NewYorkOpenMinutes) {
		t.Fatal("12:00-13:00 1h bar must not trigger 13:30")
	}
	if !IsNearSessionEnd(day+13*HourMS, day+14*HourMS, NewYorkOpenMinutes) {
		t.Fatal("13:00-14:00 1h bar MUST trigger 13:30 even though bar steps over :30")
	}
	if IsNearSessionEnd(day+14*HourMS, day+15*HourMS, NewYorkOpenMinutes) {
		t.Fatal("14:00-15:00 1h bar must not trigger 13:30")
	}

	// Negative buffer: -15 (15 minutes before 00:00 UTC = 23:45 UTC) on 1h bars
	if !IsNearSessionEnd(day+23*HourMS, day+24*HourMS, -15) {
		t.Fatal("23:00-24:00 1h bar MUST trigger -15 (23:45 UTC)")
	}

	// Exactly once per 24h across all 96 15m bars of a day for any closeMinutesUTC
	for _, closeMin := range []int{0, 480, 810, 1200, 1425} {
		hits := 0
		for i := 0; i < 96; i++ {
			bStart := day + int64(i)*15*MinuteMS
			bStop := bStart + 15*MinuteMS
			if IsNearSessionEnd(bStart, bStop, closeMin) {
				hits++
			}
		}
		if hits != 1 {
			t.Fatalf("expected exactly 1 hit across 96x15m bars for closeMin=%d, got %d", closeMin, hits)
		}
	}

	// 1d and 3d bars must return false (not intraday)
	if IsNearSessionEnd(day, day+DayMS, 0) {
		t.Fatal("1d bar must return false for IsNearSessionEnd")
	}
	if IsNearSessionEnd(day, day+3*DayMS, 1200) {
		t.Fatal("3d bar must return false for IsNearSessionEnd")
	}
}

func TestBarInUTCWindow(t *testing.T) {
	day := int64(20000) * DayMS // a Thursday-aligned UTC midnight multiple
	h := HourMS
	cases := []struct {
		start, stop int64
		s, e        int
		want        bool
	}{
		{day + 7*h, day + 8*h, 7 * 60, 9 * 60, true},             // 1h bar starts in window
		{day + 6*h, day + 7*h, 7 * 60, 9 * 60, false},            // ends exactly at window open
		{day + 6*h, day + 7*h + MinuteMS, 7 * 60, 9 * 60, true},  // reaches into window
		{day + 9*h, day + 10*h, 7 * 60, 9 * 60, false},           // after window
		{day + 0, day + 8*h, 7 * 60, 9 * 60, true},               // 8h bar covers window open
		{day + 16*h, day + 24*h, 7 * 60, 9 * 60, false},          // 8h bar without the window
		{day, day + DayMS, 7 * 60, 9 * 60, true},                 // 1d bar
		{day, day + 3*DayMS, 13*60 + 30, 14 * 60, true},          // 3d bar
		{day + 23*h, day + 24*h, 22 * 60, 2 * 60, true},          // cross-midnight window
		{day + 2*h, day + 3*h, 22 * 60, 2 * 60, false},           // just after cross-midnight window
		{day + 20*h, day + 24*h, 22 * 60, 2 * 60, true},          // 4h bar into cross-midnight window
		{day + 7*h, day + 8*h, 7 * 60, 7 * 60, false},            // zero width
		{day + 7*h, day + 7*h, 7 * 60, 9 * 60, true},             // degenerate bar -> InUTCWindow
	}
	for i, c := range cases {
		if got := BarInUTCWindow(c.start, c.stop, c.s, c.e); got != c.want {
			t.Errorf("case %d: got %v want %v", i, got, c.want)
		}
	}
}

func TestBarTouchesUTCWeekdays(t *testing.T) {
	// 1970-01-05 was a Monday
	mon := int64(4) * DayMS
	if !BarTouchesUTCWeekdays(mon, mon+HourMS, time.Monday) || BarTouchesUTCWeekdays(mon, mon+HourMS, time.Tuesday) {
		t.Fatal("intraday bar weekday")
	}
	// 3d bar Sat..Mon covers Monday
	sat := mon - 2*DayMS
	if !BarTouchesUTCWeekdays(sat, sat+3*DayMS, time.Monday) || BarTouchesUTCWeekdays(sat, sat+3*DayMS, time.Wednesday) {
		t.Fatal("3d bar weekday")
	}
	if BarTouchesUTCWeekdays(mon, mon+DayMS, time.Tuesday) {
		t.Fatal("bar ending at Tuesday 00:00 must not count Tuesday")
	}
	if BarTouchesUTCWeekdays(mon, mon+DayMS) {
		t.Fatal("no weekdays")
	}
	// pre-1970: 1969-12-27 (day -5) was a Saturday
	if !BarTouchesUTCWeekdays(-5*DayMS, -4*DayMS, time.Saturday) || BarTouchesUTCWeekdays(-5*DayMS, -4*DayMS, time.Sunday) {
		t.Fatal("negative day weekday")
	}
}
