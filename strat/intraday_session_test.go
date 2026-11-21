package strat

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/orm"
	ta "github.com/banbox/banta"
)

func testUTCMS(year int, month time.Month, day, hour, min int) int64 {
	return time.Date(year, month, day, hour, min, 0, 0, time.UTC).UnixMilli()
}

func assertAlmostEqual(t *testing.T, name string, got, want float64) {
	t.Helper()
	if math.IsNaN(got) || math.Abs(got-want) > 1e-9 {
		t.Fatalf("%s = %.10f, want %.10f", name, got, want)
	}
}

func makeTestJob(t *testing.T, stgy *TradeStrat, symbol, tf string, sid int32) *StratJob {
	t.Helper()
	env, err := ta.NewBarEnv("binance", "linear", symbol, tf)
	if err != nil {
		t.Fatalf("NewBarEnv(%s, %s): %v", symbol, tf, err)
	}
	return &StratJob{
		Strat:     stgy,
		Env:       env,
		DataHub:   NewDataHub(),
		Symbol:    &orm.ExSymbol{ID: sid, Symbol: symbol, Market: "linear"},
		TimeFrame: tf,
		Account:   "default",
	}
}

func feedJobBar(job *StratJob, startMS, tfMS int64, o, h, l, c, v float64, warmup bool) {
	job.SetWarmUp(warmup)
	job.Env.OnBar2(startMS, startMS+tfMS, o, h, l, c, v, c*v, v*0.5, 10)
	job.InitBar(nil)
}

func TestOpeningRangeNoLookaheadMultiAnchorAndTimeframes(t *testing.T) {
	stgy := &TradeStrat{Name: "test:orb"}

	// 1) 5m bars with 13:30 UTC (810m) anchor, 30m ORB (6 bars: 13:30..14:00)
	t.Run("5m_NY_1330_30m", func(t *testing.T) {
		job := makeTestJob(t, stgy, "BTC/USDT:USDT", "5m", 1)
		tfMS := 5 * btime.MinuteMS
		anchor := btime.NewYorkOpenMinutes // 810
		base := testUTCMS(2025, 4, 10, 13, 20)

		// Pre-open bars 13:20-13:25, 13:25-13:30 (belong to previous NY session day, 0 ORB bars seen)
		for i := 0; i < 2; i++ {
			feedJobBar(job, base+int64(i)*tfMS, tfMS, 50000, 50999, 49001, 50100, 100, false)
			orb := job.OpeningRange(anchor, 30)
			if !orb.Applicable || orb.Ready {
				t.Fatalf("pre-open bar %d: Applicable=%v Ready=%v, want Applicable=true Ready=false", i, orb.Applicable, orb.Ready)
			}
		}

		// 6 ORB bars [13:30, 14:00): highs/lows deliberately peak mid-window
		orbBars := [][5]float64{
			{60000, 60200, 59900, 60100, 10}, // 13:30-13:35 (open=60000)
			{60100, 60850, 60050, 60700, 20}, // 13:35-13:40 (max high=60850)
			{60700, 60750, 59720, 59800, 15}, // 13:40-13:45 (min low=59720)
			{59800, 60300, 59750, 60200, 12}, // 13:45-13:50
			{60200, 60400, 60100, 60350, 18}, // 13:50-13:55
			{60350, 60500, 60200, 60420, 25}, // 13:55-14:00 (close=60420, total vol=100)
		}
		openStart := testUTCMS(2025, 4, 10, 13, 30)
		for i, b := range orbBars {
			feedJobBar(job, openStart+int64(i)*tfMS, tfMS, b[0], b[1], b[2], b[3], b[4], false)
			orb := job.OpeningRange(anchor, 30)
			if i < 5 {
				if orb.Ready || !orb.Forming {
					t.Fatalf("forming bar %d: Ready=%v Forming=%v, want Ready=false Forming=true", i, orb.Ready, orb.Forming)
				}
				if orb.High != 0 || orb.Low != 0 {
					t.Fatalf("forming bar %d must not expose unlocked High/Low: got %v/%v", i, orb.High, orb.Low)
				}
			} else {
				// 6th bar closes right at 14:00 -> Ready=true, JustCompleted=true!
				if !orb.Ready || orb.Forming || !orb.JustCompleted || orb.InPostRange {
					t.Fatalf("completion bar: %+v", orb)
				}
				// Idempotent second call on same bar preserves JustCompleted=true and Volume=100
				orbDup := job.OpeningRange(anchor, 30)
				if orbDup != orb {
					t.Fatalf("duplicate call on same bar mutated snapshot: %+v vs %+v", orbDup, orb)
				}
				assertAlmostEqual(t, "orb.Open", orb.Open, 60000)
				assertAlmostEqual(t, "orb.High", orb.High, 60850)
				assertAlmostEqual(t, "orb.Low", orb.Low, 59720)
				assertAlmostEqual(t, "orb.Close", orb.Close, 60420)
				assertAlmostEqual(t, "orb.Volume", orb.Volume, 100)
				if orb.BarCount != 6 {
					t.Fatalf("orb.BarCount = %d, want 6", orb.BarCount)
				}
			}
		}

		// Post-ORB bar 14:00-14:05 makes new extremes -> locked ORB must NOT change!
		feedJobBar(job, openStart+6*tfMS, tfMS, 60420, 62000, 58000, 61500, 999, false)
		post := job.OpeningRange(anchor, 30)
		if !post.Ready || post.JustCompleted || !post.InPostRange {
			t.Fatalf("post-range bar flags wrong: %+v", post)
		}
		assertAlmostEqual(t, "locked High", post.High, 60850)
		assertAlmostEqual(t, "locked Low", post.Low, 59720)
		assertAlmostEqual(t, "locked Volume", post.Volume, 100)
	})

	// 2) Cross-midnight 15m ORB anchored at 23:45 UTC (1425m) for 60m -> [23:45 Day 1, 00:45 Day 2)
	t.Run("15m_CrossMidnight_2345_60m", func(t *testing.T) {
		job := makeTestJob(t, stgy, "BTC/USDT:USDT", "15m", 1)
		tfMS := 15 * btime.MinuteMS
		anchor := 23*60 + 45
		start := testUTCMS(2025, 4, 10, 23, 45)
		bars := [][5]float64{
			{100, 105, 99, 104, 10},  // 23:45-24:00 (before UTC midnight)
			{104, 112, 103, 110, 20}, // 00:00-00:15 (after UTC midnight, SAME session!)
			{110, 111, 96, 98, 30},   // 00:15-00:30
			{98, 103, 97, 102, 40},   // 00:30-00:45 -> completes 60m ORB
		}
		for i, b := range bars {
			feedJobBar(job, start+int64(i)*tfMS, tfMS, b[0], b[1], b[2], b[3], b[4], false)
			orb := job.OpeningRange(anchor, 60)
			if i < 3 {
				if orb.Ready {
					t.Fatalf("cross-midnight bar %d prematurely Ready", i)
				}
			} else {
				if !orb.Ready || !orb.JustCompleted {
					t.Fatalf("cross-midnight bar 3 expected Ready & JustCompleted: %+v", orb)
				}
				assertAlmostEqual(t, "Open", orb.Open, 100)
				assertAlmostEqual(t, "High", orb.High, 112)
				assertAlmostEqual(t, "Low", orb.Low, 96)
				assertAlmostEqual(t, "Close", orb.Close, 102)
				assertAlmostEqual(t, "Volume", orb.Volume, 100)
			}
		}
		// Next day's 23:45-24:00 resets Ready to false!
		nextDayStart := start + btime.DayMS
		feedJobBar(job, nextDayStart, tfMS, 105, 108, 104, 107, 15, false)
		resetOrb := job.OpeningRange(anchor, 60)
		if resetOrb.Ready || !resetOrb.Forming {
			t.Fatalf("next session rollover failed to reset Ready: %+v", resetOrb)
		}
	})

	// 3) 1h bars with 00:00 UTC and 08:00 UTC anchors, 60m ORB (single 1h opening bar)
	t.Run("1h_0000_and_0800_60m", func(t *testing.T) {
		job := makeTestJob(t, stgy, "BTC/USDT:USDT", "1h", 1)
		tfMS := btime.HourMS
		dayStart := testUTCMS(2025, 4, 10, 0, 0)
		for h := 0; h < 12; h++ {
			hi := 1000.0 + float64(h*10)
			lo := 990.0 - float64(h*5)
			feedJobBar(job, dayStart+int64(h)*tfMS, tfMS, 995, hi, lo, 998, 50, false)
			orb0 := job.OpeningRange(btime.UTC0Minutes, 60)
			if !orb0.Ready {
				t.Fatalf("hour %d: 00:00 60m ORB on 1h bar should be Ready after first bar", h)
			}
			assertAlmostEqual(t, "orb0.High", orb0.High, 1000)
			assertAlmostEqual(t, "orb0.Low", orb0.Low, 990)

			orb8 := job.OpeningRange(btime.LondonCashOpenMinutes, 60) // 08:00 UTC
			if h < 8 {
				if orb8.Ready {
					t.Fatalf("hour %d: 08:00 ORB must not be Ready before 09:00 close", h)
				}
			} else {
				if !orb8.Ready {
					t.Fatalf("hour %d: 08:00 60m ORB must be Ready from h=8 (08:00-09:00) onward", h)
				}
				assertAlmostEqual(t, "orb8.High", orb8.High, 1080)
				assertAlmostEqual(t, "orb8.Low", orb8.Low, 950)
			}
		}
	})
}

func TestOpeningRangeIncompatibleTimeframeProtection(t *testing.T) {
	stgy := &TradeStrat{Name: "test:orb_guard"}

	// 1d timeframe -> always Applicable=false, Ready=false (even with AllowSingleBarFallback=true)
	job1d := makeTestJob(t, stgy, "BTC/USDT:USDT", "1d", 1)
	feedJobBar(job1d, testUTCMS(2025, 4, 10, 0, 0), btime.DayMS, 60000, 62000, 59000, 61000, 1000, false)
	if orb := job1d.OpeningRange(0, 60); orb.Applicable || orb.Ready {
		t.Fatalf("1d OpeningRange must return Applicable=false Ready=false, got %+v", orb)
	}
	if orb := job1d.OpeningRangeWithOpts(0, 60, OpeningRangeOptions{AllowSingleBarFallback: true}); orb.Applicable || orb.Ready {
		t.Fatalf("1d OpeningRangeWithOpts must still return Applicable=false Ready=false, got %+v", orb)
	}

	// 4h timeframe with 15m/30m/60m ORB -> default Applicable=false, Ready=false
	job4h := makeTestJob(t, stgy, "BTC/USDT:USDT", "4h", 1)
	feedJobBar(job4h, testUTCMS(2025, 4, 10, 0, 0), 4*btime.HourMS, 60000, 61500, 59500, 61000, 500, false)
	for _, mins := range []int{15, 30, 60} {
		orb := job4h.OpeningRange(0, mins)
		if orb.Applicable || orb.Ready {
			t.Fatalf("4h OpeningRange(%dm) must return Applicable=false Ready=false, got %+v", mins, orb)
		}
	}
	// Opt-in single-bar fallback on 4h widens EffectiveRangeMinutes to 240 with DegradedSingleBar=true
	orbFallback := job4h.OpeningRangeWithOpts(0, 30, OpeningRangeOptions{AllowSingleBarFallback: true})
	if !orbFallback.Applicable || !orbFallback.Ready || !orbFallback.DegradedSingleBar || orbFallback.EffectiveRangeMinutes != 240 {
		t.Fatalf("4h fallback unexpected snapshot: %+v", orbFallback)
	}

	// 1h timeframe with misaligned 13:30 UTC anchor -> Applicable=false, Ready=false
	job1h := makeTestJob(t, stgy, "BTC/USDT:USDT", "1h", 1)
	feedJobBar(job1h, testUTCMS(2025, 4, 10, 13, 0), btime.HourMS, 60000, 61000, 59000, 60500, 200, false)
	feedJobBar(job1h, testUTCMS(2025, 4, 10, 14, 0), btime.HourMS, 60500, 61200, 60100, 61000, 200, false)
	if orb := job1h.OpeningRange(btime.NewYorkOpenMinutes, 60); orb.Applicable || orb.Ready {
		t.Fatalf("1h with 13:30 anchor must return Applicable=false Ready=false, got %+v", orb)
	}
}

func TestSessionRangeAsianSweepAndCrossMidnight(t *testing.T) {
	stgy := &TradeStrat{Name: "test:asian_sweep"}
	job := makeTestJob(t, stgy, "ETH/USDT:USDT", "1h", 2)
	dayStart := testUTCMS(2025, 5, 1, 0, 0)

	// Hours 0..7 (00:00..08:00 UTC Asian Session): highs reach 2150, lows reach 1980
	for h := 0; h < 24; h++ {
		o, hi, lo, c := 2050.0, 2080.0, 2020.0, 2060.0
		if h == 3 {
			hi = 2150.0
		}
		if h == 6 {
			lo = 1980.0
		}
		if h >= 8 {
			// Post-Asia London/NY moves far outside [1980, 2150] — Asian range must stay locked!
			hi = 2300.0
			lo = 1850.0
		}
		feedJobBar(job, dayStart+int64(h)*btime.HourMS, btime.HourMS, o, hi, lo, c, 10, false)
		asia := job.SessionRange(btime.AsiaOpenMinutes, btime.AsiaCloseMinutes)
		if h < 7 {
			if !asia.InSession || asia.Completed {
				t.Fatalf("hour %d: expected InSession=true Completed=false, got %+v", h, asia)
			}
		} else if h == 7 {
			// 07:00-08:00 closes at 08:00 -> Completed=true, JustCompleted=true
			if asia.InSession || !asia.Completed || !asia.JustCompleted || asia.InPostSession {
				t.Fatalf("hour 7 (08:00 close): unexpected flags %+v", asia)
			}
			assertAlmostEqual(t, "asia.High", asia.High, 2150)
			assertAlmostEqual(t, "asia.Low", asia.Low, 1980)
		} else {
			if asia.InSession || !asia.Completed || asia.JustCompleted || !asia.InPostSession {
				t.Fatalf("hour %d: expected Completed=true InPostSession=true, got %+v", h, asia)
			}
			assertAlmostEqual(t, "locked asia.High", asia.High, 2150)
			assertAlmostEqual(t, "locked asia.Low", asia.Low, 1980)
		}
	}

	// Next day 00:00-01:00 resets Completed=false while Day 2 Asian range forms
	feedJobBar(job, dayStart+24*btime.HourMS, btime.HourMS, 2060, 2070, 2050, 2065, 10, false)
	asiaDay2 := job.SessionRange(btime.AsiaOpenMinutes, btime.AsiaCloseMinutes)
	if asiaDay2.Completed || !asiaDay2.InSession {
		t.Fatalf("Day 2 00:00-01:00 must reset Completed=false, got %+v", asiaDay2)
	}
}

func TestPrevDayOHLCDualThrustAndRBreakerExactTruth(t *testing.T) {
	stgy := &TradeStrat{Name: "test:dt_rb"}
	job := makeTestJob(t, stgy, "BTC/USDT:USDT", "1h", 1)

	// Construct 3 complete days (24x1h bars each) with known exact daily (O, H, L, C, V):
	// Day 1: O=100, H=120, L=90,  C=110, V=240
	// Day 2: O=110, H=135, L=105, C=130, V=480
	// Day 3: O=130, H=140, L=115, C=120, V=720
	type daySpec struct {
		o, h, l, c, barVol float64
	}
	specs := []daySpec{
		{100, 120, 90, 110, 10},
		{110, 135, 105, 130, 20},
		{130, 140, 115, 120, 30},
	}
	day0 := testUTCMS(2025, 6, 1, 0, 0)
	for d, sp := range specs {
		for h := 0; h < 24; h++ {
			bo, bhi, blo, bc := (sp.o+sp.c)*0.5, (sp.o+sp.c)*0.5+1, (sp.o+sp.c)*0.5-1, (sp.o+sp.c)*0.5
			if h == 0 {
				bo = sp.o
			}
			if h == 7 {
				bhi = sp.h
			}
			if h == 15 {
				blo = sp.l
			}
			if h == 23 {
				bc = sp.c
			}
			// Simulate warmup=true during Day 1 & Day 2 where strategy does NOT call DualThrust/RBreaker yet!
			isWarm := d < 2
			feedJobBar(job, day0+int64(d)*btime.DayMS+int64(h)*btime.HourMS, btime.HourMS, bo, bhi, blo, bc, sp.barVol, isWarm)

			if d == 0 {
				if _, ok := job.PrevDayOHLC(0); ok {
					t.Fatalf("Day 1 hour %d must have no PrevDayOHLC yet", h)
				}
			}
			if d == 2 {
				// Throughout ALL 24 hours of Day 3 (including h=23 closing at 24:00!),
				// PrevDayOHLC(0) MUST remain constant at Day 2's exact OHLC!
				prev, ok := job.PrevDayOHLC(0)
				if !ok {
					t.Fatalf("Day 3 hour %d expected PrevDayOHLC ok=true", h)
				}
				assertAlmostEqual(t, "Day3->Day2 PDO", prev.Open, 110)
				assertAlmostEqual(t, "Day3->Day2 PDH", prev.High, 135)
				assertAlmostEqual(t, "Day3->Day2 PDL", prev.Low, 105)
				assertAlmostEqual(t, "Day3->Day2 PDC", prev.Close, 130)
				assertAlmostEqual(t, "Day3->Day2 Vol", prev.Volume, 480)
			}
		}
	}

	// Now feed Day 4 first bar [00:00, 01:00]: Open=125, High=128, Low=122, Close=126
	feedJobBar(job, day0+3*btime.DayMS, btime.HourMS, 125, 128, 122, 126, 50, false)

	// 1) Verify PrevDayOHLC(0) == Day 3 (O=130, H=140, L=115, C=120, V=720)
	prev, ok := job.PrevDayOHLC(0)
	if !ok {
		t.Fatal("Day 4 PrevDayOHLC failed")
	}
	assertAlmostEqual(t, "PDO", prev.Open, 130)
	assertAlmostEqual(t, "PDH", prev.High, 140)
	assertAlmostEqual(t, "PDL", prev.Low, 115)
	assertAlmostEqual(t, "PDC", prev.Close, 120)
	assertAlmostEqual(t, "Vol", prev.Volume, 720)

	// 2) Verify PrevNDaysStats(0, 3) & DualThrustBands(0, 3, k1=0.6, k2=0.4):
	// Over Days 1..3:
	//   HH = max(120, 135, 140) = 140
	//   HC = max(110, 130, 120) = 130
	//   LC = min(110, 130, 120) = 110
	//   LL = min(90,  105, 115) = 90
	//   HH - LC = 140 - 110 = 30
	//   HC - LL = 130 - 90  = 40
	//   Range = max(30, 40) = 40
	//   dayOpen = 125 -> upper = 125 + 0.6*40 = 149, lower = 125 - 0.4*40 = 109
	stats, ok := job.PrevNDaysStats(0, 3)
	if !ok {
		t.Fatal("PrevNDaysStats(0, 3) failed (warmup replay should have recovered Days 1..2)")
	}
	assertAlmostEqual(t, "HH", stats.HH, 140)
	assertAlmostEqual(t, "HC", stats.HC, 130)
	assertAlmostEqual(t, "LC", stats.LC, 110)
	assertAlmostEqual(t, "LL", stats.LL, 90)
	assertAlmostEqual(t, "DualThrustRange", stats.DualThrustRange, 40)
	assertAlmostEqual(t, "AvgRange", stats.AvgRange, (30.0+30.0+25.0)/3.0)

	upper, lower, dayOpen, ok := job.DualThrustBands(0, 3, 0.6, 0.4)
	if !ok {
		t.Fatal("DualThrustBands failed")
	}
	assertAlmostEqual(t, "dayOpen", dayOpen, 125)
	assertAlmostEqual(t, "upper", upper, 149)
	assertAlmostEqual(t, "lower", lower, 109)

	// Feed a second bar on Day 4 with extreme prices -> DualThrust bands & dayOpen must NOT drift!
	feedJobBar(job, day0+3*btime.DayMS+btime.HourMS, btime.HourMS, 126, 160, 100, 155, 50, false)
	upper2, lower2, dayOpen2, ok := job.DualThrustBands(0, 3, 0.6, 0.4)
	if !ok {
		t.Fatal("DualThrustBands bar 2 failed")
	}
	assertAlmostEqual(t, "dayOpen2", dayOpen2, 125)
	assertAlmostEqual(t, "upper2", upper2, 149)
	assertAlmostEqual(t, "lower2", lower2, 109)

	// 3) Verify RBreakerLevels(0) from Day 3 (H=140, L=115, C=120):
	//   Pivot  = (140 + 115 + 120) / 3 = 125
	//   BBreak = 140 + 2*(125 - 115) = 160
	//   SSetup = 125 + (140 - 115)   = 150
	//   SEnter = 2*125 - 115         = 135
	//   BEnter = 2*125 - 140         = 110
	//   BSetup = 125 - (140 - 115)   = 100
	//   SBreak = 115 - 2*(140 - 125) = 85
	rb, ok := job.RBreakerLevels(0)
	if !ok {
		t.Fatal("RBreakerLevels failed")
	}
	assertAlmostEqual(t, "Pivot", rb.Pivot, 125)
	assertAlmostEqual(t, "BBreak", rb.BBreak, 160)
	assertAlmostEqual(t, "SSetup", rb.SSetup, 150)
	assertAlmostEqual(t, "SEnter", rb.SEnter, 135)
	assertAlmostEqual(t, "BEnter", rb.BEnter, 110)
	assertAlmostEqual(t, "BSetup", rb.BSetup, 100)
	assertAlmostEqual(t, "SBreak", rb.SBreak, 85)
	assertAlmostEqual(t, "TodayOpen", rb.TodayOpen, 125)
	assertAlmostEqual(t, "TodayHigh", rb.TodayHigh, 160)
	assertAlmostEqual(t, "TodayLow", rb.TodayLow, 100)
	if !(rb.BBreak > rb.SSetup && rb.SSetup > rb.SEnter && rb.SEnter > rb.Pivot &&
		rb.Pivot > rb.BEnter && rb.BEnter > rb.BSetup && rb.BSetup > rb.SBreak) {
		t.Fatalf("R-Breaker strict ordering violated: %+v", rb)
	}
}

func TestPartialInitialDayFragmentRejectedForNonZeroAnchor(t *testing.T) {
	// Stream starts at 00:00 UTC, anchor = 08:00 UTC (480m).
	// Hours 00:00..08:00 are only an 8-hour tail of yesterday's 08:00 session and
	// MUST NOT be treated as a full completed day at 08:00 UTC!
	stgy := &TradeStrat{Name: "test:partial_init"}
	job := makeTestJob(t, stgy, "BTC/USDT:USDT", "1h", 1)
	day0 := testUTCMS(2025, 6, 1, 0, 0)

	for h := 0; h < 36; h++ {
		feedJobBar(job, day0+int64(h)*btime.HourMS, btime.HourMS, 100+float64(h), 105+float64(h), 95+float64(h), 102+float64(h), 10, false)
		_, ok := job.PrevDayOHLC(btime.LondonCashOpenMinutes)
		if h < 32 {
			// First full 08:00-anchored day is h=8..31 (Day 1 08:00 .. Day 2 08:00);
			// PrevDayOHLC(480) must stay false until h=32 (Day 2 08:00-09:00)!
			if ok {
				t.Fatalf("hour %d: partial initial fragment leaked as completed day", h)
			}
		} else {
			if !ok {
				t.Fatalf("hour %d: expected first full 08:00 session day to be completed", h)
			}
		}
	}
}

func TestMultiSymbolJobIsolationConcurrentAndInterleaved(t *testing.T) {
	// Shared TradeStrat between BTC (~60000) and ETH (~2500) jobs!
	sharedStrat := &TradeStrat{Name: "shared:intraday"}
	jobBTC := makeTestJob(t, sharedStrat, "BTC/USDT:USDT", "15m", 1)
	jobETH := makeTestJob(t, sharedStrat, "ETH/USDT:USDT", "15m", 2)

	tfMS := 15 * btime.MinuteMS
	day0 := testUTCMS(2025, 7, 1, 0, 0)
	totalBars := 96 * 3 // 3 days of 15m bars

	// First run interleaved on Day 1 & Day 2, then concurrent goroutines on Day 3
	for i := 0; i < 96*2; i++ {
		ts := day0 + int64(i)*tfMS
		feedJobBar(jobBTC, ts, tfMS, 60000, 61000, 59000, 60500, 100, false)
		feedJobBar(jobETH, ts, tfMS, 2500, 2600, 2400, 2550, 50, false)
		_ = jobBTC.OpeningRange(0, 30)
		_ = jobETH.OpeningRange(0, 30)
		_, _, _, _ = jobBTC.DualThrustBands(0, 1, 0.5, 0.5)
		_, _, _, _ = jobETH.DualThrustBands(0, 1, 0.5, 0.5)
		_, _ = jobBTC.RBreakerLevels(0)
		_, _ = jobETH.RBreakerLevels(0)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	errCh := make(chan string, 4)

	go func() {
		defer wg.Done()
		for i := 96 * 2; i < totalBars; i++ {
			ts := day0 + int64(i)*tfMS
			feedJobBar(jobBTC, ts, tfMS, 62000, 62500, 61800, 62100, 120, false)
			orb := jobBTC.OpeningRange(0, 30)
			if i%96 >= 1 && (!orb.Ready || orb.High != 62500 || orb.Low != 61800) {
				errCh <- "BTC ORB contaminated"
				return
			}
			up, lo, dOpen, ok := jobBTC.DualThrustBands(0, 2, 0.5, 0.5)
			// Days 1&2 BTC: HH=61000, HC=60500, LC=60500, LL=59000 -> Range = max(500, 1500) = 1500
			// Day 3 dOpen=62000 -> up=62750, lo=61250
			if !ok || dOpen != 62000 || up != 62750 || lo != 61250 {
				errCh <- "BTC DualThrust contaminated"
				return
			}
			rb, ok := jobBTC.RBreakerLevels(0)
			if !ok || rb.Prev.High != 61000 || rb.Prev.Low != 59000 {
				errCh <- "BTC RBreaker contaminated"
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		for i := 96 * 2; i < totalBars; i++ {
			ts := day0 + int64(i)*tfMS
			feedJobBar(jobETH, ts, tfMS, 2700, 2750, 2680, 2720, 60, false)
			orb := jobETH.OpeningRange(0, 30)
			if i%96 >= 1 && (!orb.Ready || orb.High != 2750 || orb.Low != 2680) {
				errCh <- "ETH ORB contaminated"
				return
			}
			up, lo, dOpen, ok := jobETH.DualThrustBands(0, 2, 0.5, 0.5)
			// Days 1&2 ETH: HH=2600, HC=2550, LC=2550, LL=2400 -> Range = max(50, 150) = 150
			// Day 3 dOpen=2700 -> up=2775, lo=2625
			if !ok || dOpen != 2700 || up != 2775 || lo != 2625 {
				errCh <- "ETH DualThrust contaminated"
				return
			}
			rb, ok := jobETH.RBreakerLevels(0)
			if !ok || rb.Prev.High != 2600 || rb.Prev.Low != 2400 {
				errCh <- "ETH RBreaker contaminated"
				return
			}
		}
	}()

	wg.Wait()
	close(errCh)
	for msg := range errCh {
		t.Fatal(msg)
	}
}
