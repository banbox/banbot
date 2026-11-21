package strat

import (
	"math"
	"sync"

	"github.com/banbox/banbot/btime"
	ta "github.com/banbox/banta"
)

const (
	defaultMaxPrevDays     = 64
	maxAllowedPrevDays     = 366
	maxIntradayBarSpanHist = 2304
	trimIntradayBarSpansTo = 1600
)

// OpeningRangeOptions configures optional behavior for OpeningRangeTracker.
type OpeningRangeOptions struct {
	// AllowSingleBarFallback widens EffectiveRangeMinutes to one bar
	// (TFMSecs / 60000) when the intraday timeframe is coarser than
	// rangeMinutes (e.g. 1h bar with 30m requested ORB), setting
	// DegradedSingleBar=true. Daily or coarser timeframes (>= 1d) always
	// remain Applicable=false, Ready=false.
	AllowSingleBarFallback bool
}

// OpeningRangeSnapshot represents the no-lookahead state of an anchor-aligned
// intraday opening range on the current bar.
type OpeningRangeSnapshot struct {
	SessionDay            int64   // SessionDayIndex(barStartMS, AnchorMinutesUTC)
	SessionStartMS        int64   // UTC millisecond timestamp of today's session open
	RangeEndMS            int64   // SessionStartMS + EffectiveRangeMinutes*60000
	AnchorMinutesUTC      int     // Normalized session anchor in [0, 1439]
	RequestedRangeMinutes int     // Caller-requested opening range duration in minutes
	EffectiveRangeMinutes int     // Effective duration in minutes covered by the opening range
	Open                  float64 // First bar's open within the opening range (0 until Ready)
	High                  float64 // Highest high within the opening range (0 until Ready)
	Low                   float64 // Lowest low within the opening range (0 until Ready)
	Close                 float64 // Last bar's close within the opening range (0 until Ready)
	Mid                   float64 // (High + Low) / 2 once Ready
	Width                 float64 // High - Low once Ready
	Volume                float64 // Cumulative volume within the opening range (0 until Ready)
	BarCount              int     // Number of valid bars observed inside [SessionStartMS, RangeEndMS)
	Applicable            bool    // False when timeframe >= 1d, > rangeMinutes (without fallback), or misaligned
	Forming               bool    // True while Applicable && TimeStop < RangeEndMS
	Ready                 bool    // True ONLY when Applicable && TimeStop >= RangeEndMS && BarCount >= 1
	JustCompleted         bool    // True only on the first observed bar of SessionDay where Ready becomes true
	InPostRange           bool    // True when Ready && current bar opened at or after RangeEndMS
	DegradedSingleBar     bool    // True when AllowSingleBarFallback widened EffectiveRangeMinutes to 1 bar
}

// SessionRangeSnapshot represents the no-lookahead range of a recurring UTC
// session window [StartMinUTC, EndMinUTC) (supporting cross-midnight windows
// such as 22:00-02:00 UTC as well as intra-day windows such as 00:00-08:00 UTC).
type SessionRangeSnapshot struct {
	SessionDay      int64   // SessionDayIndex(barStartMS, StartMinUTC)
	WindowStartMS   int64   // UTC start timestamp of the current cycle's session window
	WindowEndMS     int64   // WindowStartMS + DurationMinutes*60000
	StartMinUTC     int     // Normalized session start minute in [0, 1439]
	EndMinUTC       int     // Normalized session end minute in [0, 1439]
	DurationMinutes int     // Window length in minutes in (0, 1440)
	Open            float64 // Session open price (0 until Completed)
	High            float64 // Session highest high (0 until Completed)
	Low             float64 // Session lowest low (0 until Completed)
	Close           float64 // Session closing price (0 until Completed)
	Mid             float64 // (High + Low) / 2 once Completed
	Width           float64 // High - Low once Completed
	Volume          float64 // Cumulative session volume (0 until Completed)
	BarCount        int     // Number of valid bars observed inside [WindowStartMS, WindowEndMS)
	Applicable      bool    // False when timeframe >= 1d, > window duration, or misaligned
	InSession       bool    // True while Applicable && bar lies inside [WindowStartMS, WindowEndMS) && TimeStop < WindowEndMS
	Completed       bool    // True ONLY when Applicable && TimeStop >= WindowEndMS && BarCount >= 1
	JustCompleted   bool    // True only on the first observed bar of SessionDay where Completed becomes true
	InPostSession   bool    // True when Completed && current bar opened at or after WindowEndMS
}

// PrevOHLC holds the OHLCV summary of one completed anchor-aligned trading day
// (PDO / PDH / PDL / PDC — Fairy Four Prices / 菲阿里四价).
type PrevOHLC struct {
	SessionDay     int64   // SessionDayIndex of the completed trading day
	SessionStartMS int64   // UTC start timestamp (ms) of the completed trading day
	SessionEndMS   int64   // SessionStartMS + btime.DayMS
	Open           float64 // PDO: Previous Day Open
	High           float64 // PDH: Previous Day High
	Low            float64 // PDL: Previous Day Low
	Close          float64 // PDC: Previous Day Close
	Pivot          float64 // Classic floor pivot: (High + Low + Close) / 3
	Range          float64 // High - Low
	Volume         float64 // Cumulative base volume of the completed trading day
	BarCount       int     // Number of valid bars observed in the completed trading day
}

// PrevNDaysSummary aggregates statistics over the most recent N completed
// anchor-aligned trading days (strictly excluding the current incomplete day).
type PrevNDaysSummary struct {
	Days            int        // Number of completed days aggregated (== len(History))
	Open            float64    // Open of the oldest day in the N-day window
	Close           float64    // Close of the most recent completed day (PDC)
	HH              float64    // Highest High across the N completed days
	HC              float64    // Highest Close across the N completed days
	LC              float64    // Lowest Close across the N completed days
	LL              float64    // Lowest Low across the N completed days
	DualThrustRange float64    // max(HH - LC, HC - LL)
	AvgRange        float64    // Mean daily (High - Low) across the N completed days (ADR)
	TotalVolume     float64    // Sum of daily volumes across the N completed days
	AvgVolume       float64    // TotalVolume / Days
	History         []PrevOHLC // Chronological copy (oldest to newest) of the N completed days
}

// RBreakerSnap holds the six classic R-Breaker intraday levels computed from
// the previous completed trading day's (High, Low, Close), along with today's
// running intraday extremes for reversal condition checks.
//
// Standard Pivot R-Breaker formulas (Pivot = (H + L + C) / 3):
//   - BBreak = H + 2*(Pivot - L)   (Breakout Buy / 突破买入价)
//   - SSetup = Pivot + (H - L)     (Observation Sell / 观察卖出价)
//   - SEnter = 2*Pivot - L         (Reversal Sell / 反转卖出价)
//   - BEnter = 2*Pivot - H         (Reversal Buy / 反转买入价)
//   - BSetup = Pivot - (H - L)     (Observation Buy / 观察买入价)
//   - SBreak = L - 2*(H - Pivot)   (Breakout Sell / 突破卖出价)
type RBreakerSnap struct {
	SessionDay int64    // Current session day index
	BBreak     float64  // Breakout buy level (突破买入价)
	SSetup     float64  // Setup sell level (观察卖出价)
	SEnter     float64  // Reversal sell entry level (反转卖出价)
	Pivot      float64  // Floor pivot (H + L + C) / 3
	BEnter     float64  // Reversal buy entry level (反转买入价)
	BSetup     float64  // Setup buy level (观察买入价)
	SBreak     float64  // Breakout sell level (突破卖出价)
	Prev       PrevOHLC // Previous completed session day OHLCV
	TodayOpen  float64  // Current session day's opening price
	TodayHigh  float64  // Current session day's highest high observed up through current bar
	TodayLow   float64  // Current session day's lowest low observed up through current bar
}

// OpeningRangeTracker tracks the opening range [anchor, anchor + rangeMinutes)
// of each anchor-aligned UTC session day without lookahead bias.
type OpeningRangeTracker struct {
	anchorMin int
	reqMin    int
	opts      OpeningRangeOptions

	hasDay        bool
	curDay        int64
	sessionStart  int64
	open          float64
	high          float64
	low           float64
	close         float64
	volume        float64
	barCount      int
	completedOnce bool

	lastStopMS int64
	lastSnap   OpeningRangeSnapshot
}

// NewOpeningRangeTracker creates a strict no-lookahead OpeningRangeTracker.
func NewOpeningRangeTracker(anchorMinutesUTC, rangeMinutes int) *OpeningRangeTracker {
	return NewOpeningRangeTrackerWithOpts(anchorMinutesUTC, rangeMinutes, OpeningRangeOptions{})
}

// NewOpeningRangeTrackerWithOpts creates an OpeningRangeTracker with explicit options.
func NewOpeningRangeTrackerWithOpts(anchorMinutesUTC, rangeMinutes int, opts OpeningRangeOptions) *OpeningRangeTracker {
	return &OpeningRangeTracker{
		anchorMin: btime.NormalizeDayMinutes(anchorMinutesUTC),
		reqMin:    rangeMinutes,
		opts:      opts,
	}
}

// Reset clears all accumulated session state on t.
func (t *OpeningRangeTracker) Reset() {
	if t == nil {
		return
	}
	*t = OpeningRangeTracker{
		anchorMin: t.anchorMin,
		reqMin:    t.reqMin,
		opts:      t.opts,
	}
}

// Snapshot returns the most recently computed OpeningRangeSnapshot.
func (t *OpeningRangeTracker) Snapshot() OpeningRangeSnapshot {
	if t == nil {
		return OpeningRangeSnapshot{}
	}
	return t.lastSnap
}

// Update advances t using the current bar of env and returns the snapshot.
func (t *OpeningRangeTracker) Update(env *ta.BarEnv) OpeningRangeSnapshot {
	if t == nil {
		return OpeningRangeSnapshot{}
	}
	o, h, l, c, v, ok := readEnvCurrentOHLCV(env)
	if !ok {
		return OpeningRangeSnapshot{
			AnchorMinutesUTC:      t.anchorMin,
			RequestedRangeMinutes: t.reqMin,
		}
	}
	return t.UpdateBar(env.TimeStart, env.TimeStop, env.TFMSecs, o, h, l, c, v)
}

// UpdateBar advances t with one closed bar [barStartMS, barStopMS] and returns
// the resulting no-lookahead OpeningRangeSnapshot. Repeated calls with the same
// barStopMS are idempotent.
func (t *OpeningRangeTracker) UpdateBar(barStartMS, barStopMS, tfMS int64, open, high, low, close, volume float64) OpeningRangeSnapshot {
	if t == nil {
		return OpeningRangeSnapshot{}
	}
	if t.lastStopMS > 0 && barStopMS == t.lastStopMS {
		return t.lastSnap
	}
	if t.lastStopMS > 0 && barStopMS < t.lastStopMS {
		t.Reset()
	}
	base := OpeningRangeSnapshot{
		AnchorMinutesUTC:      t.anchorMin,
		RequestedRangeMinutes: t.reqMin,
	}
	if barStopMS <= barStartMS {
		return base
	}
	if tfMS <= 0 {
		tfMS = barStopMS - barStartMS
	}
	barSpan := barStopMS - barStartMS
	if t.reqMin <= 0 || t.reqMin >= btime.MinutesPerDay || tfMS >= btime.DayMS || barSpan >= btime.DayMS {
		t.lastStopMS = barStopMS
		t.lastSnap = base
		return base
	}
	if !validOHLC(open, high, low, close) {
		return t.lastSnap
	}
	if math.IsNaN(volume) || math.IsInf(volume, 0) || volume < 0 {
		volume = 0
	}

	effMin := t.reqMin
	degraded := false
	reqMS := int64(t.reqMin) * btime.MinuteMS
	if tfMS > reqMS {
		if !t.opts.AllowSingleBarFallback || tfMS%btime.MinuteMS != 0 {
			t.lastStopMS = barStopMS
			t.lastSnap = base
			return base
		}
		effMin = int(tfMS / btime.MinuteMS)
		if effMin <= 0 || effMin >= btime.MinutesPerDay {
			t.lastStopMS = barStopMS
			t.lastSnap = base
			return base
		}
		degraded = true
	}

	dayIdx := btime.SessionDayIndex(barStartMS, t.anchorMin)
	sessStart := btime.SessionStartMS(barStartMS, t.anchorMin)
	effMS := int64(effMin) * btime.MinuteMS
	rangeEnd := sessStart + effMS

	// Verify that the bar grid can align with sessStart rather than straddling it.
	if barSpan == tfMS && ((barStartMS-sessStart)%tfMS) != 0 {
		base.SessionDay = dayIdx
		base.SessionStartMS = sessStart
		base.RangeEndMS = rangeEnd
		t.lastStopMS = barStopMS
		t.lastSnap = base
		return base
	}

	if !t.hasDay || dayIdx != t.curDay {
		t.hasDay = true
		t.curDay = dayIdx
		t.sessionStart = sessStart
		t.open = 0
		t.high = 0
		t.low = 0
		t.close = 0
		t.volume = 0
		t.barCount = 0
		t.completedOnce = false
	}

	if barStartMS >= sessStart && barStopMS <= rangeEnd {
		if t.barCount == 0 {
			t.open = open
			t.high = high
			t.low = low
		} else {
			if high > t.high {
				t.high = high
			}
			if low < t.low {
				t.low = low
			}
		}
		t.close = close
		t.volume += volume
		t.barCount++
	}

	snap := OpeningRangeSnapshot{
		SessionDay:            dayIdx,
		SessionStartMS:        sessStart,
		RangeEndMS:            rangeEnd,
		AnchorMinutesUTC:      t.anchorMin,
		RequestedRangeMinutes: t.reqMin,
		EffectiveRangeMinutes: effMin,
		BarCount:              t.barCount,
		Applicable:            true,
		Forming:               barStopMS < rangeEnd,
		DegradedSingleBar:     degraded,
	}
	if barStopMS >= rangeEnd && t.barCount >= 1 {
		snap.Ready = true
		snap.JustCompleted = !t.completedOnce
		snap.InPostRange = barStartMS >= rangeEnd
		snap.Open = t.open
		snap.High = t.high
		snap.Low = t.low
		snap.Close = t.close
		snap.Mid = 0.5 * (t.high + t.low)
		snap.Width = t.high - t.low
		snap.Volume = t.volume
		t.completedOnce = true
	}

	t.lastStopMS = barStopMS
	t.lastSnap = snap
	return snap
}

// SessionRangeTracker tracks the OHLCV range of a recurring UTC window
// [startMinUTC, endMinUTC) (such as 00:00-08:00 UTC Asian Range) and locks the
// completed range for subsequent sessions in the same 24h cycle.
type SessionRangeTracker struct {
	startMin int
	endMin   int
	durMin   int
	validCfg bool

	inner      *OpeningRangeTracker
	lastStopMS int64
	lastSnap   SessionRangeSnapshot
}

// NewSessionRangeTracker creates a tracker for [startMinUTC, endMinUTC).
// Cross-midnight windows (startMinUTC > endMinUTC, e.g. 22:00-02:00 UTC) are supported.
func NewSessionRangeTracker(startMinUTC, endMinUTC int) *SessionRangeTracker {
	sMin := btime.NormalizeDayMinutes(startMinUTC)
	eMin := btime.NormalizeDayMinutes(endMinUTC)
	durMin := 0
	if startMinUTC != endMinUTC && endMinUTC-startMinUTC < btime.MinutesPerDay && sMin != eMin {
		if sMin < eMin {
			durMin = eMin - sMin
		} else {
			durMin = (btime.MinutesPerDay - sMin) + eMin
		}
	}
	valid := durMin > 0 && durMin < btime.MinutesPerDay
	var inner *OpeningRangeTracker
	if valid {
		inner = NewOpeningRangeTracker(sMin, durMin)
	}
	return &SessionRangeTracker{
		startMin: sMin,
		endMin:   eMin,
		durMin:   durMin,
		validCfg: valid,
		inner:    inner,
	}
}

// Reset clears all accumulated state on t.
func (t *SessionRangeTracker) Reset() {
	if t == nil {
		return
	}
	if t.inner != nil {
		t.inner.Reset()
	}
	t.lastStopMS = 0
	t.lastSnap = SessionRangeSnapshot{}
}

// Snapshot returns the latest SessionRangeSnapshot.
func (t *SessionRangeTracker) Snapshot() SessionRangeSnapshot {
	if t == nil {
		return SessionRangeSnapshot{}
	}
	return t.lastSnap
}

// Update advances t using the current bar of env.
func (t *SessionRangeTracker) Update(env *ta.BarEnv) SessionRangeSnapshot {
	if t == nil {
		return SessionRangeSnapshot{}
	}
	o, h, l, c, v, ok := readEnvCurrentOHLCV(env)
	if !ok {
		return SessionRangeSnapshot{
			StartMinUTC:     t.startMin,
			EndMinUTC:       t.endMin,
			DurationMinutes: t.durMin,
		}
	}
	return t.UpdateBar(env.TimeStart, env.TimeStop, env.TFMSecs, o, h, l, c, v)
}

// UpdateBar advances t with one closed bar [barStartMS, barStopMS].
func (t *SessionRangeTracker) UpdateBar(barStartMS, barStopMS, tfMS int64, open, high, low, close, volume float64) SessionRangeSnapshot {
	if t == nil {
		return SessionRangeSnapshot{}
	}
	if t.lastStopMS > 0 && barStopMS == t.lastStopMS {
		return t.lastSnap
	}
	if t.lastStopMS > 0 && barStopMS < t.lastStopMS {
		t.Reset()
	}
	base := SessionRangeSnapshot{
		StartMinUTC:     t.startMin,
		EndMinUTC:       t.endMin,
		DurationMinutes: t.durMin,
	}
	if !t.validCfg || t.inner == nil {
		t.lastStopMS = barStopMS
		t.lastSnap = base
		return base
	}
	orb := t.inner.UpdateBar(barStartMS, barStopMS, tfMS, open, high, low, close, volume)
	// Also ensure the window end aligns with the bar grid when barSpan == tfMS.
	barSpan := barStopMS - barStartMS
	effTF := tfMS
	if effTF <= 0 {
		effTF = barSpan
	}
	if orb.Applicable && effTF > 0 && barSpan == effTF && ((orb.RangeEndMS-orb.SessionStartMS)%effTF) != 0 {
		// If the session duration is not divisible by the timeframe, a bar would
		// straddle WindowEndMS; mark inapplicable to avoid partial/leaky windows.
		base.SessionDay = orb.SessionDay
		base.WindowStartMS = orb.SessionStartMS
		base.WindowEndMS = orb.RangeEndMS
		t.lastStopMS = barStopMS
		t.lastSnap = base
		return base
	}
	snap := SessionRangeSnapshot{
		SessionDay:      orb.SessionDay,
		WindowStartMS:   orb.SessionStartMS,
		WindowEndMS:     orb.RangeEndMS,
		StartMinUTC:     t.startMin,
		EndMinUTC:       t.endMin,
		DurationMinutes: t.durMin,
		Open:            orb.Open,
		High:            orb.High,
		Low:             orb.Low,
		Close:           orb.Close,
		Mid:             orb.Mid,
		Width:           orb.Width,
		Volume:          orb.Volume,
		BarCount:        orb.BarCount,
		Applicable:      orb.Applicable,
		InSession:       orb.Applicable && barStartMS >= orb.SessionStartMS && barStopMS < orb.RangeEndMS,
		Completed:       orb.Ready,
		JustCompleted:   orb.JustCompleted,
		InPostSession:   orb.InPostRange,
	}
	t.lastStopMS = barStopMS
	t.lastSnap = snap
	return snap
}

type activeDayAcc struct {
	dayIdx       int64
	startMS      int64
	firstStartMS int64
	open         float64
	high         float64
	low          float64
	close        float64
	volume       float64
	barCount     int
	startedFull  bool // true if entered via rollover or first bar started at session open
}

// PrevPeriodTracker maintains a rolling no-lookahead history of completed
// anchor-aligned session days plus the active day's running intraday OHLCV.
type PrevPeriodTracker struct {
	anchorMin int
	maxDays   int

	hasCur        bool
	sawPriorDay   bool
	cur           activeDayAcc
	completedDays []PrevOHLC

	lastStopMS int64
	lastTFMS   int64
}

// NewPrevPeriodTracker creates a rolling completed-day tracker for anchorMinutesUTC.
func NewPrevPeriodTracker(anchorMinutesUTC, maxDays int) *PrevPeriodTracker {
	if maxDays <= 0 {
		maxDays = defaultMaxPrevDays
	}
	if maxDays > maxAllowedPrevDays {
		maxDays = maxAllowedPrevDays
	}
	return &PrevPeriodTracker{
		anchorMin: btime.NormalizeDayMinutes(anchorMinutesUTC),
		maxDays:   maxDays,
	}
}

// Reset clears all accumulated history on t.
func (t *PrevPeriodTracker) Reset() {
	if t == nil {
		return
	}
	*t = PrevPeriodTracker{
		anchorMin: t.anchorMin,
		maxDays:   t.maxDays,
	}
}

func (t *PrevPeriodTracker) ensureCapacity(nDays int) {
	if t == nil || nDays <= t.maxDays {
		return
	}
	if nDays > maxAllowedPrevDays {
		nDays = maxAllowedPrevDays
	}
	t.maxDays = nDays
}

// Update advances t using the current bar of env.
func (t *PrevPeriodTracker) Update(env *ta.BarEnv) {
	if t == nil {
		return
	}
	o, h, l, c, v, ok := readEnvCurrentOHLCV(env)
	if !ok {
		return
	}
	t.UpdateBar(env.TimeStart, env.TimeStop, env.TFMSecs, o, h, l, c, v)
}

// UpdateBar advances t with one closed bar [barStartMS, barStopMS].
// Multi-day bars (> 24h) or daily bars misaligned with anchorMinutesUTC are
// ignored so coarse/misaligned series never fabricate daily levels.
func (t *PrevPeriodTracker) UpdateBar(barStartMS, barStopMS, tfMS int64, open, high, low, close, volume float64) {
	if t == nil || barStopMS <= barStartMS {
		return
	}
	if t.lastStopMS > 0 && barStopMS == t.lastStopMS {
		return
	}
	if t.lastStopMS > 0 && barStopMS < t.lastStopMS {
		t.Reset()
	}
	barSpan := barStopMS - barStartMS
	if tfMS <= 0 {
		tfMS = barSpan
	}
	t.lastTFMS = tfMS
	if tfMS > btime.DayMS || barSpan > btime.DayMS {
		t.lastStopMS = barStopMS
		return
	}
	// Daily (24h) bars can only represent anchorMinutesUTC when barStartMS
	// aligns with that anchor (e.g. 00:00 UTC 1d bars for anchor=0).
	if (tfMS == btime.DayMS || barSpan == btime.DayMS) && btime.SessionStartMS(barStartMS, t.anchorMin) != barStartMS {
		t.lastStopMS = barStopMS
		return
	}
	if !validOHLC(open, high, low, close) {
		return
	}
	if math.IsNaN(volume) || math.IsInf(volume, 0) || volume < 0 {
		volume = 0
	}

	// Use barStopMS-1 to classify the bar's session day:
	//   - For aligned bars (including the final bar [23:45, 24:00] of day D),
	//     SessionDayIndex(barStopMS-1, anchor) == SessionDayIndex(barStartMS, anchor) == D.
	//   - If an unaligned bar straddles an anchor (e.g. 13:00-14:00 across 13:30),
	//     day D-1 finalizes cleanly BEFORE this bar so post-anchor prices never
	//     leak backward into day D-1's PrevDayOHLC.
	dayIdx := btime.SessionDayIndex(barStopMS-1, t.anchorMin)
	sessStart := dayIdx*btime.DayMS + int64(t.anchorMin)*btime.MinuteMS

	if !t.hasCur {
		tol := tfMS
		if tol < btime.MinuteMS {
			tol = btime.MinuteMS
		}
		startedAtOpen := barStartMS >= sessStart && (barStartMS-sessStart) < tol
		t.hasCur = true
		t.cur = activeDayAcc{
			dayIdx:       dayIdx,
			startMS:      sessStart,
			firstStartMS: barStartMS,
			open:         open,
			high:         high,
			low:          low,
			close:        close,
			volume:       volume,
			barCount:     1,
			startedFull:  startedAtOpen,
		}
		t.lastStopMS = barStopMS
		return
	}

	if dayIdx > t.cur.dayIdx {
		if t.cur.barCount > 0 && (t.sawPriorDay || t.cur.startedFull) {
			done := PrevOHLC{
				SessionDay:     t.cur.dayIdx,
				SessionStartMS: t.cur.startMS,
				SessionEndMS:   t.cur.startMS + btime.DayMS,
				Open:           t.cur.open,
				High:           t.cur.high,
				Low:            t.cur.low,
				Close:          t.cur.close,
				Pivot:          (t.cur.high + t.cur.low + t.cur.close) / 3.0,
				Range:          t.cur.high - t.cur.low,
				Volume:         t.cur.volume,
				BarCount:       t.cur.barCount,
			}
			t.completedDays = append(t.completedDays, done)
			if len(t.completedDays) > t.maxDays {
				excess := len(t.completedDays) - t.maxDays
				copy(t.completedDays, t.completedDays[excess:])
				t.completedDays = t.completedDays[:t.maxDays]
			}
		}
		t.sawPriorDay = true
		t.cur = activeDayAcc{
			dayIdx:       dayIdx,
			startMS:      sessStart,
			firstStartMS: barStartMS,
			open:         open,
			high:         high,
			low:          low,
			close:        close,
			volume:       volume,
			barCount:     1,
			startedFull:  true,
		}
		t.lastStopMS = barStopMS
		return
	}

	if dayIdx == t.cur.dayIdx {
		if high > t.cur.high {
			t.cur.high = high
		}
		if low < t.cur.low {
			t.cur.low = low
		}
		t.cur.close = close
		t.cur.volume += volume
		t.cur.barCount++
	}
	t.lastStopMS = barStopMS
}

// PrevDayOHLC returns the most recently completed anchor-aligned trading day's
// OHLCV (PDO/PDH/PDL/PDC). Throughout any bar of day D, it stays constant at
// day D-1's full-day values with zero lookahead.
func (t *PrevPeriodTracker) PrevDayOHLC() (PrevOHLC, bool) {
	if t == nil || len(t.completedDays) == 0 || t.lastTFMS > btime.DayMS {
		return PrevOHLC{}, false
	}
	return t.completedDays[len(t.completedDays)-1], true
}

// PrevNDaysStats aggregates OHLCV statistics across the last nDays completed
// anchor-aligned trading days. Returns (PrevNDaysSummary{}, false) when fewer
// than nDays complete days have been observed.
func (t *PrevPeriodTracker) PrevNDaysStats(nDays int) (PrevNDaysSummary, bool) {
	if t == nil || nDays <= 0 || nDays > maxAllowedPrevDays || t.lastTFMS > btime.DayMS {
		return PrevNDaysSummary{}, false
	}
	t.ensureCapacity(nDays)
	total := len(t.completedDays)
	if total < nDays {
		return PrevNDaysSummary{}, false
	}
	window := t.completedDays[total-nDays:]
	hist := make([]PrevOHLC, nDays)
	copy(hist, window)

	hh := hist[0].High
	hc := hist[0].Close
	lc := hist[0].Close
	ll := hist[0].Low
	sumRange := 0.0
	sumVol := 0.0
	for i := range hist {
		d := hist[i]
		if d.High > hh {
			hh = d.High
		}
		if d.Close > hc {
			hc = d.Close
		}
		if d.Close < lc {
			lc = d.Close
		}
		if d.Low < ll {
			ll = d.Low
		}
		sumRange += d.Range
		sumVol += d.Volume
	}
	dtRange := math.Max(hh-lc, hc-ll)
	return PrevNDaysSummary{
		Days:            nDays,
		Open:            hist[0].Open,
		Close:           hist[nDays-1].Close,
		HH:              hh,
		HC:              hc,
		LC:              lc,
		LL:              ll,
		DualThrustRange: dtRange,
		AvgRange:        sumRange / float64(nDays),
		TotalVolume:     sumVol,
		AvgVolume:       sumVol / float64(nDays),
		History:         hist,
	}, true
}

// DualThrustBands computes today's Dual Thrust upper and lower breakout bands
// from the past nDays completed session days and today's opening price:
//
//	Range = max(HH - LC, HC - LL)
//	upper = dayOpen + k1 * Range
//	lower = dayOpen - k2 * Range
//
// Only intraday timeframes (TFMSecs < 86400000) are eligible; daily or coarser
// bars return ok=false.
func (t *PrevPeriodTracker) DualThrustBands(nDays int, k1, k2 float64) (upper, lower, dayOpen float64, ok bool) {
	if t == nil || !t.hasCur || t.cur.barCount == 0 || t.lastTFMS <= 0 || t.lastTFMS >= btime.DayMS {
		return 0, 0, 0, false
	}
	if math.IsNaN(k1) || math.IsInf(k1, 0) || k1 < 0 || math.IsNaN(k2) || math.IsInf(k2, 0) || k2 < 0 {
		return 0, 0, 0, false
	}
	stats, ok := t.PrevNDaysStats(nDays)
	if !ok || !isFinitePositive(t.cur.open) {
		return 0, 0, 0, false
	}
	dayOpen = t.cur.open
	upper = dayOpen + k1*stats.DualThrustRange
	lower = dayOpen - k2*stats.DualThrustRange
	return upper, lower, dayOpen, true
}

// CalcRBreakerLevels computes standard floor-pivot R-Breaker six levels from a
// completed day's OHLC.
func CalcRBreakerLevels(prev PrevOHLC) RBreakerSnap {
	pivot := (prev.High + prev.Low + prev.Close) / 3.0
	span := prev.High - prev.Low
	return RBreakerSnap{
		BBreak: prev.High + 2.0*(pivot-prev.Low),
		SSetup: pivot + span,
		SEnter: 2.0*pivot - prev.Low,
		Pivot:  pivot,
		BEnter: 2.0*pivot - prev.High,
		BSetup: pivot - span,
		SBreak: prev.Low - 2.0*(prev.High-pivot),
		Prev:   prev,
	}
}

// CalcRBreakerLevelsBy computes parameterized R-Breaker six levels (commonly
// f1=0.35, f2=0.07, f3=0.25) from a completed day's OHLC.
func CalcRBreakerLevelsBy(prev PrevOHLC, f1, f2, f3 float64) RBreakerSnap {
	pivot := (prev.High + prev.Low + prev.Close) / 3.0
	sSetup := prev.High + f1*(prev.Close-prev.Low)
	bSetup := prev.Low - f1*(prev.High-prev.Close)
	sEnter := 0.5*(1.0+f2)*(prev.High+prev.Low) - f2*prev.Low
	bEnter := 0.5*(1.0+f2)*(prev.High+prev.Low) - f2*prev.High
	bBreak := sSetup + f3*(sSetup-bSetup)
	sBreak := bSetup - f3*(sSetup-bSetup)
	return RBreakerSnap{
		BBreak: bBreak,
		SSetup: sSetup,
		SEnter: sEnter,
		Pivot:  pivot,
		BEnter: bEnter,
		BSetup: bSetup,
		SBreak: sBreak,
		Prev:   prev,
	}
}

// RBreakerLevels computes standard six-level R-Breaker prices from the previous
// completed session day plus today's running intraday Open/High/Low.
// Only intraday timeframes (TFMSecs < 86400000) are eligible.
func (t *PrevPeriodTracker) RBreakerLevels() (RBreakerSnap, bool) {
	if t == nil || !t.hasCur || t.cur.barCount == 0 || t.lastTFMS <= 0 || t.lastTFMS >= btime.DayMS {
		return RBreakerSnap{}, false
	}
	prev, ok := t.PrevDayOHLC()
	if !ok {
		return RBreakerSnap{}, false
	}
	snap := CalcRBreakerLevels(prev)
	snap.SessionDay = t.cur.dayIdx
	snap.TodayOpen = t.cur.open
	snap.TodayHigh = t.cur.high
	snap.TodayLow = t.cur.low
	return snap, true
}

// RBreakerLevelsBy computes parameterized R-Breaker levels (f1, f2, f3) from
// the previous completed session day plus today's running intraday Open/High/Low.
func (t *PrevPeriodTracker) RBreakerLevelsBy(f1, f2, f3 float64) (RBreakerSnap, bool) {
	if t == nil || !t.hasCur || t.cur.barCount == 0 || t.lastTFMS <= 0 || t.lastTFMS >= btime.DayMS {
		return RBreakerSnap{}, false
	}
	if math.IsNaN(f1) || math.IsInf(f1, 0) || math.IsNaN(f2) || math.IsInf(f2, 0) || math.IsNaN(f3) || math.IsInf(f3, 0) {
		return RBreakerSnap{}, false
	}
	prev, ok := t.PrevDayOHLC()
	if !ok {
		return RBreakerSnap{}, false
	}
	snap := CalcRBreakerLevelsBy(prev, f1, f2, f3)
	snap.SessionDay = t.cur.dayIdx
	snap.TodayOpen = t.cur.open
	snap.TodayHigh = t.cur.high
	snap.TodayLow = t.cur.low
	return snap, true
}

type orbTrackerKey struct {
	anchorMin              int
	rangeMin               int
	allowSingleBarFallback bool
}

type sessionTrackerKey struct {
	startMin int
	endMin   int
}

type intradayBarSpan struct {
	barNum  int
	startMS int64
	stopMS  int64
}

type intradayJobState struct {
	mu            sync.Mutex
	lastBarNum    int
	lastStopMS    int64
	barSpans      []intradayBarSpan
	openingRanges map[orbTrackerKey]*OpeningRangeTracker
	sessionRanges map[sessionTrackerKey]*SessionRangeTracker
	prevPeriods   map[int]*PrevPeriodTracker
}

func (s *StratJob) intradayState() *intradayJobState {
	if s == nil {
		return nil
	}
	exec := s.executionState()
	st := exec.intraday.Load()
	if st != nil {
		return st
	}
	candidate := &intradayJobState{}
	if exec.intraday.CompareAndSwap(nil, candidate) {
		return candidate
	}
	return exec.intraday.Load()
}

func (st *intradayJobState) resetLocked() {
	st.lastBarNum = 0
	st.lastStopMS = 0
	st.barSpans = st.barSpans[:0]
	clear(st.openingRanges)
	clear(st.sessionRanges)
	clear(st.prevPeriods)
}

func (st *intradayJobState) syncEnvBarLocked(env *ta.BarEnv) bool {
	o, h, l, c, v, ok := readEnvCurrentOHLCV(env)
	if !ok {
		return false
	}
	if st.lastStopMS > 0 && (env.TimeStop < st.lastStopMS || env.BarNum < st.lastBarNum) {
		st.resetLocked()
	}
	if st.lastStopMS == env.TimeStop && st.lastBarNum == env.BarNum {
		return true
	}
	st.lastBarNum = env.BarNum
	st.lastStopMS = env.TimeStop
	st.barSpans = append(st.barSpans, intradayBarSpan{
		barNum:  env.BarNum,
		startMS: env.TimeStart,
		stopMS:  env.TimeStop,
	})
	if len(st.barSpans) > maxIntradayBarSpanHist {
		excess := len(st.barSpans) - trimIntradayBarSpansTo
		copy(st.barSpans, st.barSpans[excess:])
		st.barSpans = st.barSpans[:trimIntradayBarSpansTo]
	}
	for _, tr := range st.openingRanges {
		tr.UpdateBar(env.TimeStart, env.TimeStop, env.TFMSecs, o, h, l, c, v)
	}
	for _, tr := range st.sessionRanges {
		tr.UpdateBar(env.TimeStart, env.TimeStop, env.TFMSecs, o, h, l, c, v)
	}
	for _, tr := range st.prevPeriods {
		tr.UpdateBar(env.TimeStart, env.TimeStop, env.TFMSecs, o, h, l, c, v)
	}
	return true
}

func (st *intradayJobState) replaySpansLocked(env *ta.BarEnv, feed func(startMS, stopMS, tfMS int64, o, h, l, c, v float64)) {
	if env == nil || env.Close == nil || len(st.barSpans) == 0 {
		return
	}
	avail := env.Close.Len()
	if env.Open == nil || env.High == nil || env.Low == nil || avail == 0 {
		return
	}
	for _, sp := range st.barSpans {
		offset := env.BarNum - sp.barNum
		if offset < 0 || offset >= avail {
			continue
		}
		o := env.Open.Get(offset)
		h := env.High.Get(offset)
		l := env.Low.Get(offset)
		c := env.Close.Get(offset)
		v := 0.0
		if env.Volume != nil && offset < env.Volume.Len() {
			v = env.Volume.Get(offset)
		}
		feed(sp.startMS, sp.stopMS, env.TFMSecs, o, h, l, c, v)
	}
}

func (s *StratJob) observeIntradayEnvBar() {
	if s == nil || s.Env == nil || s.Env.BarNum <= 0 || s.Env.TimeStop <= s.Env.TimeStart {
		return
	}
	st := s.intradayState()
	st.mu.Lock()
	st.syncEnvBarLocked(s.Env)
	st.mu.Unlock()
}

func (st *intradayJobState) ensurePrevPeriodLocked(env *ta.BarEnv, anchorMinutesUTC, minDays int) *PrevPeriodTracker {
	anchor := btime.NormalizeDayMinutes(anchorMinutesUTC)
	if st.prevPeriods == nil {
		st.prevPeriods = make(map[int]*PrevPeriodTracker, 2)
	}
	tr, exists := st.prevPeriods[anchor]
	if !exists {
		capDays := defaultMaxPrevDays
		if minDays > capDays {
			capDays = minDays
		}
		tr = NewPrevPeriodTracker(anchor, capDays)
		st.replaySpansLocked(env, tr.UpdateBar)
		st.prevPeriods[anchor] = tr
		return tr
	}
	if minDays > tr.maxDays {
		// If a later call requests a deeper window than previously retained,
		// rebuild from available cached barSpans when that provides more history.
		tr.ensureCapacity(minDays)
		if len(tr.completedDays) < minDays && len(st.barSpans) > 1 {
			rebuilt := NewPrevPeriodTracker(anchor, tr.maxDays)
			st.replaySpansLocked(env, rebuilt.UpdateBar)
			if len(rebuilt.completedDays) > len(tr.completedDays) {
				tr = rebuilt
				st.prevPeriods[anchor] = tr
			}
		}
	}
	return tr
}

// OpeningRange returns the strict no-lookahead OpeningRangeSnapshot for this
// StratJob on [anchorMinutesUTC, anchorMinutesUTC + rangeMinutes).
// State is stored per StratJob and never shared across symbols or jobs.
func (s *StratJob) OpeningRange(anchorMinutesUTC, rangeMinutes int) OpeningRangeSnapshot {
	return s.OpeningRangeWithOpts(anchorMinutesUTC, rangeMinutes, OpeningRangeOptions{})
}

// OpeningRangeWithOpts returns the no-lookahead OpeningRangeSnapshot for this
// StratJob with explicit OpeningRangeOptions.
func (s *StratJob) OpeningRangeWithOpts(anchorMinutesUTC, rangeMinutes int, opts OpeningRangeOptions) OpeningRangeSnapshot {
	normAnchor := btime.NormalizeDayMinutes(anchorMinutesUTC)
	if s == nil || s.Env == nil {
		return OpeningRangeSnapshot{
			AnchorMinutesUTC:      normAnchor,
			RequestedRangeMinutes: rangeMinutes,
		}
	}
	st := s.intradayState()
	st.mu.Lock()
	defer st.mu.Unlock()
	if !st.syncEnvBarLocked(s.Env) {
		return OpeningRangeSnapshot{
			AnchorMinutesUTC:      normAnchor,
			RequestedRangeMinutes: rangeMinutes,
		}
	}
	key := orbTrackerKey{
		anchorMin:              normAnchor,
		rangeMin:               rangeMinutes,
		allowSingleBarFallback: opts.AllowSingleBarFallback,
	}
	if st.openingRanges == nil {
		st.openingRanges = make(map[orbTrackerKey]*OpeningRangeTracker, 2)
	}
	tr, exists := st.openingRanges[key]
	if !exists {
		tr = NewOpeningRangeTrackerWithOpts(normAnchor, rangeMinutes, opts)
		st.replaySpansLocked(s.Env, func(startMS, stopMS, tfMS int64, o, h, l, c, v float64) {
			tr.UpdateBar(startMS, stopMS, tfMS, o, h, l, c, v)
		})
		st.openingRanges[key] = tr
	}
	return tr.Snapshot()
}

// SessionRange returns the no-lookahead SessionRangeSnapshot for this StratJob
// over the recurring UTC window [startMinUTC, endMinUTC) (e.g. 00:00-08:00 UTC
// Asian Range for London/NY sweep or breakout strategies).
func (s *StratJob) SessionRange(startMinUTC, endMinUTC int) SessionRangeSnapshot {
	sMin := btime.NormalizeDayMinutes(startMinUTC)
	eMin := btime.NormalizeDayMinutes(endMinUTC)
	if s == nil || s.Env == nil {
		return SessionRangeSnapshot{StartMinUTC: sMin, EndMinUTC: eMin}
	}
	st := s.intradayState()
	st.mu.Lock()
	defer st.mu.Unlock()
	if !st.syncEnvBarLocked(s.Env) {
		return SessionRangeSnapshot{StartMinUTC: sMin, EndMinUTC: eMin}
	}
	key := sessionTrackerKey{startMin: sMin, endMin: eMin}
	if st.sessionRanges == nil {
		st.sessionRanges = make(map[sessionTrackerKey]*SessionRangeTracker, 2)
	}
	tr, exists := st.sessionRanges[key]
	if !exists {
		tr = NewSessionRangeTracker(startMinUTC, endMinUTC)
		st.replaySpansLocked(s.Env, func(startMS, stopMS, tfMS int64, o, h, l, c, v float64) {
			tr.UpdateBar(startMS, stopMS, tfMS, o, h, l, c, v)
		})
		st.sessionRanges[key] = tr
	}
	return tr.Snapshot()
}

// PrevDayOHLC returns the previous completed anchor-aligned session day's OHLCV
// (PDO / PDH / PDL / PDC — Fairy Four Prices) for this StratJob without lookahead.
func (s *StratJob) PrevDayOHLC(anchorMinutesUTC int) (PrevOHLC, bool) {
	if s == nil || s.Env == nil {
		return PrevOHLC{}, false
	}
	st := s.intradayState()
	st.mu.Lock()
	defer st.mu.Unlock()
	if !st.syncEnvBarLocked(s.Env) {
		return PrevOHLC{}, false
	}
	tr := st.ensurePrevPeriodLocked(s.Env, anchorMinutesUTC, 1)
	return tr.PrevDayOHLC()
}

// PrevNDaysStats returns multi-day OHLCV statistics (HH, HC, LC, LL, ADR,
// DualThrustRange, History) over the last nDays completed session days for this StratJob.
func (s *StratJob) PrevNDaysStats(anchorMinutesUTC, nDays int) (PrevNDaysSummary, bool) {
	if s == nil || s.Env == nil || nDays <= 0 || nDays > maxAllowedPrevDays {
		return PrevNDaysSummary{}, false
	}
	st := s.intradayState()
	st.mu.Lock()
	defer st.mu.Unlock()
	if !st.syncEnvBarLocked(s.Env) {
		return PrevNDaysSummary{}, false
	}
	tr := st.ensurePrevPeriodLocked(s.Env, anchorMinutesUTC, nDays)
	return tr.PrevNDaysStats(nDays)
}

// DualThrustBands returns today's Dual Thrust upper/lower bands and today's
// anchor open price for this StratJob:
//
//	Range = max(HH - LC, HC - LL) over past nDays completed trading days
//	upper = dayOpen + k1 * Range
//	lower = dayOpen - k2 * Range
func (s *StratJob) DualThrustBands(anchorMinutesUTC, nDays int, k1, k2 float64) (upper, lower, dayOpen float64, ok bool) {
	if s == nil || s.Env == nil || nDays <= 0 || nDays > maxAllowedPrevDays {
		return 0, 0, 0, false
	}
	st := s.intradayState()
	st.mu.Lock()
	defer st.mu.Unlock()
	if !st.syncEnvBarLocked(s.Env) {
		return 0, 0, 0, false
	}
	tr := st.ensurePrevPeriodLocked(s.Env, anchorMinutesUTC, nDays)
	return tr.DualThrustBands(nDays, k1, k2)
}

// RBreakerLevels returns today's six standard R-Breaker levels (BBreak, SSetup,
// SEnter, BEnter, BSetup, SBreak) computed from the previous completed trading
// day's (H, L, C), together with today's running intraday Open/High/Low.
func (s *StratJob) RBreakerLevels(anchorMinutesUTC int) (RBreakerSnap, bool) {
	if s == nil || s.Env == nil {
		return RBreakerSnap{}, false
	}
	st := s.intradayState()
	st.mu.Lock()
	defer st.mu.Unlock()
	if !st.syncEnvBarLocked(s.Env) {
		return RBreakerSnap{}, false
	}
	tr := st.ensurePrevPeriodLocked(s.Env, anchorMinutesUTC, 1)
	return tr.RBreakerLevels()
}

// RBreakerLevelsBy returns today's parameterized R-Breaker levels (f1, f2, f3)
// computed from the previous completed trading day's (H, L, C).
func (s *StratJob) RBreakerLevelsBy(anchorMinutesUTC int, f1, f2, f3 float64) (RBreakerSnap, bool) {
	if s == nil || s.Env == nil {
		return RBreakerSnap{}, false
	}
	st := s.intradayState()
	st.mu.Lock()
	defer st.mu.Unlock()
	if !st.syncEnvBarLocked(s.Env) {
		return RBreakerSnap{}, false
	}
	tr := st.ensurePrevPeriodLocked(s.Env, anchorMinutesUTC, 1)
	return tr.RBreakerLevelsBy(f1, f2, f3)
}

func readEnvCurrentOHLCV(env *ta.BarEnv) (open, high, low, close, volume float64, ok bool) {
	if env == nil || env.BarNum <= 0 || env.TimeStop <= env.TimeStart {
		return 0, 0, 0, 0, 0, false
	}
	if env.Open == nil || env.High == nil || env.Low == nil || env.Close == nil || env.Close.Len() == 0 {
		return 0, 0, 0, 0, 0, false
	}
	open = env.Open.Get(0)
	high = env.High.Get(0)
	low = env.Low.Get(0)
	close = env.Close.Get(0)
	if !validOHLC(open, high, low, close) {
		return 0, 0, 0, 0, 0, false
	}
	if env.Volume != nil && env.Volume.Len() > 0 {
		v := env.Volume.Get(0)
		if !math.IsNaN(v) && !math.IsInf(v, 0) && v >= 0 {
			volume = v
		}
	}
	return open, high, low, close, volume, true
}

func validOHLC(open, high, low, close float64) bool {
	if math.IsNaN(open) || math.IsInf(open, 0) ||
		math.IsNaN(high) || math.IsInf(high, 0) ||
		math.IsNaN(low) || math.IsInf(low, 0) ||
		math.IsNaN(close) || math.IsInf(close, 0) {
		return false
	}
	return high >= low && open > 0 && high > 0 && low > 0 && close > 0
}
