package opt

import (
	"math"
	"testing"

	"github.com/banbox/banbot/utils"
)

const (
	replayHour = int64(3600000)
	replayDay  = 24 * replayHour
	replayS    = 20000 * replayDay // an epoch-day aligned start (also aligned to 4h)
)

// linePrice: close of the base bar starting at start = base + hours since replayS (slope per hour).
func linePrice(base, slope float64) ReplayPriceFunc {
	return func(_, _ string, start int64) (float64, bool) {
		return base + slope*float64(start-replayS)/float64(replayHour), true
	}
}

func near(t *testing.T, name string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Errorf("%s: got %v want %v", name, got, want)
	}
}

// One long order held 7h while the price falls 1/h: the stepwise drawdown, the daily Ulcer inputs and the plot samples
// are checked against numbers worked out by hand.
func TestReplayEquityHandWorkedLong(t *testing.T) {
	p := ReplayParams{
		Orders: []ReplayOrder{{Symbol: "X", TF: "1h", EntryAvg: 100, Filled: 1, EntryFee: 0.1, Profit: -10.2,
			CreateMS: replayS + 3*replayHour, EnterMS: replayS + 3*replayHour, ExitMS: replayS + 10*replayHour}},
		Init: 1000, StartMS: replayS, EndMS: replayS + 2*replayDay, StepMS: replayHour,
		PlotTimes:    []int64{replayS + 3*replayHour, replayS + 9*replayHour, replayS + 12*replayHour, replayS + 2*replayDay},
		FirstSymbols: map[string]bool{"X": true}, Price: linePrice(100, -1),
	}
	r, err := ReplayEquity(p)
	if err != nil || r.NeedsRerun {
		t.Fatalf("%v %+v", err, r)
	}
	// T=3h: first event of the entry step, price of the bar that started at 2h (98): (98-100)*1 - 2*0.1
	// T=9h: bar 8h -> 92: -8.2; from T=10h the order is closed: 1000-10.2
	want := []float64{997.8, 991.8, 989.8, 989.8}
	for i, w := range want {
		near(t, "plot", r.PlotReal[i], w)
	}
	near(t, "min equity", r.MinEquity, 989.8)
	near(t, "max dd pct", r.MaxDrawDownPct, 1.02)
	near(t, "max dd val", r.MaxDrawDownVal, 10.2)
	near(t, "show dd pct", r.ShowDrawDownPct, (997.8-989.8)/997.8*100)
	wantDaily := [][2]float64{{float64(replayS), 1000}, {float64(replayS + replayDay), 989.8}, {float64(replayS + 2*replayDay), 989.8}}
	if len(r.DailyEquity) != len(wantDaily) {
		t.Fatalf("daily equity %v", r.DailyEquity)
	}
	for i, d := range wantDaily {
		near(t, "daily ms", r.DailyEquity[i][0], d[0])
		near(t, "daily eq", r.DailyEquity[i][1], d[1])
	}
	years := float64(2*replayDay) / (utils.UlcerYearDays * float64(replayDay))
	ui, upi, err := utils.CalcUlcerPerformance([]float64{1000, 989.8, 989.8}, years, 0)
	if err != nil || r.UPI == nil || r.UlcerIndex == nil {
		t.Fatalf("ulcer %v %v", err, r)
	}
	near(t, "ui", *r.UlcerIndex, *ui)
	near(t, "upi", *r.UPI, *upi)
	if *r.UPI >= 0 {
		t.Errorf("a losing run has a negative UPI, got %v", *r.UPI)
	}
}

// A symbol that is not the step's first event: its entry / exit of the current step are not counted yet, its price is
// the previous bar's close.
func TestReplayEquityLateSymbol(t *testing.T) {
	p := ReplayParams{
		Orders: []ReplayOrder{{Symbol: "B", TF: "1h", EntryAvg: 100, Filled: 1, EntryFee: 0.1, Profit: 4.8,
			CreateMS: replayS + 3*replayHour, EnterMS: replayS + 3*replayHour, ExitMS: replayS + 5*replayHour}},
		Init: 1000, StartMS: replayS, EndMS: replayS + replayDay, StepMS: replayHour,
		PlotTimes:    []int64{replayS + 3*replayHour, replayS + 4*replayHour, replayS + 5*replayHour, replayS + 6*replayHour},
		FirstSymbols: map[string]bool{"A": true}, Price: linePrice(100, 1),
	}
	r, err := ReplayEquity(p)
	if err != nil {
		t.Fatal(err)
	}
	// 3h: not entered yet; 4h: the late symbol shows the previous bar, the one that started at 2h closes at 102: 2-0.2;
	// 5h (its exit step): still open, bar from 3h closes at 103: 3-0.2; 6h: closed, +4.8
	want := []float64{1000, 1001.8, 1002.8, 1004.8}
	for i, w := range want {
		near(t, "plot", r.PlotReal[i], w)
	}
}

// A 4h order on 1h steps is only marked on a close of its own 4h bars.
func TestReplayEquity4hOrderMarksOnOwnBars(t *testing.T) {
	p := ReplayParams{
		Orders: []ReplayOrder{{Symbol: "X", TF: "4h", EntryAvg: 100, Filled: 2, EntryFee: 0.5, Profit: 10,
			CreateMS: replayS + 5*replayHour, EnterMS: replayS + 5*replayHour, ExitMS: replayS + 12*replayHour}},
		Init: 1000, StartMS: replayS, EndMS: replayS + replayDay, StepMS: replayHour,
		PlotTimes:    []int64{replayS + 7*replayHour, replayS + 8*replayHour, replayS + 11*replayHour, replayS + 12*replayHour},
		FirstSymbols: map[string]bool{"X": true}, Price: linePrice(100, 1),
	}
	r, err := ReplayEquity(p)
	if err != nil {
		t.Fatal(err)
	}
	// before the first 4h close after the entry (8h) the Profit is 0; at 8h the 1h bar that started at 7h closes at 107:
	// (107-100)*2 - 2*0.5 = 13, unchanged until 12h; then the realized profit
	want := []float64{1000, 1013, 1013, 1010}
	for i, w := range want {
		near(t, "plot", r.PlotReal[i], w)
	}
	if ReplayBaseTF("4h") != "1h" || ReplayBaseTF("3d") != "1d" || ReplayBaseTF("15m") != "15m" || ReplayBaseTF("30m") != "15m" ||
		ReplayBaseTF("5m") != "5m" || ReplayStepMS("8h") != replayHour {
		t.Error("base timeframes")
	}
}

// A replayed equity <= 0 means the real run was liquidated (its orders would differ): no metric, a rerun marker.
func TestReplayEquityNeedsRerun(t *testing.T) {
	p := ReplayParams{
		Orders: []ReplayOrder{{Symbol: "X", TF: "1h", EntryAvg: 100, Filled: 20, Profit: -200,
			CreateMS: replayS + 3*replayHour, EnterMS: replayS + 3*replayHour, ExitMS: replayS + 10*replayHour}},
		Init: 100, StartMS: replayS, EndMS: replayS + replayDay, StepMS: replayHour,
		FirstSymbols: map[string]bool{"X": true}, Price: linePrice(100, -1),
	}
	r, err := ReplayEquity(p)
	if err != nil {
		t.Fatal(err)
	}
	if !r.NeedsRerun || r.UPI != nil || r.MinEquity > 0 {
		t.Fatalf("expected a rerun marker, got %+v", r)
	}
	if _, err = ReplayEquity(ReplayParams{Init: 0}); err == nil {
		t.Error("invalid input must be an error")
	}
}
