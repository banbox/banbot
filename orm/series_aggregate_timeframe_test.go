package orm

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/banbox/banexg"
	utils2 "github.com/banbox/banexg/utils"
	"github.com/jackc/pgx/v5"
)

// Aggregated timeframes (4h/8h/3d...) are not stored: portfolio (factor/statarb)
// storage reads go through QuerySeriesFields with a field projection, exactly
// like stored timeframes. These tests pin that the aggregated bars equal the
// ones the time-series engine builds from the same stored sub-timeframe rows.

const (
	aggHourMS = int64(3600 * 1000)
	aggDayMS  = 24 * aggHourMS
)

// aggT0 is 2024-01-01 00:00 UTC: a 3d bucket start (origin 1970-01-02) and a multiple of 8h/4h.
const aggT0 = int64(1_704_067_200_000)

type aggFakeBar struct {
	timeMS                         int64
	open, high, low, close, volume float64
	quote, buyVolume               float64
	tradeNum                       int64
	extra                          map[string]any
}

func aggFakeHours(startMS int64, n int) []aggFakeBar {
	bars := make([]aggFakeBar, n)
	for i := range bars {
		base := 100 + 10*math.Sin(float64(i)/7) + float64(i)*0.01
		bars[i] = aggFakeBar{
			timeMS: startMS + int64(i)*aggHourMS,
			open:   base, high: base + 1 + float64(i%5)*0.1, low: base - 1 - float64(i%3)*0.1, close: base + 0.3*math.Cos(float64(i)),
			volume: 10 + float64(i%11), quote: 1000 + float64(i), buyVolume: 5 + float64(i%4), tradeNum: int64(100 + i),
		}
	}
	return bars
}

// aggFakeDays builds the complete UTC days of kline_1d from the 1h rows (an incomplete day is not stored).
func aggFakeDays(hours []aggFakeBar) []aggFakeBar {
	var days []aggFakeBar
	for i := 0; i < len(hours); {
		day := hours[i].timeMS / aggDayMS * aggDayMS
		j := i
		for j < len(hours) && hours[j].timeMS < day+aggDayMS {
			j++
		}
		g := hours[i:j]
		if int64(len(g)) == 24 && g[0].timeMS == day {
			bar := aggFakeBar{timeMS: day, open: g[0].open, high: g[0].high, low: g[0].low, close: g[23].close}
			for _, b := range g {
				bar.high, bar.low = math.Max(bar.high, b.high), math.Min(bar.low, b.low)
				bar.volume += b.volume
				bar.quote += b.quote
				bar.buyVolume += b.buyVolume
				bar.tradeNum += b.tradeNum
			}
			days = append(days, bar)
		}
		i = j
	}
	return days
}

type aggFakeDB struct {
	DBTX
	quest bool
	hours []aggFakeBar
	calls int
}

var (
	aggSQLProjection = regexp.MustCompile(`(?is)select\s+(?:cast\(ts as long\)/1000|time),(.*?)\s+from`)
	aggSQLLower      = regexp.MustCompile(`(?i)(?:time|ts)\s*>=\s*(?:cast\()?(\d+)`)
	aggSQLUpper      = regexp.MustCompile(`(?i)(?:time|ts)\s*<\s*(?:cast\()?(\d+)`)
	aggSQLLimit      = regexp.MustCompile(`(?i)limit\s+(\d+)`)
)

func (d *aggFakeDB) Query(_ context.Context, sql string, _ ...interface{}) (pgx.Rows, error) {
	d.calls++
	table := "kline_1h"
	source := d.hours
	if strings.Contains(sql, "kline_1d") {
		table, source = "kline_1d", aggFakeDays(d.hours)
	}
	if !strings.Contains(sql, table) {
		return nil, fmt.Errorf("fake db serves only kline_1h/kline_1d, got: %s", sql)
	}
	cols := []string{}
	for _, c := range strings.Split(aggSQLProjection.FindStringSubmatch(sql)[1], ",") {
		cols = append(cols, strings.Trim(strings.TrimSpace(c), `"`))
	}
	scale := int64(1)
	if d.quest {
		scale = 1000
	}
	lo, _ := strconv.ParseInt(aggSQLLower.FindStringSubmatch(sql)[1], 10, 64)
	hi, _ := strconv.ParseInt(aggSQLUpper.FindStringSubmatch(sql)[1], 10, 64)
	lo, hi = lo/scale, hi/scale
	limit := math.MaxInt
	if m := aggSQLLimit.FindStringSubmatch(sql); m != nil {
		limit, _ = strconv.Atoi(m[1])
	}
	out := [][]any{}
	for _, b := range source {
		if b.timeMS < lo || b.timeMS >= hi {
			continue
		}
		if len(out) >= limit {
			break
		}
		row := []any{b.timeMS}
		for _, c := range cols {
			switch c {
			case "open":
				row = append(row, b.open)
			case "high":
				row = append(row, b.high)
			case "low":
				row = append(row, b.low)
			case "close":
				row = append(row, b.close)
			case "volume":
				row = append(row, b.volume)
			case "sid":
				row = append(row, int32(7))
			case "quote":
				row = append(row, b.quote)
			case "buy_volume":
				row = append(row, b.buyVolume)
			case "trade_num":
				row = append(row, b.tradeNum)
			default:
				return nil, fmt.Errorf("unexpected column %q", c)
			}
		}
		out = append(out, row)
	}
	return newInterfaceRows(out), nil
}

func aggQueries(t *testing.T, quest bool, hours []aggFakeBar) (*Queries, *ExSymbol, *aggFakeDB) {
	t.Helper()
	exs := &ExSymbol{ID: 7, Exchange: "runtime", Market: banexg.MarketLinear, Symbol: "BTC/USDT:USDT"}
	db := &aggFakeDB{quest: quest, hours: hours}
	exchange := &localReadExchange{info: &banexg.ExgInfo{ID: exs.Exchange, MarketType: exs.Market}, market: &banexg.Market{Symbol: exs.Symbol}}
	endMS := hours[len(hours)-1].timeMS + aggHourMS
	q := NewWithStorage(db, NewStorage(nil, quest, t.Name())).WithExchange(exchange).
		WithKlineRuntimeOptions(KlineRuntimeOptions{Backtest: true, NowMS: endMS, ClockValid: true})
	return q, exs, db
}

func aggVal(t *testing.T, row *DataSeries, field string) float64 {
	t.Helper()
	v, err := row.FloatValue(field)
	if err != nil {
		t.Fatalf("bar %d: %v", row.TimeMS, err)
	}
	return v
}

// aggExpected builds the reference bars by hand from complete 1h buckets.
func aggExpected(hours []aggFakeBar, tfMS, startMS, endMS int64) []map[string]float64 {
	_, off := utils2.GetTfAlignOrigin(int(tfMS / 1000))
	offMS := int64(off) * 1000
	out := []map[string]float64{}
	groups := map[int64][]aggFakeBar{}
	keys := []int64{}
	for _, b := range hours {
		if b.timeMS < startMS || b.timeMS >= endMS {
			continue
		}
		k := utils2.AlignTfMSecsOffset(b.timeMS, tfMS, offMS)
		if _, ok := groups[k]; !ok {
			keys = append(keys, k)
		}
		groups[k] = append(groups[k], b)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for _, k := range keys {
		g := groups[k]
		if int64(len(g))*aggHourMS != tfMS {
			continue // only complete buckets
		}
		bar := map[string]float64{"time": float64(k), "open": g[0].open, "high": g[0].high, "low": g[0].low, "close": g[len(g)-1].close}
		for _, b := range g {
			bar["high"] = math.Max(bar["high"], b.high)
			bar["low"] = math.Min(bar["low"], b.low)
			bar["volume"] += b.volume
		}
		out = append(out, bar)
	}
	return out
}

func TestAggregatedTimeframeProjectionMatchesFullRead(t *testing.T) {
	hours := aggFakeHours(aggT0, 24*30)
	for _, quest := range []bool{true, false} {
		for _, tf := range []string{"30m", "2h", "4h", "8h", "12h", "3d"} {
			if tf == "30m" {
				continue // sub-hour needs kline_15m rows; covered by ValidateKlineTimeframe
			}
			t.Run(fmt.Sprintf("quest=%v/%s", quest, tf), func(t *testing.T) {
				q, exs, _ := aggQueries(t, quest, hours)
				tfMS := int64(utils2.TFToSecs(tf)) * 1000
				endMS := aggT0 + 30*aggDayMS
				full, err := q.QuerySeriesFields(exs, tf, nil, aggT0, endMS, 0, false)
				if err != nil {
					t.Fatal(err)
				}
				closeOnly, err := q.QuerySeriesFields(exs, tf, []string{"close"}, aggT0, endMS, 0, false)
				if err != nil {
					t.Fatalf("projected aggregated read failed (the original 'series event missing field open'): %v", err)
				}
				if len(full) == 0 || len(full) != len(closeOnly) {
					t.Fatalf("full=%d projected=%d bars", len(full), len(closeOnly))
				}
				for i := range full {
					if full[i].TimeMS != closeOnly[i].TimeMS || full[i].EndMS != closeOnly[i].EndMS {
						t.Fatalf("bar %d time mismatch", i)
					}
					if full[i].EndMS != full[i].TimeMS+tfMS || full[i].TimeFrame != tf || !full[i].Closed {
						t.Fatalf("bar %d metadata: %+v", i, full[i])
					}
					if aggVal(t, full[i], "close") != aggVal(t, closeOnly[i], "close") {
						t.Fatalf("bar %d close differs between projections", i)
					}
					if len(closeOnly[i].Values) != 1 {
						t.Fatalf("projection leaked helper columns: %v", closeOnly[i].Values)
					}
				}
				// hand-built reference: complete buckets, UTC aligned (3d origin 1970-01-02)
				want := aggExpected(hours, tfMS, aggT0, endMS)
				if len(want) != len(full) {
					t.Fatalf("bars=%d want %d", len(full), len(want))
				}
				for i, w := range want {
					row := full[i]
					if float64(row.TimeMS) != w["time"] {
						t.Fatalf("bar %d start=%d want %v", i, row.TimeMS, w["time"])
					}
					for _, f := range []string{"open", "high", "low", "close", "volume"} {
						if got := aggVal(t, row, f); math.Abs(got-w[f]) > 1e-9 {
							t.Fatalf("bar %d %s=%v want %v", i, f, got, w[f])
						}
					}
					if tf == "3d" {
						if (row.TimeMS-aggDayMS)%(3*aggDayMS) != 0 {
							t.Fatalf("3d bar %d not anchored at 1970-01-02: %d", i, row.TimeMS)
						}
					} else if row.TimeMS%tfMS != 0 {
						t.Fatalf("%s bar %d not UTC aligned: %d", tf, i, row.TimeMS)
					}
				}
			})
		}
	}
}

func TestAggregatedTimeframeMatchesIncrementalResample(t *testing.T) {
	// The time-series feeder resamples 1h rows incrementally (prev chain); the
	// closed bars must equal the portfolio read.
	hours := aggFakeHours(aggT0, 24*20)
	q, exs, _ := aggQueries(t, false, hours)
	endMS := aggT0 + 20*aggDayMS
	for _, tf := range []string{"4h", "8h", "3d"} {
		tfMS := int64(utils2.TFToSecs(tf)) * 1000
		read, err := q.QuerySeriesFields(exs, tf, []string{"close"}, aggT0, endMS, 0, false)
		if err != nil {
			t.Fatal(err)
		}
		all, err := q.QuerySeriesFields(exs, "1h", nil, aggT0, endMS, 0, false)
		if err != nil {
			t.Fatal(err)
		}
		var prev []*DataSeries
		for _, row := range all {
			next, _, err := ResampleDataSeries(exs, tf, []*DataSeries{row}, prev, tfMS, 0, aggHourMS, 0, false)
			if err != nil {
				t.Fatal(err)
			}
			prev = next
		}
		// the last bar of the chain is the open (or just closed) one; compare closed bars
		got := prev
		if len(got) < len(read) {
			t.Fatalf("%s: incremental %d < read %d", tf, len(got), len(read))
		}
		for i, row := range read {
			if got[i].TimeMS != row.TimeMS || aggVal(t, got[i], "close") != aggVal(t, row, "close") {
				t.Fatalf("%s bar %d: incremental (%d,%v) vs read (%d,%v)", tf, i, got[i].TimeMS, got[i].Values["close"], row.TimeMS, row.Values["close"])
			}
		}
	}
}

func TestAggregatedTimeframePagesConcatenateToFullRead(t *testing.T) {
	hours := aggFakeHours(aggT0, 24*60)
	// punch holes so pages end mid-bucket
	hours = append(hours[:50:50], hours[53:]...)
	hours = append(hours[:300:300], hours[301:]...)
	endMS := aggT0 + 60*aggDayMS
	for _, quest := range []bool{true, false} {
		for _, tf := range []string{"4h", "8h", "3d"} {
			q, exs, _ := aggQueries(t, quest, hours)
			full, err := q.QuerySeriesFields(exs, tf, []string{"close"}, aggT0, endMS, 0, false)
			if err != nil {
				t.Fatal(err)
			}
			for _, limit := range []int{1, 3, 7, 50} {
				var paged []*DataSeries
				offset := aggT0
				for guard := 0; guard < 1000; guard++ {
					rows, err := q.QuerySeriesFields(exs, tf, []string{"close"}, offset, endMS, limit, false)
					if err != nil {
						t.Fatal(err)
					}
					if len(rows) == 0 {
						break
					}
					if len(rows) > limit {
						t.Fatalf("page exceeded limit: %d > %d", len(rows), limit)
					}
					paged = append(paged, rows...)
					offset = rows[len(rows)-1].TimeMS + 1
				}
				if len(paged) != len(full) {
					t.Fatalf("quest=%v %s limit=%d: paged %d bars, full %d", quest, tf, limit, len(paged), len(full))
				}
				for i := range full {
					if paged[i].TimeMS != full[i].TimeMS || aggVal(t, paged[i], "close") != aggVal(t, full[i], "close") {
						t.Fatalf("quest=%v %s limit=%d: bar %d differs (%d vs %d)", quest, tf, limit, i, paged[i].TimeMS, full[i].TimeMS)
					}
				}
			}
		}
	}
}

func TestAggregatedTimeframeIncompleteBuckets(t *testing.T) {
	hours := aggFakeHours(aggT0, 24*9)
	// trailing partial bucket: the last 4h bucket only has 2 of its 4 hours -> dropped
	hours = hours[:len(hours)-2]
	q, exs, _ := aggQueries(t, false, hours)
	endMS := aggT0 + 9*aggDayMS
	rows, err := q.QuerySeriesFields(exs, "4h", []string{"close"}, aggT0, endMS, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	wantBars := (24*9 - 4) / 4
	if len(rows) != wantBars {
		t.Fatalf("4h bars=%d want %d (trailing partial bucket must not be emitted)", len(rows), wantBars)
	}
	if last := rows[len(rows)-1]; last.EndMS > hours[len(hours)-1].timeMS+aggHourMS {
		t.Fatalf("last bar %d ends after the last source row: lookahead", last.EndMS)
	}
	// the 3d bucket not fully covered by the data is not emitted either
	rows, err = q.QuerySeriesFields(exs, "3d", []string{"close"}, aggT0, endMS, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("3d bars=%d want 2 (third bucket is incomplete)", len(rows))
	}
}

func TestAggregatedTimeframeHasNoLookahead(t *testing.T) {
	hours := aggFakeHours(aggT0, 24*12)
	q, exs, _ := aggQueries(t, false, hours)
	endMS := aggT0 + 12*aggDayMS
	for _, tf := range []string{"4h", "8h", "3d"} {
		base, err := q.QuerySeriesFields(exs, tf, []string{"close", "high"}, aggT0, endMS, 0, false)
		if err != nil {
			t.Fatal(err)
		}
		cut := base[len(base)/2].EndMS // decision time of a middle bar
		altered := append([]aggFakeBar(nil), hours...)
		for i := range altered {
			if altered[i].timeMS >= cut { // rewrite everything the bar could not have seen yet
				altered[i].close, altered[i].high, altered[i].low, altered[i].open, altered[i].volume = 1, 2, 0.5, 1, 99
			}
		}
		q2, exs2, _ := aggQueries(t, false, altered)
		again, err := q2.QuerySeriesFields(exs2, tf, []string{"close", "high"}, aggT0, endMS, 0, false)
		if err != nil {
			t.Fatal(err)
		}
		for i := range base {
			if base[i].EndMS > cut {
				break
			}
			if again[i].TimeMS != base[i].TimeMS || aggVal(t, again[i], "close") != aggVal(t, base[i], "close") || aggVal(t, again[i], "high") != aggVal(t, base[i], "high") {
				t.Fatalf("%s: bar %d (end %d <= cut %d) changed when only later data changed", tf, i, base[i].EndMS, cut)
			}
		}
	}
}

func TestAggregatedTimeframeBatchProjection(t *testing.T) {
	hours := aggFakeHours(aggT0, 24*10)
	q, exs, _ := aggQueries(t, false, hours)
	endMS := aggT0 + 10*aggDayMS
	want, err := q.QuerySeriesFields(exs, "8h", []string{"close"}, aggT0, endMS, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	var got []*DataSeries
	err = q.querySeriesBatchFieldsRaw(map[int32]*ExSymbol{exs.ID: exs}, "8h", []string{"close"}, aggT0, endMS, 0,
		func(_ int32, rows []*DataSeries) { got = rows })
	if err != nil {
		t.Fatalf("batch aggregated read: %v", err)
	}
	if len(got) != len(want) || len(got[0].Values) != 1 {
		t.Fatalf("batch rows=%d want %d values=%v", len(got), len(want), got[0].Values)
	}
	for i := range want {
		if got[i].TimeMS != want[i].TimeMS || aggVal(t, got[i], "close") != aggVal(t, want[i], "close") {
			t.Fatalf("batch bar %d differs", i)
		}
	}
}

func TestStoredTimeframeReadIsNotWidened(t *testing.T) {
	// stored timeframes keep returning exactly the requested projection from the table itself
	hours := aggFakeHours(aggT0, 48)
	q, exs, db := aggQueries(t, false, hours)
	rows, err := q.QuerySeriesFields(exs, "1h", []string{"close"}, aggT0, aggT0+48*aggHourMS, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 48 || len(rows[0].Values) != 1 || db.calls != 1 {
		t.Fatalf("stored read rows=%d values=%v calls=%d", len(rows), rows[0].Values, db.calls)
	}
}

func TestValidateKlineTimeframe(t *testing.T) {
	for _, tf := range []string{"1m", "5m", "15m", "1h", "1d", "30m", "2h", "4h", "8h", "12h", "3d", "1w"} {
		if err := ValidateKlineTimeframe(tf); err != nil {
			t.Errorf("%s should be supported: %v", tf, err)
		}
	}
	for _, tf := range []string{"", "abc", "30s", "90s", "0h"} {
		err := ValidateKlineTimeframe(tf)
		if err == nil {
			t.Errorf("%q should be rejected", tf)
			continue
		}
		if tf == "30s" || tf == "90s" {
			if !strings.Contains(err.Error(), "1m, 5m, 15m, 1h, 1d") {
				t.Errorf("%q error must list the stored set: %v", tf, err)
			}
		}
	}
	if !IsStoredKlineTimeframe("1h") || IsStoredKlineTimeframe("4h") {
		t.Fatal("IsStoredKlineTimeframe")
	}
}

func TestAggregatedWarmupCoversRequestedBars(t *testing.T) {
	// SubscriptionWarmupStart = anchor - N*tf; the aggregated reader aligns the
	// start up to a bucket boundary, so N complete buckets precede the first
	// in-range bucket even when the anchor is not bucket aligned (e.g. 3d).
	hours := aggFakeHours(aggT0, 24*60)
	q, exs, _ := aggQueries(t, false, hours)
	anchor := aggT0 + 20*aggDayMS + 5*aggHourMS // unaligned for 3d and 4h
	for _, tf := range []string{"4h", "8h", "3d"} {
		tfMS := int64(utils2.TFToSecs(tf)) * 1000
		const warm = 5
		start, err := SubscriptionWarmupStart(Subscription{Source: SeriesSourceKline, TimeFrame: tf, ExSymbol: exs, WarmupNum: warm}, anchor)
		if err != nil {
			t.Fatal(err)
		}
		rows, readErr := q.QuerySeriesFields(exs, tf, []string{"close"}, start, anchor+10*aggDayMS, 0, false)
		if readErr != nil {
			t.Fatal(readErr)
		}
		before := 0
		for _, r := range rows {
			if r.TimeMS < anchor && r.EndMS <= anchor+tfMS { // buckets starting before the anchor
				before++
			}
		}
		firstInRange := int64(-1)
		for _, r := range rows {
			if r.TimeMS >= anchor {
				firstInRange = r.TimeMS
				break
			}
		}
		if firstInRange < 0 || before != warm {
			t.Fatalf("%s: %d bars start before the anchor, want %d (first in-range %d)", tf, before, warm, firstInRange)
		}
	}
}
