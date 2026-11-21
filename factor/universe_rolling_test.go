package factor

import (
	"math"
	"slices"
	"testing"
	"time"

	"github.com/banbox/banbot/orm"
)

func TestRollingUniverseConfigValidationAndDefaults(t *testing.T) {
	norm, err := NormalizeRollingUniverseConfig(RollingUniverseConfig{TopN: 30})
	if err != nil {
		t.Fatal(err)
	}
	if !norm.Enabled() || norm.Mode != "rolling_liquid_top" || norm.LookbackDays != DefaultRollingLookbackDays || norm.MinListingDays != DefaultRollingMinListingDays || norm.ReconstituteDays != DefaultRollingReconstituteDays {
		t.Fatalf("unexpected defaults: %+v", norm)
	}

	barNorm, err := NormalizeRollingUniverseConfig(RollingUniverseConfig{TopN: 5, LookbackBars: 10, MinAgeBars: 4, ReconstituteBars: 12, AlwaysInclude: []string{" btc ", "BTC", "ETH/USDT:USDT"}})
	if err != nil {
		t.Fatal(err)
	}
	if barNorm.LookbackDays != 0 || barNorm.LookbackBars != 10 || barNorm.MinAgeBars != 4 || barNorm.ReconstituteBars != 12 || len(barNorm.AlwaysInclude) != 2 {
		t.Fatalf("unexpected bar-mode config: %+v", barNorm)
	}

	staticNorm, err := NormalizeRollingUniverseConfig(RollingUniverseConfig{Mode: "static"})
	if err != nil || staticNorm.Enabled() {
		t.Fatalf("static mode must be disabled without error: %+v err=%v", staticNorm, err)
	}

	invalidCases := []RollingUniverseConfig{
		{Mode: "rolling_liquid_top", TopN: 0},
		{Mode: "static", TopN: 10},
		{Mode: "unknown_mode", TopN: 10},
		{TopN: 10, LookbackDays: 10, LookbackBars: 10},
		{TopN: 10, MinListingDays: 7, MinAgeBars: 7},
		{TopN: 10, ReconstituteDays: 7, ReconstituteBars: 7},
		{TopN: 10, ReconstituteDays: 7, Schedule: "monthly"},
		{TopN: 10, Schedule: "quarterly"},
		{TopN: 10, AlwaysInclude: []string{" "}},
	}
	for i, tc := range invalidCases {
		if _, err := NormalizeRollingUniverseConfig(tc); err == nil {
			t.Fatalf("case %d (%+v) expected error", i, tc)
		}
	}

	sidMap := map[int32]string{1: "BTC/USDT:USDT", 2: "ETH/USDT:USDT", 3: "SOL/USDT:USDT"}
	sids, err := ResolveAlwaysIncludeSIDs([]string{"btc", "SOL/USDT:USDT"}, []int32{1, 2, 3}, sidMap)
	if err != nil || !slices.Equal(sids, []int32{1, 3}) {
		t.Fatalf("ResolveAlwaysIncludeSIDs got %v err=%v", sids, err)
	}
	if _, err := ResolveAlwaysIncludeSIDs([]string{"DOGE"}, []int32{1, 2, 3}, sidMap); err == nil {
		t.Fatal("expected missing always_include symbol error")
	}
}

func makeRollingKlineRecord(sid int32, openMS, endMS int64, closePrice, volume, quote float64) VersionRecord {
	vals := map[string]any{"close": closePrice, "volume": volume}
	if quote > 0 {
		vals["quote"] = quote
	}
	return VersionRecord{
		Series: orm.DataSeries{
			Source:    orm.SeriesSourceKline,
			Sid:       sid,
			TimeFrame: "1d",
			TimeMS:    openMS,
			EndMS:     endMS,
			Closed:    true,
			Values:    vals,
		},
		EventTime:     endMS,
		Revision:      1,
		AvailableAt:   endMS,
		IngestedAt:    endMS,
		SourceVersion: "v1",
	}
}

func TestRollingUniverseSelectorNoLookaheadTieBreakAndSchedules(t *testing.T) {
	day := int64(86_400_000)
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	sidMap := map[int32]string{
		1: "AAA/USDT:USDT",
		2: "BBB/USDT:USDT",
		3: "CCC/USDT:USDT",
		4: "CORE/USDT:USDT",
	}
	base := Universe{Version: "u0", Investable: []int32{1, 2, 3, 4}, Reference: []int32{1, 2, 3, 4}, Tradable: []int32{1, 2, 3, 4}, Evaluation: []int32{1, 2, 3, 4}, Tracked: []int32{1, 2, 3, 4}}
	cfg := RollingUniverseConfig{
		Mode:           "rolling_liquid_top",
		TopN:           2,
		LookbackDays:   5,
		MinListingDays: 5,
		Schedule:       "monthly",
		AlwaysInclude:  []string{"CORE"},
	}

	selA, err := NewRollingUniverseSelector(cfg, base, sidMap, day, orm.SeriesSourceKline, "1d")
	if err != nil {
		t.Fatal(err)
	}
	selB, err := NewRollingUniverseSelector(cfg, base, sidMap, day, orm.SeriesSourceKline, "1d")
	if err != nil {
		t.Fatal(err)
	}

	// Seed 5 days of pre-2024-01-01 history for SIDs 1, 2, 4 (SID 3 lists later on Jan 28 -> only 4 days old on Feb 1!).
	for d := -5; d < 0; d++ {
		openMS := t0 + int64(d)*day
		endMS := openMS + day
		recs := []VersionRecord{
			// AAA and BBB tie on turnover (1000 = 10 * 100 fallback vs explicit quote 1000): AAA wins tie-break!
			makeRollingKlineRecord(1, openMS, endMS, 10, 100, 0),
			makeRollingKlineRecord(2, openMS, endMS, 20, 50, 1000),
			makeRollingKlineRecord(4, openMS, endMS, 5, 1, 5), // tiny turnover, kept via AlwaysInclude
		}
		// Inject future post-cutoff bars into selB to verify Observe strictly ignores EventTime > gridMS / AvailableAt > decisionMS.
		future := makeRollingKlineRecord(3, t0+10*day, t0+11*day, 100, 1e9, 1e11)
		selA.Observe(recs, endMS, endMS, 0)
		selB.Observe(append(slices.Clone(recs), future), endMS, endMS, 0)
	}

	// Jan 1 close (grid = t0 + day): first monthly reconstitution -> Top-2 [AAA, BBB] + AlwaysInclude [CORE] = [1, 2, 4].
	jan1Close := t0 + day
	jan1Recs := []VersionRecord{
		makeRollingKlineRecord(1, t0, jan1Close, 10, 100, 0),
		makeRollingKlineRecord(2, t0, jan1Close, 20, 40, 800), // AAA=1000, BBB < AAA now
		makeRollingKlineRecord(4, t0, jan1Close, 5, 1, 5),
	}
	selA.Observe(jan1Recs, jan1Close, jan1Close, 0)
	selB.Observe(append(slices.Clone(jan1Recs), makeRollingKlineRecord(3, jan1Close, jan1Close+day, 100, 1e9, 1e11)), jan1Close, jan1Close, 0)

	aliveJan := map[int32]bool{1: true, 2: true, 4: true}
	uA, recA := selA.Select(jan1Close, aliveJan, false)
	uB, recB := selB.Select(jan1Close, aliveJan, false)
	if !recA || !recB || !slices.Equal(uA.Investable, []int32{1, 2, 4}) || !slices.Equal(uA.Investable, uB.Investable) {
		t.Fatalf("Jan 1 reconstitution mismatch: uA=%v uB=%v", uA.Investable, uB.Investable)
	}

	// Advance through January: CCC lists on Jan 28 (open = Jan 28, close = Jan 29) with massive turnover 1e8.
	for d := 1; d < 31; d++ {
		openMS := t0 + int64(d)*day
		endMS := openMS + day
		recs := []VersionRecord{
			makeRollingKlineRecord(1, openMS, endMS, 10, 10, 100), // AAA turnover drops to 100
			makeRollingKlineRecord(2, openMS, endMS, 20, 50, 1000),
			makeRollingKlineRecord(4, openMS, endMS, 5, 1, 5),
		}
		alive := map[int32]bool{1: true, 2: true, 4: true}
		if d >= 27 { // Jan 28 open .. Jan 31 open -> 4 daily bars by Feb 1 00:00 UTC (< 5 min_listing_days)
			recs = append(recs, makeRollingKlineRecord(3, openMS, endMS, 50, 2e6, 1e8))
			alive[3] = true
		}
		selA.Observe(recs, endMS, endMS, 0)
		u, rec := selA.Select(endMS, alive, false)
		// Note endMS = Jan 31 + 1d = Feb 1 00:00 UTC on d == 30!
		if d < 30 {
			if rec || !slices.Equal(u.Investable, []int32{1, 2, 4}) {
				t.Fatalf("day %d mid-month changed universe: rec=%v inv=%v", d, rec, u.Investable)
			}
		} else {
			// Feb 1 00:00 UTC: monthly reconstitution triggers, but CCC only has 4 days (< min_listing_days=5), so CCC is STILL excluded!
			if !rec || slices.Contains(u.Investable, int32(3)) || !slices.Equal(u.Investable, []int32{1, 2, 4}) {
				t.Fatalf("Feb 1 should exclude young CCC (age 4d < 5d): rec=%v inv=%v", rec, u.Investable)
			}
			// Yet CCC IS in Tracked so its TS indicators warm up throughout February!
			if !slices.Contains(u.Tracked, int32(3)) {
				t.Fatalf("Feb 1 Tracked must include alive CCC for warmup: %v", u.Tracked)
			}
		}
	}

	// Advance through February (29 days in 2024 leap year) to March 1 00:00 UTC: CCC now > 30 days old, enters Top-2, AAA drops out!
	var mar1Univ Universe
	var mar1Rec bool
	for d := 31; d <= 60; d++ {
		openMS := t0 + int64(d)*day
		endMS := openMS + day
		recs := []VersionRecord{
			makeRollingKlineRecord(1, openMS, endMS, 10, 5, 50),
			makeRollingKlineRecord(2, openMS, endMS, 20, 50, 1000),
			makeRollingKlineRecord(3, openMS, endMS, 50, 2e6, 1e8),
			makeRollingKlineRecord(4, openMS, endMS, 5, 1, 5),
		}
		alive := map[int32]bool{1: true, 2: true, 3: true, 4: true}
		selA.Observe(recs, endMS, endMS, 0)
		u, rec := selA.Select(endMS, alive, false)
		if d == 59 { // open Feb 29 -> endMS = Mar 1 00:00 UTC
			mar1Univ, mar1Rec = u, rec
		}
	}
	if !mar1Rec || !slices.Equal(mar1Univ.Investable, []int32{2, 3, 4}) {
		t.Fatalf("Mar 1 reconstitution expected [2, 3, 4] (AAA dropped, CCC added, CORE kept), got rec=%v inv=%v", mar1Rec, mar1Univ.Investable)
	}
}

func TestRollingUniverseSessionAndBatchWarmupParity(t *testing.T) {
	hour := int64(3_600_000)
	t0 := int64(1_700_000_000_000) - int64(1_700_000_000_000)%hour
	sidMap := map[int32]string{1: "A/USDT:USDT", 2: "B/USDT:USDT", 3: "C/USDT:USDT"}
	base := Universe{Version: "u0", Investable: []int32{1, 2, 3}, Reference: []int32{1, 2, 3}, Tradable: []int32{1, 2, 3}, Evaluation: []int32{1, 2, 3}, Tracked: []int32{1, 2, 3}}

	closeNode := Field(orm.SeriesSourceKline, "close", "1h")
	retNode := Return(closeNode, 3)
	rankNode := PctRank(retNode)
	plan, err := Compile(map[string]*Node{"score": rankNode})
	if err != nil {
		t.Fatal(err)
	}

	sel, err := NewRollingUniverseSelector(RollingUniverseConfig{
		TopN:             2,
		LookbackBars:     3,
		MinAgeBars:       3,
		ReconstituteBars: 4,
	}, base, sidMap, hour, orm.SeriesSourceKline, "1h")
	if err != nil {
		t.Fatal(err)
	}

	sess, err := NewSession(plan)
	if err != nil {
		t.Fatal(err)
	}

	var snapshots []*Snapshot
	var sessionFrames []Frame
	// Seed 3 pre-bars for SIDs 1 & 2; SID 3 lists at bar 2 and enters Investable at reconstitution bar 6 with Return(3) already warmed up!
	for step := -3; step < 8; step++ {
		openMS := t0 + int64(step)*hour
		grid := openMS + hour
		var recs []VersionRecord
		alive := map[int32]bool{1: true, 2: true}
		recs = append(recs,
			VersionRecord{Series: orm.DataSeries{Source: orm.SeriesSourceKline, Sid: 1, TimeFrame: "1h", TimeMS: openMS, EndMS: grid, Closed: true, Values: map[string]any{"close": 100 + float64(step+4), "volume": 10.0, "quote": 1000.0}}, EventTime: grid, Revision: 1, AvailableAt: grid, IngestedAt: grid, SourceVersion: "v1"},
			VersionRecord{Series: orm.DataSeries{Source: orm.SeriesSourceKline, Sid: 2, TimeFrame: "1h", TimeMS: openMS, EndMS: grid, Closed: true, Values: map[string]any{"close": 200 + float64(step+4)*2, "volume": 10.0, "quote": 2000.0}}, EventTime: grid, Revision: 1, AvailableAt: grid, IngestedAt: grid, SourceVersion: "v1"},
		)
		if step >= 1 {
			alive[3] = true
			recs = append(recs, VersionRecord{Series: orm.DataSeries{Source: orm.SeriesSourceKline, Sid: 3, TimeFrame: "1h", TimeMS: openMS, EndMS: grid, Closed: true, Values: map[string]any{"close": 50 + float64(step)*5, "volume": 100.0, "quote": 50000.0}}, EventTime: grid, Revision: 1, AvailableAt: grid, IngestedAt: grid, SourceVersion: "v1"})
		}
		sel.Observe(recs, grid, grid, 0)
		warming := step < 0
		u, _ := sel.Select(grid, alive, warming)
		spec := SnapshotSpec{
			TrackedQuotesOnly: false,
			GridTime:          grid,
			DecisionTime:      grid,
			Universe:          u,
			SIDMap:            sidMap,
			Schemas:           map[string]string{orm.SeriesSourceKline: "s1"},
			SourceVersions:    map[string]string{orm.SeriesSourceKline: "v1"},
			AdjustmentVersion: "raw",
			VisibilityPolicy:  "available-at",
		}
		var reqs []Requirement
		for _, sid := range activeFactorSIDs(spec) {
			reqs = append(reqs, Requirement{SID: sid, Source: orm.SeriesSourceKline, TimeFrame: "1h", EventTime: grid})
		}
		snap, err := Freeze(spec, recs, reqs)
		if err != nil {
			t.Fatalf("step %d Freeze: %v", step, err)
		}
		snapshots = append(snapshots, snap)
		if warming {
			if err := sess.Warmup(snap); err != nil {
				t.Fatal(err)
			}
		} else {
			fr, err := sess.Evaluate(snap)
			if err != nil {
				t.Fatal(err)
			}
			sessionFrames = append(sessionFrames, fr)
		}
	}

	batchFrames, err := plan.Batch(snapshots, len(snapshots))
	if err != nil {
		t.Fatal(err)
	}
	postWarm := batchFrames[3:]
	// At step == 4 (index 4 in postWarm, second reconstitution at t0+5h), SID 3 has age 4h >= 3h and 4 bars (0..3 -> 3-bar return valid!).
	frameAtReconst := sessionFrames[4]
	if !slices.Equal(snapshots[7].Spec().Universe.Investable, []int32{2, 3}) {
		t.Fatalf("expected Investable [2, 3] at second reconstitution, got %v", snapshots[7].Spec().Universe.Investable)
	}
	if got := frameAtReconst.Values["score"][3]; got.Validity != Valid || math.IsNaN(got.Value) {
		t.Fatalf("SID 3 indicator should already be warmed up upon entering Investable, got %+v", got)
	}
	for i := range sessionFrames {
		for sid, sv := range sessionFrames[i].Values["score"] {
			bv := postWarm[i].Values["score"][sid]
			if sv.Validity != bv.Validity || (sv.Validity == Valid && math.Abs(sv.Value-bv.Value) > 1e-12) {
				t.Fatalf("frame %d sid %d Session=%+v Batch=%+v", i, sid, sv, bv)
			}
		}
	}
}
