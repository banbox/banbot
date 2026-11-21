package factor

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/banbox/banbot/orm"
)

func TestTrueMTFDailySMACausalAlignmentStalenessAndForkParity(t *testing.T) {
	const (
		hour = int64(3600 * 1000)
		day  = 24 * hour
	)

	// Verify SubTF and AsOfNode produce identical compiled plans and input specs.
	goAsOf, err := New().
		Add("d_sma5", AsOfNode(SMA(Field("kline", "close", "1d"), 5), "1h", 30*hour)).
		Add("d_ema3", AsOfNode(EMA(Field("kline", "close", "1d"), 3), "1h", 0)).
		Add("h_close", Field("kline", "close", "1h")).
		Compile()
	if err != nil {
		t.Fatal(err)
	}
	goSubTF, err := New().
		Add("d_sma5", SubTF("1d", "1h", func(d SubTFContext) *Node { return SMA(d.Close(), 5) }, 30*hour)).
		Add("d_ema3", SubTF("1d", "1h", func(d SubTFContext) *Node { return EMA(d.Close(), 3) })).
		Add("h_close", Field("kline", "close", "1h")).
		Compile()
	if err != nil {
		t.Fatal(err)
	}
	if goAsOf.Hash() != goSubTF.Hash() {
		t.Fatalf("SubTF and AsOfNode hashes differ: %s vs %s", goAsOf.Hash(), goSubTF.Hash())
	}
	// WarmupLength on Plan is in 1h bars: max(4*24=96, 2*24=48) = 96.
	if goAsOf.WarmupLength() != 96 {
		t.Fatalf("expected 1h warmup 96, got %d", goAsOf.WarmupLength())
	}
	inputs := goAsOf.Inputs()
	if len(inputs) != 2 {
		t.Fatalf("expected 2 inputs (1d and 1h), got %+v", inputs)
	}
	// Sorted by source+"\x00"+timeframe -> "kline/1d" then "kline/1h".
	if inputs[0].TimeFrame != "1d" || !inputs[0].AsOfLatest || inputs[0].WarmupLength != 5 || inputs[0].MaxAge != 0 {
		t.Fatalf("unexpected 1d input spec (one unlimited + one 30h -> unlimited stream MaxAge=0): %+v", inputs[0])
	}
	if inputs[1].TimeFrame != "1h" || inputs[1].AsOfLatest || inputs[1].WarmupLength != 96 {
		t.Fatalf("unexpected 1h input spec: %+v", inputs[1])
	}

	// Single MaxAge plan propagates MaxAge to InputSpec for barrier enforcement.
	strictPlan, err := New().
		Add("d_sma5", AsOfNode(SMA(Field("kline", "close", "1d"), 5), "1h", 30*hour)).
		Compile()
	if err != nil {
		t.Fatal(err)
	}
	if strictPlan.Inputs()[0].MaxAge != 30*hour {
		t.Fatalf("expected strict InputSpec MaxAge=%d, got %+v", 30*hour, strictPlan.Inputs()[0])
	}

	// Build 7 days of hourly snapshots starting at Day 1 close (t = 1*day .. 7*day),
	// with a deliberately missing Day 6 daily bar (stalling the 1d feed for 48 hours
	// between 5*day and 7*day so hours > 5*day + 30h exceed maxStale = 30h!), plus
	// injected unclosed intra-day 1d records carrying poison value 999999.0 to prove
	// zero lookahead into unfinished daily bars.
	dailyCloses := map[int64]map[int32]float64{
		1 * day: {1: 100, 2: 200},
		2 * day: {1: 110, 2: 220},
		3 * day: {1: 120, 2: 240},
		4 * day: {1: 130, 2: 260},
		5 * day: {1: 140, 2: 280},
		// Day 6 intentionally omitted to trigger staleness after 5*day + 30*hour!
		7 * day: {1: 200, 2: 400},
	}

	var snapshots []*Snapshot
	var gridTimes []int64
	sids := []int32{1, 2}
	u := Universe{Version: "u-mtf", Investable: sids, Reference: sids, Tradable: sids, Evaluation: sids, Static: true}
	sidMap := map[int32]string{1: "ETH/USDT:USDT", 2: "SOL/USDT:USDT"}

	for at := 1 * day; at <= 7*day; at += hour {
		var rows []VersionRecord
		var reqs []Requirement
		for _, sid := range sids {
			// Hourly close varies every hour so a buggy hourly SMA(5) would immediately diverge.
			hClose := float64(sid*1000) + float64(at/hour)
			rows = append(rows, Record(orm.DataSeries{
				Source: "kline", Sid: sid, TimeMS: at - hour, EndMS: at, TimeFrame: "1h", Closed: true,
				Values: map[string]any{"close": hClose},
			}, 1, at, at, "v1"))
			reqs = append(reqs, Requirement{SID: sid, Source: "kline", TimeFrame: "1h", EventTime: at})

			// Add all completed 1d bars up to at.
			for dEnd, bySID := range dailyCloses {
				if dEnd <= at {
					rows = append(rows, Record(orm.DataSeries{
						Source: "kline", Sid: sid, TimeMS: dEnd - day, EndMS: dEnd, TimeFrame: "1d", Closed: true,
						Values: map[string]any{"close": bySID[sid]},
					}, 1, dEnd, dEnd, "v1"))
				}
			}
			// Inject an UNCLOSED intra-day 1d bar with EventTime == at and poison value 999999.
			if at%day != 0 {
				nextDayEnd := (at/day + 1) * day
				rows = append(rows, VersionRecord{
					Series: orm.DataSeries{
						Source: "kline", Sid: sid, TimeMS: nextDayEnd - day, EndMS: nextDayEnd,
						TimeFrame: "1d", Closed: false, Values: map[string]any{"close": 999999.0},
					},
					EventTime: at, Revision: 1, AvailableAt: at, IngestedAt: at, SourceVersion: "v1",
				})
			}
			reqs = append(reqs, Requirement{SID: sid, Source: "kline", TimeFrame: "1d", EventTime: at, AsOfLatest: true})
		}
		snap, err := Freeze(SnapshotSpec{
			GridTime: at, DecisionTime: at, ReplayTime: at,
			Universe:         u,
			SIDMap:           sidMap,
			Schemas:          map[string]string{"kline": "s1"},
			SourceVersions:   map[string]string{"kline": "v1"},
			VisibilityPolicy: "available-at",
		}, rows, reqs)
		if err != nil {
			t.Fatalf("t=%d freeze err: %v", at, err)
		}
		if !snap.Status().Ready {
			t.Fatalf("t=%d snapshot not ready: %+v", at, snap.Status())
		}
		snapshots = append(snapshots, snap)
		gridTimes = append(gridTimes, at)
	}

	// Evaluate via Batch, direct Session, and forked RoundBarrier Session (with abandoned forks).
	batchFrames, err := goAsOf.Batch(snapshots, len(snapshots))
	if err != nil {
		t.Fatal(err)
	}
	sess, err := NewSession(goAsOf)
	if err != nil {
		t.Fatal(err)
	}
	barrierSess, err := NewSession(goAsOf)
	if err != nil {
		t.Fatal(err)
	}

	for i, snap := range snapshots {
		tGrid := gridTimes[i]
		sFrame, err := sess.Evaluate(snap)
		if err != nil {
			t.Fatalf("step %d session err: %v", i, err)
		}

		// Exercise fork/abandon before real commit on every step to verify subAssets isolation.
		abandoned, _, err := barrierSess.fork()
		if err != nil {
			t.Fatalf("step %d fork err: %v", i, err)
		}
		if _, err := abandoned.Evaluate(snap); err != nil {
			t.Fatalf("step %d abandoned eval err: %v", i, err)
		}
		// Real commit via a second independent fork from untouched barrierSess.
		committedFork, rev, err := barrierSess.fork()
		if err != nil {
			t.Fatal(err)
		}
		bFrame, err := committedFork.Evaluate(snap)
		if err != nil {
			t.Fatal(err)
		}
		if err := barrierSess.commit(committedFork, rev); err != nil {
			t.Fatal(err)
		}

		compareTechnicalFrames(t, batchFrames[i], sFrame, fmt.Sprintf("mtf_sess/%d", i))
		compareTechnicalFrames(t, batchFrames[i], bFrame, fmt.Sprintf("mtf_fork/%d", i))

		gotSMA1 := sFrame.Values["d_sma5"][1]
		gotSMA2 := sFrame.Values["d_sma5"][2]
		gotEMA1 := sFrame.Values["d_ema3"][1]

		switch {
		case tGrid < 5*day:
			// Days 1..4 (first 96 hourly grids): fewer than 5 completed daily bars -> MUST remain Warmup!
			if gotSMA1.Validity != Warmup || gotSMA2.Validity != Warmup {
				t.Fatalf("t=%dh expected Warmup before 5 daily closes, got sid1=%+v sid2=%+v", tGrid/hour, gotSMA1, gotSMA2)
			}
			if tGrid >= 3*day {
				// Day 3: EMA(3) seeds to SMA(100,110,120) = 110; Day 4: 0.5*130 + 0.5*110 = 120.
				wantEMA := 110.0
				if tGrid >= 4*day {
					wantEMA = 120.0
				}
				compareNumeric(t, Numeric{wantEMA, Valid}, gotEMA1, fmt.Sprintf("t=%dh d_ema3", tGrid/hour))
			}
		case tGrid >= 5*day && tGrid <= 5*day+30*hour:
			// Day 5 completed: exact average of [100,110,120,130,140] = 120 (sid 1) and 240 (sid 2),
			// unaffected by hourly fluctuations or unclosed intra-day poison bars!
			compareNumeric(t, Numeric{120.0, Valid}, gotSMA1, fmt.Sprintf("t=%dh d_sma5 sid1", tGrid/hour))
			compareNumeric(t, Numeric{240.0, Valid}, gotSMA2, fmt.Sprintf("t=%dh d_sma5 sid2", tGrid/hour))
			// EMA(3) at Day 5: 0.5*140 + 0.5*120 = 130.
			compareNumeric(t, Numeric{130.0, Valid}, gotEMA1, fmt.Sprintf("t=%dh d_ema3", tGrid/hour))
		case tGrid > 5*day+30*hour && tGrid < 7*day:
			// Exceeded maxStale (30h) on d_sma5 -> d_sma5 becomes Missing, while unlimited d_ema3 stays Valid=130!
			if gotSMA1.Validity != Missing || gotSMA2.Validity != Missing {
				t.Fatalf("t=%dh expected Missing when daily bar exceeds maxStale=30h, got %+v", tGrid/hour, gotSMA1)
			}
			compareNumeric(t, Numeric{130.0, Valid}, gotEMA1, fmt.Sprintf("t=%dh unlimited d_ema3", tGrid/hour))
		case tGrid == 7*day:
			// Day 7 completed with close=200: last 5 completed daily bars are [110,120,130,140,200] -> mean = 140!
			compareNumeric(t, Numeric{140.0, Valid}, gotSMA1, "t=7d d_sma5 sid1 recovery")
			compareNumeric(t, Numeric{280.0, Valid}, gotSMA2, "t=7d d_sma5 sid2 recovery")
			// EMA(3) steps once from 130 with 200 -> 0.5*200 + 0.5*130 = 165.
			compareNumeric(t, Numeric{165.0, Valid}, gotEMA1, "t=7d d_ema3 recovery")
		}
	}

	// Verify 1d nodes stepped ONLY 6 times per asset (12 total across 2 assets), NOT 145*2 = 290 times!
	updates := sess.Updates()
	foundSubNode := false
	for idx, n := range goAsOf.nodes {
		if n.spec.Operator == "sma" && n.spec.TimeFrame == "1d" {
			foundSubNode = true
			if updates[n.id] != 12 {
				t.Fatalf("expected 1d SMA node (idx=%d) to update 12 times (6 days x 2 assets), got %d", idx, updates[n.id])
			}
		}
	}
	if !foundSubNode {
		t.Fatal("1d SMA node missing from compiled plan")
	}

	// Barrier-level staleness check matching AsOfField when Requirement carries MaxAge from strictPlan.Inputs().
	staleGrid := 5*day + 31*hour
	staleRow := Record(orm.DataSeries{Source: "kline", Sid: 1, TimeMS: 4 * day, EndMS: 5 * day, TimeFrame: "1d", Closed: true, Values: map[string]any{"close": 140.0}}, 1, 5*day, 5*day, "v1")
	staleSnap, err := Freeze(SnapshotSpec{
		GridTime: staleGrid, DecisionTime: staleGrid, ReplayTime: staleGrid,
		Universe: Universe{Version: "u1", Investable: []int32{1}, Reference: []int32{1}, Static: true},
		SIDMap:   map[int32]string{1: "ETH/USDT:USDT"}, Schemas: map[string]string{"kline": "s1"}, SourceVersions: map[string]string{"kline": "v1"}, VisibilityPolicy: "available-at",
	}, []VersionRecord{staleRow}, []Requirement{{SID: 1, Source: "kline", TimeFrame: "1d", EventTime: staleGrid, AsOfLatest: true, MaxAge: strictPlan.Inputs()[0].MaxAge}})
	if err != nil {
		t.Fatal(err)
	}
	if staleSnap.Status().Ready || len(staleSnap.Status().Invalid) != 1 {
		t.Fatalf("expected Freeze barrier to mark stale 1d stream invalid, got %+v", staleSnap.Status())
	}
}

func TestRefAssetFieldBetaAndReferenceOnlyUniverseParity(t *testing.T) {
	const hour = int64(3600 * 1000)

	closeField := Field("kline", "close", "1h")
	btcClose := RefAssetField("BTC/USDT:USDT", "kline", "close", "1h")
	assetRet := Return(closeField, 1)
	btcRet := Return(btcClose, 1)
	beta20 := TsBeta(assetRet, btcRet, 20)
	corr20 := TsCorr(assetRet, btcRet, 20)
	btcRegime := Gt(btcClose, SMA(btcClose, 5))

	plan, err := New().
		Add("beta", beta20).
		Add("corr", corr20).
		Add("btc_bull", btcRegime).
		Add("z_beta", ZScore(beta20)).
		Compile()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(plan.ReferenceSymbols(), []string{"BTC/USDT:USDT"}) {
		t.Fatalf("unexpected ReferenceSymbols: %+v", plan.ReferenceSymbols())
	}

	// Generate 25 hourly bars:
	// SID 1 = BTC/USDT:USDT (varying returns +/-)
	// SID 2 = ETH/USDT:USDT (exact 2.0x BTC return + 0.001 alpha -> Beta = 2.0, Corr = 1.0)
	// SID 3 = SOL/USDT:USDT (exact -0.5x BTC return + 0.002 alpha -> Beta = -0.5, Corr = -1.0)
	// SID 4 = DOGE/USDT:USDT (exact 1.0x BTC return -> Beta = 1.0, Corr = 1.0)
	btcRets := []float64{
		0, 0.01, -0.02, 0.015, 0.005, -0.01, 0.025, -0.015, 0.02, -0.005,
		0.012, -0.018, 0.008, 0.022, -0.012, 0.016, -0.009, 0.014, 0.019, -0.011,
		0.021, 0.015, 0.018, 0.012, 0.025,
	}
	prices := map[int32][]float64{
		1: make([]float64, len(btcRets)),
		2: make([]float64, len(btcRets)),
		3: make([]float64, len(btcRets)),
		4: make([]float64, len(btcRets)),
	}
	prices[1][0], prices[2][0], prices[3][0], prices[4][0] = 50000, 3000, 100, 0.2
	for i := 1; i < len(btcRets); i++ {
		r := btcRets[i]
		prices[1][i] = prices[1][i-1] * (1 + r)
		prices[2][i] = prices[2][i-1] * (1 + 2.0*r + 0.001)
		prices[3][i] = prices[3][i-1] * (1 - 0.5*r + 0.002)
		prices[4][i] = prices[4][i-1] * (1 + 1.0*r)
	}

	sidMap := map[int32]string{
		1: "BTC/USDT:USDT",
		2: "ETH/USDT:USDT",
		3: "SOL/USDT:USDT",
		4: "DOGE/USDT:USDT",
	}

	// Test TWO universe configurations:
	// 1. "btc_in_pool": BTC (SID 1) is in Reference & Investable -> verify BTC itself has Beta == 1.0!
	// 2. "btc_ref_only": BTC (SID 1) is NOT in Investable and NOT in Reference (altcoin-only universe [2,3,4],
	//    BTC resolved strictly via SIDMap + ScopedInputs!).
	for _, mode := range []string{"btc_in_pool", "btc_ref_only"} {
		t.Run(mode, func(t *testing.T) {
			u := Universe{Version: "u-" + mode, Investable: []int32{1, 2, 3, 4}, Reference: []int32{1, 2, 3, 4}, Tradable: []int32{1, 2, 3, 4}, Evaluation: []int32{1, 2, 3, 4}, Static: true}
			if mode == "btc_ref_only" {
				u = Universe{Version: "u-" + mode, Investable: []int32{2, 3, 4}, Reference: []int32{2, 3, 4}, Tradable: []int32{2, 3, 4}, Evaluation: []int32{2, 3, 4}, Static: true}
			}

			var snaps []*Snapshot
			for i := 0; i < len(btcRets); i++ {
				at := int64(i+1) * hour
				var rows []VersionRecord
				var reqs []Requirement
				for _, sid := range []int32{1, 2, 3, 4} {
					rows = append(rows, Record(orm.DataSeries{
						Source: "kline", Sid: sid, TimeMS: at - hour, EndMS: at, TimeFrame: "1h", Closed: true,
						Values: map[string]any{"close": prices[sid][i]},
					}, 1, at, at, "v1"))
					reqs = append(reqs, Requirement{SID: sid, Source: "kline", TimeFrame: "1h", EventTime: at})
				}
				snap, err := Freeze(SnapshotSpec{
					GridTime: at, DecisionTime: at, ReplayTime: at,
					Universe: u, SIDMap: sidMap,
					Schemas: map[string]string{"kline": "s1"}, SourceVersions: map[string]string{"kline": "v1"},
					VisibilityPolicy: "available-at",
				}, rows, reqs)
				if err != nil {
					t.Fatal(err)
				}
				snaps = append(snaps, snap)
			}

			batch, err := plan.Batch(snaps, len(snaps))
			if err != nil {
				t.Fatal(err)
			}
			sess, err := NewSession(plan)
			if err != nil {
				t.Fatal(err)
			}
			for i, snap := range snaps {
				f, err := sess.Evaluate(snap)
				if err != nil {
					t.Fatal(err)
				}
				compareTechnicalFrames(t, batch[i], f, fmt.Sprintf("%s/%d", mode, i))
			}

			last := batch[len(batch)-1]
			if mode == "btc_in_pool" {
				if got := last.Values["beta"][1]; got.Validity != Valid || math.Abs(got.Value-1.0) > 1e-9 {
					t.Fatalf("BTC Beta vs BTC should be 1.0, got %+v", got)
				}
			} else {
				if _, present := last.Values["beta"][1]; present {
					t.Fatalf("reference-only BTC must not appear as active frame row: %+v", last.Values["beta"])
				}
			}
			if got := last.Values["beta"][2]; got.Validity != Valid || math.Abs(got.Value-2.0) > 1e-9 {
				t.Fatalf("2x altcoin Beta should be 2.0, got %+v", got)
			}
			if got := last.Values["corr"][2]; got.Validity != Valid || math.Abs(got.Value-1.0) > 1e-9 {
				t.Fatalf("2x altcoin Corr should be 1.0, got %+v", got)
			}
			if got := last.Values["beta"][3]; got.Validity != Valid || math.Abs(got.Value - -0.5) > 1e-9 {
				t.Fatalf("-0.5x altcoin Beta should be -0.5, got %+v", got)
			}
			if got := last.Values["corr"][3]; got.Validity != Valid || math.Abs(got.Value - -1.0) > 1e-9 {
				t.Fatalf("-0.5x altcoin Corr should be -1.0, got %+v", got)
			}
			if got := last.Values["beta"][4]; got.Validity != Valid || math.Abs(got.Value-1.0) > 1e-9 {
				t.Fatalf("1x altcoin Beta should be 1.0, got %+v", got)
			}
			// Last 5 BTC returns are positive -> BTC close > SMA(5) -> btc_bull == 1.0 across all active assets.
			compareNumeric(t, Numeric{1.0, Valid}, last.Values["btc_bull"][2], "btc_bull broadcast")
		})
	}
}

func TestMTFAndRefValidationRejections(t *testing.T) {
	c1h := Field("kline", "close", "1h")
	c1d := Field("kline", "close", "1d")
	c15m := Field("kline", "close", "15m")

	badCases := map[string]*Node{
		"mixed_without_asof": Add(c1h, c1d),
		"finer_sub_tf":       AsOfNode(SMA(c15m, 5), "1h", 0),
		"negative_max_age":   AsOfNode(SMA(c1d, 5), "1h", -1),
		"event_sub_tf":       AsOfNode(Field("funding", "rate", "event"), "1h", 0),
		"empty_ref_symbol":   RefAssetField("", "kline", "close", "1h"),
		"empty_ref_field":    RefAssetField("BTC/USDT:USDT", "kline", "", "1h"),
		"zero_age_asof_ref":  AsOfRefAssetField("BTC/USDT:USDT", "funding", "rate", "event", "1h", 0),
	}
	for name, bad := range badCases {
		if _, err := New().Add("out", bad).Compile(); err == nil {
			t.Fatalf("expected %s to fail Compile", name)
		}
	}

	// Duplicate symbol mapping in SIDMap must fail Freeze.
	_, err := Freeze(SnapshotSpec{
		GridTime: 1000, DecisionTime: 1000, ReplayTime: 1000,
		Universe:         Universe{Version: "u1", Investable: []int32{1, 2}, Reference: []int32{1, 2}, Static: true},
		SIDMap:           map[int32]string{1: "BTC/USDT:USDT", 2: "BTC/USDT:USDT"},
		Schemas:          map[string]string{"kline": "s1"},
		SourceVersions:   map[string]string{"kline": "v1"},
		VisibilityPolicy: "available-at",
	}, nil, []Requirement{{SID: 1, Source: "kline", TimeFrame: "1h", EventTime: 1000}})
	if err == nil || !strings.Contains(err.Error(), "ambiguously mapped") {
		t.Fatalf("expected duplicate symbol mapping error, got %v", err)
	}

	// Missing reference symbol in SIDMap must fail Session.Evaluate / Plan.Batch clearly.
	refPlan, err := New().Add("btc", RefAssetField("BTC/USDT:USDT", "prices", "close", "1h")).Compile()
	if err != nil {
		t.Fatal(err)
	}
	snap := testSnapshot(t, 1000, map[int32]map[string]any{1: {"close": 100.0}})
	sess, _ := NewSession(refPlan)
	if _, err := sess.Evaluate(snap); err == nil || !strings.Contains(err.Error(), "BTC/USDT:USDT") {
		t.Fatalf("expected missing reference symbol error, got %v", err)
	}
}
