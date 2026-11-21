package expr

import (
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/orm"
)

func TestFA04MTFAndRefDSLPlanEquivalenceAndExecution(t *testing.T) {
	const (
		hour = int64(3600 * 1000)
		day  = 24 * hour
	)

	// 1. Exact Plan.Hash() parity between mtf.asof, asof_tf, bare field shorthands, and Go Builder!
	specA := Spec{
		SchemaVersion: 1,
		TimeFrame:     "1h",
		Bindings:      map[string]Binding{"kline": {Source: "kline", TimeFrame: "1h"}},
		Params:        map[string]float64{"max_stale": float64(48 * hour)},
		Lets: map[string]string{
			"typical": "(high + low + close) / 3",
		},
		Outputs: map[string]string{
			"d_sma":   `mtf.asof("1d", ts.sma(close, 5))`,
			"d_atr":   `mtf.asof("1d", ts.atr(14), param.max_stale)`,
			"d_cci":   `mtf.asof("1d", ts.cci(factor.typical, 10))`,
			"h_cci":   `ts.cci(factor.typical, 10)`,
			"btc_ret": `ts.ret(ref.field("BTC/USDT:USDT", "close"), 1)`,
		},
	}
	specB := CloneSpec(specA)
	specB.Outputs["d_sma"] = `asof_tf("1d", ts.sma(kline.close, 5))`
	specB.Outputs["d_atr"] = `asof_tf("1d", ts.atr(kline.high, kline.low, kline.close, 14), 172800000)`
	specB.Outputs["btc_ret"] = `ts.return(ref.field("BTC/USDT:USDT", "kline", "close"), 1)`

	planA := mustCompile(t, specA)
	planB := mustCompile(t, specB)
	if planA.Hash() != planB.Hash() {
		t.Fatalf("DSL shorthand/alias plan hash mismatch: %s vs %s", planA.Hash(), planB.Hash())
	}

	dHigh := factor.Field("kline", "high", "1d")
	dLow := factor.Field("kline", "low", "1d")
	dClose := factor.Field("kline", "close", "1d")
	dTyp := factor.Div(factor.Add(factor.Add(dHigh, dLow), dClose), factor.Constant(3, "1d"))
	hTyp := factor.Div(factor.Add(factor.Add(factor.Field("kline", "high", "1h"), factor.Field("kline", "low", "1h")), factor.Field("kline", "close", "1h")), factor.Constant(3, "1h"))
	goPlan, err := factor.New().
		Add("d_sma", factor.AsOfNode(factor.SMA(dClose, 5), "1h", 0)).
		Add("d_atr", factor.AsOfNode(factor.ATR(dHigh, dLow, dClose, 14), "1h", 48*hour)).
		Add("d_cci", factor.AsOfNode(factor.CCI(dTyp, 10), "1h", 0)).
		Add("h_cci", factor.CCI(hTyp, 10)).
		Add("btc_ret", factor.Return(factor.RefAssetField("BTC/USDT:USDT", "kline", "close", "1h"), 1)).
		Compile()
	if err != nil {
		t.Fatal(err)
	}
	if planA.Hash() != goPlan.Hash() {
		t.Fatalf("DSL vs Go plan hash mismatch: %s vs %s", planA.Hash(), goPlan.Hash())
	}

	// 2. End-to-end DSL execution:
	//    - mtf.asof("1d", ts.sma(close, 5))
	//    - mtf.asof("1d", ref.field("BTC/USDT:USDT", "close") > ts.sma(ref.field("BTC/USDT:USDT", "close"), 3))
	//    - ts.beta(ts.ret(close, 1), ts.ret(ref.field("BTC/USDT:USDT", "close"), 1), 20)
	//    - ref.field("BTC/USDT:USDT", "funding_rate", "rate")
	execSpec := Spec{
		SchemaVersion: 1,
		TimeFrame:     "1h",
		Bindings: map[string]Binding{
			"kline":        {Source: "kline", TimeFrame: "1h"},
			"funding_rate": {Source: "funding", TimeFrame: "event", Sampling: "asof", MaxAgeMS: 8 * hour},
		},
		Outputs: map[string]string{
			"d_sma5":     `mtf.asof("1d", ts.sma(close, 5))`,
			"btc_d_bull": `mtf.asof("1d", ref.field("BTC/USDT:USDT", "close") > ts.sma(ref.field("BTC/USDT:USDT", "close"), 3))`,
			"btc_beta":   `ts.beta(ts.ret(close, 1), ts.ret(ref.field("BTC/USDT:USDT", "close"), 1), 20)`,
			"btc_fund":   `ref.field("BTC/USDT:USDT", "funding_rate", "rate")`,
		},
	}
	execPlan := mustCompile(t, execSpec)
	if !reflect.DeepEqual(execPlan.ReferenceSymbols(), []string{"BTC/USDT:USDT"}) {
		t.Fatalf("unexpected reference symbols: %+v", execPlan.ReferenceSymbols())
	}

	// Simulate 5 full days (120 hourly grids: 1*day .. 5*day + 23*hour? Start at 1*day for 5 days: 1*day .. 5*day = 97 hourly grids).
	// SID 1 = BTC/USDT:USDT (reference-only, NOT in Investable!)
	// SID 2 = ETH/USDT:USDT (2.0x hourly return vs BTC)
	// SID 3 = SOL/USDT:USDT (1.0x hourly return vs BTC, identical prices to BTC to verify Beta == 1.0)
	sidMap := map[int32]string{
		1: "BTC/USDT:USDT",
		2: "ETH/USDT:USDT",
		3: "SOL/USDT:USDT",
	}
	uAltOnly := factor.Universe{
		Version:    "alt-only",
		Investable: []int32{2, 3},
		Reference:  []int32{2, 3},
		Tradable:   []int32{2, 3},
		Evaluation: []int32{2, 3},
		Static:     true,
	}

	btcPrice := 40000.0
	ethPrice := 2500.0
	solPrice := 40000.0
	var snapshots []*factor.Snapshot
	for step := 0; step <= 4*24; step++ {
		at := 1*day + int64(step)*hour
		if step > 0 {
			// Deterministic non-constant hourly return oscillation.
			r := 0.005 * math.Sin(float64(step)*0.7)
			btcPrice *= 1 + r
			ethPrice *= 1 + 2.0*r + 0.0003
			solPrice *= 1 + r
		}
		var rows []factor.VersionRecord
		var reqs []factor.Requirement
		for _, sid := range []int32{1, 2, 3} {
			p := btcPrice
			if sid == 2 {
				p = ethPrice
			} else if sid == 3 {
				p = solPrice
			}
			rows = append(rows, factor.Record(orm.DataSeries{
				Source: "kline", Sid: sid, TimeMS: at - hour, EndMS: at, TimeFrame: "1h", Closed: true,
				Values: map[string]any{"close": p},
			}, 1, at, at, "v1"))
			reqs = append(reqs, factor.Requirement{SID: sid, Source: "kline", TimeFrame: "1h", EventTime: at})

			// Completed 1d bars at multiples of day: Day d has close = 100*sid + 10*d.
			for d := int64(1); d*day <= at; d++ {
				dCloseVal := float64(100*int64(sid) + 10*d)
				rows = append(rows, factor.Record(orm.DataSeries{
					Source: "kline", Sid: sid, TimeMS: (d - 1) * day, EndMS: d * day, TimeFrame: "1d", Closed: true,
					Values: map[string]any{"close": dCloseVal},
				}, 1, d*day, d*day, "v1"))
			}
			reqs = append(reqs, factor.Requirement{SID: sid, Source: "kline", TimeFrame: "1d", EventTime: at, AsOfLatest: true})
		}
		// Funding rate stream ONLY provided for BTC (SID 1), NOT for altcoins!
		rows = append(rows, factor.Record(orm.DataSeries{
			Source: "funding", Sid: 1, TimeMS: at, EndMS: at, TimeFrame: "event", Closed: true,
			Values: map[string]any{"rate": 0.00015},
		}, 1, at, at, "f1"))
		reqs = append(reqs, factor.Requirement{SID: 1, Source: "funding", TimeFrame: "event", EventTime: at, AsOfLatest: true, MaxAge: 8 * hour})

		snap, err := factor.Freeze(factor.SnapshotSpec{
			GridTime: at, DecisionTime: at, ReplayTime: at,
			Universe: uAltOnly, SIDMap: sidMap,
			Schemas:          map[string]string{"kline": "s1", "funding": "f1"},
			SourceVersions:   map[string]string{"kline": "v1", "funding": "f1"},
			VisibilityPolicy: "available-at",
		}, rows, reqs)
		if err != nil {
			t.Fatal(err)
		}
		snapshots = append(snapshots, snap)
	}

	batch, err := execPlan.Batch(snapshots, len(snapshots))
	if err != nil {
		t.Fatal(err)
	}
	sess, err := factor.NewSession(execPlan)
	if err != nil {
		t.Fatal(err)
	}
	for i, snap := range snapshots {
		frame, err := sess.Evaluate(snap)
		if err != nil {
			t.Fatalf("step %d session evaluate err: %v", i, err)
		}
		for col, vals := range frame.Values {
			for sid, got := range vals {
				want := batch[i].Values[col][sid]
				if got.Validity != want.Validity || (got.Validity == factor.Valid && math.Abs(got.Value-want.Value) > 1e-10) {
					t.Fatalf("step %d col %s sid %d mismatch: sess=%+v batch=%+v", i, col, sid, got, want)
				}
			}
		}
	}

	// At final snapshot (t = 5*day):
	// - d_sma5 for SID 2 (ETH): daily closes were [210, 220, 230, 240, 250] -> exact mean = 230.0!
	// - btc_d_bull: BTC daily closes [110, 120, 130, 140, 150], 150 > SMA(130,140,150)=140 -> 1.0!
	// - btc_beta: SID 2 (2x altcoin) == 2.0, SID 3 (1x BTC clone) == 1.0!
	// - btc_fund: 0.00015 broadcast to both SID 2 and SID 3 without requiring altcoin funding streams!
	final := batch[len(batch)-1]
	if got := final.Values["d_sma5"][2]; got.Validity != factor.Valid || math.Abs(got.Value-230.0) > 1e-10 {
		t.Fatalf("expected 5-day daily SMA=230.0, got %+v", got)
	}
	// Check an hour before 5*day (step = 4*24 - 1 = 95, t = 4*day + 23*hour): still only 4 completed days -> Warmup!
	prevHour := batch[len(batch)-2]
	if got := prevHour.Values["d_sma5"][2]; got.Validity != factor.Warmup {
		t.Fatalf("expected Warmup at Day 4 23:00 (96 hourly bars elapsed, only 4 daily closes), got %+v", got)
	}
	if got := final.Values["btc_d_bull"][2]; got.Validity != factor.Valid || got.Value != 1.0 {
		t.Fatalf("expected btc_d_bull=1.0, got %+v", got)
	}
	if got := final.Values["btc_beta"][2]; got.Validity != factor.Valid || math.Abs(got.Value-2.0) > 1e-9 {
		t.Fatalf("expected ETH 2x beta=2.0, got %+v", got)
	}
	if got := final.Values["btc_beta"][3]; got.Validity != factor.Valid || math.Abs(got.Value-1.0) > 1e-9 {
		t.Fatalf("expected 1x clone beta=1.0, got %+v", got)
	}
	if got := final.Values["btc_fund"][2]; got.Validity != factor.Valid || math.Abs(got.Value-0.00015) > 1e-12 {
		t.Fatalf("expected broadcast BTC funding=0.00015, got %+v", got)
	}
}

func TestFA04DSLCompileErrors(t *testing.T) {
	invalidFormulas := []string{
		`mtf.asof("1d")`,
		`mtf.asof(1, close)`,
		`mtf.asof("15m", ts.sma(close, 5))`,
		`mtf.asof("event", close)`,
		`mtf.asof("1d", close, -100)`,
		`mtf.asof("1d", close, 1.5)`,
		`mtf.asof("1d", label.future)`,
		`ref.field("BTC/USDT:USDT")`,
		`ref.field("", "close")`,
		`ref.field(close, "close")`,
		`ref.field("BTC/USDT:USDT", "")`,
		`ref.field("BTC/USDT:USDT", "", "close")`,
		`ref.field("BTC/USDT:USDT", "kline", "close", "extra")`,
	}
	for _, src := range invalidFormulas {
		t.Run(src, func(t *testing.T) {
			_, err := Compile(specFor(src))
			if err == nil || !strings.Contains(err.Error(), "outputs.score:") {
				t.Fatalf("expected compile error with location for %q, got %v", src, err)
			}
		})
	}

	// Reserved binding aliases "mtf" and "ref" must be rejected.
	for _, reserved := range []string{"mtf", "ref"} {
		s := specFor("kline.close")
		s.Bindings[reserved] = Binding{Source: "prices", TimeFrame: "1h"}
		if _, err := Compile(s); err == nil {
			t.Fatalf("accepted reserved binding alias %q", reserved)
		}
	}
}
