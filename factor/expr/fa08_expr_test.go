package expr_test

import (
	"math"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/expr"
	"github.com/banbox/banbot/orm"
)

func TestSectorAndQuantileGroupExpressionsParity(t *testing.T) {
	spec := expr.Spec{
		SchemaVersion: 1,
		TimeFrame:     "1h",
		Bindings: map[string]expr.Binding{
			"kline": {Source: "kline", TimeFrame: "1h"},
		},
		Params: map[string]float64{
			"buckets": 3,
		},
		Outputs: map[string]string{
			"sec_code":     "sector_code()",
			"sec_d1":       "cs.sector_demean(kline.close)",
			"sec_d2":       "group.sector_demean(kline.close)",
			"sec_d3":       "group.demean(kline.close, sector)",
			"sec_d4":       `group.demean(kline.close, "sector")`,
			"sec_d5":       "group.demean(kline.close, sector_code())",
			"sec_z1":       "cs.sector_zscore(kline.close)",
			"sec_z2":       "group.zscore(kline.close, sector)",
			"q_demean":     "group.quantile_demean(kline.close, ts.sma(kline.quote, 2), param.buckets)",
			"q_zscore":     "group.quantile_zscore(kline.close, ts.sma(kline.quote, 2), 3)",
			"q_rank":       "group.quantile_rank(kline.close, ts.sma(kline.quote, 2), 3)",
			"q_pct_rank":   "group.quantile_pct_rank(kline.close, ts.sma(kline.quote, 2), 3)",
			"q_smooth_ema": "ts.ema(group.quantile_demean(ts.return(positive(kline.close), 1), ts.sma(kline.quote, 2), 3), 2)",
		},
	}

	compiled, err := expr.Compile(spec)
	if err != nil {
		t.Fatalf("expr.Compile failed: %v", err)
	}

	// Verify Plan hash equivalence with Go Builder!
	cNode := factor.Field("kline", "close", "1h")
	qNode := factor.Field("kline", "quote", "1h")
	liq := factor.SMA(qNode, 2)
	goPlan, err := factor.New().
		Add("sec_code", factor.SectorCode("1h")).
		Add("sec_d1", factor.SectorDemean(cNode)).
		Add("sec_d2", factor.SectorDemean(cNode)).
		Add("sec_d3", factor.SectorDemean(cNode)).
		Add("sec_d4", factor.SectorDemean(cNode)).
		Add("sec_d5", factor.SectorDemean(cNode)).
		Add("sec_z1", factor.SectorZScore(cNode)).
		Add("sec_z2", factor.SectorZScore(cNode)).
		Add("q_demean", factor.QuantileGroupDemean(cNode, liq, 3)).
		Add("q_zscore", factor.QuantileGroupZScore(cNode, liq, 3)).
		Add("q_rank", factor.QuantileGroupRank(cNode, liq, 3)).
		Add("q_pct_rank", factor.QuantileGroupPctRank(cNode, liq, 3)).
		Add("q_smooth_ema", factor.EMA(factor.QuantileGroupDemean(factor.Return(factor.Positive(cNode), 1), liq, 3), 2)).
		Compile()
	if err != nil {
		t.Fatal(err)
	}
	if compiled.Hash() != goPlan.Hash() {
		t.Fatalf("expr Plan hash %s != Go Builder Plan hash %s", compiled.Hash(), goPlan.Hash())
	}

	sids := []int32{1, 2, 3, 4, 5, 6}
	sidMap := map[int32]string{
		1: "1000PEPE/USDT:USDT", // Meme
		2: "DOGE/USDT:USDT",     // Meme
		3: "UNI/USDT:USDT",      // DeFi
		4: "AAVE/USDT:USDT",     // DeFi
		5: "BTC/USDT:USDT",      // L1
		6: "SOL/USDT:USDT",      // L1
	}
	u := factor.Universe{Version: "u-expr", Investable: sids, Reference: sids, Tradable: sids, Evaluation: sids, Tracked: sids, Static: true}
	const hour = int64(3_600_000)

	closes := [][]float64{
		{10, 20, 50, 70, 60000, 150},
		{12, 18, 55, 65, 61200, 165},
		{15, 21, 52, 74, 60600, 180},
		{14, 24, 60, 80, 62000, 175},
	}
	quotes := [][]float64{
		{100, 120, 500, 600, 5000, 4000},
		{110, 130, 520, 620, 5200, 4100},
		{115, 140, 510, 630, 5100, 4200},
		{120, 150, 530, 640, 5300, 4300},
	}

	snapshots := make([]*factor.Snapshot, len(closes))
	for stepIdx := range closes {
		at := int64(stepIdx+1) * hour
		recs := make([]factor.VersionRecord, 0, len(sids))
		reqs := make([]factor.Requirement, 0, len(sids))
		for i, sid := range sids {
			reqs = append(reqs, factor.Requirement{SID: sid, Source: "kline", TimeFrame: "1h", EventTime: at})
			recs = append(recs, factor.VersionRecord{
				Series: orm.DataSeries{
					Source: "kline", TimeFrame: "1h", Sid: sid, TimeMS: at, EndMS: at, Closed: true,
					Values: map[string]any{"close": closes[stepIdx][i], "quote": quotes[stepIdx][i]},
				},
				EventTime:     at,
				AvailableAt:   at,
				IngestedAt:    at,
				Revision:      1,
				SourceVersion: "v1",
			})
		}
		snap, err := factor.Freeze(factor.SnapshotSpec{
			GridTime:          at,
			DecisionTime:      at,
			Universe:          u,
			SIDMap:            sidMap,
			Schemas:           map[string]string{"kline": "s1"},
			SourceVersions:    map[string]string{"kline": "v1"},
			AdjustmentVersion: "raw",
			VisibilityPolicy:  "available-at",
		}, recs, reqs)
		if err != nil {
			t.Fatal(err)
		}
		snapshots[stepIdx] = snap
	}

	batchFrames, err := compiled.Batch(snapshots, 1000)
	if err != nil {
		t.Fatal(err)
	}
	sess, err := factor.NewSession(compiled)
	if err != nil {
		t.Fatal(err)
	}

	for tIdx, snap := range snapshots {
		sf, err := sess.Evaluate(snap)
		if err != nil {
			t.Fatal(err)
		}
		bf := batchFrames[tIdx]
		// Meme {1,2}, DeFi {3,4}, L1 {5,6} sector sums must all be 0!
		for _, pair := range [][2]int32{{1, 2}, {3, 4}, {5, 6}} {
			sum := sf.Values["sec_d1"][pair[0]].Value + sf.Values["sec_d1"][pair[1]].Value
			if math.Abs(sum) > 1e-9 {
				t.Fatalf("t=%d sector pair %v sum=%v != 0", tIdx, pair, sum)
			}
		}
		for _, outName := range compiled.Outputs() {
			for _, sid := range sids {
				sv := sf.Values[outName][sid]
				bv := bf.Values[outName][sid]
				if sv.Validity != bv.Validity || (sv.Validity == factor.Valid && math.Abs(sv.Value-bv.Value) > 1e-12) {
					t.Fatalf("Session vs Batch mismatch t=%d out=%s sid=%d: sess=%+v batch=%+v", tIdx, outName, sid, sv, bv)
				}
			}
		}
	}
}

func TestSectorAndQuantileExpressionValidationErrors(t *testing.T) {
	badExprs := []string{
		"sector_code(1)",
		"cs.sector_demean()",
		"cs.sector_demean(kline.close, kline.open)",
		"group.demean(kline.close, kline.sector)",
		`group.zscore(kline.close, "not_sector")`,
		"group.quantile_demean(kline.close, kline.quote)",
		"group.quantile_demean(kline.close, kline.quote, 1)",
		"group.quantile_demean(kline.close, kline.quote, 2.5)",
		"group.quantile_zscore(kline.close, kline.quote, 1001)",
	}
	for _, bad := range badExprs {
		_, err := expr.Compile(expr.Spec{
			SchemaVersion: 1,
			TimeFrame:     "1h",
			Bindings:      map[string]expr.Binding{"kline": {Source: "kline", TimeFrame: "1h"}},
			Outputs:       map[string]string{"x": bad},
		})
		if err == nil {
			t.Fatalf("expected invalid expression %q to fail compilation", bad)
		}
	}
}
