package expr

import (
	"fmt"
	"math"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/orm"
)

func TestFA02MultiStageCSToTSExpressionsAndGoParity(t *testing.T) {
	c := factor.Field("prices", "close", "1h")
	o := factor.Field("prices", "open", "1h")
	v := factor.Field("prices", "volume", "1h")
	sector := factor.Field("prices", "sector", "1h")
	ret1 := factor.Return(c, 1)

	macdL, macdS, macdH := factor.MACD(factor.PctRank(c), 2, 5, 3)
	bbU, bbM, bbL := factor.BBands(factor.ZScore(c), 4, 2.0, 1.5)

	cases := []struct {
		name       string
		formula    string
		node       *factor.Node
		wantStages int
		wantWarmup int
	}{
		{
			name:       "smooth_pct_rank",
			formula:    "ts.sma(cs.pct_rank(kline.volume), 5)",
			node:       factor.SMA(factor.PctRank(v), 5),
			wantStages: 3,
			wantWarmup: 4,
		},
		{
			name:       "decay_cs_zscore",
			formula:    "ts.decay_linear(cs.zscore(kline.close), 5)",
			node:       factor.DecayLinear(factor.ZScore(c), 5),
			wantStages: 3,
			wantWarmup: 4,
		},
		{
			name:       "ivol_demean_4stage",
			formula:    "cs.zscore(ts.std(ts.return(kline.close, 1) - cs.mean(ts.return(kline.close, 1)), 6, 0))",
			node:       factor.ZScore(factor.StdDev(factor.Sub(ret1, factor.CsMean(ret1)), 6, 0)),
			wantStages: 4,
			wantWarmup: 6,
		},
		{
			name:       "ivol_residual_4stage",
			formula:    "cs.zscore(ts.std(group.residual(ts.return(kline.close, 1), kline.open), 6, 0))",
			node:       factor.ZScore(factor.StdDev(factor.Residual(ret1, o), 6, 0)),
			wantStages: 4,
			wantWarmup: 6,
		},
		{
			name:       "residual_momentum_group_demean",
			formula:    `ts.sum(group.demean(ts.return(kline.close, 1), "kline", "sector"), 5)`,
			node:       factor.TsSum(factor.GroupDemeanByField(ret1, sector), 5),
			wantStages: 3,
			wantWarmup: 5,
		},
		{
			name:       "residual_ts_zscore_ols",
			formula:    "ts.zscore(group.ols(ts.return(kline.close, 1), kline.open, kline.volume), 5)",
			node:       factor.TsZScore(factor.MultiResidual(ret1, o, v), 5),
			wantStages: 3,
			wantWarmup: 5,
		},
		{
			name:       "dual_cs_corr",
			formula:    "ts.corr(cs.pct_rank(kline.close), cs.pct_rank(kline.volume), 6)",
			node:       factor.TsCorr(factor.PctRank(c), factor.PctRank(v), 6),
			wantStages: 3,
			wantWarmup: 5,
		},
		{
			name:       "rsi_over_cs_then_pct_rank",
			formula:    "cs.pct_rank(ts.rsi(cs.zscore(kline.close), 4))",
			node:       factor.PctRank(factor.RSI(factor.ZScore(c), 4)),
			wantStages: 4,
			wantWarmup: 4,
		},
		{
			name:       "macd_line_over_cs",
			formula:    "ts.macd(cs.pct_rank(kline.close), 2, 5, 3)",
			node:       macdL,
			wantStages: 3,
			wantWarmup: 4,
		},
		{
			name:       "macd_signal_over_cs",
			formula:    "ts.macd_signal(cs.pct_rank(kline.close), 2, 5, 3)",
			node:       macdS,
			wantStages: 3,
			wantWarmup: 6,
		},
		{
			name:       "macd_hist_over_cs",
			formula:    "ts.macd_hist(cs.pct_rank(kline.close), 2, 5, 3)",
			node:       macdH,
			wantStages: 3,
			wantWarmup: 6,
		},
		{
			name:       "bbands_upper_over_cs",
			formula:    "ts.bbands_upper(cs.zscore(kline.close), 4, 2, 1.5)",
			node:       bbU,
			wantStages: 3,
			wantWarmup: 3,
		},
		{
			name:       "bbands_middle_over_cs",
			formula:    "ts.bbands_middle(cs.zscore(kline.close), 4, 2, 1.5)",
			node:       bbM,
			wantStages: 3,
			wantWarmup: 3,
		},
		{
			name:       "bbands_lower_over_cs",
			formula:    "ts.bbands_lower(cs.zscore(kline.close), 4, 2, 1.5)",
			node:       bbL,
			wantStages: 3,
			wantWarmup: 3,
		},
	}

	combinedSpec := Spec{
		SchemaVersion: 1,
		TimeFrame:     "1h",
		Bindings:      map[string]Binding{"kline": {Source: "prices", TimeFrame: "1h"}},
		Outputs:       map[string]string{},
	}
	goOutputs := map[string]*factor.Node{}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dsl := mustCompile(t, specFor(tc.formula))
			goPlan, err := factor.Compile(map[string]*factor.Node{"score": tc.node})
			if err != nil {
				t.Fatal(err)
			}
			if dsl.Hash() != goPlan.Hash() || dsl.NodeCount() != goPlan.NodeCount() || dsl.StageCount() != goPlan.StageCount() || dsl.WarmupLength() != goPlan.WarmupLength() || dsl.StateRetention() != goPlan.StateRetention() {
				t.Fatalf("%s DSL/Go mismatch: dsl=(hash=%s nodes=%d stages=%d warmup=%d ret=%d) go=(hash=%s nodes=%d stages=%d warmup=%d ret=%d)",
					tc.name, dsl.Hash(), dsl.NodeCount(), dsl.StageCount(), dsl.WarmupLength(), dsl.StateRetention(),
					goPlan.Hash(), goPlan.NodeCount(), goPlan.StageCount(), goPlan.WarmupLength(), goPlan.StateRetention())
			}
			if dsl.StageCount() != tc.wantStages || dsl.WarmupLength() != tc.wantWarmup {
				t.Fatalf("%s got stages=%d warmup=%d, want stages=%d warmup=%d", tc.name, dsl.StageCount(), dsl.WarmupLength(), tc.wantStages, tc.wantWarmup)
			}
		})
		combinedSpec.Outputs[tc.name] = tc.formula
		goOutputs[tc.name] = tc.node
	}

	dslCombined := mustCompile(t, combinedSpec)
	goCombined, err := factor.Compile(goOutputs)
	if err != nil || dslCombined.Hash() != goCombined.Hash() {
		t.Fatalf("combined multi-stage DSL/Go plan mismatch: err=%v %s vs %s", err, dslCombined.Hash(), goCombined.Hash())
	}

	// Execute across 24 steps with dynamic PIT asset joining at step 8 and missing data at step 11.
	var snaps []*factor.Snapshot
	for step := 0; step < 24; step++ {
		at := int64(step+1) * 3600_000
		sids := []int32{1, 2, 3, 4}
		ver := "u1"
		if step >= 8 {
			sids = []int32{1, 2, 3, 4, 5}
			ver = "u2"
		}
		sidMap := map[int32]string{}
		var rows []factor.VersionRecord
		var reqs []factor.Requirement
		for _, sid := range sids {
			sidMap[sid] = fmt.Sprintf("S%d", sid)
			s := float64(sid)
			ti := float64(step + 1)
			sec := "tech"
			if sid%2 == 0 {
				sec = "fin"
			}
			fields := map[string]any{
				"close":  50 + s*4 + math.Sin(ti*0.35*s)*3 + 0.3*ti*s,
				"open":   15 + s*1.5 + math.Cos(ti*0.21*s),
				"volume": 200 + s*25 + math.Sin(ti*0.49+s)*30,
				"sector": sec,
			}
			if step == 11 && sid == 1 {
				fields["close"] = nil
			}
			rows = append(rows, factor.Record(orm.DataSeries{
				Source: "prices", Sid: sid, TimeMS: at - 1, EndMS: at, Closed: true, TimeFrame: "1h", Values: fields,
			}, 1, at, at, "prices-v1"))
			reqs = append(reqs, factor.Requirement{SID: sid, Source: "prices", TimeFrame: "1h", EventTime: at})
		}
		snap, err := factor.Freeze(factor.SnapshotSpec{
			DecisionTime:     at,
			ReplayTime:       at,
			Universe:         factor.Universe{Version: ver, Investable: sids, Reference: sids, Tradable: sids, Evaluation: sids, Static: false},
			SIDMap:           sidMap,
			Schemas:          map[string]string{"prices": "s1"},
			SourceVersions:   map[string]string{"prices": "prices-v1"},
			VisibilityPolicy: "published-and-received",
		}, rows, reqs)
		if err != nil {
			t.Fatal(err)
		}
		snaps = append(snaps, snap)
	}

	batchFrames, err := dslCombined.Batch(snaps, len(snaps))
	if err != nil {
		t.Fatal(err)
	}
	sess, err := factor.NewSession(dslCombined)
	if err != nil {
		t.Fatal(err)
	}
	for i, snap := range snaps {
		frame, err := sess.Evaluate(snap)
		if err != nil {
			t.Fatal(err)
		}
		for name, col := range frame.Values {
			for sid, got := range col {
				want := batchFrames[i].Values[name][sid]
				if got.Validity != want.Validity || (got.Validity == factor.Valid && math.Abs(got.Value-want.Value) > 1e-9*(1+math.Abs(want.Value))) {
					t.Fatalf("%s t=%d sid=%d got %+v want %+v", name, i, sid, got, want)
				}
			}
		}
	}
}
