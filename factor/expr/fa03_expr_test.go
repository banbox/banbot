package expr

import (
	"math"
	"testing"

	"github.com/banbox/banbot/factor"
)

func TestFA03ExprGoEquivalenceShorthandsAndParity(t *testing.T) {
	o := factor.Field("prices", "open", "1h")
	h := factor.Field("prices", "high", "1h")
	l := factor.Field("prices", "low", "1h")
	c := factor.Field("prices", "close", "1h")
	v := factor.Field("prices", "volume", "1h")
	bv := factor.Field("prices", "buy_volume", "1h")

	cases := []struct {
		name      string
		formula   string
		shorthand string
		node      *factor.Node
	}{
		// 1. Adaptive & Moving Averages
		{"dema", "ts.dema(kline.close, 4)", "", factor.DEMA(c, 4)},
		{"tema", "ts.tema(kline.close, 3)", "", factor.TEMA(c, 3)},
		{"hma", "ts.hma(kline.close, 9)", "", factor.HMA(c, 9)},
		{"kama", "ts.kama(kline.close, 5)", "", factor.KAMA(c, 5)},
		{"kama_by", "ts.kama(kline.close, 5, 2, 20)", "", factor.KAMABy(c, 5, 2, 20)},
		{"alma_full", "ts.alma(kline.close, 6, 0.75, 5.0)", "", factor.ALMA(c, 6, 0.75, 5.0)},
		{"alma_def", "ts.alma(kline.close, 6)", "", factor.ALMA(c, 6, 0.85, 6.0)},
		{"trima", "ts.trima(kline.close, 5)", "", factor.TRIMA(c, 5)},
		{"er", "ts.er(kline.close, 5)", "", factor.ER(c, 5)},
		{"kalman1d", "ts.kalman1d(kline.close, 0.05, 0.5)", "", factor.Kalman1D(c, 0.05, 0.5)},
		{"hurst", "ts.hurst(kline.close, 10)", "", factor.Hurst(c, 10)},

		// 2. Directional, Trend & Channel
		{"adx", "ts.adx(kline.high, kline.low, kline.close, 4)", "ts.adx(4)", factor.ADX(h, l, c, 4)},
		{"dmi_plus", "ts.dmi_plus(kline.high, kline.low, kline.close, 4)", "ts.dmi_plus(4)", factor.PlusDI(h, l, c, 4)},
		{"plus_di", "ts.plus_di(kline.high, kline.low, kline.close, 4)", "ts.plus_di(4)", factor.PlusDI(h, l, c, 4)},
		{"dmi_minus", "ts.dmi_minus(kline.high, kline.low, kline.close, 4)", "ts.dmi_minus(4)", factor.MinusDI(h, l, c, 4)},
		{"minus_di", "ts.minus_di(kline.high, kline.low, kline.close, 4)", "ts.minus_di(4)", factor.MinusDI(h, l, c, 4)},
		{"natr", "ts.natr(kline.high, kline.low, kline.close, 4)", "ts.natr(4)", factor.NATR(h, l, c, 4)},
		{"chop", "ts.chop(kline.high, kline.low, kline.close, 4)", "ts.chop(4)", factor.CHOP(h, l, c, 4)},
		{"aroon_up", "ts.aroon_up(kline.high, kline.low, 4)", "ts.aroon_up(4)", factor.AroonUp(h, l, 4)},
		{"aroon_down", "ts.aroon_down(kline.high, kline.low, 4)", "ts.aroon_down(4)", factor.AroonDown(h, l, 4)},
		{"aroon_osc", "ts.aroon_osc(kline.high, kline.low, 4)", "ts.aroon_osc(4)", factor.AroonOsc(h, l, 4)},
		{"vortex_plus", "ts.vortex_plus(kline.high, kline.low, kline.close, 4)", "ts.vortex_plus(4)", factor.VortexPlus(h, l, c, 4)},
		{"vortex_minus", "ts.vortex_minus(kline.high, kline.low, kline.close, 4)", "ts.vortex_minus(4)", factor.VortexMinus(h, l, c, 4)},
		{"donchian_upper", "ts.donchian_upper(kline.high, kline.low, 4)", "ts.donchian_upper(4)", factor.DonchianUpper(h, l, 4)},
		{"donchian_mid", "ts.donchian_mid(kline.high, kline.low, 4)", "ts.donchian_mid(4)", factor.DonchianMid(h, l, 4)},
		{"donchian_middle", "ts.donchian_middle(kline.high, kline.low, 4)", "ts.donchian_middle(4)", factor.DonchianMid(h, l, 4)},
		{"donchian_lower", "ts.donchian_lower(kline.high, kline.low, 4)", "ts.donchian_lower(4)", factor.DonchianLower(h, l, 4)},
		{"keltner_upper", "ts.keltner_upper(kline.high, kline.low, kline.close, 4, 2.0)", "ts.keltner_upper(4, 2.0)", factor.KeltnerUpper(h, l, c, 4, 2.0)},
		{"keltner_mid", "ts.keltner_mid(kline.high, kline.low, kline.close, 4, 2.0)", "ts.keltner_mid(4, 2.0)", factor.KeltnerMid(h, l, c, 4, 2.0)},
		{"keltner_middle", "ts.keltner_middle(kline.high, kline.low, kline.close, 4, 2.0)", "ts.keltner_middle(4, 2.0)", factor.KeltnerMid(h, l, c, 4, 2.0)},
		{"keltner_lower", "ts.keltner_lower(kline.high, kline.low, kline.close, 4, 2.0)", "ts.keltner_lower(4, 2.0)", factor.KeltnerLower(h, l, c, 4, 2.0)},
		{"supertrend", "ts.supertrend(kline.high, kline.low, kline.close, 4, 3.0)", "ts.supertrend(4, 3.0)", factor.Supertrend(h, l, c, 4, 3.0)},
		{"supertrend_dir", "ts.supertrend_dir(kline.high, kline.low, kline.close, 4, 3.0)", "ts.supertrend_dir(4, 3.0)", factor.SupertrendDir(h, l, c, 4, 3.0)},
		{"sar_full", "ts.sar(kline.high, kline.low, 0.02, 0.02, 0.2)", "ts.sar(0.02, 0.02, 0.2)", factor.SAR(h, l, 0.02, 0.02, 0.2)},
		{"sar_def", "ts.sar(kline.high, kline.low)", "ts.sar()", factor.SAR(h, l, 0.02, 0.02, 0.2)},
		{"stc", "ts.stc(kline.close, 3, 6, 4)", "", factor.STC(c, 3, 6, 4)},
		{"ichi_conv", "ts.ichimoku_conversion(kline.high, kline.low, 4)", "ts.ichimoku_conversion(4)", factor.IchimokuConversion(h, l, 4)},
		{"ichi_base", "ts.ichimoku_base(kline.high, kline.low, 6)", "ts.ichimoku_base(6)", factor.IchimokuBase(h, l, 6)},

		// 3. Volume & Orderflow (+ existing OHLCV kline shorthands)
		{"cmf", "ts.cmf(kline.high, kline.low, kline.close, kline.volume, 4)", "ts.cmf(4)", factor.CMF(h, l, c, v, 4)},
		{"adl", "ts.adl(kline.high, kline.low, kline.close, kline.volume)", "ts.adl()", factor.ADL(h, l, c, v)},
		{"adosc", "ts.adosc(kline.high, kline.low, kline.close, kline.volume, 3, 6)", "ts.adosc(3, 6)", factor.ChaikinOsc(h, l, c, v, 3, 6)},
		{"chaikin_osc", "ts.chaikin_osc(kline.high, kline.low, kline.close, kline.volume, 3, 6)", "ts.chaikin_osc(3, 6)", factor.ChaikinOsc(h, l, c, v, 3, 6)},
		{"volume_delta", "ts.volume_delta(kline.volume, kline.buy_volume)", "ts.volume_delta()", factor.VolumeDelta(v, bv)},
		{"cvd", "ts.cvd(kline.volume, kline.buy_volume)", "ts.cvd()", factor.CVD(v, bv)},
		{"rolling_cvd", "ts.rolling_cvd(kline.volume, kline.buy_volume, 4)", "ts.rolling_cvd(4)", factor.RollingCVD(v, bv, 4)},
		{"taker_buy_ratio", "ts.taker_buy_ratio(kline.volume, kline.buy_volume, 4)", "ts.taker_buy_ratio(4)", factor.TakerBuyRatio(v, bv, 4)},
		{"atr_short", "ts.atr(kline.high, kline.low, kline.close, 4)", "ts.atr(4)", factor.ATR(h, l, c, 4)},
		{"mfi_short", "ts.mfi(kline.high, kline.low, kline.close, kline.volume, 4)", "ts.mfi(4)", factor.MFI(h, l, c, v, 4)},
		{"obv_short", "ts.obv(kline.close, kline.volume)", "ts.obv()", factor.OBV(c, v)},

		// 4. OHLC Range Volatility Estimators
		{"parkinson_vol", "ts.parkinson_vol(kline.high, kline.low, 4)", "ts.parkinson_vol(4)", factor.ParkinsonVol(h, l, 4)},
		{"garman_klass_vol", "ts.garman_klass_vol(kline.open, kline.high, kline.low, kline.close, 4)", "ts.garman_klass_vol(4)", factor.GarmanKlassVol(o, h, l, c, 4)},
		{"yang_zhang_vol", "ts.yang_zhang_vol(kline.open, kline.high, kline.low, kline.close, 4)", "ts.yang_zhang_vol(4)", factor.YangZhangVol(o, h, l, c, 4)},

		// 5. Multi-stage CS -> TS compositions
		{"cs_to_kama", "cs.zscore(ts.kama(cs.pct_rank(kline.close) + 1, 4))", "", factor.ZScore(factor.KAMA(factor.Add(factor.PctRank(c), factor.Constant(1, "1h")), 4))},
		{"cs_to_hma", "cs.zscore(ts.hma(cs.pct_rank(kline.volume) + 1, 4))", "", factor.ZScore(factor.HMA(factor.Add(factor.PctRank(v), factor.Constant(1, "1h")), 4))},
	}

	combinedSpec := Spec{
		SchemaVersion: 1,
		TimeFrame:     "1h",
		Bindings:      map[string]Binding{"kline": {Source: "prices", TimeFrame: "1h"}},
		Outputs:       map[string]string{},
	}
	combinedGo := map[string]*factor.Node{}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dslPlan := mustCompile(t, specFor(tc.formula))
			goPlan, err := factor.Compile(map[string]*factor.Node{"score": tc.node})
			if err != nil {
				t.Fatal(err)
			}
			if dslPlan.Hash() != goPlan.Hash() || dslPlan.WarmupLength() != goPlan.WarmupLength() || dslPlan.StateRetention() != goPlan.StateRetention() {
				t.Fatalf("DSL/Go mismatch on %s: dsl=%s(%d/%d) go=%s(%d/%d)",
					tc.name, dslPlan.Hash(), dslPlan.WarmupLength(), dslPlan.StateRetention(),
					goPlan.Hash(), goPlan.WarmupLength(), goPlan.StateRetention())
			}
			if tc.shorthand != "" {
				shortPlan := mustCompile(t, specFor(tc.shorthand))
				if shortPlan.Hash() != goPlan.Hash() {
					t.Fatalf("shorthand %q hash %s != explicit %s", tc.shorthand, shortPlan.Hash(), goPlan.Hash())
				}
			}
		})
		combinedSpec.Outputs[tc.name] = tc.formula
		combinedGo[tc.name] = tc.node
	}

	allDSL := mustCompile(t, combinedSpec)
	allGo, err := factor.Compile(combinedGo)
	if err != nil || allDSL.Hash() != allGo.Hash() {
		t.Fatalf("combined plan mismatch: err=%v dsl=%s go=%s", err, allDSL.Hash(), allGo.Hash())
	}

	// Execute end-to-end across 24 snapshots (including a missing bar gap) and assert Session == Batch.
	var snaps []*factor.Snapshot
	for i := 0; i < 24; i++ {
		rows := make(map[int32]map[string]any, 3)
		for sid := int32(1); sid <= 3; sid++ {
			base := 100.0 + float64(sid)*20.0 + 5.0*math.Sin(float64(i+int(sid))*0.5) + float64(i)*0.4
			op := base - 0.5
			hi := base + 2.0
			lo := base - 2.2
			vol := 50.0 + float64((i*int(sid))%15)
			bv := vol * (0.4 + 0.05*float64((i+int(sid))%5))
			rows[sid] = map[string]any{
				"open": op, "high": hi, "low": lo, "close": base,
				"volume": vol, "buy_volume": bv,
			}
		}
		if i == 9 {
			delete(rows[1], "buy_volume")
		}
		snaps = append(snaps, snapshot(t, int64(i+1)*1000, rows))
	}

	batch, err := allDSL.Batch(snaps, len(snaps))
	if err != nil {
		t.Fatal(err)
	}
	sess, err := factor.NewSession(allDSL)
	if err != nil {
		t.Fatal(err)
	}
	for i, s := range snaps {
		frame, err := sess.Evaluate(s)
		if err != nil {
			t.Fatal(err)
		}
		for name, col := range frame.Values {
			for sid, got := range col {
				want := batch[i].Values[name][sid]
				if got.Validity != want.Validity || (got.Validity == factor.Valid && math.Abs(got.Value-want.Value) > 1e-9) {
					t.Fatalf("%s t=%d sid=%d got %+v want %+v", name, i, sid, got, want)
				}
			}
		}
	}
}

func TestFA03ExprRejectsInvalidParametersAndShorthandsWithoutKline(t *testing.T) {
	badFormulas := []string{
		"ts.hma(kline.close, 1)",
		"ts.hurst(kline.close, 7)",
		"ts.chop(kline.high, kline.low, kline.close, 1)",
		"ts.chop(1)",
		"ts.yang_zhang_vol(kline.open, kline.high, kline.low, kline.close, 1)",
		"ts.yang_zhang_vol(1)",
		"ts.kama(kline.close, 5, 10, 10)",
		"ts.kama(kline.close, 5, 12, 10)",
		"ts.kama(kline.close, 5, 2)",
		"ts.alma(kline.close, 5, -0.1, 6.0)",
		"ts.alma(kline.close, 5, 1.1, 6.0)",
		"ts.alma(kline.close, 5, 0.85, 0)",
		"ts.alma(kline.close, 5, 0.85)",
		"ts.kalman1d(kline.close, 0, 1.0)",
		"ts.kalman1d(kline.close, 1.0, -0.1)",
		"ts.kalman1d(kline.close, 1.0)",
		"ts.supertrend(kline.high, kline.low, kline.close, 10, 0)",
		"ts.supertrend(10, -1)",
		"ts.supertrend_dir(10, 0)",
		"ts.keltner_upper(10, -0.5)",
		"ts.sar(kline.high, kline.low, 0.2, 0.02, 0.1)",
		"ts.sar(kline.high, kline.low, 0, 0.02, 0.2)",
		"ts.sar(kline.high, kline.low, 0.02, 0.02)",
		"ts.sar(0.02)",
		"ts.stc(kline.close, 10, 10, 5)",
		"ts.stc(kline.close, 12, 10, 5)",
		"ts.adosc(10, 10)",
		"ts.chaikin_osc(12, 10)",
		"ts.adx(kline.close, 14)",
		"ts.adx(kline.high, kline.low, 14)",
		"ts.volume_delta(kline.volume)",
		"ts.rolling_cvd(kline.volume, 10)",
	}
	for _, src := range badFormulas {
		t.Run(src, func(t *testing.T) {
			if _, err := Compile(specFor(src)); err == nil {
				t.Fatalf("accepted invalid formula %q", src)
			}
		})
	}

	// Without a "kline" binding, 0-node shorthand must fail closed with a clear message.
	noKlineSpec := Spec{
		SchemaVersion: 1,
		TimeFrame:     "1h",
		Bindings:      map[string]Binding{"bar": {Source: "prices", TimeFrame: "1h"}},
		Outputs:       map[string]string{"score": "ts.adx(14)"},
	}
	if _, err := Compile(noKlineSpec); err == nil {
		t.Fatal("accepted kline shorthand when kline binding is absent")
	}
	noKlineSpec.Outputs["score"] = "ts.adx(bar.high, bar.low, bar.close, 14)"
	if _, err := Compile(noKlineSpec); err != nil {
		t.Fatalf("explicit non-kline binding failed: %v", err)
	}
}
