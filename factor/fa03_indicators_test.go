package factor

import (
	"fmt"
	"math"
	"testing"

	"github.com/banbox/banta/tav"
)

type fa03Case struct {
	name      string
	node      *Node
	warmup    int
	retention int
	want      []float64
}

func fa03APINodes() []fa03Case {
	o := Field("prices", "open", "1h")
	h := Field("prices", "high", "1h")
	l := Field("prices", "low", "1h")
	c := Field("prices", "close", "1h")
	v := Field("prices", "volume", "1h")
	bv := Field("prices", "buy_volume", "1h")

	return []fa03Case{
		// 1. Adaptive & Moving Averages (scalar TS)
		{name: "dema", node: DEMA(c, 3), warmup: 4, retention: 5},
		{name: "tema", node: TEMA(c, 3), warmup: 6, retention: 7},
		{name: "hma", node: HMA(c, 9), warmup: 10, retention: 11},
		{name: "kama", node: KAMA(c, 4), warmup: 4, retention: 5},
		{name: "kama-by", node: KAMABy(c, 4, 2, 10), warmup: 4, retention: 5},
		{name: "alma", node: ALMA(c, 5, 0.85, 6.0), warmup: 4, retention: 5},
		{name: "trima", node: TRIMA(c, 5), warmup: 4, retention: 5},
		{name: "er", node: ER(c, 4), warmup: 4, retention: 5},
		{name: "kalman1d", node: Kalman1D(c, 0.05, 0.5), warmup: 0, retention: 2},
		{name: "hurst", node: Hurst(c, 10), warmup: 9, retention: 10},

		// 2. Directional, Trend & Channel
		{name: "adx", node: ADX(h, l, c, 4), warmup: 7, retention: 8},
		{name: "dmi-plus", node: PlusDI(h, l, c, 4), warmup: 4, retention: 5},
		{name: "dmi-minus", node: MinusDI(h, l, c, 4), warmup: 4, retention: 5},
		{name: "natr", node: NATR(h, l, c, 4), warmup: 4, retention: 5},
		{name: "chop", node: CHOP(h, l, c, 4), warmup: 4, retention: 5},
		{name: "aroon-up", node: AroonUp(h, l, 4), warmup: 4, retention: 5},
		{name: "aroon-down", node: AroonDown(h, l, 4), warmup: 4, retention: 5},
		{name: "aroon-osc", node: AroonOsc(h, l, 4), warmup: 4, retention: 5},
		{name: "vortex-plus", node: VortexPlus(h, l, c, 4), warmup: 4, retention: 5},
		{name: "vortex-minus", node: VortexMinus(h, l, c, 4), warmup: 4, retention: 5},
		{name: "donchian-upper", node: DonchianUpper(h, l, 4), warmup: 3, retention: 4},
		{name: "donchian-mid", node: DonchianMid(h, l, 4), warmup: 3, retention: 4},
		{name: "donchian-lower", node: DonchianLower(h, l, 4), warmup: 3, retention: 4},
		{name: "keltner-upper", node: KeltnerUpper(h, l, c, 4, 2.0), warmup: 4, retention: 5},
		{name: "keltner-mid", node: KeltnerMid(h, l, c, 4, 2.0), warmup: 3, retention: 4},
		{name: "keltner-lower", node: KeltnerLower(h, l, c, 4, 2.0), warmup: 4, retention: 5},
		{name: "supertrend", node: Supertrend(h, l, c, 4, 2.5), warmup: 4, retention: 5},
		{name: "supertrend-dir", node: SupertrendDir(h, l, c, 4, 2.5), warmup: 4, retention: 5},
		{name: "sar", node: SAR(h, l, 0.02, 0.02, 0.2), warmup: 1, retention: 2},
		{name: "stc", node: STC(c, 3, 6, 4), warmup: 5, retention: 6},
		{name: "ichimoku-conversion", node: IchimokuConversion(h, l, 4), warmup: 3, retention: 4},
		{name: "ichimoku-base", node: IchimokuBase(h, l, 6), warmup: 5, retention: 6},

		// 3. Volume & Orderflow
		{name: "cmf", node: CMF(h, l, c, v, 4), warmup: 3, retention: 4},
		{name: "adl", node: ADL(h, l, c, v), warmup: 0, retention: 2},
		{name: "adosc", node: ChaikinOsc(h, l, c, v, 3, 6), warmup: 5, retention: 6},
		{name: "volume-delta", node: VolumeDelta(v, bv), warmup: 0, retention: 2},
		{name: "cvd", node: CVD(v, bv), warmup: 0, retention: 2},
		{name: "rolling-cvd", node: RollingCVD(v, bv, 4), warmup: 3, retention: 4},
		{name: "taker-buy-ratio", node: TakerBuyRatio(v, bv, 4), warmup: 3, retention: 4},

		// 4. OHLC Range Volatility Estimators
		{name: "parkinson-vol", node: ParkinsonVol(h, l, 4), warmup: 3, retention: 4},
		{name: "garman-klass-vol", node: GarmanKlassVol(o, h, l, c, 4), warmup: 3, retention: 4},
		{name: "yang-zhang-vol", node: YangZhangVol(o, h, l, c, 4), warmup: 4, retention: 5},
	}
}

func TestFA03IndicatorContractsLookbacksAndValidation(t *testing.T) {
	cases := fa03APINodes()
	if len(cases) != 42 {
		t.Fatalf("expected 42 new scalar indicator cases, got %d", len(cases))
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := Compile(map[string]*Node{"value": tc.node})
			if err != nil {
				t.Fatal(err)
			}
			if plan.WarmupLength() != tc.warmup || plan.StateRetention() != tc.retention {
				t.Fatalf("warmup/retention = %d/%d, want %d/%d", plan.WarmupLength(), plan.StateRetention(), tc.warmup, tc.retention)
			}
			if tc.node.Spec.Operator != tc.name || !tc.node.Spec.Batch || !tc.node.Spec.Incremental || tc.node.Spec.Version != "indicators-1/banta-0.4.1" {
				t.Fatalf("unexpected spec: %+v", tc.node.Spec)
			}
			if plan.Inputs()[0].WarmupLength != tc.warmup {
				t.Fatalf("subscription warmup = %d, want %d", plan.Inputs()[0].WarmupLength, tc.warmup)
			}

			// Mutated contracts must fail closed.
			bad := map[string]func(*Node){
				"missing dep":  func(n *Node) { n.Inputs = n.Inputs[:len(n.Inputs)-1] },
				"extra dep":    func(n *Node) { n.Inputs = append(n.Inputs, n.Inputs[0]) },
				"nil dep":      func(n *Node) { n.Inputs[0] = nil },
				"unknown parm": func(n *Node) { n.Spec.Parameters["unknown"] = 1 },
				"no batch":     func(n *Node) { n.Spec.Batch = false },
				"no inc":       func(n *Node) { n.Spec.Incremental = false },
			}
			for param := range tc.node.Spec.Parameters {
				p := param
				bad["missing "+p] = func(n *Node) { delete(n.Spec.Parameters, p) }
				bad["nan "+p] = func(n *Node) { n.Spec.Parameters[p] = math.NaN() }
				bad["inf "+p] = func(n *Node) { n.Spec.Parameters[p] = math.Inf(1) }
				bad["neg "+p] = func(n *Node) { n.Spec.Parameters[p] = -0.5 }
			}
			for mName, mutate := range bad {
				cp := cloneIndicatorNode(tc.node)
				mutate(cp)
				if _, err := Compile(map[string]*Node{"value": cp}); err == nil {
					t.Fatalf("accepted malformed %s (%s)", tc.name, mName)
				}
			}
		})
	}

	// Verify multi-output helpers and ADOSC alias CSE identity.
	h, l, c, v := Field("prices", "high", "1h"), Field("prices", "low", "1h"), Field("prices", "close", "1h"), Field("prices", "volume", "1h")
	pdi, mdi, adx := DMI(h, l, c, 14)
	au, ad, ao := Aroon(h, l, 14)
	vp, vm := Vortex(h, l, c, 14)
	du, dm, dl := Donchian(h, l, 20)
	ku, km, kl := Keltner(h, l, c, 20, 2.0)
	multiPlan, err := New().
		Add("pdi", pdi).Add("mdi", mdi).Add("adx", adx).
		Add("au", au).Add("ad", ad).Add("ao", ao).
		Add("vp", vp).Add("vm", vm).
		Add("du", du).Add("dm", dm).Add("dl", dl).
		Add("ku", ku).Add("km", km).Add("kl", kl).
		Add("c1", ChaikinOsc(h, l, c, v, 3, 10)).
		Add("c2", ADOSC(h, l, c, v, 3, 10)).
		Compile()
	if err != nil {
		t.Fatal(err)
	}
	p1, _ := Compile(map[string]*Node{"x": ChaikinOsc(h, l, c, v, 3, 10)})
	p2, _ := Compile(map[string]*Node{"x": ADOSC(h, l, c, v, 3, 10)})
	if p1.Hash() != p2.Hash() || multiPlan == nil {
		t.Fatalf("ADOSC alias hash mismatch: %s vs %s", p1.Hash(), p2.Hash())
	}

	// Specific domain rejections.
	invalidNodes := []*Node{
		HMA(c, 1),
		Hurst(c, 7),
		CHOP(h, l, c, 1),
		YangZhangVol(c, h, l, c, 1),
		KAMABy(c, 5, 10, 10),
		KAMABy(c, 5, 12, 10),
		ALMA(c, 5, 1.1, 6.0),
		ALMA(c, 5, 0.85, 0),
		Kalman1D(c, 0, 1.0),
		Kalman1D(c, 1.0, 0),
		Supertrend(h, l, c, 5, 0),
		SupertrendDir(h, l, c, 5, 0),
		SAR(h, l, 0, 0.02, 0.2),
		SAR(h, l, 0.02, 0, 0.2),
		SAR(h, l, 0.2, 0.02, 0.1),
		STC(c, 10, 10, 5),
		STC(c, 12, 10, 5),
		ChaikinOsc(h, l, c, v, 10, 10),
		ChaikinOsc(h, l, c, v, 12, 10),
	}
	for i, bad := range invalidNodes {
		if _, err := Compile(map[string]*Node{"value": bad}); err == nil {
			t.Fatalf("invalid domain case %d (%s) was accepted", i, bad.Spec.Operator)
		}
	}
}

func fa03OHLCVData(length int, shape string) (open, high, low, close, volume, buyVol []float64) {
	open = make([]float64, length)
	high = make([]float64, length)
	low = make([]float64, length)
	close = make([]float64, length)
	volume = make([]float64, length)
	buyVol = make([]float64, length)
	for i := 0; i < length; i++ {
		c := 120 + 9*math.Sin(float64(i)/3.2) + 3*math.Cos(float64(i)/5.1) + float64(i)*0.12
		o := c - 0.8*math.Sin(float64(i)*0.7)
		hi := max(o, c) + 1.8 + float64(i%4)*0.3
		lo := min(o, c) - 1.6 - float64(i%3)*0.25
		switch shape {
		case "flat":
			o, hi, lo, c = 100, 100, 100, 100
		case "up":
			c = 100 + float64(i)*1.2
			o = c - 0.5
			hi = c + 1.5
			lo = o - 1.0
		case "down":
			c = 400 - float64(i)*1.1
			o = c + 0.6
			hi = o + 1.2
			lo = c - 1.4
		}
		v := 20 + float64((i*7)%19)
		bv := v * (0.35 + 0.05*float64(i%7))
		open[i], high[i], low[i], close[i], volume[i], buyVol[i] = o, hi, lo, c, v, bv
	}
	return
}

func fa03ReferenceCases(period int, open, high, low, close, volume, buyVol []float64) []fa03Case {
	o := Field("prices", "open", "1h")
	h := Field("prices", "high", "1h")
	l := Field("prices", "low", "1h")
	c := Field("prices", "close", "1h")
	v := Field("prices", "volume", "1h")
	bv := Field("prices", "buy_volume", "1h")

	e1 := tav.EMA(close, period)
	e2 := tav.EMA(e1, period)
	e3 := tav.EMA(e2, period)
	temaWant := make([]float64, len(close))
	for i := range temaWant {
		temaWant[i] = 3*e1[i] - 3*e2[i] + e3[i]
	}

	hmaPeriod := max(2, period)
	chopPeriod := max(2, period)
	yzPeriod := max(2, period)
	hurstPeriod := max(8, period)

	aroonUp, aroonOsc, aroonDown := tav.Aroon(high, low, period)
	vortexP, vortexM := tav.Vortex(high, low, close, period)
	donUp, donMid, donLow := tav.Donchian(high, low, period)
	kcUp, kcMid, kcLow := tav.KeltnerChannel(high, low, close, period, 2.0)
	stLine, stDir := tav.SupertrendDir(high, low, close, period, 2.5)
	_, ichiConv, _ := tav.Donchian(high, low, period)
	_, ichiBase, _ := tav.Donchian(high, low, period+2)

	var pdiWant, mdiWant, adxWant []float64
	pdiWant, mdiWant = tav.PluMinDI(high, low, close, period)
	adxWant = tav.ADX(high, low, close, period)

	return []fa03Case{
		{name: "dema", node: DEMA(c, period), want: tav.DEMA(close, period)},
		{name: "tema", node: TEMA(c, period), want: temaWant},
		{name: "hma", node: HMA(c, hmaPeriod), want: tav.HMA(close, hmaPeriod)},
		{name: "kama", node: KAMA(c, period), want: tav.KAMA(close, period)},
		{name: "kama-by", node: KAMABy(c, period, 3, 15), want: tav.KAMABy(close, period, 3, 15)},
		{name: "alma", node: ALMA(c, period, 0.85, 6.0), want: tav.ALMA(close, period, 6.0, 0.85)},
		{name: "trima", node: TRIMA(c, period), want: tav.TRIMA(close, period)},
		{name: "er", node: ER(c, period), want: tav.ER(close, period)},
		{name: "kalman1d", node: Kalman1D(c, 0.05, 0.5), want: tav.Kalman1D(close, 0.05, 0.5)},
		{name: "hurst", node: Hurst(c, hurstPeriod), want: tav.Hurst(close, hurstPeriod)},
		{name: "adx", node: ADX(h, l, c, period), want: adxWant},
		{name: "dmi-plus", node: PlusDI(h, l, c, period), want: pdiWant},
		{name: "dmi-minus", node: MinusDI(h, l, c, period), want: mdiWant},
		{name: "natr", node: NATR(h, l, c, period), want: tav.NATR(high, low, close, period)},
		{name: "chop", node: CHOP(h, l, c, chopPeriod), want: tav.CHOP(high, low, close, chopPeriod)},
		{name: "aroon-up", node: AroonUp(h, l, period), want: aroonUp},
		{name: "aroon-down", node: AroonDown(h, l, period), want: aroonDown},
		{name: "aroon-osc", node: AroonOsc(h, l, period), want: aroonOsc},
		{name: "vortex-plus", node: VortexPlus(h, l, c, period), want: vortexP},
		{name: "vortex-minus", node: VortexMinus(h, l, c, period), want: vortexM},
		{name: "donchian-upper", node: DonchianUpper(h, l, period), want: donUp},
		{name: "donchian-mid", node: DonchianMid(h, l, period), want: donMid},
		{name: "donchian-lower", node: DonchianLower(h, l, period), want: donLow},
		{name: "keltner-upper", node: KeltnerUpper(h, l, c, period, 2.0), want: kcUp},
		{name: "keltner-mid", node: KeltnerMid(h, l, c, period, 2.0), want: kcMid},
		{name: "keltner-lower", node: KeltnerLower(h, l, c, period, 2.0), want: kcLow},
		{name: "supertrend", node: Supertrend(h, l, c, period, 2.5), want: stLine},
		{name: "supertrend-dir", node: SupertrendDir(h, l, c, period, 2.5), want: stDir},
		{name: "sar", node: SAR(h, l, 0.02, 0.02, 0.2), want: tav.SAR(high, low, 0.02, 0.2)},
		{name: "stc", node: STC(c, 3, 8, period), want: tav.STC(close, period, 3, 8, 0.5)},
		{name: "ichimoku-conversion", node: IchimokuConversion(h, l, period), want: ichiConv},
		{name: "ichimoku-base", node: IchimokuBase(h, l, period+2), want: ichiBase},
		{name: "cmf", node: CMF(h, l, c, v, period), want: tav.CMF(high, low, close, volume, period)},
		{name: "adl", node: ADL(h, l, c, v), want: tav.ADL(high, low, close, volume)},
		{name: "adosc", node: ChaikinOsc(h, l, c, v, 3, 8), want: tav.ADOSC(high, low, close, volume, 3, 8)},
		{name: "volume-delta", node: VolumeDelta(v, bv), want: tav.VolumeDelta(volume, buyVol)},
		{name: "cvd", node: CVD(v, bv), want: tav.CVD(volume, buyVol)},
		{name: "rolling-cvd", node: RollingCVD(v, bv, period), want: tav.RollingCVD(volume, buyVol, period)},
		{name: "taker-buy-ratio", node: TakerBuyRatio(v, bv, period), want: tav.TakerBuyRatio(volume, buyVol, period)},
		{name: "parkinson-vol", node: ParkinsonVol(h, l, period), want: tav.ParkinsonVol(high, low, period)},
		{name: "garman-klass-vol", node: GarmanKlassVol(o, h, l, c, period), want: tav.GarmanKlassVol(open, high, low, close, period)},
		{name: "yang-zhang-vol", node: YangZhangVol(o, h, l, c, yzPeriod), want: tav.YangZhangVol(open, high, low, close, yzPeriod)},
	}
}

func fa03Snapshots(t *testing.T, open, high, low, close, volume, buyVol []float64) []*Snapshot {
	t.Helper()
	snaps := make([]*Snapshot, len(close))
	for i := range snaps {
		snaps[i] = testSnapshot(t, int64(i+1)*1000, map[int32]map[string]any{
			1: {
				"open": open[i], "high": high[i], "low": low[i], "close": close[i],
				"volume": volume[i], "buy_volume": buyVol[i],
			},
		})
	}
	return snaps
}

func TestFA03IndicatorsMatchPublishedTavAndWarmupStepParity(t *testing.T) {
	for _, shape := range []string{"wave", "flat", "up", "down"} {
		for _, period := range []int{2, 8, 20} {
			t.Run(fmt.Sprintf("%s/period%d", shape, period), func(t *testing.T) {
				open, high, low, close, volume, buyVol := fa03OHLCVData(96, shape)
				cases := fa03ReferenceCases(period, open, high, low, close, volume, buyVol)
				b := New()
				for _, tc := range cases {
					b.Add(tc.name, tc.node)
				}
				// Also include distinct start != inc SAR to exercise general Parabolic SAR parameters.
				b.Add("sar_custom", SAR(Field("prices", "high", "1h"), Field("prices", "low", "1h"), 0.01, 0.03, 0.25))
				plan, err := b.Compile()
				if err != nil {
					t.Fatal(err)
				}

				snaps := fa03Snapshots(t, open, high, low, close, volume, buyVol)
				batch, err := plan.Batch(snaps, len(snaps))
				if err != nil {
					t.Fatal(err)
				}

				// Exercise Session Warmup for the first 15 grids followed by live Evaluate!
				sess, err := NewSession(plan)
				if err != nil {
					t.Fatal(err)
				}
				const warmGrids = 15
				for i := 0; i < warmGrids; i++ {
					if err := sess.Warmup(snaps[i]); err != nil {
						t.Fatal(err)
					}
				}
				for i := warmGrids; i < len(snaps); i++ {
					frame, err := sess.Evaluate(snaps[i])
					if err != nil {
						t.Fatal(err)
					}
					compareTechnicalFrames(t, batch[i], frame, fmt.Sprintf("warmup+eval/%d", i))
				}

				// Also verify step-by-step Evaluate from grid 0 matches both Batch and published tav.
				fullSess, _ := NewSession(plan)
				for i, snap := range snaps {
					frame, err := fullSess.Evaluate(snap)
					if err != nil {
						t.Fatal(err)
					}
					compareTechnicalFrames(t, batch[i], frame, fmt.Sprintf("full-eval/%d", i))
					for _, tc := range cases {
						want := technicalNumeric(tc.want[i])
						compareNumeric(t, want, frame.Values[tc.name][1], fmt.Sprintf("tav/%s/%d", tc.name, i))
					}
				}
			})
		}
	}
}

func TestFA03MissingBarsBoundedStateForkRollbackCommitAndDynamicPIT(t *testing.T) {
	// 240 bars > 2*StateRetention (~20) ensures Session series trimming actively runs across all indicators.
	const totalBars = 240
	open, high, low, close, volume, buyVol := fa03OHLCVData(totalBars, "wave")
	cases := fa03ReferenceCases(5, open, high, low, close, volume, buyVol)
	b := New()
	for _, tc := range cases {
		b.Add(tc.name, tc.node)
	}
	plan, err := b.Compile()
	if err != nil {
		t.Fatal(err)
	}

	allSnaps := make([]*Snapshot, totalBars)
	for i := 0; i < totalBars; i++ {
		fields1 := map[string]any{
			"open": open[i], "high": high[i], "low": low[i], "close": close[i],
			"volume": volume[i], "buy_volume": buyVol[i],
		}
		fields2 := map[string]any{
			"open": open[i] * 1.1, "high": high[i] * 1.1, "low": low[i] * 1.1, "close": close[i] * 1.1,
			"volume": volume[i] + 5, "buy_volume": buyVol[i] + 2,
		}
		switch i {
		case 3:
			fields1["close"] = nil
		case 11:
			delete(fields1, "buy_volume")
		case 19:
			fields1["high"] = "corrupt"
		case 28:
			fields1["open"] = math.NaN()
		case 37:
			fields1["volume"] = math.Inf(1)
		}
		if i >= 70 && i < 95 {
			delete(fields1, "low")
		}
		allSnaps[i] = testSnapshot(t, int64(i+1)*1000, map[int32]map[string]any{1: fields1, 2: fields2})
	}

	fullBatch, err := plan.Batch(allSnaps, len(allSnaps))
	if err != nil {
		t.Fatal(err)
	}

	sess, err := NewSession(plan)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < totalBars-2; i++ {
		frame, err := sess.Evaluate(allSnaps[i])
		if err != nil {
			t.Fatal(err)
		}
		compareTechnicalFrames(t, fullBatch[i], frame, fmt.Sprintf("missing-parity/%d", i))

		// At bar 55 and bar 150 (after Session trimming!), fork a speculative candidate,
		// mutate its state across 6 bars, and discard it without committing.
		if i == 55 || i == 150 {
			cand, rev, err := sess.fork()
			if err != nil {
				t.Fatalf("fork at %d failed: %v", i, err)
			}
			for j := i + 1; j <= i+6; j++ {
				// Feed wild shock bars to the speculative fork to force SAR flips, Supertrend flips, Kalman/KAMA shifts!
				shock := testSnapshot(t, int64(j+1)*1000, map[int32]map[string]any{
					1: {"open": 999.0, "high": 1500.0, "low": 10.0, "close": 12.0, "volume": 50000.0, "buy_volume": 49000.0},
					2: {"open": 50.0, "high": 60.0, "low": 1.0, "close": 2.0, "volume": 1000.0, "buy_volume": 10.0},
				})
				if _, err := cand.Evaluate(shock); err != nil {
					t.Fatal(err)
				}
			}
			if sess.revision != rev {
				t.Fatal("speculative fork mutated committed session revision")
			}
		}
	}

	// Verify bounded series retention.
	for _, asset := range sess.assets {
		for _, series := range asset.env.Items {
			if len(series.Data) > 2*plan.StateRetention() {
				t.Fatalf("series Data grew unbounded: len=%d retention=%d", len(series.Data), plan.StateRetention())
			}
		}
	}

	// Fork, evaluate bar totalBars-2, commit, and verify bar totalBars-1 matches fullBatch!
	cand, rev, err := sess.fork()
	if err != nil {
		t.Fatal(err)
	}
	fSecondLast, err := cand.Evaluate(allSnaps[totalBars-2])
	if err != nil {
		t.Fatal(err)
	}
	compareTechnicalFrames(t, fullBatch[totalBars-2], fSecondLast, "fork-eval")
	if err := sess.commit(cand, rev); err != nil {
		t.Fatal(err)
	}
	fLast, err := sess.Evaluate(allSnaps[totalBars-1])
	if err != nil {
		t.Fatal(err)
	}
	compareTechnicalFrames(t, fullBatch[totalBars-1], fLast, "post-commit-continuity")

	// Dynamic PIT universe departure & re-entry parity across all 42 indicators.
	pitLen := 36
	pitSnaps := make([]*Snapshot, pitLen)
	for i := 0; i < pitLen; i++ {
		sids := []int32{1, 2}
		uVer := "pit-u-0"
		if i >= 14 && i < 22 {
			sids = []int32{2, 3}
			uVer = "pit-u-1"
		} else if i >= 22 {
			uVer = "pit-u-2"
		}
		vals := make(map[int32]map[string]any, len(sids))
		for _, sid := range sids {
			scale := 1.0 + 0.1*float64(sid-1)
			vals[sid] = map[string]any{
				"open": open[i] * scale, "high": high[i] * scale, "low": low[i] * scale, "close": close[i] * scale,
				"volume": volume[i] * scale, "buy_volume": buyVol[i] * scale,
			}
		}
		base := testSnapshot(t, int64(i+1)*1000, vals)
		spec := base.Spec()
		spec.Universe.Version = uVer
		var rows []VersionRecord
		var reqs []Requirement
		for _, sid := range sids {
			r, _ := base.Row(sid, "prices", "1h")
			rows = append(rows, r)
			reqs = append(reqs, Requirement{SID: sid, Source: "prices", TimeFrame: "1h", EventTime: spec.GridTime})
		}
		pitSnaps[i], err = Freeze(spec, rows, reqs)
		if err != nil {
			t.Fatal(err)
		}
	}
	pitBatch, err := plan.Batch(pitSnaps, len(pitSnaps))
	if err != nil {
		t.Fatal(err)
	}
	pitSess, _ := NewSession(plan)
	for i, s := range pitSnaps {
		got, err := pitSess.Evaluate(s)
		if err != nil {
			t.Fatal(err)
		}
		compareTechnicalFrames(t, pitBatch[i], got, fmt.Sprintf("dynamic-pit/%d", i))
	}
}

func TestFA03MultiStageCSToTSIndicatorsParity(t *testing.T) {
	c := Field("prices", "close", "1h")
	h := Field("prices", "high", "1h")
	l := Field("prices", "low", "1h")
	v := Field("prices", "volume", "1h")

	// Cross-sectional StageCS outputs fed directly into new StageTS adaptive/trend/statistical indicators!
	rankC := Add(PctRank(c), Constant(1.0, "1h"))
	rankH := Add(PctRank(h), Constant(1.5, "1h"))
	rankL := Add(PctRank(l), Constant(0.5, "1h"))
	rankV := Add(PctRank(v), Constant(1.0, "1h"))

	plan, err := New().
		Add("cs_dema", ZScore(DEMA(rankC, 3))).
		Add("cs_hma", ZScore(HMA(rankC, 4))).
		Add("cs_kama", ZScore(KAMA(rankC, 4))).
		Add("cs_alma", ZScore(ALMA(rankC, 4, 0.85, 6.0))).
		Add("cs_kalman", ZScore(Kalman1D(rankC, 0.05, 0.2))).
		Add("cs_hurst", ZScore(Hurst(rankC, 8))).
		Add("cs_adx", ZScore(ADX(rankH, rankL, rankC, 3))).
		Add("cs_cmf", ZScore(CMF(rankH, rankL, rankC, rankV, 3))).
		Compile()
	if err != nil {
		t.Fatal(err)
	}
	if plan.StageCount() != 4 {
		t.Fatalf("expected 4 stages (TS -> CS -> TS -> CS), got %d", plan.StageCount())
	}

	open, high, low, close, volume, _ := fa03OHLCVData(28, "wave")
	snaps := make([]*Snapshot, 28)
	for i := range snaps {
		snaps[i] = testSnapshot(t, int64(i+1)*1000, map[int32]map[string]any{
			1: {"close": close[i], "high": high[i], "low": low[i], "volume": volume[i]},
			2: {"close": open[i] * 1.05, "high": high[i] * 1.08, "low": low[i] * 1.02, "volume": volume[i] * 1.4},
			3: {"close": close[27-i], "high": high[27-i], "low": low[27-i], "volume": volume[27-i] + 3},
			4: {"close": 100 + float64(i%5)*3, "high": 105 + float64(i%5)*3, "low": 95 + float64(i%5)*3, "volume": 30 + float64(i)},
		})
	}
	batch, err := plan.Batch(snaps, len(snaps))
	if err != nil {
		t.Fatal(err)
	}
	sess, _ := NewSession(plan)
	for i, s := range snaps {
		got, err := sess.Evaluate(s)
		if err != nil {
			t.Fatal(err)
		}
		compareTechnicalFrames(t, batch[i], got, fmt.Sprintf("cs-ts-cs/%d", i))
	}
}
