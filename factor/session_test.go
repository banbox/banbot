package factor

import (
	"fmt"
	"math"
	"reflect"
	"testing"
)

func compareNumeric(t *testing.T, want, got Numeric, where string) {
	t.Helper()
	if want.Validity != got.Validity {
		t.Fatalf("%s validity: want %s got %s (%g/%g)", where, want.Validity, got.Validity, want.Value, got.Value)
	}
	if want.Validity == Valid && math.Abs(want.Value-got.Value) > 1e-10+1e-8*math.Abs(want.Value) {
		t.Fatalf("%s value: want %.14g got %.14g", where, want.Value, got.Value)
	}
}

func TestSessionBatchParityWarmupNaNsAndRecursiveChunks(t *testing.T) {
	price := Field("prices", "close", "1h")
	plan, err := New().Add("lag", Lag(price, 3)).Add("return", Return(price, 2)).Add("ema", EMA(price, 5)).Add("stddev0", StdDev(price, 5, 0)).Add("stddev1", StdDev(price, 5, 1)).Add("ts-cs-ts", EMA(ZScore(Return(price, 1)), 3)).Compile()
	if err != nil {
		t.Fatal(err)
	}
	snapshots := make([]*Snapshot, 140)
	for i := range snapshots {
		values := make(map[int32]map[string]any)
		for sid := int32(1); sid <= 4; sid++ {
			fields := map[string]any{"close": 100 + float64(sid)*math.Sin(float64(i)/7) + float64(i)*float64(sid)/20}
			switch i {
			case 2, 14, 72:
				fields["close"] = nil
			case 20:
				fields["close"] = math.NaN()
			case 41:
				delete(fields, "close")
			case 43:
				fields["close"] = "bad"
			}
			values[sid] = fields
		}
		snapshots[i] = testSnapshot(t, int64(i+1)*1000, values)
	}
	batch, err := plan.Batch(snapshots, len(snapshots))
	if err != nil {
		t.Fatal(err)
	}
	for _, chunkSize := range []int{1, 7, 33, len(snapshots)} {
		session, err := NewSession(plan)
		if err != nil {
			t.Fatal(err)
		}
		for start := 0; start < len(snapshots); start += chunkSize {
			for i := start; i < min(start+chunkSize, len(snapshots)); i++ {
				frame, err := session.Evaluate(snapshots[i])
				if err != nil {
					t.Fatal(err)
				}
				for name, column := range frame.Values {
					for sid, got := range column {
						compareNumeric(t, batch[i].Values[name][sid], got, fmt.Sprintf("%s t=%d sid=%d", name, i, sid))
					}
				}
			}
		}
		for _, asset := range session.assets {
			for _, series := range asset.env.Items {
				if len(series.Data) > 2*plan.StateRetention() {
					t.Fatalf("unbounded banta series: %d > %d", len(series.Data), 2*plan.StateRetention())
				}
			}
		}
	}
}

func TestSessionSharedNodeOnceAndImmutableConsumers(t *testing.T) {
	plan, err := New().Add("account-a", EMA(Field("prices", "close", "1h"), 3)).Add("account-b", EMA(Field("prices", "close", "1h"), 3)).Compile()
	if err != nil {
		t.Fatal(err)
	}
	if plan.NodeCount() != 2 {
		t.Fatal("account aliases duplicated computation")
	}
	session, _ := NewSession(plan)
	snapshot := testSnapshot(t, 1000, map[int32]map[string]any{1: {"close": 10.0}, 2: {"close": 20.0}})
	first, err := session.Evaluate(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	counts := session.Updates()
	first.Values["account-a"][1] = Numeric{999, Valid}
	second, err := session.Evaluate(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(counts, session.Updates()) {
		t.Fatal("consumer updated shared banta state twice")
	}
	if second.Values["account-a"][1].Validity != Warmup {
		t.Fatal("consumer mutated frame cache")
	}
	for _, count := range counts {
		if count != 2 {
			t.Fatalf("shared TS updated %d times want once per asset", count)
		}
	}
	otherSession, _ := NewSession(plan)
	if _, err := otherSession.Evaluate(snapshot); err != nil {
		t.Fatal(err)
	}
	if otherSession.assets[1].env == session.assets[1].env {
		t.Fatal("separate sessions shared mutable banta owner")
	}
}

func TestSessionTSCSOrderingAndGroupOperators(t *testing.T) {
	price := Field("prices", "close", "1h")
	x := Field("prices", "x", "1h")
	plan, err := New().Add("rank", Rank(price)).Add("z", ZScore(price)).Add("winsor", Winsorize(price, 0.25)).Add("median", Quantile(price, 0.5)).Add("demean", GroupDemean(price, "prices", "sector")).Add("groupz", GroupZScore(price, "prices", "sector")).Add("residual", Residual(price, x)).Add("after-cs", EMA(Rank(price), 2)).Compile()
	if err != nil {
		t.Fatal(err)
	}
	session, _ := NewSession(plan)
	for i := 1; i <= 3; i++ {
		snapshot := testSnapshot(t, int64(i)*1000, map[int32]map[string]any{1: {"close": 5.0, "x": 1.0, "sector": "A"}, 2: {"close": 7.0, "x": 2.0, "sector": "A"}, 3: {"close": 9.0, "x": 3.0, "sector": "B"}, 4: {"close": 11.0, "x": 4.0, "sector": "B"}})
		frame, err := session.Evaluate(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		for sid := int32(1); sid <= 4; sid++ {
			compareNumeric(t, Numeric{float64(sid - 1), Valid}, frame.Values["rank"][sid], "rank")
			compareNumeric(t, Numeric{0, Valid}, frame.Values["residual"][sid], "regression")
			compareNumeric(t, Numeric{8, Valid}, frame.Values["median"][sid], "median")
			if i >= 2 {
				compareNumeric(t, Numeric{float64(sid - 1), Valid}, frame.Values["after-cs"][sid], "TS-CS-TS")
			}
		}
		compareNumeric(t, Numeric{-1, Valid}, frame.Values["demean"][1], "group mean")
		compareNumeric(t, Numeric{1, Valid}, frame.Values["groupz"][2], "group z")
		compareNumeric(t, Numeric{6.5, Valid}, frame.Values["winsor"][1], "winsor")
	}
}

func TestSessionDefault26Bars24AssetsAndConstantMissing(t *testing.T) {
	plan, err := MomentumVolatility("prices", "close", "1h", 24)
	if err != nil {
		t.Fatal(err)
	}
	session, _ := NewSession(plan)
	var last Frame
	for i := 0; i < 26; i++ {
		values := make(map[int32]map[string]any)
		for sid := int32(1); sid <= 24; sid++ {
			rateSid := sid
			if sid == 2 {
				rateSid = 1
			}
			value := 100 + float64(rateSid)*float64(i)/10 + math.Sin(float64(i)*float64(rateSid)/13)
			fields := map[string]any{"close": value, "tradable": true, "sector": "all"}
			if sid == 24 {
				fields["close"] = nil
			}
			values[sid] = fields
		}
		snapshot := testSnapshot(t, int64(i+1)*3600000, values)
		last, err = session.Evaluate(snapshot)
		if err != nil {
			t.Fatal(err)
		}
	}
	if last.Values["score"][1].Validity != Valid || last.Values["volatility"][1].Validity != Valid {
		t.Fatal("26 bars did not yield 24 valid hourly returns")
	}
	if last.Values["score"][24].Validity == Valid {
		t.Fatal("NULL asset scored")
	}
	compareNumeric(t, last.Values["score"][1], last.Values["score"][2], "deterministic equal-score assets")
	constantPlan, _ := New().Add("score", ZScore(Field("prices", "close", "1h"))).Compile()
	constantSession, _ := NewSession(constantPlan)
	constant, err := constantSession.Evaluate(testSnapshot(t, 1000, map[int32]map[string]any{1: {"close": 7.0}, 2: {"close": 7.0}}))
	if err != nil {
		t.Fatal(err)
	}
	for _, score := range constant.Values["score"] {
		compareNumeric(t, Numeric{0, Valid}, score, "constant zscore")
	}
	incomplete, err := Freeze(testSnapshot(t, 2000, map[int32]map[string]any{1: {"close": 7.0}, 2: {"close": 7.0}}).Spec(), []VersionRecord{testRecord(1, 2000, map[string]any{"close": 7.0})}, []Requirement{{SID: 1, Source: "prices", Frequency: "1h", EventTime: 2000}, {SID: 2, Source: "prices", Frequency: "1h", EventTime: 2000}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := constantSession.Evaluate(incomplete); err == nil {
		t.Fatal("missing reference stream allowed partial ranking")
	}
}

func TestSessionDynamicUniverseTrackedContinuityWarmupAndBoundedRemoval(t *testing.T) {
	price := Field("prices", "close", "1h")
	plan, err := New().Add("ema", EMA(price, 2)).Add("after-cs", EMA(Rank(price), 2)).Compile()
	if err != nil {
		t.Fatal(err)
	}
	session, _ := NewSession(plan)
	makeSnapshot := func(event int64, version string, reference, tracked []int32) *Snapshot {
		values := make(map[int32]map[string]any)
		for _, sid := range append(append([]int32(nil), reference...), tracked...) {
			values[sid] = map[string]any{"close": float64(sid) * 10}
		}
		base := testSnapshot(t, event, values)
		spec := base.Spec()
		spec.Universe.Version = version
		spec.Universe.Reference = reference
		spec.Universe.Investable = reference
		spec.Universe.Tradable = reference
		spec.Universe.Tracked = tracked
		spec.Universe.Evaluation = reference
		rows := []VersionRecord{}
		need := []Requirement{}
		for sid := range values {
			row, _ := base.Row(sid, "prices", "1h")
			rows = append(rows, row)
			need = append(need, Requirement{SID: sid, Source: "prices", Frequency: "1h", EventTime: event})
		}
		snapshot, err := Freeze(spec, rows, need)
		if err != nil {
			t.Fatal(err)
		}
		return snapshot
	}
	snapshots := []*Snapshot{makeSnapshot(1000, "v1", []int32{1, 2}, nil), makeSnapshot(2000, "v2", []int32{2, 3}, []int32{1}), makeSnapshot(3000, "v3", []int32{2, 3}, nil), makeSnapshot(4000, "v4", []int32{1, 2, 3}, nil)}
	frames := make([]Frame, len(snapshots))
	for i, snapshot := range snapshots {
		frames[i], err = session.Evaluate(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		if i == 1 {
			compareNumeric(t, Numeric{10, Valid}, frames[i].Values["ema"][1], "tracked raw EMA continues")
			compareNumeric(t, Numeric{0.5, Valid}, frames[i].Values["after-cs"][2], "rank pool changed but downstream history continues")
			if frames[i].Values["ema"][3].Validity != Warmup {
				t.Fatal("new asset did not warm up")
			}
		}
		if i == 2 && session.assets[1] != nil {
			t.Fatal("departed untracked asset retained unbounded state")
		}
		if i == 3 && frames[i].Values["ema"][1].Validity != Warmup {
			t.Fatal("removed asset reentry silently reused old state")
		}
	}
	batch, err := plan.Batch(snapshots, len(snapshots))
	if err != nil {
		t.Fatal(err)
	}
	for i := range frames {
		for name, column := range frames[i].Values {
			for sid, value := range column {
				compareNumeric(t, value, batch[i].Values[name][sid], "dynamic cached/batch fallback")
			}
		}
	}
	before := session.Updates()
	oldVersion := makeSnapshot(5000, "v4", []int32{2, 3}, nil)
	if _, err := session.Evaluate(oldVersion); err == nil {
		t.Fatal("unversioned pool change accepted")
	}
	if !reflect.DeepEqual(before, session.Updates()) {
		t.Fatal("rejected pool mutated state")
	}
}

func TestSessionSourceIdentityAndCompleteActiveInputGate(t *testing.T) {
	plan, _ := New().Add("ema", EMA(Field("prices", "close", "1h"), 2)).Compile()
	session, _ := NewSession(plan)
	if _, err := session.Evaluate(testSnapshot(t, 1000, map[int32]map[string]any{1: {"close": 10.0}})); err != nil {
		t.Fatal(err)
	}
	before := session.Updates()
	base := testSnapshot(t, 2000, map[int32]map[string]any{1: {"close": 11.0}})
	spec := base.Spec()
	spec.SourceVersions["prices"] = "prices-v2"
	row, _ := base.Row(1, "prices", "1h")
	row.SourceVersion = "prices-v2"
	need := []Requirement{{SID: 1, Source: "prices", Frequency: "1h", EventTime: 2000}}
	changed, err := Freeze(spec, []VersionRecord{row}, need)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Evaluate(changed); err == nil {
		t.Fatal("source version mutated recursive cache identity")
	}
	spec = base.Spec()
	spec.Universe.Tracked = []int32{2}
	spec.SIDMap[2] = "C"
	spec.Universe.Version = "with-tracked"
	row.SourceVersion = "prices-v1"
	missingTracked, err := Freeze(spec, []VersionRecord{row}, need)
	if err != nil {
		t.Fatal(err)
	}
	if !missingTracked.Status().Ready {
		t.Fatal("fixture did not test declared coverage")
	}
	if _, err := session.Evaluate(missingTracked); err == nil {
		t.Fatal("tracked missing bar advanced TS")
	}
	row.EventTime = 1000
	need[0].AsOfLatest = true
	stale, err := Freeze(base.Spec(), []VersionRecord{row}, need)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Evaluate(stale); err == nil {
		t.Fatal("old TS bar used as new observation")
	}
	if !reflect.DeepEqual(before, session.Updates()) {
		t.Fatal("admission failure advanced recursive state")
	}
}

func TestCustomExplicitPureContractAndNaNNormalization(t *testing.T) {
	field := Field("prices", "close", "1h")
	custom := Custom("custom-v1", []*Node{field}, func(values []Numeric) Numeric { return Numeric{values[0].Value * 2, Valid} })
	plan, err := New().Add("double", custom).Compile()
	if err != nil {
		t.Fatal(err)
	}
	snapshot := testSnapshot(t, 1000, map[int32]map[string]any{1: {"close": 3.0}, 2: {"close": nil}})
	session, _ := NewSession(plan)
	frame, err := session.Evaluate(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	compareNumeric(t, Numeric{6, Valid}, frame.Values["double"][1], "custom")
	if frame.Values["double"][2].Validity != NonFinite {
		t.Fatal("custom emitted valid NaN")
	}
	batch, err := plan.Batch([]*Snapshot{snapshot}, 1)
	if err != nil {
		t.Fatal(err)
	}
	compareNumeric(t, frame.Values["double"][1], batch[0].Values["double"][1], "custom batch")
}

func TestMixedSourceAsOfFrequencyVisibilityAndExplicitSampling(t *testing.T) {
	close := Field("prices", "close", "1h")
	slow := AsOfField("fundamental", "value", "1d", "1h", 2500)
	plan, err := New().Add("score", Linear([]*Node{close, slow}, []float64{1, 1})).Add("sampled-ema", EMA(slow, 2)).Add("group", GroupDemean(close, "classification", "sector", "event")).Compile()
	if err != nil {
		t.Fatal(err)
	}
	inputs := plan.Inputs()
	if len(inputs) != 3 {
		t.Fatalf("subscription source/frequency union: %#v", inputs)
	}
	var snapshots []*Snapshot
	session, _ := NewSession(plan)
	for i := 1; i <= 3; i++ {
		event := int64(i) * 1000
		base := testSnapshot(t, event, map[int32]map[string]any{1: {"close": 10.0}, 2: {"close": 30.0}})
		spec := base.Spec()
		spec.SourceVersions["fundamental"] = "fundamental-v1"
		spec.SourceVersions["classification"] = "classification-v1"
		spec.Schemas["fundamental"] = "value:int"
		spec.Schemas["classification"] = "sector:string"
		rows := []VersionRecord{}
		need := []Requirement{}
		for sid := int32(1); sid <= 2; sid++ {
			price, _ := base.Row(sid, "prices", "1h")
			rows = append(rows, price)
			need = append(need, Requirement{SID: sid, Source: "prices", Frequency: "1h", EventTime: event})
			fundamental := testRecord(sid, 500, map[string]any{"value": int32(2)})
			fundamental.Series.Source = "fundamental"
			fundamental.Series.TimeFrame = "1d"
			fundamental.SourceVersion = "fundamental-v1"
			rows = append(rows, fundamental)
			future := fundamental
			future.Revision = 2
			future.AvailableAt = 2500
			future.IngestedAt = 2800
			future.Series.Values = map[string]any{"value": int32(4)}
			rows = append(rows, future)
			need = append(need, Requirement{SID: sid, Source: "fundamental", Frequency: "1d", EventTime: event, AsOfLatest: true, MaxAge: 2500})
			classification := fundamental
			classification.Series.Source = "classification"
			classification.Series.TimeFrame = "event"
			classification.Series.Values = map[string]any{"sector": "A"}
			classification.SourceVersion = "classification-v1"
			rows = append(rows, classification)
			need = append(need, Requirement{SID: sid, Source: "classification", Frequency: "event", EventTime: event, AsOfLatest: true})
		}
		snapshot, err := Freeze(spec, rows, need)
		if err != nil {
			t.Fatal(err)
		}
		snapshots = append(snapshots, snapshot)
		frame, err := session.Evaluate(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		want := 12.0
		if i == 3 {
			want = 14
		}
		compareNumeric(t, Numeric{want, Valid}, frame.Values["score"][1], "daily asof revision visibility")
		compareNumeric(t, Numeric{-10, Valid}, frame.Values["group"][1], "event-frequency classification")
		if i == 2 {
			compareNumeric(t, Numeric{2, Valid}, frame.Values["sampled-ema"][1], "explicit decision-grid sampling")
		}
	}
	batch, err := plan.Batch(snapshots, 3)
	if err != nil {
		t.Fatal(err)
	}
	compareNumeric(t, Numeric{10.0 / 3, Valid}, batch[2].Values["sampled-ema"][1], "asof sampled EMA parity")
	if _, err := New().Add("bad", AsOfField("fundamental", "value", "1d", "1h", 0)).Compile(); err == nil {
		t.Fatal("asof source without finite staleness contract accepted")
	}
}
