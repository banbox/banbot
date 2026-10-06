package factor

import (
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/banbox/banta/tav"
)

type technicalTestCase struct {
	name string
	node *Node
	want []float64
}

// The expected columns call the published tav functions directly, independent
// of the runtime dispatch, covering all scalar outputs of the new families.
func technicalTestCases(period int, data [][]float64) []technicalTestCase {
	high, low, close, volume := data[0], data[1], data[2], data[3]
	h, l, c, v := Field("prices", "high", "1h"), Field("prices", "low", "1h"), Field("prices", "signal", "1h"), Field("prices", "weight", "1h")
	macd, signal, hist := MACD(c, 2, 5, 3)
	ml, ms := tav.MACD(close, 2, 5, 3)
	mh := make([]float64, len(ml))
	for i := range mh {
		mh[i] = ml[i] - ms[i]
	}
	upper, middle, lower := BBands(c, period, 2.5, 1.5)
	bu, bm, bl := tav.BBANDS(close, period, 2.5, 1.5)
	return []technicalTestCase{
		{"sma", SMA(c, period), tav.SMA(close, period)},
		{"rma", RMA(c, period), tav.RMA(close, period)},
		{"wma", WMA(c, period), tav.WMA(close, period)},
		{"vwma", VWMA(c, v, period), tav.VWMA(close, volume, period)},
		{"rsi", RSI(c, period), tav.RSI(close, period)},
		{"roc", ROC(c, period), tav.ROC(close, period)},
		{"mom", MOM(c, period), tav.MOM(close, period)},
		{"tr", TR(h, l, c), tav.TR(high, low, close)},
		{"atr", ATR(h, l, c, period), tav.ATR(high, low, close, period)},
		{"cci", CCI(c, period), tav.CCI(close, period)},
		{"stoch", Stoch(h, l, c, period), tav.Stoch(high, low, close, period)},
		{"willr", WillR(h, l, c, period), tav.WillR(high, low, close, period)},
		{"obv", OBV(c, v), tav.OBV(close, volume)},
		{"mfi", MFI(h, l, c, v, period), tav.MFI(high, low, close, volume, period)},
		{"highest", Highest(c, period), tav.Highest(close, period)},
		{"lowest", Lowest(c, period), tav.Lowest(close, period)},
		{"macd", macd, ml}, {"macd-signal", signal, ms}, {"macd-hist", hist, mh},
		{"bbands-upper", upper, bu}, {"bbands-middle", middle, bm}, {"bbands-lower", lower, bl},
	}
}

func technicalData(length int, shape string) [][]float64 {
	data := make([][]float64, 4)
	for i := range data {
		data[i] = make([]float64, length)
	}
	for i := 0; i < length; i++ {
		close := 100 + 7*math.Sin(float64(i)/3) + float64(i)/20
		switch shape {
		case "flat":
			close = 100
		case "up":
			close = 100 + float64(i)
		case "down":
			close = 300 - float64(i)
		}
		data[0][i], data[1][i], data[2][i], data[3][i] = close+2, close-3, close, 10+float64(i%7)
		if shape == "flat" {
			data[0][i], data[1][i] = close, close
		}
	}
	return data
}

func technicalSnapshots(t *testing.T, data [][]float64) []*Snapshot {
	t.Helper()
	snapshots := make([]*Snapshot, len(data[0]))
	for i := range snapshots {
		snapshots[i] = testSnapshot(t, int64(i+1)*1000, map[int32]map[string]any{1: {
			"high": data[0][i], "low": data[1][i], "signal": data[2][i], "weight": data[3][i],
			"custom_integer": int16(i), "custom_null": nil,
		}})
	}
	return snapshots
}

func technicalPlan(t *testing.T, cases []technicalTestCase) *Plan {
	t.Helper()
	builder := New()
	for _, item := range cases {
		builder.Add(item.name, item.node)
	}
	plan, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func technicalNumeric(value float64) Numeric {
	result := numeric(value)
	if math.IsNaN(value) {
		result.Validity = Warmup
	}
	return result
}

func compareTechnicalFrames(t *testing.T, want, got Frame, where string) {
	t.Helper()
	for name, column := range want.Values {
		if len(column) != len(got.Values[name]) {
			t.Fatalf("%s/%s membership differs", where, name)
		}
		for sid, value := range column {
			compareNumeric(t, value, got.Values[name][sid], fmt.Sprintf("%s/%s/%d", where, name, sid))
		}
	}
}

func TestTechnicalIndicatorsMatchPublishedTav(t *testing.T) {
	for _, shape := range []string{"wave", "flat", "up", "down"} {
		for _, period := range []int{1, 5, 120} {
			t.Run(fmt.Sprintf("%s/period%d", shape, period), func(t *testing.T) {
				data := technicalData(100, shape)
				cases := technicalTestCases(period, data)
				if len(cases) != 22 {
					t.Fatal("all 22 operators must be covered")
				}
				plan := technicalPlan(t, cases)
				snapshots := technicalSnapshots(t, data)
				batch, err := plan.Batch(snapshots, len(snapshots))
				if err != nil {
					t.Fatal(err)
				}
				session, _ := NewSession(plan)
				for i, snapshot := range snapshots {
					frame, err := session.Evaluate(snapshot)
					if err != nil {
						t.Fatal(err)
					}
					for _, item := range cases {
						want := technicalNumeric(item.want[i])
						compareNumeric(t, want, batch[i].Values[item.name][1], fmt.Sprintf("batch/%s/%d", item.name, i))
						compareNumeric(t, want, frame.Values[item.name][1], fmt.Sprintf("session/%s/%d", item.name, i))
					}
				}
				for _, size := range []int{1, 2, 7} {
					prefix, err := plan.Batch(snapshots[:size], size)
					if err != nil {
						t.Fatal(err)
					}
					for i := range prefix {
						compareTechnicalFrames(t, batch[i], prefix[i], "short-history causal prefix")
					}
				}
			})
		}
	}
}

func TestTechnicalJointValidityChunksBoundedStateAndFork(t *testing.T) {
	data := technicalData(321, "wave")
	plan := technicalPlan(t, technicalTestCases(5, data))
	allSnapshots := technicalSnapshots(t, data)
	snapshots := allSnapshots[:320]
	for i := range snapshots {
		fields := map[string]any{"high": data[0][i], "low": data[1][i], "signal": data[2][i], "weight": data[3][i], "custom_integer": int16(i), "custom_null": nil}
		switch i {
		case 2:
			fields["signal"] = nil
		case 14:
			fields["weight"] = math.Inf(1)
		case 20:
			fields["high"] = "bad"
		case 41:
			delete(fields, "low")
		case 43:
			fields["signal"] = math.NaN()
		case 48:
			fields["signal"] = math.Inf(-1)
		}
		if i >= 80 && i < 200 {
			fields["signal"] = nil
		}
		snapshots[i] = testSnapshot(t, int64(i+1)*1000, map[int32]map[string]any{1: fields})
	}
	batch, err := plan.Batch(snapshots, len(snapshots))
	if err != nil {
		t.Fatal(err)
	}
	for _, chunk := range []int{1, 7, 39} {
		session, _ := NewSession(plan)
		for start := 0; start < len(snapshots); start += chunk {
			for i := start; i < min(start+chunk, len(snapshots)); i++ {
				frame, err := session.Evaluate(snapshots[i])
				if err != nil {
					t.Fatal(err)
				}
				compareTechnicalFrames(t, batch[i], frame, fmt.Sprintf("chunk%d/grid%d", chunk, i))
				if i == 65 {
					candidate, revision, err := session.fork()
					if err != nil {
						t.Fatal(err)
					}
					for id, series := range session.assets[1].technical {
						for j, input := range series {
							other := candidate.assets[1].technical[id][j]
							if input == other || &input.Data[0] == &other.Data[0] || other.Env == input.Env {
								t.Fatal("fork shared technical inputs")
							}
						}
					}
					// Advancing an abandoned speculative owner must leave native
					// recursive and window state in the original owner untouched.
					for j := i + 1; j < i+5; j++ {
						if _, err := candidate.Evaluate(snapshots[j]); err != nil {
							t.Fatal(err)
						}
					}
					if session.revision != revision {
						t.Fatal("fork advanced live owner")
					}
				}
			}
		}
		for _, asset := range session.assets {
			for _, series := range asset.env.Items {
				if len(series.Data) > 2*plan.StateRetention() {
					t.Fatalf("unbounded technical series: %d", len(series.Data))
				}
			}
		}
		candidate, revision, err := session.fork()
		if err != nil {
			t.Fatal(err)
		}
		if err := session.commit(candidate, revision); err != nil {
			t.Fatal(err)
		}
		compareTechnicalFrames(t, batch[len(batch)-1], session.latest, "adopt fork")
		// Verify adopted recursive/window state on a fresh logical grid, not
		// just the copied Frame, against a replay from the same origin.
		continued, err := session.Evaluate(allSnapshots[320])
		if err != nil {
			t.Fatal(err)
		}
		replayed, err := plan.Batch(allSnapshots, len(allSnapshots))
		if err != nil {
			t.Fatal(err)
		}
		compareTechnicalFrames(t, replayed[320], continued, "adopted next-grid continuity")
	}
	for _, item := range []struct {
		grid int
		name string
		want Validity
	}{{2, "rsi", Null}, {14, "vwma", NonFinite}, {14, "mfi", NonFinite}, {20, "tr", NotNumeric}, {41, "stoch", Missing}, {43, "cci", NonFinite}, {48, "obv", NonFinite}} {
		if got := batch[item.grid].Values[item.name][1].Validity; got != item.want {
			t.Fatalf("%s grid%d validity %s want %s", item.name, item.grid, got, item.want)
		}
	}
	row, _ := snapshots[250].Row(1, "prices", "1h")
	if _, ok := row.Series.Values["custom_integer"].(int16); !ok || row.Series.Values["custom_null"] != nil {
		t.Fatal("arbitrary DataSeries field types or NULL changed")
	}
}

func TestTechnicalDynamicUniverseContinuityAndReentry(t *testing.T) {
	data := technicalData(20, "wave")
	plan := technicalPlan(t, technicalTestCases(3, data))
	snapshots := make([]*Snapshot, 20)
	for i := range snapshots {
		sids := []int32{1, 2}
		if i >= 6 && i < 12 {
			sids = []int32{2, 3}
		}
		values := make(map[int32]map[string]any)
		for _, sid := range sids {
			values[sid] = map[string]any{"high": data[0][i], "low": data[1][i], "signal": data[2][i], "weight": data[3][i]}
		}
		base := testSnapshot(t, int64(i+1)*1000, values)
		spec := base.Spec()
		spec.Universe.Version = fmt.Sprintf("universe-%d", i/6)
		rows := make([]VersionRecord, 0, len(sids))
		requirements := make([]Requirement, 0, len(sids))
		for _, sid := range sids {
			row, _ := base.Row(sid, "prices", "1h")
			rows = append(rows, row)
			requirements = append(requirements, Requirement{SID: sid, Source: "prices", TimeFrame: "1h", EventTime: spec.GridTime})
		}
		var err error
		snapshots[i], err = Freeze(spec, rows, requirements)
		if err != nil {
			t.Fatal(err)
		}
	}
	batch, err := plan.Batch(snapshots, len(snapshots))
	if err != nil {
		t.Fatal(err)
	}
	session, _ := NewSession(plan)
	for i, snapshot := range snapshots {
		frame, err := session.Evaluate(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		compareTechnicalFrames(t, batch[i], frame, "dynamic universe")
		if i == 6 && session.assets[1] != nil {
			t.Fatal("departed asset retained technical state")
		}
		if i == 12 {
			if frame.Values["rma"][1].Validity != Warmup || frame.Values["rma"][2].Validity != Valid {
				t.Fatal("reentry did not reset or surviving asset lost history")
			}
			compareNumeric(t, Numeric{data[3][i], Valid}, frame.Values["obv"][1], "reentry OBV seed")
		}
	}
}

func TestTechnicalDistinctSecondaryInputsAndUnknownBatch(t *testing.T) {
	close := Field("prices", "signal", "1h")
	a := VWMA(close, Field("prices", "a", "1h"), 3)
	b := VWMA(close, Field("prices", "b", "1h"), 3)
	plan, err := New().Add("a", a).Add("b", b).Compile()
	if err != nil {
		t.Fatal(err)
	}
	snapshots := make([]*Snapshot, 25)
	for i := range snapshots {
		snapshots[i] = testSnapshot(t, int64(i+1)*1000, map[int32]map[string]any{1: {"signal": 100 + float64(i), "a": 1 + float64(i%2), "b": 1 + float64(i%7)}})
	}
	batch, err := plan.Batch(snapshots, len(snapshots))
	if err != nil {
		t.Fatal(err)
	}
	session, _ := NewSession(plan)
	for i, snapshot := range snapshots {
		frame, err := session.Evaluate(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		compareTechnicalFrames(t, batch[i], frame, "distinct volume identities")
	}
	if reflect.DeepEqual(batch[24].Values["a"], batch[24].Values["b"]) {
		t.Fatal("fixture failed to distinguish secondary dependencies")
	}
	// Corrupt an already compiled node to prove batch dispatch fails closed
	// instead of indexing a nil column or producing an accidental zero.
	plan.nodes[len(plan.nodes)-1].spec.Operator = "unregistered"
	if _, err := plan.Batch(snapshots, len(snapshots)); err == nil {
		t.Fatal("unknown batch operator was accepted")
	}
}

func TestTechnicalGeneratedOverflowPreservesNonFinite(t *testing.T) {
	input := Field("prices", "signal", "1h")
	plan, err := New().Add("mom", MOM(input, 1)).Add("sma", SMA(input, 2)).Add("rsi", RSI(input, 1)).Compile()
	if err != nil {
		t.Fatal(err)
	}
	snapshots := []*Snapshot{
		testSnapshot(t, 1000, map[int32]map[string]any{1: {"signal": -math.MaxFloat64}}),
		testSnapshot(t, 2000, map[int32]map[string]any{1: {"signal": math.MaxFloat64}}),
		testSnapshot(t, 3000, map[int32]map[string]any{1: {"signal": math.MaxFloat64}}),
	}
	batch, err := plan.Batch(snapshots, len(snapshots))
	if err != nil {
		t.Fatal(err)
	}
	session, _ := NewSession(plan)
	for i, snapshot := range snapshots {
		frame, err := session.Evaluate(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		compareTechnicalFrames(t, batch[i], frame, "overflow")
	}
	if batch[1].Values["mom"][1].Validity != NonFinite || batch[2].Values["sma"][1].Validity != NonFinite {
		t.Fatal("generated infinity was relabeled as warmup")
	}
	if batch[1].Values["rsi"][1].Validity == Valid {
		t.Fatal("overflowing RSI was mistaken for a flat seed")
	}
	for _, period := range []int{2, 1000} {
		data := technicalData(period+8, "wave")
		for i := range data[2] {
			data[2][i] = (2*float64(min(i, period))/float64(period) - 1) * math.MaxFloat64
		}
		if period == 2 {
			copy(data[2], []float64{-1e308, 0, 1e308})
		}
		plan, err := New().Add("rsi", RSI(input, period)).Compile()
		if err != nil {
			t.Fatal(err)
		}
		snapshots := technicalSnapshots(t, data)
		batch, err := plan.Batch(snapshots, len(snapshots))
		if err != nil {
			t.Fatal(err)
		}
		want := tav.RSI(data[2], period)
		session, _ := NewSession(plan)
		for i, snapshot := range snapshots {
			frame, err := session.Evaluate(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			compareNumeric(t, technicalNumeric(want[i]), frame.Values["rsi"][1], fmt.Sprintf("RSI seed arithmetic period%d/grid%d", period, i))
			compareTechnicalFrames(t, batch[i], frame, "RSI overflowing seed")
		}
	}
}
