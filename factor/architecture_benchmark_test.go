package factor

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"testing"

	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
)

func architectureSize(t testing.TB, name string, fallback int) int {
	t.Helper()
	if value := os.Getenv("BANBOT_ARCH_BENCH_" + name); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n <= 0 {
			t.Fatalf("BANBOT_ARCH_BENCH_%s requires a positive integer", name)
		}
		return n
	}
	return fallback
}

func architecturePlan(t testing.TB) *Plan {
	t.Helper()
	close := Field("prices", "close", "1h")
	builder := New().Add("momentum", Return(close, 24)).Add("volatility", StdDev(Return(close, 1), 24, 1))
	for i := 0; i < 18; i++ {
		builder.Add(fmt.Sprintf("ema-%02d", i), EMA(Return(close, 1), i+2))
	}
	plan, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func architectureValues(bar int, sid int32, width int) map[string]any {
	values := map[string]any{
		"close":   100 + math.Sin(float64(bar)/24+float64(sid)*.7) + float64(bar)/100000,
		"integer": int64(9007199254740993), "text": "sector-A", "flag": true,
		"nullable": nil, "json": map[string]any{"items": []any{int32(9), "x"}},
	}
	for i := len(values); i < width; i++ {
		values[fmt.Sprintf("field-%03d", i)] = float64(i)
	}
	return values
}

func architectureCheck(t testing.TB, raw func(string) (any, bool)) {
	t.Helper()
	for key, want := range map[string]any{"integer": int64(9007199254740993), "text": "sector-A", "flag": true, "nullable": nil} {
		if got, present := raw(key); !present || got != want {
			t.Fatalf("%s type/value/NULL changed: %T %v", key, got, got)
		}
	}
	if _, present := raw("missing"); present {
		t.Fatal("missing field became present")
	}
	if value, present := raw("json"); !present || value.(map[string]any)["items"].([]any)[0] != int32(9) {
		t.Fatal("nested field type changed")
	}
}

// BenchmarkArchitectureHotPath measures three real computation paths using the
// same deterministic typed DataSeries feed. TS includes SetData/DataHub/OnBar;
// CS includes immutable Freeze and a 20-output Session; mixed includes both.
// It intentionally excludes DB/provider I/O, execution, and result persistence.
// One operation replays ASSETS * HOURS events; HOURS=17520 selects two years.
func BenchmarkArchitectureHotPath(b *testing.B) {
	assets := architectureSize(b, "ASSETS", 24)
	hours := architectureSize(b, "HOURS", 32)
	for _, mode := range []string{"TS", "CS", "mixed"} {
		for _, width := range []int{8, 32, 128} {
			for _, consumers := range []int{1, 10} {
				b.Run(fmt.Sprintf("%s/columns-%d/consumers-%d", mode, width, consumers), func(b *testing.B) {
					var plan *Plan
					if mode != "TS" {
						plan = architecturePlan(b)
					}
					var retained, updates uint64
					var peakHeap uint64
					b.ReportAllocs()
					b.ResetTimer()
					for iteration := 0; iteration < b.N; iteration++ {
						var session *Session
						if mode != "TS" {
							var err error
							session, err = NewSession(plan)
							if err != nil {
								b.Fatal(err)
							}
						}
						jobs := make([][]*strat.StratJob, assets)
						callbacks := uint64(0)
						if mode != "CS" {
							for i := range jobs {
								for j := 0; j < consumers; j++ {
									job := &strat.StratJob{Symbol: &orm.ExSymbol{ID: int32(i + 1)}, DataHub: strat.NewDataHub(32)}
									job.Strat = &strat.TradeStrat{OnBar: func(job *strat.StratJob) {
										fields := job.Data(&strat.DataSub{Source: "prices", TimeFrame: "1h"})
										if fields == nil || fields.Float64("close") <= 0 {
											b.Fatal("TS callback lost feed")
										}
										callbacks++
									}}
									jobs[i] = append(jobs[i], job)
								}
							}
						}
						for bar := 1; bar <= hours; bar++ {
							values := make(map[int32]map[string]any, assets)
							for i := 0; i < assets; i++ {
								sid := int32(i + 1)
								fields := architectureValues(bar, sid, width)
								values[sid] = fields
								event := testRecord(sid, int64(bar)*3600000, fields).Series
								for _, job := range jobs[i] {
									job.SetData(&event)
									job.Strat.OnBar(job)
								}
								if bar == hours && len(jobs[i]) > 0 {
									architectureCheck(b, jobs[i][0].DataHub.Get("1h", "prices", sid).RawValue)
								}
							}
							if mode != "TS" {
								snapshot := testSnapshot(b, int64(bar)*3600000, values)
								for consumer := 0; consumer < consumers; consumer++ {
									frame, err := session.Evaluate(snapshot)
									if err != nil || len(frame.Values) != 20 {
										b.Fatalf("CS output=%d error=%v", len(frame.Values), err)
									}
								}
								if bar == hours {
									row, _ := snapshot.Row(1, "prices", "1h")
									architectureCheck(b, func(key string) (any, bool) { v, ok := row.Series.Values[key]; return v, ok })
								}
							}
							if bar%64 == 0 || bar == hours {
								var memory runtime.MemStats
								runtime.ReadMemStats(&memory)
								peakHeap = max(peakHeap, memory.HeapAlloc)
							}
						}
						if mode != "CS" && callbacks != uint64(assets*hours*consumers) {
							b.Fatal("TS callback sequence incomplete")
						}
						updates = 0
						if session != nil {
							retained = uint64(session.RetainedValues())
							for _, count := range session.Updates() {
								updates += count
							}
						}
					}
					b.ReportMetric(float64(assets*hours), "asset-events/op")
					b.ReportMetric(float64(retained), "retained-values")
					b.ReportMetric(float64(updates), "node-updates/op")
					b.ReportMetric(float64(peakHeap)/1024/1024, "sampled-heap-MiB")
				})
			}
		}
	}
}

func TestArchitectureHotPathBoundedHistory(t *testing.T) {
	plan := architecturePlan(t)
	session, err := NewSession(plan)
	if err != nil {
		t.Fatal(err)
	}
	const assets = 4
	for bar := 1; bar <= 1024; bar++ {
		values := map[int32]map[string]any{}
		for sid := int32(1); sid <= assets; sid++ {
			values[sid] = architectureValues(bar, sid, 32)
		}
		if _, err := session.Evaluate(testSnapshot(t, int64(bar)*3600000, values)); err != nil {
			t.Fatal(err)
		}
		if bar == 512 || bar == 1024 {
			// Each registered indicator array is trimmed to at most twice
			// retention; node scratch arrays may be registered by banta too.
			arrays := 0
			for _, asset := range session.assets {
				arrays += len(asset.env.Items)
			}
			if n := session.RetainedValues(); n > arrays*2*plan.retention {
				t.Fatalf("history %d retained %d exceeds bound %d", bar, n, arrays*2*plan.retention)
			}
			t.Logf("hours=%d arrays=%d retained-values=%d bound=%d", bar, arrays, session.RetainedValues(), arrays*2*plan.retention)
		}
	}
}
