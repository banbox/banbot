package strat

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"testing"

	"github.com/banbox/banbot/orm"
	ta "github.com/banbox/banta"
)

// This harness deliberately uses the public APIs present in v0.5.7 too, so
// the same file can benchmark that preserved checkout without porting code.
// SMA(5)/SMA(20)/Cross mirrors banstrats/ma.Demo's decision calculation.
// It measures map-based intake, indicators and callbacks, excluding execution,
// providers and reports. Signals are observed, never sent to a real account.
func runArchitectureDualMA(t testing.TB, assets, hours, consumers int) (uint64, uint64) {
	t.Helper()
	jobs := make([][]*StratJob, assets)
	var signals, peakHeap uint64
	for asset := range jobs {
		symbol := &orm.ExSymbol{ID: int32(asset + 1), Symbol: fmt.Sprintf("asset-%d", asset)}
		env, err := ta.NewBarEnv("fixture", "linear", symbol.Symbol, "1h")
		if err != nil {
			t.Fatal(err)
		}
		env.MaxCache = 512
		for range consumers {
			jobs[asset] = append(jobs[asset], &StratJob{Symbol: symbol, Env: env, TimeFrame: "1h", DataHub: NewDataHub(512), Strat: &TradeStrat{OnData: func(job *StratJob, event DataEvent) {
				if event.Float64("close") != job.Env.Close.Get(0) {
					t.Fatal("callback lost input")
				}
				if cross := ta.Cross(ta.SMA(job.Env.Close, 5), ta.SMA(job.Env.Close, 20)); cross == 1 || cross == -1 {
					signals++
				}
			}}})
		}
	}
	const hour int64 = 3600000
	for bar := 1; bar <= hours; bar++ {
		for asset, consumers := range jobs {
			price := 100 + math.Sin(float64(bar)/8+float64(asset))*10
			at := int64(bar) * hour
			event := &orm.DataSeries{Source: "kline", Sid: int32(asset + 1), TimeFrame: "1h", TimeMS: at, EndMS: at + hour, Closed: true, Values: map[string]any{
				"open": price, "high": price + 1, "low": price - 1, "close": price, "volume": 100.0,
				"integer": int64(9007199254740993), "text": "sector", "nullable": nil,
			}}
			if err := consumers[0].Env.OnBar(at, price, price+1, price-1, price, 100, 0, 0, 0); err != nil {
				t.Fatal(err)
			}
			for _, job := range consumers {
				fields := job.SetData(event)
				job.Strat.OnData(job, DataEvent{DataFields: fields, Role: DataRoleMain, Symbol: job.Symbol})
				if bar == hours {
					big, ok := fields.RawValue("integer")
					if !ok || big != int64(9007199254740993) || !fields.Has("nullable") || fields.Has("missing") {
						t.Fatal("typed arbitrary fields changed")
					}
				}
			}
		}
		if bar%64 == 0 || bar == hours {
			var stats runtime.MemStats
			runtime.ReadMemStats(&stats)
			peakHeap = max(peakHeap, stats.HeapAlloc)
		}
	}
	return signals, peakHeap
}

func dualMASize(t testing.TB, key string, fallback int) int {
	t.Helper()
	if raw := os.Getenv("BANBOT_ARCH_BENCH_" + key); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 {
			t.Fatalf("%s requires positive integer", key)
		}
		return value
	}
	return fallback
}

func BenchmarkArchitectureDualMA(b *testing.B) {
	assets, hours := dualMASize(b, "ASSETS", 24), dualMASize(b, "HOURS", 128)
	for _, consumers := range []int{1, 10} {
		b.Run(fmt.Sprintf("consumers-%d", consumers), func(b *testing.B) {
			b.ReportAllocs()
			var signals, peak uint64
			for range b.N {
				signals, peak = runArchitectureDualMA(b, assets, hours, consumers)
			}
			b.ReportMetric(float64(assets*hours), "asset-bars/op")
			b.ReportMetric(float64(signals), "signals/op")
			b.ReportMetric(float64(peak)/1024/1024, "sampled-heap-MiB")
		})
	}
}

func TestArchitectureDualMACallbackFanout(t *testing.T) {
	one, _ := runArchitectureDualMA(t, 2, 128, 1)
	ten, _ := runArchitectureDualMA(t, 2, 128, 10)
	if one == 0 || ten != one*10 {
		t.Fatalf("signal fanout: one=%d ten=%d", one, ten)
	}
}
