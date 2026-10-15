package live

import (
	"testing"
	"time"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/runtime"
)

func TestPerformanceDateUsesRuntimeLocation(t *testing.T) {
	previous := btime.LocShow
	btime.LocShow = time.FixedZone("legacy", -8*60*60)
	t.Cleanup(func() { btime.LocShow = previous })
	snapshot, err := config.LoadRuntimeSnapshot(&config.CmdArgs{
		DataDir: t.TempDir(), NoDefault: true, TimeZone: "Asia/Shanghai",
		ConfigData: `
exchange:
  name: binance
market_type: linear
time_start: "2024-01-01"
time_end: "2024-01-02"
stake_currency: [USDT]
`,
	})
	if err != nil {
		t.Fatal(err)
	}
	process := runtime.NewProcess()
	t.Cleanup(process.Close)
	rt, createErr := process.NewRuntime(runtime.Options{Config: snapshot.View(), DisplayLocation: snapshot.Location()})
	if createErr != nil {
		t.Fatal(createErr)
	}
	deps := rt.BizDeps()
	handlers := newAPIHandlers(&deps)
	timestamp := time.Date(2026, 9, 30, 23, 0, 0, 0, time.UTC).UnixMilli()
	for format, expected := range map[string]string{"2006-01": "2026-10", "2006-01-02": "2026-10-01"} {
		if got := handlers.performanceDate(timestamp, format); got != expected {
			t.Fatalf("format %q = %q, want %q", format, got, expected)
		}
	}
}
