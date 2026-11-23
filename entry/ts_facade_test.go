package entry

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
)

func tsFacadeSpec(t *testing.T, yaml string) *config.RunSpec {
	t.Helper()
	spec, err := config.LoadRunSpec(&config.CmdArgs{NoDefault: true, DataDir: t.TempDir(), ConfigData: yaml}, false)
	if err != nil {
		t.Fatal(err)
	}
	return spec
}

// execution.ts_facade was removed: every value, at the execution level or per
// account, fails config loading with a message that says what to do.
func TestTSFacadeKeyRejectedEverywhere(t *testing.T) {
	for _, value := range []string{"shared", "hub", "sideways"} {
		for name, body := range map[string]string{
			"execution": strings.Replace(timeSeriesExecutionYAML(), "execution:\n", "execution:\n  live_provider: custom\n  ts_facade: "+value+"\n", 1),
			"account":   strings.Replace(timeSeriesExecutionYAML(), "accounts: {default: {}}", "accounts: {default: {ts_facade: "+value+"}}", 1),
		} {
			_, err := config.LoadRunSpec(&config.CmdArgs{NoDefault: true, DataDir: t.TempDir(), ConfigData: body}, false)
			if err == nil || !strings.Contains(err.Error(), "ts_facade") || !strings.Contains(err.Error(), "removed") || !strings.Contains(err.Error(), "delete this key") {
				t.Errorf("%s/%s: ts_facade accepted or unclear error: %v", name, value, err)
			}
		}
	}
}

// Without the key every provider builds its account configuration.
func TestTSFacadeUnsetAccepted(t *testing.T) {
	for name, body := range map[string]string{
		"default hub provider": timeSeriesExecutionYAML(),
		"banexg":               strings.Replace(timeSeriesExecutionYAML(), "execution:\n", "execution:\n  live_provider: banexg\n", 1),
		"embedded":             strings.Replace(timeSeriesExecutionYAML(), "execution:\n", "execution:\n  live_provider: custom\n", 1),
	} {
		if _, err := buildTimeSeriesLiveConfigs(tsFacadeSpec(t, body)); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// A panic of the Hub order manager constructor (refused legacy import) fails
// the account's startup with the cause instead of crashing the process.
func TestBindTimeSeriesJobsConvertsPanicToError(t *testing.T) {
	err := bindTimeSeriesJobs(func() error { panic("unmappable legacy order") })
	if err == nil || !strings.Contains(err.Error(), "unmappable legacy order") || strings.Contains(err.Error(), "ts_facade") {
		t.Fatalf("panic not reported as startup error: %v", err)
	}
	want := errors.New("bind failed")
	if got := bindTimeSeriesJobs(func() error { return want }); !errors.Is(got, want) {
		t.Fatalf("bind error lost: %v", got)
	}
}

// take_over_strat is supported by the Hub order manager: the owner adopts
// external trades and HubOrderMgr renders the adopted lots
// (biz/hub_order_takeover.go), so there is no takeover refusal.
func TestTSFacadeHubAcceptsTakeover(t *testing.T) {
	src, err := os.ReadFile("time_series_execution.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(src), "not yet supported with ts_facade: hub") {
		t.Fatal("take_over_strat must not be refused")
	}
}
