package strat

import (
	"fmt"
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	ta "github.com/banbox/banta"
)

func TestRouteDataSelectsOneHandler(t *testing.T) {
	called := ""
	handler := RouteData(DataHandlers{
		Main:   func(_ *StratJob, _ DataEvent) { called += "main" },
		Info:   func(_ *StratJob, _ DataEvent) { called += "info" },
		Custom: func(_ *StratJob, _ DataEvent) { called += "custom" },
	})
	for role, want := range map[DataRole]string{
		DataRoleMain: "main", DataRoleInfo: "info", DataRoleCustom: "custom",
	} {
		called = ""
		handler(nil, DataEvent{Role: role})
		if called != want {
			t.Fatalf("role %v called %q, want %q", role, called, want)
		}
	}
}

func TestDataRoleHelpers(t *testing.T) {
	tests := []struct {
		role      DataRole
		wantMain  bool
		wantKline bool
	}{
		{DataRoleMain, true, true},
		{DataRoleInfo, false, true},
		{DataRoleCustom, false, false},
	}
	for _, test := range tests {
		event := DataEvent{Role: test.role}
		if event.IsMain() != test.wantMain || event.IsKline() != test.wantKline {
			t.Fatalf("role %v: IsMain=%v IsKline=%v", test.role, event.IsMain(), event.IsKline())
		}
	}
}

func TestNewRejectsOnDataWithLegacyDataCallbacks(t *testing.T) {
	for _, legacy := range []string{"OnBar", "OnInfoBar"} {
		t.Run(legacy, func(t *testing.T) {
			name := "test_dual_" + strings.ToLower(legacy)
			StratMake[name] = func(*config.RunPolicyConfig) *TradeStrat {
				stgy := &TradeStrat{
					OnData: func(*StratJob, DataEvent) {},
				}
				if legacy == "OnBar" {
					stgy.OnBar = func(*StratJob) {}
				} else {
					stgy.OnInfoBar = func(*StratJob, *ta.BarEnv, string, string) {}
				}
				return stgy
			}
			defer delete(StratMake, name)

			defer func() {
				got := fmt.Sprint(recover())
				if !strings.Contains(got, "OnData cannot be combined with "+legacy) || !strings.Contains(got, "strat.RouteData") {
					t.Fatalf("unexpected validation panic: %q", got)
				}
			}()
			New(&config.RunPolicyConfig{Name: name})
		})
	}
}
