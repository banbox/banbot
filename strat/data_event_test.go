package strat

import "testing"

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
