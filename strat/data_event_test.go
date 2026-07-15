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
		name  string
		build func(FnOnData) FnOnData
		want  int
	}{
		{"kline", KlineData, 2},
		{"custom", CustomData, 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			called := 0
			handler := test.build(func(_ *StratJob, _ DataEvent) { called++ })
			for _, role := range []DataRole{DataRoleMain, DataRoleInfo, DataRoleCustom} {
				handler(nil, DataEvent{Role: role})
			}
			if called != test.want {
				t.Fatalf("handler called %d times, want %d", called, test.want)
			}
		})
	}
}
