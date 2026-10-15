package research

import (
	"encoding/json"
	"testing"
)

func TestLinearModelParameters(t *testing.T) {
	modelRegistry.RLock()
	factory := modelRegistry.factories["linear-ridge-v1"]
	modelRegistry.RUnlock()
	if factory == nil {
		t.Fatal("linear model is not registered")
	}
	for _, tc := range []struct {
		name    string
		raw     string
		wantErr bool
		ridge   float64
	}{
		{name: "default"},
		{name: "single document with whitespace", raw: " \n {\"Ridge\":0.1} \t\n", ridge: 0.1},
		{name: "trailing object", raw: `{"Ridge":0.1} {"Ridge":-1}`, wantErr: true},
		{name: "trailing number", raw: `{"Ridge":0.1} 42`, wantErr: true},
		{name: "trailing malformed JSON", raw: `{"Ridge":0.1} {`, wantErr: true},
		{name: "unknown field", raw: `{"Ridge":0.1,"Unknown":1}`, wantErr: true},
		{name: "negative ridge", raw: `{"Ridge":-1}`, wantErr: true},
		{name: "nonfinite ridge", raw: `{"Ridge":1e999}`, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var raw json.RawMessage
			if tc.raw != "" {
				raw = json.RawMessage(tc.raw)
			}
			trainer, err := factory(raw)
			if (err != nil) != tc.wantErr {
				t.Fatalf("factory error = %v, want error %v", err, tc.wantErr)
			}
			if !tc.wantErr && trainer.(linearTrainer).Ridge != tc.ridge {
				t.Fatalf("ridge = %v, want %v", trainer.(linearTrainer).Ridge, tc.ridge)
			}
		})
	}
}
