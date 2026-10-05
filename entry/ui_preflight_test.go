package entry

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/orm"
	"gopkg.in/yaml.v3"
)

func TestUIStaticPreflightDoesNotReadArchive(t *testing.T) {
	dir, path := factorYAMLFixture(t)
	if err := os.Remove(filepath.Join(dir, "data.gob")); err != nil {
		t.Fatal(err)
	}
	spec, err := loadFactorYAMLSpec([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)
	inspection, err := InspectBacktestRunSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.DataChecked || inspection.ExecutionMode != "weights" || len(inspection.Strategies) != 1 || inspection.Strategies[0].ID != "MomentumVol" || len(inspection.Warnings) == 0 {
		t.Fatalf("bad inspection: %+v", inspection)
	}
	if err := ValidateBacktestRunSpec(spec); err == nil {
		t.Fatal("full backtest preflight accepted missing archive")
	}
	after, _ := os.ReadFile(path)
	if string(before) != string(after) {
		t.Fatal("static inspection rewrote source")
	}
	if databases, _ := filepath.Glob(filepath.Join(dir, "*.db")); len(databases) != 0 {
		t.Fatal("static inspection opened execution storage")
	}
}

func TestUIStaticPreflightUsesBacktestModesAndDefaults(t *testing.T) {
	for _, mode := range []string{"weights", "events", "research"} {
		t.Run(mode, func(t *testing.T) {
			spec := storageEntrySpec(t, "static-approximation").WithExecutionMode(mode, "test")
			inspection, err := InspectBacktestRunSpec(spec)
			if mode == "research" {
				if err == nil || !strings.Contains(err.Error(), "weights or events") {
					t.Fatalf("research accepted: %v", err)
				}
				return
			}
			if err != nil || inspection.ExecutionMode != mode || inspection.Strategies[0].TimeFrame != "1h" {
				t.Fatalf("inspection=%+v error=%v", inspection, err)
			}
			if err := ValidateBacktestRunSpec(spec); err != nil {
				t.Fatalf("CLI preflight differs: %v", err)
			}
		})
	}
	if _, err := InspectBacktestRunSpec((*config.RunSpec)(nil)); err == nil {
		t.Fatal("nil spec accepted")
	}
}

func TestUIStaticPreflightRejectsMixedWeightsAndMultipleFactorPeriods(t *testing.T) {
	base := storageEntrySpec(t, "static-approximation")
	raw, err := base.EffectiveYAML(false)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ mode, reason string }{
		{"weights", "mixed backtest requires"},
		{"events", "one decision timeframe"},
	} {
		var root map[string]any
		if err := yaml.Unmarshal(raw, &root); err != nil {
			t.Fatal(err)
		}
		policies := root["run_policy"].([]any)
		policy := policies[0].(map[string]any)
		if test.mode == "weights" {
			policy["capital_weight"] = 0.5
			root["run_policy"] = append(policies, map[string]any{"name": "trend", "engine": "time_series", "id": "trend", "capital_weight": 0.5})
		} else {
			policy["run_timeframes"] = []string{"1h", "4h"}
		}
		body, err := yaml.Marshal(root)
		if err != nil {
			t.Fatal(err)
		}
		spec, loadErr := config.LoadRunSpec(&config.CmdArgs{NoDefault: true, DataDir: t.TempDir(), ConfigData: string(body)}, false)
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		_, err = InspectBacktestRunSpec(spec.WithExecutionMode(test.mode, "test"))
		if err == nil || !strings.Contains(err.Error(), test.reason) {
			t.Fatalf("bad config admitted: %v, expected %s", err, test.reason)
		}
	}
}

func TestUIStaticPreflightDefersOnlyArchiveDerivedFunding(t *testing.T) {
	dir, path := factorYAMLFixture(t)
	archive := filepath.Join(dir, "data.gob")
	store, err := factor.OpenVersionStore(archive, 100)
	if err != nil {
		t.Fatal(err)
	}
	const hour int64 = 3600000
	if err := store.Put(factor.VersionRecord{Series: orm.DataSeries{Source: "funding", Sid: 1, TimeMS: hour - 1, EndMS: hour, TimeFrame: "event", Closed: true, Values: map[string]any{"rate": float64(0)}}, EventTime: hour, AvailableAt: hour, IngestedAt: hour, Revision: 1, SourceVersion: "v1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Export(filepath.Join(dir, "funding.gob")); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := strings.Replace(string(raw), ", funding_policy: explicit-zero", "", 1)
	text = strings.Replace(text, "archive: data.gob", "archive: funding.gob", 1)
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	spec, err := loadFactorYAMLSpec([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	inspection, err := InspectBacktestRunSpec(spec)
	if err != nil {
		t.Fatal("valid archive-derived funding rejected", err)
	}
	if _, exists := inspection.Strategies[0].Resolved["funding_policy"]; exists {
		t.Fatal("unchecked archive funding was presented as resolved")
	}
	if err := ValidateBacktestRunSpec(spec); err != nil {
		t.Fatal("CLI preflight differs", err)
	}
	bad := strings.Replace(text, "mode: weights", "mode: weights, funding_policy: invalid", 1)
	if err := os.WriteFile(path, []byte(bad), 0600); err != nil {
		t.Fatal(err)
	}
	spec, err = loadFactorYAMLSpec([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := InspectBacktestRunSpec(spec); err == nil || !strings.Contains(err.Error(), "funding must") {
		t.Fatalf("invalid explicit policy accepted: %v", err)
	}
}
