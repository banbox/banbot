package entry

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/factor/statistics"
)

func TestFactorYAMLEmptyLabelsDisablesResearch(t *testing.T) {
	dir, path := factorYAMLFixture(t)
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	body = append(body, []byte("      research: {labels: []}\n      decision: {max_pending: 1}\n")...)
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	spec, err := loadFactorYAMLSpec([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	configs, err := buildFactorConfigs(spec, runner.Weights)
	if err != nil {
		t.Fatal(err)
	}
	if len(configs[0].Manifest.Labels) != 0 {
		t.Fatal("explicit empty labels did not override the default")
	}
	out := filepath.Join(dir, "result")
	if err := RunBackTest(&config.CmdArgs{Configs: config.ArrString{path}, NoDefault: true, OutPath: out}); err != nil {
		t.Fatal(err)
	}
	artifact, err := os.ReadFile(filepath.Join(out, "strategy-1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var result runner.RunArtifact
	if err := json.Unmarshal(artifact, &result); err != nil {
		t.Fatal(err)
	}
	if result.Status != "complete" || result.Result.TargetsAccepted == 0 || result.Result.MaxPendingEvaluations != 0 || len(result.Result.Summary) != 0 || len(result.Result.Manifest.Labels) != 0 {
		t.Fatalf("disabled research did not complete normal trading: %+v", result)
	}
}

func TestFactorYAMLResearchPanelSwitch(t *testing.T) {
	for _, tc := range []struct {
		value, want string
		bad         bool
	}{{"none", "none", false}, {"all", "", false}, {"", "", false}, {"half", "", true}, {"off", "", true}} {
		_, path := factorYAMLFixture(t)
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		body = append(body, []byte("      research: {panel: \""+tc.value+"\"}\n")...)
		if err := os.WriteFile(path, body, 0600); err != nil {
			t.Fatal(err)
		}
		spec, err := loadFactorYAMLSpec([]string{path})
		var configs []runner.Config
		if err == nil {
			configs, err = buildFactorConfigs(spec, runner.Weights)
		}
		if tc.bad {
			if err == nil {
				t.Fatalf("panel %q accepted", tc.value)
			}
			continue
		}
		if err != nil {
			t.Fatalf("panel %q: %v", tc.value, err)
		}
		if configs[0].Panel != tc.want {
			t.Fatalf("panel %q -> %q, want %q", tc.value, configs[0].Panel, tc.want)
		}
	}
}

// The production entry path prepares the portfolio config before Run; the pairs result must still be reported.
func TestFactorYAMLPairsStrategyReportsPairs(t *testing.T) {
	err := runner.RegisterPairs("test-entry-pairs", func(p *runner.Params) (runner.PairsSpec, error) {
		return runner.PairsSpec{Pairs: []runner.Pair{{Y: "sid:1", X: "sid:2"}}, Window: 3, Band: statistics.BandConfig{Entry: 1, Exit: .2}}, p.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	dir, path := factorYAMLFixture(t)
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	body = []byte(strings.Replace(string(body), "name: MomentumVol", "name: test-entry-pairs", 1))
	body = []byte(strings.Replace(string(body), "    params: {window: 2, k: 1}\n", "", 1))
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "result")
	if err := RunBackTest(&config.CmdArgs{Configs: config.ArrString{path}, NoDefault: true, OutPath: out}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(out, "run.json"))
	if err != nil {
		t.Fatal(err)
	}
	var run struct{ Results []runner.Result }
	if err := json.Unmarshal(raw, &run); err != nil {
		t.Fatal(err)
	}
	if len(run.Results) != 1 || len(run.Results[0].Pairs) != 1 || run.Results[0].Pairs[0].Y != "sid:1" || run.Results[0].Pairs[0].Bars == 0 || run.Results[0].Attribution == nil {
		t.Fatalf("pairs/attribution missing from run.json: %s", raw)
	}
}
