package entry

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/factor/runner"
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
	spec, err := loadFactorRunSpec([]string{path}, "")
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
