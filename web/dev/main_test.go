package dev

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func TestValidateWebAuthRequiresPasswordOnPublicBind(t *testing.T) {
	if err := validateWebAuth("0.0.0.0", ""); err == nil {
		t.Fatal("expected password requirement for 0.0.0.0")
	}
	if err := validateWebAuth("0.0.0.0", "secret"); err != nil {
		t.Fatal(err)
	}
	if err := validateWebAuth("127.0.0.1", ""); err != nil {
		t.Fatal(err)
	}
}

func TestMakeNewStratGeneratesCompilableProject(t *testing.T) {
	root := t.TempDir()
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source path")
	}
	banbotRoot := filepath.Clean(filepath.Join(filepath.Dir(sourceFile), "..", ".."))
	goMod := "module example.com/teststrats\n\ngo 1.24.0\n\nreplace github.com/banbox/banbot => " + strconv.Quote(banbotRoot) + "\n\nrequire github.com/banbox/banbot v0.0.0\n"
	mainGo := "package main\n\nfunc main() {}\n"

	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte(goMod), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "main.go"), []byte(mainGo), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "scratch"), 0o755); err != nil {
		t.Fatal(err)
	}
	oldWorkDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(root); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(oldWorkDir); err != nil {
			t.Errorf("restore working directory: %v", err)
		}
	})
	t.Setenv("BanStratDir", "")

	if err := makeNewStrat("scratch", "Demo"); err != nil {
		t.Fatal(err)
	}
	generatedTest := `package scratch

import (
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/strat"
)

func TestGeneratedStrategyLoadsAndRuns(t *testing.T) {
	strategy := strat.New(&config.RunPolicyConfig{Name: "scratch:Demo"})
	if strategy.OnBar == nil {
		t.Fatal("generated strategy has no OnBar rule")
	}
	strategy.OnBar(&strat.StratJob{})
}
`
	if err := os.WriteFile(filepath.Join(root, "scratch", "generated_test.go"), []byte(generatedTest), 0o644); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command("go", "test", "-mod=mod", "./...")
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("generated strategy project does not compile: %v\n%s", err, output)
	}
	generated, err := os.ReadFile(filepath.Join(root, "scratch", "Demo.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(generated), "OnBar: func(s *strat.StratJob)") {
		t.Fatalf("generated strategy is missing its executable OnBar rule: %s", generated)
	}
}
