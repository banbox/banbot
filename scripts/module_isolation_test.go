//go:build module_isolation

// Run from the repository root: go test -v scripts/module_isolation_test.go
// This source-tree check needs tmp/go.mod, which Go module zip files omit.
package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestScratchModuleIsolation(t *testing.T) {
	boundary, err := os.ReadFile(filepath.Join("..", "tmp", "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	// Reproduce a local scratch program without fetching external modules.
	t.Setenv("GO111MODULE", "on")
	t.Setenv("GOWORK", "off")
	t.Setenv("GOPROXY", "off")
	t.Setenv("GOSUMDB", "off")
	t.Setenv("GOFLAGS", "")
	t.Setenv("GOTOOLCHAIN", "local")
	for _, isolated := range []bool{false, true} {
		name := "without_boundary"
		if isolated {
			name = "with_boundary"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			files := map[string]string{
				"go.mod":             "module example.com/banbot\n\ngo 1.26.0\n",
				"main.go":            "package main\nfunc main() {}\n",
				"tmp/replay/main.go": "package main\nimport _ \"github.com/banbox/banstrats/ma\"\nfunc main() {}\n",
			}
			if isolated {
				files["tmp/go.mod"] = string(boundary)
			}
			for path, contents := range files {
				path = filepath.Join(root, path)
				if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(contents), 0644); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "go", "mod", "tidy", "-diff")
			cmd.Dir = root
			output, err := cmd.CombinedOutput()
			if !isolated {
				if err == nil || !strings.Contains(string(output), "github.com/banbox/banstrats/ma") {
					t.Fatalf("scratch import must reach tidy without the boundary: err=%v\n%s", err, output)
				}
				return
			}
			if err != nil {
				t.Fatalf("tidy must ignore scratch imports: %v\n%s", err, output)
			}
			cmd = exec.CommandContext(ctx, "go", "list", "./...")
			cmd.Dir = root
			output, err = cmd.CombinedOutput()
			if err != nil || strings.TrimSpace(string(output)) != "example.com/banbot" {
				t.Fatalf("recursive package listing must exclude scratch packages: err=%v\n%s", err, output)
			}
		})
	}
}
