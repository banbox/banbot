package config

import (
	"os"
	"path/filepath"
	"testing"
)

func migrationFixture(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func configBackups(t *testing.T, path string) []string {
	t.Helper()
	paths, err := filepath.Glob(path + ".bak.*")
	if err != nil {
		t.Fatal(err)
	}
	return paths
}

func TestConfigAtomicEditorRejectsStaleBytes(t *testing.T) {
	path := migrationFixture(t, "config.yml", "name: initial\n")
	if err := WriteConfigAtomic(path, []byte("name: initial\n"), []byte("name: edited\n")); err != nil {
		t.Fatal(err)
	}
	if err := WriteConfigAtomic(path, []byte("name: initial\n"), []byte("name: stale\n")); err == nil {
		t.Fatal("stale editor overwrote newer data")
	}
	raw, _ := os.ReadFile(path)
	if string(raw) != "name: edited\n" {
		t.Fatal("stale write changed source")
	}
}
