package biz

import (
	"os"
	"path/filepath"
	"testing"
)

func TestInitDataDirAtUsesExplicitDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := InitDataDirAt(dir); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"config.yml", "config.local.yml", "zh-CN/messages.json", "en-US/messages.json"} {
		if _, err := os.Stat(filepath.Join(dir, path)); err != nil {
			t.Fatalf("missing initialized %s: %v", path, err)
		}
	}
}
