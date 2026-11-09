package dev

import (
	"path/filepath"
	"testing"
)

func TestParsePathRejectsEscape(t *testing.T) {
	dataDir := t.TempDir()
	s := newTestDevServer(dataDir)
	if _, err := s.parsePath("@../outside.yml"); err == nil {
		t.Fatal("expected @.. escape to be rejected")
	}
	if p, err := s.parsePath("@sub/a.yml"); err != nil || p != filepath.Join(dataDir, "sub", "a.yml") {
		t.Fatalf("valid data path rejected: %v %v", p, err)
	}
}
