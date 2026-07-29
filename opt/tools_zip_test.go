package opt

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestZipBacktestResultRequiresBoundedOrdersArtifact(t *testing.T) {
	dir := t.TempDir()
	if _, err := ZipBacktestResult(dir, false, true, 4); err == nil ||
		!strings.Contains(err.Error(), "requires orders.gob") {
		t.Fatalf("missing orders error=%v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "orders.gob"), []byte("12345"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ZipBacktestResult(dir, false, true, 4); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized orders error=%v", err)
	}
}
