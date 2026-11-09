package data

import (
	"os"
	"path/filepath"
	"testing"
)

func TestIsRawContractEmptyName(t *testing.T) {
	if isRawContract("") {
		t.Fatal("empty name must not be a raw contract")
	}
}

func TestConvertFilesNonZipFileReturnsError(t *testing.T) {
	f := filepath.Join(t.TempDir(), "a.txt")
	if e := os.WriteFile(f, []byte("x"), 0o644); e != nil {
		t.Fatal(e)
	}
	err := convertFiles(f, t.TempDir(), ".csv", 1, func(a, b string) string { return b }, nil)
	if err == nil {
		t.Fatal("expected error when input is not a zip file")
	}
}
