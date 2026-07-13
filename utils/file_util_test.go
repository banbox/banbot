package utils

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestReadFileRange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.log")
	content := bytes.Repeat([]byte("line with deterministic test data\n"), 200)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}

	var all, lines []byte
	pos := int64(-1)
	var err error
	for {
		lines, pos, err = ReadFileRange(path, -1024, pos)
		if err != nil {
			t.Fatal(err)
		}
		if len(lines) == 0 {
			break
		}
		merge := make([]byte, 0, len(all)+len(lines))
		merge = append(merge, lines...)
		merge = append(merge, all...)
		all = merge
	}
	textA := string(all)

	pos = 0
	all = nil
	for {
		lines, pos, err = ReadFileRange(path, 1024, pos)
		if err != nil {
			t.Fatal(err)
		}
		if len(lines) == 0 {
			break
		}
		all = append(all, lines...)
	}
	if textB := string(all); textA != textB {
		t.Fatalf("forward and backward reads differ: backward=%d bytes, forward=%d bytes", len(textA), len(textB))
	}
	if !bytes.Equal(all, content) {
		t.Fatalf("read content differs from fixture: got %d bytes, want %d", len(all), len(content))
	}
}
