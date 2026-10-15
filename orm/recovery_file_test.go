package orm

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestRecoveryFilePublishReplaceAndResync(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "recovery.json")
	for _, payload := range [][]byte{[]byte("old"), []byte("new complete snapshot")} {
		file, err := os.CreateTemp(root, "recovery-")
		if err != nil {
			t.Fatal(err)
		}
		if err := writeAndSyncExSymbolRecoveryFile(file, payload); err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		if err := publishRecoveryFile(file.Name(), target); err != nil {
			t.Fatal(err)
		}
		if err := syncDirectory(root); err != nil {
			t.Fatal(err)
		}
		if err := syncExistingExSymbolRecoveryFile(target); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(target)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("published payload %q: %v", got, err)
		}
	}
	if err := syncDirectory(target); err == nil {
		t.Fatal("regular file accepted as recovery directory")
	}
	if err := syncDirectory(filepath.Join(root, "missing")); err == nil {
		t.Fatal("missing recovery directory accepted")
	}
}
