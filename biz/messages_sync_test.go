package biz

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSyncMessagesFileKeepsUnparsableUserFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messages.json")
	const user = "{not json"
	if err := os.WriteFile(path, []byte(user), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := syncMessagesFile(path, []byte(`{"a":"b"}`)); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(path)
	if string(got) != user {
		t.Fatalf("user file overwritten: %q", got)
	}
}

func TestSyncMessagesFileMergesAndCreates(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "messages.json")
	if err := syncMessagesFile(path, []byte(`{"a":"b"}`)); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(path); string(got) != `{"a":"b"}` {
		t.Fatalf("missing file not created from embedded: %q", got)
	}
	if err := os.WriteFile(path, []byte(`{"a":"user"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := syncMessagesFile(path, []byte(`{"a":"b","c":"d"}`)); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(path)
	if !strings.Contains(string(got), `"a":"user"`) || !strings.Contains(string(got), `"c":"d"`) {
		t.Fatalf("merge result wrong: %s", got)
	}
}
