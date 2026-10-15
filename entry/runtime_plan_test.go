package entry

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCompiledBinaryRegistersHiddenInspectDataPlanCommand(t *testing.T) {
	root := NewRootCommand()
	command, _, err := root.Find([]string{"internal", "inspect-data-plan"})
	if err != nil {
		t.Fatal(err)
	}
	if command == root || command.Name() != "inspect-data-plan" {
		t.Fatalf("inspect-data-plan command not found: %v", command.CommandPath())
	}
	internal, _, err := root.Find([]string{"internal"})
	if err != nil || !internal.Hidden {
		t.Fatalf("internal command must be hidden, command=%v err=%v", internal, err)
	}
}

func TestWriteRuntimePlanOutputOnlyTruncatesPrecreatedFile(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "output.json")
	if err := os.WriteFile(path, []byte("stale output that must be truncated"), 0600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = writeRuntimePlanOutput(path, []byte(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"ok":true}` || !os.SameFile(before, after) {
		t.Fatalf("output=%q same_file=%v", data, os.SameFile(before, after))
	}
	if err = writeRuntimePlanOutput(filepath.Join(root, "missing.json"), []byte("x")); err == nil {
		t.Fatal("writer created an output file that was not precreated")
	}
}
