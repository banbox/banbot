package entry

import "testing"

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
