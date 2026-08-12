package entry

import (
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banexg/errs"
	"github.com/spf13/cobra"
)

func TestBacktestCommandsExposeStrictFlag(t *testing.T) {
	root := NewRootCommand()
	for _, path := range [][]string{
		{"backtest"}, {"optimize"}, {"bt-opt"}, {"tool", "sim-bt"}, {"tool", "test-pickers"}, {"tool", "bt-result"},
	} {
		command, _, err := root.Find(path)
		if err != nil {
			t.Fatal(err)
		}
		if command.Flags().Lookup("bt-strict") == nil {
			t.Fatalf("%v does not expose --bt-strict", path)
		}
	}
}

func TestStrictFlagPopulatesCommandArgs(t *testing.T) {
	for _, tc := range []struct {
		flag    string
		want    bool
		wantSet bool
	}{
		{want: false, wantSet: false},
		{flag: "--bt-strict", want: true, wantSet: true},
		{flag: "--bt-strict=false", want: false, wantSet: true},
	} {
		var captured *config.CmdArgs
		command := newConfigCommand("capture", "capture args", func(args *config.CmdArgs) *errs.Error {
			captured = args
			return nil
		}, true, bindBTStrict)
		root := &cobra.Command{Use: "test"}
		root.AddCommand(command)
		commandArgs := []string{"capture"}
		if tc.flag != "" {
			commandArgs = append(commandArgs, tc.flag)
		}
		root.SetArgs(commandArgs)
		if err := root.Execute(); err != nil {
			t.Fatal(err)
		}
		if captured == nil || captured.BTStrictSet != tc.wantSet || captured.BTStrict != tc.want {
			t.Fatalf("%s populated args = %+v", tc.flag, captured)
		}
	}
}
