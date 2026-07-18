package entry

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banexg/errs"
	"github.com/spf13/cobra"
)

func TestRootCommandExposesCobraHelp(t *testing.T) {
	root := NewRootCommand()
	var output bytes.Buffer
	root.SetOut(&output)
	root.SetErr(&output)
	root.SetArgs([]string{"backtest", "--help"})

	if err := root.Execute(); err != nil {
		t.Fatalf("help returned error: %v", err)
	}
	help := output.String()
	for _, want := range []string{"Usage:", "banbot backtest", "--stake-amount", "--config"} {
		if !strings.Contains(help, want) {
			t.Fatalf("help does not contain %q:\n%s", want, help)
		}
	}
	if strings.Contains(help, "--medium") {
		t.Fatalf("backtest unexpectedly exposes an unrelated flag:\n%s", help)
	}
}

func TestConfigCommandParsesRepeatedConfigAndLegacyFlags(t *testing.T) {
	var captured *config.CmdArgs
	command := newConfigCommand("capture", "capture args", func(args *config.CmdArgs) *errs.Error {
		captured = args
		return nil
	}, true, bindPairs)
	root := &cobra.Command{Use: "test"}
	root.AddCommand(command)
	root.SetArgs(normalizeLegacyFlags(root, []string{
		"capture", "--config", "first.yml", "-config", "second.yml", "-pairs", "BTC/USDT,ETH/USDT",
	}))

	if err := root.Execute(); err != nil {
		t.Fatalf("execute returned error: %v", err)
	}
	if captured == nil {
		t.Fatal("command was not executed")
	}
	if want := []string{"first.yml", "second.yml"}; !reflect.DeepEqual([]string(captured.Configs), want) {
		t.Fatalf("configs = %v, want %v", captured.Configs, want)
	}
	if want := []string{"BTC/USDT", "ETH/USDT"}; !reflect.DeepEqual(captured.Pairs, want) {
		t.Fatalf("pairs = %v, want %v", captured.Pairs, want)
	}
}

func TestAddCommandSupportsCommandLocalFlags(t *testing.T) {
	before := len(extraCommands)
	t.Cleanup(func() {
		extraCommands = extraCommands[:before]
	})

	var greeting string
	command := &cobra.Command{
		Use:  "hello",
		Args: cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			if greeting != "nihao" {
				t.Fatalf("greeting = %q, want nihao", greeting)
			}
			return nil
		},
	}
	command.Flags().StringVar(&greeting, "greeting", "hello", "greeting text")
	AddCommand("", command)

	root := NewRootCommand()
	root.SetArgs([]string{"hello", "--greeting", "nihao"})
	if err := root.Execute(); err != nil {
		t.Fatalf("custom command returned error: %v", err)
	}
}

func TestUnknownCommandReturnsCobraError(t *testing.T) {
	root := NewRootCommand()
	root.SetArgs([]string{"does-not-exist"})

	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("error = %v, want an unknown command error", err)
	}
}

func TestLegacyUnderscoreCommandsRemainAliases(t *testing.T) {
	root := NewRootCommand()
	tests := []struct {
		args []string
		want string
	}{
		{args: []string{"bt_opt"}, want: "bt-opt"},
		{args: []string{"tool", "list_strats"}, want: "list-strats"},
		{args: []string{"live", "down_order"}, want: "down-order"},
	}
	for _, test := range tests {
		command, _, err := root.Find(test.args)
		if err != nil {
			t.Fatalf("find %v: %v", test.args, err)
		}
		if command.Name() != test.want {
			t.Fatalf("find %v = %q, want %q", test.args, command.Name(), test.want)
		}
	}
}

func TestNormalizeLegacyFlagsPreservesShorthandAndNegativeValues(t *testing.T) {
	root := NewRootCommand()
	input := []string{"backtest", "-h", "-config=local.yml", "--pairs", "BTC/USDT", "-1", "-.5", "--", "-config"}
	want := []string{"backtest", "-h", "--config=local.yml", "--pairs", "BTC/USDT", "-1", "-.5", "--", "-config"}
	if got := normalizeLegacyFlags(root, input); !reflect.DeepEqual(got, want) {
		t.Fatalf("normalizeLegacyFlags() = %v, want %v", got, want)
	}
	if !reflect.DeepEqual(input, []string{"backtest", "-h", "-config=local.yml", "--pairs", "BTC/USDT", "-1", "-.5", "--", "-config"}) {
		t.Fatalf("normalizeLegacyFlags mutated its input: %v", input)
	}
}

func TestImplicitWebInvocationCompatibility(t *testing.T) {
	tests := []struct {
		args []string
		want bool
	}{
		{args: nil, want: false},
		{args: []string{"web", "--port", "8000"}, want: false},
		{args: []string{"--help"}, want: false},
		{args: []string{"-help"}, want: false},
		{args: []string{"--version"}, want: false},
		{args: []string{"--host", "127.0.0.1"}, want: true},
		{args: []string{"-port", "8000"}, want: true},
	}
	for _, test := range tests {
		if got := isImplicitWebInvocation(test.args); got != test.want {
			t.Fatalf("isImplicitWebInvocation(%v) = %v, want %v", test.args, got, test.want)
		}
	}
}
