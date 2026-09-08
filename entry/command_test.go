package entry

import (
	"bytes"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	runtimectx "github.com/banbox/banbot/runtime"
	"github.com/banbox/banbot/web"
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

func TestPanicStackContainsCaller(t *testing.T) {
	stack := string(panicStack())
	if !strings.Contains(stack, "TestPanicStackContainsCaller") ||
		!strings.Contains(stack, "runtime/debug.Stack") {
		t.Fatalf("panic stack does not contain its caller: %s", stack)
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

func TestTickCommandsUseNonReentrantDataEntries(t *testing.T) {
	for _, args := range [][]string{{"tick", "convert"}, {"tick", "to-kline"}} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			root := NewRootCommand()
			root.SetArgs(args)
			done := make(chan error, 1)
			go func() { done <- root.Execute() }()

			select {
			case err := <-done:
				if err == nil {
					t.Fatal("invalid tick invocation unexpectedly succeeded")
				}
			case <-time.After(time.Second):
				t.Fatal("tick command nested the legacy gate")
			}
		})
	}
}

func TestDataImportExportCommandsUseLegacyGate(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "export",
			args: []string{"data", "export", "--no-default", "--config", filepath.Join(t.TempDir(), "missing.yml"), "--out", t.TempDir()},
		},
		{
			name: "import",
			args: []string{"data", "import", "--no-default", "--config", filepath.Join(t.TempDir(), "missing.yml"), "--in", filepath.Join(t.TempDir(), "missing")},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := NewRootCommand()
			root.SetArgs(test.args)
			unlock := runtimectx.LockLegacy()
			done := make(chan error, 1)
			go func() { done <- root.Execute() }()
			select {
			case err := <-done:
				unlock()
				t.Fatalf("data %s command bypassed the legacy gate: %v", test.name, err)
			case <-time.After(50 * time.Millisecond):
			}
			unlock()
			select {
			case err := <-done:
				if err == nil {
					t.Fatalf("data %s command unexpectedly succeeded", test.name)
				}
			case <-time.After(time.Second):
				t.Fatalf("data %s command did not run after gate release", test.name)
			}
		})
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

func TestRegisteredCommandGateModes(t *testing.T) {
	before := len(extraCommands)
	t.Cleanup(func() {
		extraCommands = extraCommands[:before]
	})

	legacyEntered := make(chan struct{})
	releaseLegacy := make(chan struct{})
	legacy := &cobra.Command{
		Use: "legacy-extension",
		RunE: func(_ *cobra.Command, _ []string) error {
			close(legacyEntered)
			<-releaseLegacy
			return nil
		},
	}
	pureRan := make(chan struct{})
	pure := &cobra.Command{
		Use: "pure-extension",
		RunE: func(_ *cobra.Command, _ []string) error {
			close(pureRan)
			return nil
		},
	}
	AddCommand("", legacy)
	AddRuntimeCommand("", pure)

	root := NewRootCommand()
	registeredLegacy, _, err := root.Find([]string{"legacy-extension"})
	if err != nil {
		t.Fatal(err)
	}
	if !hasLegacyGate(registeredLegacy) {
		t.Fatal("legacy extension is missing its gate marker")
	}
	registeredPure, _, err := root.Find([]string{"pure-extension"})
	if err != nil {
		t.Fatal(err)
	}
	if hasLegacyGate(registeredPure) {
		t.Fatal("runtime extension unexpectedly acquired the legacy gate")
	}

	unlocked := false
	unlock := runtimectx.LockLegacy()
	t.Cleanup(func() {
		if !unlocked {
			unlock()
		}
		select {
		case <-releaseLegacy:
		default:
			close(releaseLegacy)
		}
	})

	root.SetArgs([]string{"pure-extension"})
	pureDone := make(chan error, 1)
	go func() { pureDone <- root.Execute() }()
	select {
	case err := <-pureDone:
		if err != nil {
			t.Fatalf("runtime extension returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime extension unexpectedly waited for the legacy gate")
	}
	unlock()
	unlocked = true
	select {
	case <-pureRan:
	default:
		t.Fatal("runtime extension callback did not run")
	}

	legacyUnlock := runtimectx.LockLegacy()
	legacyReleased := false
	t.Cleanup(func() {
		if !legacyReleased {
			legacyUnlock()
		}
	})
	root.SetArgs([]string{"legacy-extension"})
	legacyDone := make(chan error, 1)
	go func() { legacyDone <- root.Execute() }()
	select {
	case <-legacyEntered:
		t.Fatal("legacy extension bypassed the held gate")
	case <-time.After(50 * time.Millisecond):
	}

	legacyUnlock()
	legacyReleased = true
	select {
	case <-legacyEntered:
	case <-time.After(time.Second):
		t.Fatal("legacy extension did not enter after gate release")
	}
	close(releaseLegacy)
	select {
	case err := <-legacyDone:
		if err != nil {
			t.Fatalf("legacy extension returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("legacy extension did not finish")
	}
}

func TestAddCommandDoesNotDoubleGateAlreadyGatedCommand(t *testing.T) {
	before := len(extraCommands)
	t.Cleanup(func() {
		extraCommands = extraCommands[:before]
	})

	command := newConfigCommand("already-gated-extension", "capture", func(*config.CmdArgs) *errs.Error {
		return nil
	}, false)
	AddCommand("", command)
	root := NewRootCommand()
	root.SetArgs([]string{"already-gated-extension"})

	done := make(chan error, 1)
	go func() { done <- root.Execute() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("already gated extension returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("already gated extension appears to have nested the legacy gate")
	}
}

func TestAddCommandCanInvokePublicLegacyEntry(t *testing.T) {
	before := len(extraCommands)
	t.Cleanup(func() {
		extraCommands = extraCommands[:before]
	})

	command := &cobra.Command{
		Use:         "nested-legacy-entry",
		Annotations: map[string]string{legacyGateAnnotation: "1"},
		RunE: func(_ *cobra.Command, _ []string) error {
			return RunSeriesDown(&config.CmdArgs{Tables: []string{"missing-nested-entry-fixture"}})
		},
	}
	AddCommand("", command)
	root := NewRootCommand()
	root.SetArgs([]string{"nested-legacy-entry"})

	done := make(chan error, 1)
	go func() { done <- root.Execute() }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("nested legacy entry unexpectedly succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("AddCommand callback deadlocked in public legacy entry")
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

func TestLegacyGateCompositionIsIdempotent(t *testing.T) {
	var calls int
	command := newConfigCommand("capture-gated", "capture", func(*config.CmdArgs) *errs.Error {
		calls++
		return nil
	}, false)
	if !hasLegacyGate(command) {
		t.Fatal("newConfigCommand is missing its legacy gate marker")
	}
	if got := withLegacyCommand(command); got != command {
		t.Fatal("withLegacyCommand replaced an already gated command")
	}

	done := make(chan error, 1)
	go func() { done <- command.RunE(command, nil) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("gated command returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("gated command appears to have nested the non-reentrant legacy lock")
	}
	if calls != 1 {
		t.Fatalf("command callback calls = %d, want 1", calls)
	}
}

func TestWithLegacyCommandRecursivelyGatesDescendants(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	grandchild := &cobra.Command{
		Use: "grandchild",
		RunE: func(_ *cobra.Command, _ []string) error {
			close(entered)
			<-release
			return nil
		},
	}
	child := &cobra.Command{Use: "child"}
	child.AddCommand(grandchild)
	parent := &cobra.Command{Use: "parent"}
	parent.AddCommand(child)

	if got := withLegacyCommand(parent); got != parent {
		t.Fatal("withLegacyCommand replaced the parent command")
	}
	if withLegacyCommand(parent) != parent {
		t.Fatal("withLegacyCommand was not idempotent for the command tree")
	}
	for _, command := range []*cobra.Command{child, grandchild} {
		if !hasLegacyGate(command) {
			t.Fatalf("descendant %q is missing its legacy gate", command.Name())
		}
	}

	root := &cobra.Command{Use: "test"}
	root.AddCommand(parent)
	root.SetArgs([]string{"parent", "child", "grandchild"})
	unlocked := false
	unlock := runtimectx.LockLegacy()
	t.Cleanup(func() {
		if !unlocked {
			unlock()
		}
		select {
		case <-release:
		default:
			close(release)
		}
	})

	done := make(chan error, 1)
	go func() { done <- root.Execute() }()
	select {
	case <-entered:
		t.Fatal("grandchild bypassed the legacy gate")
	case <-time.After(50 * time.Millisecond):
	}
	unlock()
	unlocked = true
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("grandchild did not enter after the legacy gate was released")
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("command tree returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("command tree did not finish")
	}
}

func TestRunDevUsesLegacyGate(t *testing.T) {
	unlocked := false
	unlock := runtimectx.LockLegacy()
	t.Cleanup(func() {
		if !unlocked {
			unlock()
		}
	})

	done := make(chan error, 1)
	go func() { done <- web.RunDev([]string{"--invalid-run-dev-flag"}) }()
	select {
	case err := <-done:
		t.Fatalf("RunDev bypassed the legacy gate with error: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	unlock()
	unlocked = true
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("RunDev unexpectedly accepted an invalid flag")
		}
	case <-time.After(time.Second):
		t.Fatal("RunDev did not enter after the legacy gate was released")
	}
}

func TestLegacyGateMarkersCoverRootWebAndAdjExport(t *testing.T) {
	root := NewRootCommand()
	if !hasLegacyGate(root) {
		t.Fatal("root command is missing its legacy gate marker")
	}
	command, _, err := root.Find([]string{"kline", "adj-export"})
	if err != nil {
		t.Fatalf("find adj-export: %v", err)
	}
	if !hasLegacyGate(command) {
		t.Fatal("adj-export command is missing the config-command legacy gate")
	}
	webCommand := withLegacyCommand(web.NewCommand())
	if !hasLegacyGate(webCommand) || withLegacyCommand(webCommand) != webCommand {
		t.Fatal("web command gate is not idempotent")
	}
}

func TestLegacyGateMarkersCoverEntryDataCommands(t *testing.T) {
	root := NewRootCommand()
	paths := [][]string{
		{"trade"}, {"backtest"}, {"spider"}, {"init"}, {"web"},
		{"data", "export"}, {"data", "import"},
		{"kline", "down"}, {"kline", "repair-ranges"}, {"kline", "load"},
		{"kline", "agg"}, {"kline", "export"}, {"kline", "purge"},
		{"kline", "correct"}, {"kline", "verify"}, {"kline", "adj-calc"},
		{"kline", "adj-export"}, {"series", "down"},
		{"tick", "convert"}, {"tick", "to-kline"},
		{"tool", "collect-opt"}, {"tool", "sim-bt"}, {"tool", "test-pickers"},
		{"tool", "load-cal"}, {"tool", "data-server"}, {"tool", "calc-perfs"},
		{"tool", "corr"}, {"tool", "merge-assets"}, {"tool", "cmp-orders"},
		{"tool", "list-strats"}, {"tool", "bt-factor"}, {"tool", "bt-result"},
		{"tool", "test-live-bars"}, {"live", "down-order"}, {"live", "close-order"},
	}
	for _, path := range paths {
		command, _, err := root.Find(path)
		if err != nil {
			t.Fatalf("find %v: %v", path, err)
		}
		if !hasLegacyGate(command) {
			t.Errorf("legacy command %v is missing its gate marker", path)
		}
	}

	for _, path := range [][]string{{"series", "list"}, {"internal", "inspect-data-plan"}} {
		command, _, err := root.Find(path)
		if err != nil {
			t.Fatalf("find pure command %v: %v", path, err)
		}
		if hasLegacyGate(command) {
			t.Errorf("pure command %v unexpectedly has a legacy gate", path)
		}
	}
	listStrats, _, err := root.Find([]string{"tool", "list-strats"})
	if err != nil {
		t.Fatalf("find list-strats: %v", err)
	}
	if !hasLegacyGate(listStrats) {
		t.Fatal("list-strats command is missing its legacy gate")
	}
}
