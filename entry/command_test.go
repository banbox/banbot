package entry

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/web"
	"github.com/banbox/banbot/web/dev"
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
	command := newRuntimeConfigCommand("capture", "capture args", func(args *config.CmdArgs) *errs.Error {
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
	if captured.RawPairs != "BTC/USDT,ETH/USDT" {
		t.Fatalf("raw pairs = %q, want BTC/USDT,ETH/USDT", captured.RawPairs)
	}
}

func TestTickCommandsReturnOnInvalidArguments(t *testing.T) {
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
				t.Fatal("tick command did not return")
			}
		})
	}
}

func TestAddCommandSupportsCommandLocalFlags(t *testing.T) {
	commandRegistryMu.RLock()
	before := len(extraCommands)
	commandRegistryMu.RUnlock()
	t.Cleanup(func() {
		commandRegistryMu.Lock()
		defer commandRegistryMu.Unlock()
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

func TestRegisteredCommandsAreNotWrapped(t *testing.T) {
	commandRegistryMu.RLock()
	before := len(extraCommands)
	commandRegistryMu.RUnlock()
	t.Cleanup(func() {
		commandRegistryMu.Lock()
		defer commandRegistryMu.Unlock()
		extraCommands = extraCommands[:before]
	})

	command := &cobra.Command{Use: "extension", RunE: func(*cobra.Command, []string) error { return nil }}
	runtimeCommand := &cobra.Command{Use: "runtime-extension", RunE: func(*cobra.Command, []string) error { return nil }}
	AddCommand("", command)
	AddCommand("", runtimeCommand)

	root := NewRootCommand()
	registered, _, err := root.Find([]string{"extension"})
	if err != nil {
		t.Fatal(err)
	}
	if registered != command || registered.RunE == nil {
		t.Fatal("AddCommand changed the extension command")
	}
	registered, _, err = root.Find([]string{"runtime-extension"})
	if err != nil {
		t.Fatal(err)
	}
	if registered != runtimeCommand || registered.RunE == nil {
		t.Fatal("second AddCommand changed the extension command")
	}
}

func TestCommandRegistrySnapshotsConcurrentRegistration(t *testing.T) {
	commandRegistryMu.RLock()
	before := len(extraCommands)
	commandRegistryMu.RUnlock()
	t.Cleanup(func() {
		commandRegistryMu.Lock()
		extraCommands = extraCommands[:before]
		commandRegistryMu.Unlock()
	})

	var wait sync.WaitGroup
	for i := 0; i < 16; i++ {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			name := fmt.Sprintf("concurrent-extension-%d", i)
			AddCommandFactory("", func() *cobra.Command { return &cobra.Command{Use: name} })
		}(i)
	}
	for i := 0; i < 16; i++ {
		wait.Add(1)
		go func() { defer wait.Done(); _ = NewRootCommand() }()
	}
	wait.Wait()
	root := NewRootCommand()
	for i := 0; i < 16; i++ {
		if command, _, err := root.Find([]string{fmt.Sprintf("concurrent-extension-%d", i)}); err != nil || command == nil {
			t.Fatalf("concurrent command %d missing: %v", i, err)
		}
	}
}

func TestCommandFactoryDoesNotReparentCommandsAcrossRoots(t *testing.T) {
	commandRegistryMu.RLock()
	before := len(extraCommands)
	commandRegistryMu.RUnlock()
	t.Cleanup(func() {
		commandRegistryMu.Lock()
		extraCommands = extraCommands[:before]
		commandRegistryMu.Unlock()
	})

	started := make(chan struct{})
	release := make(chan struct{})
	AddCommandFactory("", func() *cobra.Command {
		return &cobra.Command{Use: "factory-extension", RunE: func(*cobra.Command, []string) error {
			close(started)
			<-release
			return nil
		}}
	})
	first := NewRootCommand()
	first.SetArgs([]string{"factory-extension"})
	done := make(chan error, 1)
	go func() { done <- first.Execute() }()
	<-started
	second := NewRootCommand()
	command, _, err := second.Find([]string{"factory-extension"})
	if err != nil || command == nil {
		close(release)
		t.Fatalf("second root command missing: %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestAddCommandRejectsReattachment(t *testing.T) {
	commandRegistryMu.RLock()
	before := len(extraCommands)
	commandRegistryMu.RUnlock()
	t.Cleanup(func() {
		commandRegistryMu.Lock()
		extraCommands = extraCommands[:before]
		commandRegistryMu.Unlock()
	})
	AddCommand("", &cobra.Command{Use: "single-root-extension"})
	_ = NewRootCommand()
	defer func() {
		if recover() == nil {
			t.Fatal("building a second root with a shared command did not panic")
		}
	}()
	_ = NewRootCommand()
}

func TestAddCommandCanInvokePublicRuntimeEntry(t *testing.T) {
	commandRegistryMu.RLock()
	before := len(extraCommands)
	commandRegistryMu.RUnlock()
	t.Cleanup(func() {
		commandRegistryMu.Lock()
		defer commandRegistryMu.Unlock()
		extraCommands = extraCommands[:before]
	})

	command := &cobra.Command{
		Use: "nested-runtime-entry",
		RunE: func(_ *cobra.Command, _ []string) error {
			return RunSeriesDown(&config.CmdArgs{DataDir: t.TempDir(), NoDefault: true, ConfigData: "invalid: ["})
		},
	}
	AddCommand("", command)
	root := NewRootCommand()
	root.SetArgs([]string{"nested-runtime-entry"})

	done := make(chan error, 1)
	go func() { done <- root.Execute() }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("nested runtime entry unexpectedly succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("AddCommand callback did not reach the public runtime entry")
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

func TestTypedWebCommandRejectsInvalidFlags(t *testing.T) {
	command := web.NewDevCommandWithFactory(func(*dev.CmdArgs) (*dev.DevServer, func(), error) {
		t.Fatal("factory must not run for invalid command arguments")
		return nil, nil, nil
	})
	command.SetArgs([]string{"--invalid-run-dev-flag"})
	if err := command.Execute(); err == nil {
		t.Fatal("typed web command unexpectedly accepted an invalid flag")
	}
}

func TestCloseOrderCommandUsesExplicitRuntime(t *testing.T) {
	root := NewRootCommand()
	_, _, err := root.Find([]string{"live", "close-order"})
	if err != nil {
		t.Fatalf("find close-order: %v", err)
	}
}

func TestDownOrderCommandUsesExplicitRuntimeAndPreservesFlags(t *testing.T) {
	root := NewRootCommand()
	command, _, err := root.Find([]string{"live", "down-order"})
	if err != nil {
		t.Fatalf("find down-order: %v", err)
	}
	for _, name := range []string{"account", "exchange", "market", "timestart", "timeend", "pairs", "force"} {
		if command.Flags().Lookup(name) == nil {
			t.Fatalf("down-order lost --%s", name)
		}
	}
}

func TestCompareOrdersCommandUsesExplicitRuntimeAndPreservesFlags(t *testing.T) {
	root := NewRootCommand()
	command, _, err := root.Find([]string{"tool", "cmp-orders"})
	if err != nil {
		t.Fatalf("find cmp-orders: %v", err)
	}
	for _, name := range []string{"account", "bt-path", "bot-name", "amt-rate", "skip-unhit"} {
		if command.Flags().Lookup(name) == nil {
			t.Fatalf("cmp-orders lost --%s", name)
		}
	}
}

func TestSnapshotForOrderIdentityDoesNotMutatePrimarySnapshot(t *testing.T) {
	primary := config.NewSnapshotWithDirs(&config.Config{Exchange: &config.ExchangeConfig{Name: "primary"}, MarketType: "spot"}, t.TempDir(), "")
	child, err := snapshotForOrderIdentity(primary, "stored", "linear")
	if err != nil {
		t.Fatal(err)
	}
	if primary.View().Exchange.Name != "primary" || primary.View().MarketType != "spot" {
		t.Fatalf("primary snapshot was mutated: %#v", primary.View())
	}
	if child.View().Exchange.Name != "stored" || child.View().MarketType != "linear" {
		t.Fatalf("child snapshot identity = %s/%s", child.View().Exchange.Name, child.View().MarketType)
	}
}
