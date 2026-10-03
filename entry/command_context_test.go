package entry

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banexg/errs"
)

func TestRuntimeCommandPassesCallerContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	called := false
	cmd := newRuntimeConfigCommandContext("capture", "capture", func(got context.Context, args *config.CmdArgs) *errs.Error {
		called = true
		if got != ctx || !args.NoDefault {
			t.Fatal("command lost caller context or arguments")
		}
		return nil
	}, false)
	cmd.SetArgs([]string{"--no-default"})
	if err := cmd.ExecuteContext(ctx); err != nil || !called {
		t.Fatal(err, called)
	}
}

func TestCanceledUnifiedEntriesDoNotLoadConfigOrOpenResources(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	args := &config.CmdArgs{NoDefault: true, Configs: config.ArrString{filepath.Join(t.TempDir(), "missing.yml")}}
	for name, run := range map[string]func() error{
		"execute": func() error {
			return ExecuteContext(ctx, []string{"backtest", "--no-default", "--config", args.Configs[0]})
		},
		"backtest": func() error { return runExplicitBackTestContext(ctx, args) },
		"trade":    func() error { return runExplicitTradeContext(ctx, args, nil) },
		"session": func() error {
			_, _, err := openExplicitEntrySessionFromSpecContext(ctx, args, nil)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := run()
			if err == nil || (!errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), context.Canceled.Error())) || strings.Contains(err.Error(), "missing.yml") {
				t.Fatalf("canceled entry attempted configuration or I/O: %v", err)
			}
		})
	}
}
