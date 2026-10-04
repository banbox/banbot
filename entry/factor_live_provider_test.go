package entry

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banexg"
)

func TestResolveFactorLiveFactoriesPrecedenceAndValidation(t *testing.T) {
	var calls []string
	register := func(name string) {
		err := RegisterFactorLiveBinding(name, func(_ context.Context, _ banexg.BanExchange, _ *config.Snapshot, c runner.Config) (FactorLiveBinding, error) {
			calls = append(calls, name+":"+c.AccountID)
			return FactorLiveBinding{}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	global, accountA, accountB, cli := "test-global", "test-account-a", "test-account-b", "test-cli"
	register(global)
	register(accountA)
	register(accountB)
	register(cli)
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yml")
	body := fmt.Sprintf("execution: {live_provider: %s}\naccounts:\n  a: {live_provider: %s}\n  b: {live_provider: %s}\nrun_policy:\n  - name: one\n    engine: factor\n    account: a\n    capital_weight: 0.5\n  - name: two\n    engine: factor\n    account: a\n    capital_weight: 0.5\n  - name: three\n    engine: factor\n    account: b\n    capital_weight: 0.5\n", global, accountA, accountB)
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	spec, err := config.LoadRunSpec(&config.CmdArgs{Configs: config.ArrString{path}, DataDir: dir}, false)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("account-overrides-and-shared-account", func(t *testing.T) {
		calls = nil
		factories, err := resolveFactorLiveFactories(spec, []string{"a", "b"}, "")
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range []runner.Config{{AccountID: "a"}, {AccountID: "a"}, {AccountID: "b"}} {
			if _, err := factories[c.AccountID](context.Background(), nil, nil, c); err != nil {
				t.Fatal(err)
			}
		}
		if got := fmt.Sprint(calls); got != "[test-account-a:a test-account-a:a test-account-b:b]" {
			t.Fatalf("provider routing = %s", got)
		}
	})
	t.Run("cli-overrides-account", func(t *testing.T) {
		calls = nil
		factories, err := resolveFactorLiveFactories(spec, []string{"a", "b"}, cli)
		if err != nil {
			t.Fatal(err)
		}
		for _, account := range []string{"a", "b"} {
			_, _ = factories[account](context.Background(), nil, nil, runner.Config{AccountID: account})
		}
		if got := fmt.Sprint(calls); got != "[test-cli:a test-cli:b]" {
			t.Fatalf("CLI provider routing = %s", got)
		}
	})
	t.Run("unknown-provider-validates-before-factory-use", func(t *testing.T) {
		before := len(calls)
		bad := filepath.Join(dir, "bad.yml")
		if err := os.WriteFile(bad, []byte("accounts: {a: {live_provider: missing-provider}}\nrun_policy: [{name: one, engine: factor, account: a}]\n"), 0600); err != nil {
			t.Fatal(err)
		}
		badSpec, err := config.LoadRunSpec(&config.CmdArgs{Configs: config.ArrString{bad}, DataDir: dir}, false)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := resolveFactorLiveFactories(badSpec, []string{"a"}, ""); err == nil {
			t.Fatal("unknown provider accepted")
		}
		if len(calls) != before {
			t.Fatal("provider factory called during validation")
		}
	})
}
