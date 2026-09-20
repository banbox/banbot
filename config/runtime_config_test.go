package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/banbox/banbot/core"
)

func TestResolveDataDirIsExplicitAndDoesNotInstallGlobal(t *testing.T) {
	oldDir := DataDir
	oldEnv, hadEnv := os.LookupEnv("BanDataDir")
	t.Cleanup(func() {
		DataDir = oldDir
		if hadEnv {
			_ = os.Setenv("BanDataDir", oldEnv)
		} else {
			_ = os.Unsetenv("BanDataDir")
		}
	})
	DataDir = "legacy"
	_ = os.Setenv("BanDataDir", filepath.Join("relative", "data"))

	got := ResolveDataDir("./runtime-data")
	want, err := filepath.Abs("./runtime-data")
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("ResolveDataDir explicit = %q, want %q", got, want)
	}
	if DataDir != "legacy" {
		t.Fatalf("ResolveDataDir installed global DataDir %q", DataDir)
	}
}

func TestResolveDataDirUsesEnvironmentWhenExplicitIsEmpty(t *testing.T) {
	oldEnv, hadEnv := os.LookupEnv("BanDataDir")
	t.Cleanup(func() {
		if hadEnv {
			_ = os.Setenv("BanDataDir", oldEnv)
		} else {
			_ = os.Unsetenv("BanDataDir")
		}
	})
	_ = os.Setenv("BanDataDir", "./env-data")
	want, err := filepath.Abs("./env-data")
	if err != nil {
		t.Fatal(err)
	}
	if got := ResolveDataDir(""); got != want {
		t.Fatalf("ResolveDataDir environment = %q, want %q", got, want)
	}
}

func TestLoadRuntimeSnapshotDoesNotInstallGlobals(t *testing.T) {
	previousDir, previousName, previousMode := DataDir, Name, core.RunMode
	args := &CmdArgs{DataDir: t.TempDir(), NoDefault: true, ConfigData: `
name: isolated
env: dry_run
exchange:
  name: binance
market_type: linear
time_start: "2024-01-01"
time_end: "2024-01-02"
stake_currency: [USDT]
pairs: [BTC]
accounts:
  zeta: {}
`}
	snapshot, err := LoadRuntimeSnapshot(args)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.View().Pairs[0] != "BTC/USDT:USDT" || snapshot.DefaultAccount() != "default" {
		t.Fatal("runtime configuration was not normalized")
	}
	if DataDir != previousDir || Name != previousName || core.RunMode != previousMode || args.Inited {
		t.Fatal("loading runtime configuration modified process or caller state")
	}
}
