package config

import (
	"testing"

	"github.com/banbox/banbot/core"
)

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
