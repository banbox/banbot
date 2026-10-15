package runtime

import (
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/utils"
)

func TestRuntimeRetainsDisplayLocationAndSystemLanguage(t *testing.T) {
	previous := core.SysLang
	core.SysLang = "legacy-language"
	t.Cleanup(func() { core.SysLang = previous })
	process := NewProcess()
	t.Cleanup(process.Close)
	for _, mode := range []string{core.RunModeLive, core.RunModeBackTest} {
		location := time.FixedZone(mode, 8*60*60)
		rt, err := process.NewRuntime(Options{Mode: mode, DisplayLocation: location})
		if err != nil {
			t.Fatal(err)
		}
		if got := rt.Config.Location(); got != location {
			t.Errorf("%s runtime snapshot location = %v, want %v", mode, got, location)
		}
		if got := rt.Config.Clone().Location(); got != location {
			t.Errorf("%s cloned snapshot location = %v, want %v", mode, got, location)
		}
		if got := rt.Core.SysLang; got != utils.GetSystemLanguage() {
			t.Errorf("%s system language = %q", mode, got)
		}
	}
	if core.SysLang != "legacy-language" {
		t.Fatal("runtime initialization modified legacy system language")
	}
}

func TestRuntimeDisplayLocationUsesSnapshotDefault(t *testing.T) {
	process := NewProcess()
	t.Cleanup(process.Close)
	rt, err := process.NewRuntime(Options{Config: &config.Config{}})
	if err != nil {
		t.Fatal(err)
	}
	if rt.Config.Location() != time.UTC {
		t.Fatalf("default display location = %v", rt.Config.Location())
	}
}
