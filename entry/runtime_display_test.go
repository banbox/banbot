package entry

import (
	"context"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/runtime"
)

func TestExplicitEntryRuntimePreservesSnapshotLocation(t *testing.T) {
	process := runtime.NewProcess()
	t.Cleanup(process.Close)
	session := &explicitEntrySession{process: process, ctx: context.Background()}
	location := time.FixedZone("entry-display", -5*60*60)
	snapshot := config.NewSnapshotWithDirs(&config.Config{Env: core.RunEnvDryRun}, t.TempDir(), "", location)
	for _, mode := range []string{core.RunModeLive, core.RunModeBackTest, core.RunModeData} {
		rt, err := session.newStorageRuntime(snapshot, mode, 0)
		if err != nil {
			t.Fatal(err)
		}
		if rt.Config.Location() != location {
			t.Fatalf("%s entry lost display location: %v", mode, rt.Config.Location())
		}
	}
}
