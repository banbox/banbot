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
	session := &explicitEntrySession{process: process, ctx: context.Background(), logArgs: config.CmdArgs{Logfile: "owned.log"}}
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
		if rt.Core.LogFile != session.logArgs.Logfile {
			t.Fatalf("%s entry lost log file: %q", mode, rt.Core.LogFile)
		}
	}
}

func TestExplicitEntryRuntimeBindsEngineContext(t *testing.T) {
	process := runtime.NewProcess()
	t.Cleanup(process.Close)
	session := &explicitEntrySession{process: process, ctx: context.Background()}
	snapshot := config.NewSnapshotWithDirs(&config.Config{Env: core.RunEnvDryRun}, t.TempDir(), "", nil)
	ctx, cancel := context.WithCancel(session.ctx)
	t.Cleanup(cancel)
	rt, err := session.newStorageRuntimeContext(ctx, snapshot, core.RunModeLive, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rt.Close(); rt.Join() })
	cancel()
	select {
	case <-rt.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("runtime did not inherit engine cancellation")
	}
	if session.ctx.Err() != nil {
		t.Fatal("engine cancellation stopped the entry session")
	}
	if canceledRT, err := session.newStorageRuntimeContext(ctx, snapshot, core.RunModeLive, 0); err == nil || canceledRT != nil {
		t.Fatal("cancelled engine allocated a runtime", err)
	}
}
