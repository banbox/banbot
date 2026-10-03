package runtime

import (
	"context"
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/runner"
)

func TestFactorComponentIsOptionalOwnedAndJoined(t *testing.T) {
	p := NewProcess()
	defer p.Close()
	r, err := p.NewRuntime(Options{Mode: core.RunModeBackTest})
	if err != nil {
		t.Fatal(err)
	}
	if r.FactorState != nil {
		t.Fatal("TS-only runtime constructed factor state")
	}
	cfg := runner.Config{Mode: runner.Research, Snapshot: factor.SnapshotSpec{SIDMap: map[int32]string{1: "original"}}}
	if err := r.InstallFactorReplay([]runner.Config{cfg}, []runner.Sink{nil}, []runner.Output{nil}); err != nil {
		t.Fatal(err)
	}
	cfg.Snapshot.SIDMap[1] = "mutated"
	if r.FactorState.configs[0].Snapshot.SIDMap[1] != "original" {
		t.Fatal("task config not frozen")
	}
	if err := r.InstallFactorReplay([]runner.Config{cfg}, []runner.Sink{nil}, []runner.Output{nil}); err == nil {
		t.Fatal("duplicate component accepted")
	}
	r.Close()
	r.Join()
	if _, err := r.FactorState.Run(context.Background()); err == nil {
		t.Fatal("closed runtime started factor replay")
	}
}
