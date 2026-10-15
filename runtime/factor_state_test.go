package runtime

import (
	"context"
	"math"
	"reflect"
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/expr"
	"github.com/banbox/banbot/factor/research"
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

type factorOwnedInput struct{}

func (*factorOwnedInput) Identity() string       { return "borrowed" }
func (*factorOwnedInput) Ranges() []runner.Chunk { return nil }
func (*factorOwnedInput) Open(context.Context, runner.Config, runner.Chunk) (runner.HistoricalInput, error) {
	return nil, nil
}

func TestFactorComponentClonesNestedConfigAndBorrowsHandles(t *testing.T) {
	p := NewProcess()
	defer p.Close()
	r, err := p.NewRuntime(Options{Mode: core.RunModeBackTest})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	cfg := runner.Config{
		Snapshot: factor.SnapshotSpec{Schemas: map[string]string{"custom": "v1"}, Universe: factor.Universe{
			Reference: []int32{3, 1, 3}, Investable: []int32{2, 1, 2}, Tracked: []int32{},
		}},
		Expressions: &expr.Spec{Params: map[string]float64{"window": 2}, Outputs: map[string]string{"score": "close"}},
		Combo:       research.ComboSpec{Columns: []string{"score"}, Weights: map[string]float64{"score": 1}},
		Manifest:    research.ManifestSpec{Parameters: map[string]float64{"window": 2}, Snapshots: []research.SnapshotReference{{Schemas: map[string]string{"custom": "v1"}, Revisions: map[string]uint64{"custom": 3}}}},
		Chunks:      []runner.Chunk{{Path: "original"}},
		Plan:        &factor.Plan{}, ComputationGroup: &runner.ComputationGroup{}, HistoricalInput: &factorOwnedInput{},
	}
	cfg.PortfolioBuilder = func(factor.Frame, factor.Universe, factor.PortfolioSpec, research.PortfolioDefinition) (*factor.TargetPortfolio, []factor.Diagnostic, error) {
		return nil, nil, nil
	}
	cfg.ObserveBatch = func(context.Context, runner.HistoricalBatch) error { return nil }
	sink, output := &runner.AccountSink{}, &runner.JSONOutput{}
	sinks, outputs := []runner.Sink{sink}, []runner.Output{output}
	if err := r.InstallFactorReplay([]runner.Config{cfg}, sinks, outputs); err != nil {
		t.Fatal(err)
	}
	owned := r.FactorState.configs[0]
	if !reflect.DeepEqual(owned.Snapshot.Universe, cfg.Snapshot.Universe) {
		t.Fatal("ownership copy changed universe order, duplicates or empty lists")
	}
	if owned.Plan != cfg.Plan || owned.ComputationGroup != cfg.ComputationGroup || owned.HistoricalInput != cfg.HistoricalInput || reflect.ValueOf(owned.PortfolioBuilder).Pointer() != reflect.ValueOf(cfg.PortfolioBuilder).Pointer() || reflect.ValueOf(owned.ObserveBatch).Pointer() != reflect.ValueOf(cfg.ObserveBatch).Pointer() {
		t.Fatal("borrowed handle identity changed")
	}
	sinks[0], outputs[0] = nil, nil
	if r.FactorState.sinks[0] != sink || r.FactorState.outputs[0] != output {
		t.Fatal("sink/output slice not owned")
	}
	cfg.Snapshot.Schemas["custom"] = "changed"
	cfg.Snapshot.Universe.Reference[0], cfg.Snapshot.Universe.Investable[0] = 9, 9
	cfg.Expressions.Params["window"], cfg.Expressions.Outputs["score"] = 9, "changed"
	cfg.Combo.Columns[0], cfg.Combo.Weights["score"] = "changed", 9
	cfg.Manifest.Parameters["window"] = 9
	cfg.Manifest.Snapshots[0].Schemas["custom"], cfg.Manifest.Snapshots[0].Revisions["custom"] = "changed", 9
	cfg.Chunks[0].Path = "changed"
	if owned.Snapshot.Universe.Reference[0] != 3 || owned.Snapshot.Universe.Investable[0] != 2 || owned.Snapshot.Schemas["custom"] != "v1" || owned.Expressions.Params["window"] != 2 || owned.Expressions.Outputs["score"] != "close" || owned.Combo.Columns[0] != "score" || owned.Combo.Weights["score"] != 1 || owned.Manifest.Parameters["window"] != 2 || owned.Manifest.Snapshots[0].Schemas["custom"] != "v1" || owned.Manifest.Snapshots[0].Revisions["custom"] != 3 || owned.Chunks[0].Path != "original" {
		t.Fatal("nested factor config aliases caller")
	}
}

func TestFactorComponentRejectsInvalidCloneBeforeInstallation(t *testing.T) {
	p := NewProcess()
	defer p.Close()
	r, err := p.NewRuntime(Options{Mode: core.RunModeBackTest})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := r.InstallFactorReplay([]runner.Config{{InitialNAV: math.NaN()}}, []runner.Sink{nil}, []runner.Output{nil}); err == nil || r.FactorState != nil {
		t.Fatal("invalid clone installed factor state")
	}
}
