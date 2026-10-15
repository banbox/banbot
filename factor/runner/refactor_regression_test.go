package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/expr"
	"github.com/banbox/banbot/factor/research"
)

func TestDecisionSessionSharingIsolationAndFailedBorrow(t *testing.T) {
	c := archiveConfig(t, false)
	c.Manifest.ExecutionMode, c.Manifest.LatencyAssumption = "weights", "test"
	plan, combo, err := compileDecision(c)
	if err != nil {
		t.Fatal(err)
	}
	makeEngine := func(c Config) *decisionEngine {
		t.Helper()
		e, err := newDecisionEngine(c, plan, combo)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(e.close)
		return e
	}
	if makeEngine(c).session == makeEngine(c).session {
		t.Fatal("unshared sessions alias")
	}
	c.ComputationGroup = NewComputationGroup()
	first, second := makeEngine(c), makeEngine(c)
	if first.session != second.session || first.shared.borrowers != 2 {
		t.Fatal("compatible sessions not shared")
	}
	changed := c
	changed.Snapshot = factor.CloneSnapshotSpec(c.Snapshot)
	changed.Snapshot.Universe.Version = "different"
	if makeEngine(changed).session == first.session {
		t.Fatal("different keys shared")
	}
	before := len(c.ComputationGroup.sessions)
	bad := c
	bad.Manifest.Currency = ""
	bad.Chunks = []Chunk{{Path: "missing-input"}}
	if _, err := newDecisionEngine(bad, plan, combo); err == nil || !strings.Contains(err.Error(), "manifest") {
		t.Fatalf("manifest must fail before input: %v", err)
	}
	bad.Manifest.Currency = c.Manifest.Currency
	if _, err := newDecisionEngine(bad, plan, combo); err == nil {
		t.Fatal("missing shared input accepted")
	}
	if len(c.ComputationGroup.sessions) != before || first.shared.borrowers != 2 {
		t.Fatal("failed engine leaked borrow")
	}
}

func TestLivePreflightManifestParityWithoutResourceBorrow(t *testing.T) {
	base := archiveConfig(t, false)
	base.ComputationGroup = NewComputationGroup()
	base.ComputationContext = ComputationContext{"fixture", "clock", "sample"}
	cases := []struct {
		name   string
		change func(*Config)
	}{
		{"valid", func(*Config) {}},
		{"builder", func(c *Config) { c.Manifest.Portfolio.Builder = "missing-refactor-builder" }},
		{"funding", func(c *Config) { c.Manifest.Costs.FundingPolicy = "unknown" }},
		{"funding-source", func(c *Config) { c.FundingSource = "" }},
		{"revision", func(c *Config) { c.Manifest.CodeRevision = "" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := base
			tc.change(&c)
			before := len(c.ComputationGroup.sessions)
			preflight := ValidateLiveConfig(c)
			if len(c.ComputationGroup.sessions) != before {
				t.Fatal("preflight borrowed Session")
			}
			live, actual := NewLive(c, &liveSink{}, func() int64 { return 3600001 }, nil)
			if fmt.Sprint(preflight) != fmt.Sprint(actual) {
				t.Fatalf("preflight=%v construct=%v", preflight, actual)
			}
			if actual != nil {
				return
			}
			defer func() { live.Stop(); _ = live.Join(context.Background()) }()
			plan, combo, err := compileLiveDecision(c)
			if err != nil {
				t.Fatal(err)
			}
			c, err = resolveDecisionPortfolio(c)
			if err != nil {
				t.Fatal(err)
			}
			c.Manifest.ExecutionMode = "trade"
			c.Manifest.LatencyAssumption = fmt.Sprintf("live completion clock; observable event after decision+%dms", c.LatencyMS)
			c.Manifest.Combo = combo
			c.Manifest.FactorPlanHash = plan.Hash()
			c.Manifest.UniverseVersion = c.Snapshot.Universe.Version
			c.Manifest.VisibilityPolicy = c.Snapshot.VisibilityPolicy
			c.Manifest.StaticUniverse = c.Snapshot.Universe.Static
			expected, err := research.BuildManifest(c.Manifest)
			if err != nil {
				t.Fatal(err)
			}
			if expected.ID() != live.engine.manifest.ID() || !reflect.DeepEqual(expected.Spec(), live.engine.manifest.Spec()) {
				t.Fatal("live manifest drift")
			}
		})
	}
}

func TestExpressionMultiColumnValidation(t *testing.T) {
	spec := expressionConfig()
	spec.Outputs["second"] = "kline.close"
	c := Config{Expressions: spec, DecisionInterval: 3600000}
	for _, tc := range []struct {
		columns []string
		weights map[string]float64
		reason  string
	}{
		{[]string{"second", "momentum"}, map[string]float64{"second": 2, "momentum": 1}, ""},
		{[]string{"momentum", "momentum"}, map[string]float64{"momentum": 1}, "repeated"},
		{[]string{"second", "absent"}, map[string]float64{"second": 1}, "unknown"},
		{[]string{"second", "momentum"}, map[string]float64{"second": 1}, "missing/nonfinite"},
		{[]string{"second", "momentum"}, map[string]float64{"second": 1, "momentum": math.Inf(1)}, "missing/nonfinite"},
	} {
		c.Combo = research.ComboSpec{Method: research.Fixed, Columns: tc.columns, Weights: tc.weights}
		_, _, err := compileDecision(c)
		if tc.reason == "" && err != nil || tc.reason != "" && (err == nil || !strings.Contains(err.Error(), tc.reason)) {
			t.Fatalf("columns=%v: %v", tc.columns, err)
		}
	}
}

func TestCloneConfigOwnedFieldsAndBorrowedHandles(t *testing.T) {
	c := paperConfig(t, archiveConfig(t, false))
	c.Expressions = expressionConfig()
	var err error
	c.Plan, _, err = compileDecision(Config{Factor: c.Factor})
	if err != nil {
		t.Fatal(err)
	}
	c.ComputationGroup = NewComputationGroup()
	factory := &validationInputFactory{}
	c.HistoricalInput = factory
	c.ObserveBatch = func(context.Context, HistoricalBatch) error { return nil }
	c.PortfolioBuilder = func(factor.Frame, factor.Universe, factor.PortfolioSpec, research.PortfolioDefinition) (*factor.TargetPortfolio, []factor.Diagnostic, error) {
		return nil, nil, nil
	}
	c.Combo = research.ComboSpec{Columns: []string{"x"}, Weights: map[string]float64{"x": 1}}
	c.Manifest.Parameters = map[string]float64{"p": 2}
	c.Manifest.Combo = c.Combo
	c.Manifest.Snapshots = []research.SnapshotReference{{ID: "ref", Schemas: map[string]string{"s": "v1"}, SourceVersions: map[string]string{"s": "v1"}, Revisions: map[string]uint64{"s": 1}}}
	before, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	copy, err := CloneConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	if copy.Plan != c.Plan || copy.ComputationGroup != c.ComputationGroup || copy.HistoricalInput != factory || reflect.ValueOf(copy.ObserveBatch).Pointer() != reflect.ValueOf(c.ObserveBatch).Pointer() || reflect.ValueOf(copy.PortfolioBuilder).Pointer() != reflect.ValueOf(c.PortfolioBuilder).Pointer() {
		t.Fatal("borrowed handles changed")
	}
	c.Chunks[0].Path = "mutated"
	c.Snapshot.Universe.Tracked[0] = 999
	c.Snapshot.SIDMap[1] = "mutated"
	c.Snapshot.Schemas["kline"] = "mutated"
	c.Snapshot.SourceVersions["kline"] = "mutated"
	c.Expressions.Bindings["kline"] = expr.Binding{Source: "changed"}
	c.Expressions.Params["window"] = 99
	c.Expressions.Lets["mom"] = "mutated"
	c.Expressions.Outputs["momentum"] = "mutated"
	c.Expressions.Combine.Weights["momentum"] = 99
	c.Combo.Columns[0] = "mutated"
	c.Combo.Weights["x"] = 99
	c.Manifest.Parameters["p"] = 99
	c.Manifest.Labels[0].Name = "mutated"
	c.Manifest.Snapshots[0].ID = "mutated"
	c.Manifest.Snapshots[0].Schemas["s"] = "mutated"
	c.Manifest.Snapshots[0].SourceVersions["s"] = "mutated"
	c.Manifest.Snapshots[0].Revisions["s"] = 99
	unit := c.Execution.Instruments[1]
	unit.ID = "mutated"
	c.Execution.Instruments[1] = unit
	after, err := json.Marshal(copy)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("owned configuration changed after original mutation:\nbefore=%s\nafter=%s", before, after)
	}
}

func TestCloneConfigRejectsNonfiniteAndPreservesEmpty(t *testing.T) {
	for _, c := range []Config{{InitialNAV: math.NaN()}, {Manifest: research.ManifestSpec{Parameters: map[string]float64{"p": math.Inf(1)}}}, {Expressions: expressionConfig()}} {
		if c.Expressions != nil {
			c.Expressions.Params["window"] = math.Inf(-1)
		}
		if _, err := CloneConfig(c); err == nil {
			t.Fatal("nonfinite config accepted")
		}
	}
	for _, c := range []Config{{}, {Chunks: []Chunk{}, Snapshot: factor.SnapshotSpec{SIDMap: map[int32]string{}, Universe: factor.Universe{Tracked: []int32{}}}, Combo: research.ComboSpec{Columns: []string{}, Weights: map[string]float64{}}, Manifest: research.ManifestSpec{Snapshots: []research.SnapshotReference{}}}} {
		copy, err := CloneConfig(c)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(c.Chunks, copy.Chunks) || !reflect.DeepEqual(c.Snapshot, copy.Snapshot) || !reflect.DeepEqual(c.Combo, copy.Combo) || !reflect.DeepEqual(c.Manifest, copy.Manifest) {
			t.Fatal("nil/empty distinction changed")
		}
	}
}

func TestCloneConfigPreservesUniverseOrderDuplicatesAndInternalBorrow(t *testing.T) {
	timeline := &replayTimeline{}
	c := Config{timeline: timeline, timelineIndex: 2, Snapshot: factor.SnapshotSpec{Universe: factor.Universe{Tracked: []int32{3, 1, 3, 2}}}}
	copy, err := CloneConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	if copy.timeline != timeline || copy.timelineIndex != 2 {
		t.Fatal("internal timeline borrow changed")
	}
	if !reflect.DeepEqual(copy.Snapshot.Universe.Tracked, []int32{3, 1, 3, 2}) || !reflect.DeepEqual(c.Snapshot.Universe.Tracked, []int32{3, 1, 3, 2}) {
		t.Fatal("configuration clone changed universe order/duplicates")
	}
}
