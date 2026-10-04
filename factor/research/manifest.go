package research

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"math"
	"slices"
	"sort"

	"github.com/banbox/banbot/factor"
)

type PortfolioDefinition struct {
	Builder                     string
	K                           int
	LongNotional, ShortNotional float64
	Mode                        factor.PortfolioMode
}
type CostSpec struct {
	FeeRate, SlippageRate float64
	FundingPolicy         string
}
type SnapshotReference struct {
	ID, ContentDigest, AdjustmentVersion string
	Schemas, SourceVersions              map[string]string
	Revisions                            map[string]uint64
}
type ManifestSpec struct {
	Currency                                                        string
	CodeRevision, FactorPlanHash, UniverseVersion, VisibilityPolicy string
	ExecutionMode, LatencyAssumption                                string
	StaticUniverse                                                  bool
	Combo                                                           ComboSpec
	Portfolio                                                       PortfolioDefinition
	Labels                                                          []LabelSpec
	Parameters                                                      map[string]float64
	Costs                                                           CostSpec
	Snapshots                                                       []SnapshotReference
}
type Manifest struct {
	spec             ManifestSpec
	id, strategyHash string
	diagnostics      []factor.Diagnostic
}

// CloneManifestSpec copies owned containers without validation or normalization.
func CloneManifestSpec(s ManifestSpec) ManifestSpec {
	s.Combo = CloneComboSpec(s.Combo)
	s.Parameters = maps.Clone(s.Parameters)
	s.Labels = slices.Clone(s.Labels)
	s.Snapshots = slices.Clone(s.Snapshots)
	for i, ref := range s.Snapshots {
		ref.Schemas = maps.Clone(ref.Schemas)
		ref.SourceVersions = maps.Clone(ref.SourceVersions)
		ref.Revisions = maps.Clone(ref.Revisions)
		s.Snapshots[i] = ref
	}
	return s
}
func hashJSON(v any) (string, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
func BuildManifest(spec ManifestSpec) (*Manifest, error) {
	if spec.Currency == "" || spec.CodeRevision == "" || spec.FactorPlanHash == "" || spec.UniverseVersion == "" || spec.VisibilityPolicy == "" || spec.ExecutionMode == "" || spec.LatencyAssumption == "" || (spec.Portfolio.K <= 0 && spec.Portfolio.Builder == "") || spec.Costs.FundingPolicy == "" {
		return nil, errors.New("research: incomplete reproducibility manifest")
	}
	if len(spec.Labels) == 0 && (spec.ExecutionMode == "research" || spec.Combo.Method == HistoryIC) {
		return nil, errors.New("research: research and history-IC manifests require labels")
	}
	if spec.Portfolio.LongNotional < 0 || spec.Portfolio.ShortNotional < 0 || (spec.Portfolio.LongNotional+spec.Portfolio.ShortNotional <= 0 && spec.Portfolio.Builder == "") || math.IsNaN(spec.Portfolio.LongNotional+spec.Portfolio.ShortNotional) || math.IsInf(spec.Portfolio.LongNotional+spec.Portfolio.ShortNotional, 0) {
		return nil, errors.New("research: invalid portfolio notional fractions")
	}
	if spec.Costs.FeeRate < 0 || spec.Costs.SlippageRate < 0 {
		return nil, errors.New("research: negative cost rate")
	}
	for _, label := range spec.Labels {
		if err := validateLabelSpec(label); err != nil {
			return nil, err
		}
	}
	spec = CloneManifestSpec(spec)
	sort.Strings(spec.Combo.Columns)
	spec.Combo.Columns = slices.Compact(spec.Combo.Columns)
	sort.Slice(spec.Labels, func(i, j int) bool { return spec.Labels[i].Name < spec.Labels[j].Name })
	sort.Slice(spec.Snapshots, func(i, j int) bool { return spec.Snapshots[i].ID < spec.Snapshots[j].ID })
	for i, ref := range spec.Snapshots {
		if ref.ID == "" || ref.ContentDigest == "" || len(ref.Schemas) == 0 || len(ref.SourceVersions) == 0 || (i > 0 && spec.Snapshots[i-1].ID == ref.ID) {
			return nil, errors.New("research: invalid snapshot lineage")
		}
	}
	for i, label := range spec.Labels {
		if i > 0 && spec.Labels[i-1].Name == label.Name {
			return nil, errors.New("research: duplicate manifest label")
		}
	}
	// The definition is mode-independent; execution assumptions, snapshot
	// lineage and the realized universe belong only to the run manifest.
	strategy := struct {
		CodeRevision, FactorPlanHash, Currency string
		Combo                                  ComboSpec
		Portfolio                              PortfolioDefinition
		Labels                                 []LabelSpec
		Parameters                             map[string]float64
		Costs                                  CostSpec
	}{spec.CodeRevision, spec.FactorPlanHash, spec.Currency, spec.Combo, spec.Portfolio, spec.Labels, spec.Parameters, spec.Costs}
	sh, err := hashJSON(strategy)
	if err != nil {
		return nil, err
	}
	id, err := hashJSON(spec)
	if err != nil {
		return nil, err
	}
	m := &Manifest{spec: spec, id: id, strategyHash: sh}
	if spec.StaticUniverse {
		m.diagnostics = append(m.diagnostics, factor.Diagnostic{Code: "static-universe", Detail: "static membership may contain survivorship bias"})
	}
	for _, label := range spec.Labels {
		if label.Overlapping {
			m.diagnostics = append(m.diagnostics, factor.Diagnostic{Code: "overlapping-labels", Detail: label.Name + ": ICIR is descriptive; no independent-sample significance claim"})
		}
	}
	return m, nil
}
func (m *Manifest) ID() string                       { return m.id }
func (m *Manifest) StrategyHash() string             { return m.strategyHash }
func (m *Manifest) Spec() ManifestSpec               { return CloneManifestSpec(m.spec) }
func (m *Manifest) Diagnostics() []factor.Diagnostic { return slices.Clone(m.diagnostics) }

// WritePanel emits one deterministic JSON row per selected scalar. Invalid
// numbers encode as null plus validity, retaining missing/NULL distinctions.
func WritePanel(w io.Writer, frame factor.Frame, columns []string, sids []int32) error {
	columns = slices.Clone(columns)
	sort.Strings(columns)
	columns = slices.Compact(columns)
	encoder := json.NewEncoder(w)
	for _, sid := range uniqueSIDs(sids) {
		for _, name := range columns {
			n, exists := frame.Values[name][sid]
			if !exists {
				n.Validity = factor.Missing
			}
			var value *float64
			if valid(n) {
				v := n.Value
				value = &v
			}
			row := struct {
				SnapshotID   string
				DecisionTime int64
				SID          int32
				Column       string
				Value        *float64
				Validity     factor.Validity
			}{frame.SnapshotID, frame.DecisionTime, sid, name, value, n.Validity}
			if err := encoder.Encode(row); err != nil {
				return err
			}
		}
	}
	return nil
}
