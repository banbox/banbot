package runner

import (
	"encoding/json"
	"maps"
	"slices"

	"github.com/banbox/banbot/factor/expr"
	"github.com/banbox/banbot/factor/research"
)

// CloneConfig freezes owned configuration containers without normalization.
// All lists retain their order, duplicates and nil/empty distinction.
// Plan, ComputationGroup, PortfolioBuilder, HistoricalInput, ObserveBatch and
// the internal replay timeline are borrowed handles, retained by identity.
// Decimal fields are immutable values. No borrowed resource is read or closed.
// JSON validation preserves the former runtime boundary's rejection of NaN/Inf
// without decoding values or losing fields excluded from serialization.
func CloneConfig(c Config) (Config, error) {
	if _, err := json.Marshal(c); err != nil {
		return Config{}, err
	}
	c.Chunks = slices.Clone(c.Chunks)
	// CloneSnapshotSpec canonicalizes universe lists, which belongs to snapshot
	// evaluation rather than this configuration ownership boundary.
	c.Snapshot.Universe.Investable = slices.Clone(c.Snapshot.Universe.Investable)
	c.Snapshot.Universe.Reference = slices.Clone(c.Snapshot.Universe.Reference)
	c.Snapshot.Universe.Tradable = slices.Clone(c.Snapshot.Universe.Tradable)
	c.Snapshot.Universe.Evaluation = slices.Clone(c.Snapshot.Universe.Evaluation)
	c.Snapshot.Universe.Tracked = slices.Clone(c.Snapshot.Universe.Tracked)
	c.Snapshot.SIDMap = maps.Clone(c.Snapshot.SIDMap)
	c.Snapshot.Schemas = maps.Clone(c.Snapshot.Schemas)
	c.Snapshot.SourceVersions = maps.Clone(c.Snapshot.SourceVersions)
	if c.Expressions != nil {
		spec := expr.CloneSpec(*c.Expressions)
		c.Expressions = &spec
	}
	c.Combo = research.CloneComboSpec(c.Combo)
	c.Manifest = research.CloneManifestSpec(c.Manifest)
	c.Execution.Instruments = maps.Clone(c.Execution.Instruments)
	return c, nil
}
