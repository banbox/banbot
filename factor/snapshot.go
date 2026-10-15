package factor

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
)

type Universe struct {
	Version    string
	Investable []int32
	Reference  []int32
	Tradable   []int32
	Evaluation []int32
	Tracked    []int32
	Static     bool
}

func cloneUniverse(u Universe) Universe {
	u.Investable = sortedSIDs(u.Investable)
	u.Reference = sortedSIDs(u.Reference)
	u.Tradable = sortedSIDs(u.Tradable)
	u.Evaluation = sortedSIDs(u.Evaluation)
	u.Tracked = sortedSIDs(u.Tracked)
	return u
}

func sortedSIDs(sids []int32) []int32 {
	copy := slices.Clone(sids)
	slices.Sort(copy)
	return slices.Compact(copy)
}

type SnapshotSpec struct {
	// TrackedQuotesOnly separates account position monitoring from factor data
	// readiness. The default retains standalone Session tracked continuity.
	TrackedQuotesOnly bool
	// GridTime identifies the logical decision observation. Zero defaults to
	// DecisionTime. DecisionTime separately fixes the actual visibility cutoff.
	GridTime          int64
	DecisionTime      int64
	ReplayTime        int64
	Universe          Universe
	SIDMap            map[int32]string
	Schemas           map[string]string
	SourceVersions    map[string]string
	AdjustmentVersion string
	VisibilityPolicy  string
}

func activeFactorSIDs(spec SnapshotSpec) []int32 {
	ids := append(slices.Clone(spec.Universe.Reference), spec.Universe.Investable...)
	if !spec.TrackedQuotesOnly {
		ids = append(ids, spec.Universe.Tracked...)
	}
	return sortedSIDs(ids)
}

func cloneSpec(s SnapshotSpec) SnapshotSpec {
	s.Universe = cloneUniverse(s.Universe)
	s.SIDMap = maps.Clone(s.SIDMap)
	s.Schemas = maps.Clone(s.Schemas)
	s.SourceVersions = maps.Clone(s.SourceVersions)
	return s
}
func CloneSnapshotSpec(s SnapshotSpec) SnapshotSpec { return cloneSpec(s) }
func CloneUniverse(u Universe) Universe             { return cloneUniverse(u) }

type Requirement struct {
	SID        int32
	Source     string
	TimeFrame  string
	EventTime  int64
	AsOfLatest bool
	MaxAge     int64
}
type StreamKey struct {
	SID       int32
	Source    string
	TimeFrame string
}
type SnapshotStatus struct {
	Expected []StreamKey
	Arrived  []StreamKey
	Invalid  []StreamKey
	Sources  []string
	Ready    bool
}

// Snapshot keeps all mutable contents private. Accessors return independent
// copies; late observations require a new snapshot/research run.
type Snapshot struct {
	id     string
	spec   SnapshotSpec
	rows   map[StreamKey]VersionRecord
	status SnapshotStatus
}

func Freeze(spec SnapshotSpec, records []VersionRecord, requirements []Requirement) (*Snapshot, error) {
	if spec.Universe.Version == "" || spec.VisibilityPolicy == "" || spec.DecisionTime <= 0 {
		return nil, errors.New("factor: snapshot requires universe version, visibility policy and decision time")
	}
	if spec.GridTime == 0 {
		spec.GridTime = spec.DecisionTime
	}
	if spec.GridTime <= 0 || spec.GridTime > spec.DecisionTime {
		return nil, errors.New("factor: logical grid must precede visibility cutoff")
	}
	spec = cloneSpec(spec)
	requirements = slices.Clone(requirements)
	sort.Slice(requirements, func(i, j int) bool {
		a, b := requirements[i], requirements[j]
		if a.SID != b.SID {
			return a.SID < b.SID
		}
		if a.Source != b.Source {
			return a.Source < b.Source
		}
		return a.TimeFrame < b.TimeFrame
	})
	allSIDs := append(append(append(append(slices.Clone(spec.Universe.Reference), spec.Universe.Investable...), spec.Universe.Tradable...), spec.Universe.Evaluation...), spec.Universe.Tracked...)
	for _, sid := range allSIDs {
		if sid <= 0 || spec.SIDMap[sid] == "" {
			return nil, fmt.Errorf("factor: SID %d missing stable symbol mapping", sid)
		}
	}
	snapshot := &Snapshot{spec: spec, rows: make(map[StreamKey]VersionRecord)}
	for _, row := range records {
		if row.Series.TimeFrame == "" || row.Series.Sid <= 0 || row.Revision == 0 {
			return nil, errors.New("factor: invalid snapshot stream identity; non-periodic sources must declare timeframe=event")
		}
		if row.EventTime > spec.GridTime || row.AvailableAt > spec.DecisionTime || (spec.ReplayTime != 0 && row.IngestedAt > spec.ReplayTime) {
			continue
		}
		if row.SourceVersion != spec.SourceVersions[row.Series.Source] || spec.Schemas[row.Series.Source] == "" {
			return nil, fmt.Errorf("factor: undeclared source/schema version %s", row.Series.Source)
		}
		key := StreamKey{row.Series.Sid, row.Series.Source, row.Series.TimeFrame}
		previous, exists := snapshot.rows[key]
		if exists && (previous.EventTime > row.EventTime || (previous.EventTime == row.EventTime && previous.Revision > row.Revision)) {
			continue
		}
		copy, err := cloneRecord(row)
		if err != nil {
			return nil, err
		}
		if exists && previous.EventTime == row.EventTime && previous.Revision == row.Revision {
			a, err := contentHash(previous)
			if err != nil {
				return nil, err
			}
			b, err := contentHash(copy)
			if err != nil {
				return nil, err
			}
			if a != b {
				return nil, errors.New("factor: conflicting snapshot revision")
			}
		}
		snapshot.rows[key] = copy
	}
	expected := make(map[StreamKey]bool)
	sourceSet := make(map[string]bool)
	for _, required := range requirements {
		if required.SID <= 0 || required.Source == "" || required.TimeFrame == "" || required.EventTime > spec.GridTime || required.MaxAge < 0 {
			return nil, errors.New("factor: invalid barrier requirement")
		}
		key := StreamKey{required.SID, required.Source, required.TimeFrame}
		if expected[key] {
			return nil, errors.New("factor: duplicate snapshot requirement")
		}
		expected[key] = true
		sourceSet[required.Source] = true
		snapshot.status.Expected = append(snapshot.status.Expected, key)
		row, arrived := snapshot.rows[key]
		if !arrived {
			continue
		}
		snapshot.status.Arrived = append(snapshot.status.Arrived, key)
		invalid := false
		if required.AsOfLatest {
			invalid = row.EventTime > required.EventTime || (required.MaxAge > 0 && required.EventTime-row.EventTime > required.MaxAge)
		} else {
			invalid = row.EventTime != required.EventTime || !row.Series.Closed
		}
		if invalid {
			snapshot.status.Invalid = append(snapshot.status.Invalid, key)
		}
	}
	for source := range sourceSet {
		snapshot.status.Sources = append(snapshot.status.Sources, source)
	}
	sort.Strings(snapshot.status.Sources)
	sortStreams(snapshot.status.Expected)
	sortStreams(snapshot.status.Arrived)
	sortStreams(snapshot.status.Invalid)
	snapshot.status.Ready = len(requirements) > 0 && len(snapshot.status.Expected) == len(snapshot.status.Arrived) && len(snapshot.status.Invalid) == 0
	var err error
	snapshot.id, err = contentHash(struct {
		Spec         SnapshotSpec
		Rows         map[StreamKey]VersionRecord
		Requirements []Requirement
	}{spec, snapshot.rows, slices.Clone(requirements)})
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func sortStreams(keys []StreamKey) {
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].SID != keys[j].SID {
			return keys[i].SID < keys[j].SID
		}
		if keys[i].Source != keys[j].Source {
			return keys[i].Source < keys[j].Source
		}
		return keys[i].TimeFrame < keys[j].TimeFrame
	})
}
func (s *Snapshot) ID() string         { return s.id }
func (s *Snapshot) Spec() SnapshotSpec { return cloneSpec(s.spec) }
func (s *Snapshot) Status() SnapshotStatus {
	v := s.status
	v.Expected = slices.Clone(v.Expected)
	v.Arrived = slices.Clone(v.Arrived)
	v.Invalid = slices.Clone(v.Invalid)
	v.Sources = slices.Clone(v.Sources)
	return v
}
func (s *Snapshot) Row(sid int32, source, timeframe string) (VersionRecord, bool) {
	row, ok := s.rows[StreamKey{sid, source, timeframe}]
	if !ok {
		return VersionRecord{}, false
	}
	copy, err := cloneRecord(row)
	if err != nil {
		panic(err)
	}
	return copy, true
}
func (s *Snapshot) Numeric(sid int32, source, timeframe, field string) Numeric {
	row, ok := s.rows[StreamKey{sid, source, timeframe}]
	if !ok {
		return Number(nil, field)
	}
	return Number(row.Series.Values, field)
}
