package factor

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"sync"

	ta "github.com/banbox/banta"
)

var ErrSnapshotIncomplete = errors.New("factor: snapshot barrier incomplete")

type Frame struct {
	GridTime     int64
	SnapshotID   string
	PlanHash     string
	DecisionTime int64
	Values       map[string]map[int32]Numeric
}

func cloneFrame(frame Frame) Frame {
	copy := frame
	copy.Values = make(map[string]map[int32]Numeric, len(frame.Values))
	for name, values := range frame.Values {
		copy.Values[name] = maps.Clone(values)
	}
	return copy
}

// CloneFrame isolates mutable consumers of a frozen computation result.
func CloneFrame(frame Frame) Frame { return cloneFrame(frame) }

type assetState struct {
	env       *ta.BarEnv
	nodes     []*ta.Series
	technical map[string][]*ta.Series
}

// Session is a computation owner, independent of trading accounts. One lock
// serializes updates; arbitrary consumers share copies of its frozen Frame.
// Revisions at already processed times require a fresh explicit replay session.
type Session struct {
	mu              sync.Mutex
	plan            *Plan
	contextHash     string
	universeHash    string
	universeVersion string
	revision        uint64
	assets          map[int32]*assetState
	latest          Frame
	lastGrid        int64
	warmSnapshot    string
	updates         map[string]uint64
}

func NewSession(plan *Plan) (*Session, error) {
	if plan == nil {
		return nil, errors.New("factor: nil plan")
	}
	return &Session{plan: plan, assets: make(map[int32]*assetState), updates: make(map[string]uint64)}, nil
}

func (s *Session) Updates() map[string]uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return maps.Clone(s.updates)
}
func (s *Session) RetainedValues() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for _, asset := range s.assets {
		for _, series := range asset.env.Items {
			total += len(series.Data)
		}
	}
	return total
}

func (s *Session) Evaluate(snapshot *Snapshot) (Frame, error) {
	return s.evaluate(snapshot, false)
}

// Warmup advances indicator state at historical logical grids using a frozen
// current visibility cutoff. It never publishes a Frame or resumes after live
// publication. Several warmup grids may share the same actual receipt time.
func (s *Session) Warmup(snapshot *Snapshot) error {
	_, err := s.evaluate(snapshot, true)
	return err
}

func (s *Session) evaluate(snapshot *Snapshot, warmup bool) (Frame, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if snapshot == nil || !snapshot.status.Ready {
		return Frame{}, ErrSnapshotIncomplete
	}
	if !warmup && snapshot.id == s.latest.SnapshotID {
		return cloneFrame(s.latest), nil
	}
	if warmup && s.latest.SnapshotID != "" {
		return Frame{}, errors.New("factor: warmup cannot follow live publication")
	}
	if warmup && snapshot.id == s.warmSnapshot {
		return Frame{}, nil
	}
	if (!warmup && s.latest.DecisionTime >= snapshot.spec.DecisionTime) || s.lastGrid >= snapshot.spec.GridTime {
		return Frame{}, errors.New("factor: old/conflicting frozen snapshot; replay in a new session")
	}
	contextHash, err := s.plan.snapshotIdentity(snapshot)
	if err != nil {
		return Frame{}, err
	}
	if s.contextHash != "" && s.contextHash != contextHash {
		return Frame{}, errors.New("factor: computation source/schema identity changed; new session required")
	}
	universeHash, err := contentHash(snapshot.spec.Universe)
	if err != nil {
		return Frame{}, err
	}
	if s.universeHash != "" && s.universeHash != universeHash && s.universeVersion == snapshot.spec.Universe.Version {
		return Frame{}, errors.New("factor: universe changed without a new version")
	}
	allSIDs := activeFactorSIDs(snapshot.spec)
	for _, sid := range allSIDs {
		if asset := s.assets[sid]; asset != nil && asset.env.Symbol != snapshot.spec.SIDMap[sid] {
			return Frame{}, fmt.Errorf("factor: SID %d identity changed", sid)
		}
	}
	for sid := range s.assets {
		if !slices.Contains(allSIDs, sid) {
			delete(s.assets, sid)
		}
	}
	for _, sid := range allSIDs {
		if s.assets[sid] == nil {
			env := &ta.BarEnv{Symbol: snapshot.spec.SIDMap[sid], TimeFrame: s.plan.timeframe, Items: make(map[int]*ta.Series), MaxCache: s.plan.retention}
			asset := &assetState{env: env, nodes: make([]*ta.Series, len(s.plan.nodes))}
			for i := range asset.nodes {
				asset.nodes[i] = env.NewSeries(nil)
			}
			s.assets[sid] = asset
		}
		s.assets[sid].env.TimeStart = snapshot.spec.GridTime - 1
		s.assets[sid].env.TimeStop = snapshot.spec.GridTime
		s.assets[sid].env.BarNum++
	}
	columns := make([]map[int32]Numeric, len(s.plan.nodes))
	for index, node := range s.plan.nodes {
		if node.spec.Kind == TS {
			columns[index] = make(map[int32]Numeric, len(allSIDs))
			for _, sid := range allSIDs {
				asset := s.assets[sid]
				value := s.evaluateTS(node, asset, sid, snapshot, columns)
				columns[index][sid] = value
				asset.nodes[index].Append(value.Value)
				s.updates[node.id]++
			}
		} else {
			columns[index] = crossSection(node, snapshot, columns)
			for _, sid := range allSIDs {
				value, exists := columns[index][sid]
				if !exists {
					value = Numeric{math.NaN(), Missing}
					columns[index][sid] = value
				}
				s.assets[sid].nodes[index].Append(value.Value)
			}
			s.updates[node.id]++
		}
	}
	// BarEnv.TrimOverflow only cuts selected OHLC roots and Series.Cut does
	// not cut the primary column of multi-column series. Trim every registered
	// array explicitly, preserving banta More recursive/window state.
	for _, asset := range s.assets {
		for _, series := range asset.env.Items {
			if len(series.Data) > 2*s.plan.retention {
				series.Data = slices.Clone(series.Data[len(series.Data)-s.plan.retention:])
			}
		}
	}
	frame := Frame{SnapshotID: snapshot.id, PlanHash: s.plan.hash, GridTime: snapshot.spec.GridTime, DecisionTime: snapshot.spec.DecisionTime, Values: make(map[string]map[int32]Numeric)}
	for name, index := range s.plan.outputs {
		frame.Values[name] = maps.Clone(columns[index])
	}
	s.contextHash = contextHash
	s.universeHash = universeHash
	s.universeVersion = snapshot.spec.Universe.Version
	s.lastGrid = snapshot.spec.GridTime
	if !warmup {
		s.latest = frame
	} else {
		s.warmSnapshot = snapshot.id
	}
	s.revision++
	return cloneFrame(frame), nil
}

// fork copies only bounded computation state, not the raw version archive.
// banta v0.4.1 CopyTo shares Data and recreates ID/Time; restore those fields
// after cloning so abandoned work cannot mutate live arrays or cache identity.
func (s *Session) fork() (*Session, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	copy := &Session{plan: s.plan, contextHash: s.contextHash, universeHash: s.universeHash, universeVersion: s.universeVersion, revision: s.revision, assets: make(map[int32]*assetState, len(s.assets)), latest: cloneFrame(s.latest), lastGrid: s.lastGrid, warmSnapshot: s.warmSnapshot, updates: maps.Clone(s.updates)}
	for sid, asset := range s.assets {
		for _, series := range asset.env.Items {
			if series.More != nil && series.DupMore == nil {
				if _, immutable := series.More.(float64); !immutable {
					return nil, 0, errors.New("factor: indicator state has no safe clone contract")
				}
			}
		}
		hasExtraState := false
		asset.env.Data.Range(func(_, _ any) bool { hasExtraState = true; return false })
		if hasExtraState {
			return nil, 0, errors.New("factor: environment extra state has no safe clone contract")
		}
		env := asset.env.Clone()
		for id, series := range env.Items {
			series.ID = id
			series.Time = asset.env.Items[id].Time
			series.Data = slices.Clone(series.Data)
		}
		nodes := make([]*ta.Series, len(asset.nodes))
		for i, series := range asset.nodes {
			nodes[i] = env.Items[series.ID]
		}
		technical := make(map[string][]*ta.Series, len(asset.technical))
		for id, series := range asset.technical {
			cloned := make([]*ta.Series, len(series))
			for i, input := range series {
				cloned[i] = env.Items[input.ID]
			}
			technical[id] = cloned
		}
		copy.assets[sid] = &assetState{env: env, nodes: nodes, technical: technical}
	}
	return copy, s.revision, nil
}

func (s *Session) commit(candidate *Session, baseRevision uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.revision != baseRevision {
		return errors.New("factor: computation owner advanced while round was running")
	}
	s.contextHash = candidate.contextHash
	s.universeHash = candidate.universeHash
	s.universeVersion = candidate.universeVersion
	s.revision = candidate.revision
	s.assets = candidate.assets
	s.latest = candidate.latest
	s.lastGrid = candidate.lastGrid
	s.warmSnapshot = candidate.warmSnapshot
	s.updates = candidate.updates
	return nil
}

func (p *Plan) snapshotIdentity(snapshot *Snapshot) (string, error) {
	if snapshot == nil || !snapshot.status.Ready {
		return "", ErrSnapshotIncomplete
	}
	contextHash, err := contentHash(struct {
		Schemas, SourceVersions map[string]string
		Adjustment, Visibility  string
		TrackedQuotesOnly       bool
	}{snapshot.spec.Schemas, snapshot.spec.SourceVersions, snapshot.spec.AdjustmentVersion, snapshot.spec.VisibilityPolicy, snapshot.spec.TrackedQuotesOnly})
	if err != nil {
		return "", err
	}
	expected := make(map[StreamKey]bool)
	for _, key := range snapshot.status.Expected {
		expected[key] = true
	}
	var active []int32
	for _, node := range p.nodes {
		if node.spec.Source == "" {
			continue
		}
		if active == nil {
			active = activeFactorSIDs(snapshot.spec)
		}
		for _, sid := range active {
			key := StreamKey{sid, node.spec.Source, node.spec.SourceTimeFrame}
			if !expected[key] {
				return "", fmt.Errorf("factor: active stream %d/%s/%s absent from declared barrier", sid, node.spec.Source, node.spec.SourceTimeFrame)
			}
			if node.spec.Operator == "field" && node.spec.AvailabilityPolicy == "asof-latest" {
				row := snapshot.rows[key]
				if row.EventTime > snapshot.spec.GridTime || snapshot.spec.GridTime-row.EventTime > node.spec.MaxAge {
					return "", fmt.Errorf("factor: asof stream %d/%s exceeds declared max age", sid, node.spec.Source)
				}
			} else if node.spec.Operator == "field" {
				row := snapshot.rows[key]
				if !row.Series.Closed || row.EventTime != snapshot.spec.GridTime {
					return "", fmt.Errorf("factor: TS stream %d/%s lacks current closed event", sid, node.spec.Source)
				}
			}
		}
	}
	return contextHash, nil
}

func (s *Session) evaluateTS(node compiledNode, asset *assetState, sid int32, snapshot *Snapshot, columns []map[int32]Numeric) Numeric {
	if node.spec.Operator == "field" {
		return snapshot.Numeric(sid, node.spec.Source, node.spec.SourceTimeFrame, node.spec.Field)
	}
	inputs := make([]Numeric, len(node.inputs))
	for i, index := range node.inputs {
		inputs[i] = columns[index][sid]
	}
	if node.evaluate != nil {
		return normalized(node.evaluate(inputs))
	}
	if value, ok := evaluatePointwise(node.spec, inputs); ok {
		return value
	}
	if node.spec.Operator == "linear" {
		value := 0.0
		for i, input := range inputs {
			if input.Validity != Valid {
				return input
			}
			value += input.Value * node.spec.Parameters[fmt.Sprintf("weight%d", i)]
		}
		return numeric(value)
	}
	if _, ok := technicalOperatorArity(node.spec.Operator); ok {
		return asset.evaluateTechnical(node, inputs)
	}
	if len(inputs) == 0 {
		return Numeric{math.NaN(), NotNumeric}
	}
	input := inputs[0]
	series := asset.nodes[node.inputs[0]]
	period := int(node.spec.Parameters["period"])
	var value float64
	switch node.spec.Operator {
	case "lag":
		value = series.Get(period)
	case "return":
		value = ta.ROCR(series, period).Get(0) - 1
	case "ema":
		value = ta.EMA(series, period).Get(0)
	case "stddev":
		result, _ := ta.StdDevBy(series, period, int(node.spec.Parameters["ddof"]))
		value = result.Get(0)
	default:
		return Numeric{math.NaN(), NotNumeric}
	}
	result := numeric(value)
	if result.Validity != Valid {
		if input.Validity != Valid && node.spec.Operator != "lag" {
			return Numeric{math.NaN(), input.Validity}
		}
		result.Validity = Warmup
	}
	return result
}
