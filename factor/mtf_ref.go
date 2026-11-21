package factor

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"

	ta "github.com/banbox/banta"
)

const (
	opAsOfNode = "asof-node"
	opRefField = "ref-field"
)

// ParseTimeFrameMS parses a periodic bar timeframe (such as "1m", "15m", "1h",
// "4h", "1d", "1w") into milliseconds without importing exchange packages.
func ParseTimeFrameMS(tf string) (int64, bool) {
	tf = strings.TrimSpace(tf)
	if len(tf) < 2 || tf == "event" {
		return 0, false
	}
	amount, err := strconv.ParseInt(tf[:len(tf)-1], 10, 64)
	if err != nil || amount <= 0 {
		return 0, false
	}
	var unitMS int64
	switch tf[len(tf)-1] {
	case 's', 'S':
		unitMS = 1000
	case 'm':
		unitMS = 60 * 1000
	case 'h', 'H':
		unitMS = 3600 * 1000
	case 'd', 'D':
		unitMS = 86400 * 1000
	case 'w', 'W':
		unitMS = 7 * 86400 * 1000
	case 'M':
		unitMS = 30 * 86400 * 1000
	case 'q', 'Q':
		unitMS = 91 * 86400 * 1000
	case 'y', 'Y':
		unitMS = 365 * 86400 * 1000
	default:
		return 0, false
	}
	if amount > math.MaxInt64/unitMS {
		return 0, false
	}
	return amount * unitMS, true
}

// AsOfNode evaluates input on its declared source bar timeframe (advancing
// stateful TS windows only when a new completed source bar closes with
// barEnd <= GridTime) and causally projects the resulting scalar onto
// decisionTimeFrame via AsOf alignment. When maxAge > 0 (milliseconds),
// observations older than GridTime - EventTime > maxAge are invalid (Missing);
// maxAge == 0 imposes no staleness upper bound.
func AsOfNode(input *Node, decisionTimeFrame string, maxAge int64) *Node {
	n := node(opAsOfNode, TS, input)
	n.Spec.TimeFrame = strings.TrimSpace(decisionTimeFrame)
	if input != nil {
		n.Spec.SourceTimeFrame = strings.TrimSpace(input.Spec.TimeFrame)
	}
	n.Spec.AvailabilityPolicy = "asof-latest"
	n.Spec.SamplingPolicy = "decision-grid"
	n.Spec.MaxAge = maxAge
	return n
}

// SubTFContext builds nodes bound to a single source bar timeframe inside SubTF.
type SubTFContext struct {
	timeframe string
}

func (c SubTFContext) TimeFrame() string { return c.timeframe }
func (c SubTFContext) Field(source, field string) *Node {
	return Field(source, field, c.timeframe)
}
func (c SubTFContext) Open() *Node      { return Field("kline", "open", c.timeframe) }
func (c SubTFContext) High() *Node      { return Field("kline", "high", c.timeframe) }
func (c SubTFContext) Low() *Node       { return Field("kline", "low", c.timeframe) }
func (c SubTFContext) Close() *Node     { return Field("kline", "close", c.timeframe) }
func (c SubTFContext) Volume() *Node    { return Field("kline", "volume", c.timeframe) }
func (c SubTFContext) BuyVolume() *Node { return Field("kline", "buy_volume", c.timeframe) }
func (c SubTFContext) RefAssetField(symbol, source, field string) *Node {
	return RefAssetField(symbol, source, field, c.timeframe)
}
func (c SubTFContext) Constant(value float64) *Node {
	return Constant(value, c.timeframe)
}

// SubTF builds a source-timeframe subgraph via build and aligns its scalar
// output onto decisionTimeFrame via AsOfNode. Optional maxAge[0] specifies the
// maximum staleness in milliseconds (0 = unlimited).
func SubTF(sourceTimeFrame, decisionTimeFrame string, build func(SubTFContext) *Node, maxAge ...int64) *Node {
	var age int64
	if len(maxAge) > 0 {
		age = maxAge[0]
	}
	var inner *Node
	if build != nil {
		inner = build(SubTFContext{timeframe: strings.TrimSpace(sourceTimeFrame)})
	}
	n := AsOfNode(inner, decisionTimeFrame, age)
	if n.Spec.SourceTimeFrame == "" {
		n.Spec.SourceTimeFrame = strings.TrimSpace(sourceTimeFrame)
	}
	return n
}

// RefAssetField reads a specific named benchmark/reference symbol's field on
// timeframe and broadcasts its scalar value to every active asset row at t.
func RefAssetField(symbol, source, field, timeframe string) *Node {
	n := node(opRefField, TS)
	n.Spec.RefSymbol = strings.TrimSpace(symbol)
	n.Spec.Source = source
	n.Spec.Field = field
	n.Spec.TimeFrame = timeframe
	return n
}

// AsOfRefAssetField samples a slower/event field of a specific named reference
// symbol onto decisionTimeFrame with an explicit maxAge (ms > 0) contract.
func AsOfRefAssetField(symbol, source, field, sourceTimeFrame, decisionTimeFrame string, maxAge int64) *Node {
	n := RefAssetField(symbol, source, field, decisionTimeFrame)
	n.Spec.SourceTimeFrame = sourceTimeFrame
	n.Spec.AvailabilityPolicy = "asof-latest"
	n.Spec.SamplingPolicy = "decision-grid"
	n.Spec.MaxAge = maxAge
	return n
}

// ScopedInputSpec refines InputSpec with per-asset versus reference-symbol scope
// so runners and storage loaders subscribe reference-only symbols even when
// absent from Universe.Investable / Universe.Reference.
type ScopedInputSpec struct {
	InputSpec
	AllAssets  bool
	RefSymbols []string
}

// ReferenceSymbols returns all distinct reference symbol names declared via
// RefAssetField / AsOfRefAssetField in deterministic sorted order.
func (p *Plan) ReferenceSymbols() []string {
	if p == nil {
		return nil
	}
	seen := make(map[string]bool)
	for _, n := range p.nodes {
		if n.spec.RefSymbol != "" {
			seen[n.spec.RefSymbol] = true
		}
	}
	if len(seen) == 0 {
		return nil
	}
	out := slices.Collect(maps.Keys(seen))
	sort.Strings(out)
	return out
}

// ScopedInputs returns input stream subscriptions annotated with whether all
// active universe assets and/or specific reference symbols require each stream.
func (p *Plan) ScopedInputs() []ScopedInputSpec {
	if p == nil {
		return nil
	}
	_, scoped := p.buildInputs()
	return scoped
}

func validateMTFOrRefNode(spec NodeSpec, count int) (bool, error) {
	switch spec.Operator {
	case opRefField:
		if count != 0 || spec.RefSymbol == "" || spec.Source == "" || spec.Field == "" {
			return true, errors.New("factor: ref-field requires symbol, source, field and no dependencies")
		}
		if len(spec.Parameters) != 0 {
			return true, errors.New("factor: ref-field does not accept scalar parameters")
		}
		return true, nil
	case opAsOfNode:
		if count != 1 {
			return true, errors.New("factor: asof-node requires one dependency")
		}
		if len(spec.Parameters) != 0 {
			return true, errors.New("factor: asof-node does not accept scalar parameters")
		}
		if spec.AvailabilityPolicy != "asof-latest" || spec.SamplingPolicy != "decision-grid" {
			return true, errors.New("factor: asof-node requires decision-grid asof-latest policy")
		}
		if spec.MaxAge < 0 {
			return true, errors.New("factor: asof-node max age must be nonnegative")
		}
		srcMS, okSrc := ParseTimeFrameMS(spec.SourceTimeFrame)
		dstMS, okDst := ParseTimeFrameMS(spec.TimeFrame)
		if !okSrc || !okDst {
			return true, fmt.Errorf("factor: asof-node requires valid bar timeframes (got %q -> %q)", spec.SourceTimeFrame, spec.TimeFrame)
		}
		if srcMS < dstMS {
			return true, fmt.Errorf("factor: sub-timeframe %q cannot be finer than target timeframe %q", spec.SourceTimeFrame, spec.TimeFrame)
		}
		return true, nil
	default:
		return false, nil
	}
}

func asofNodeWarmup(spec NodeSpec, depWarmup int) (int, error) {
	srcMS, okSrc := ParseTimeFrameMS(spec.SourceTimeFrame)
	dstMS, okDst := ParseTimeFrameMS(spec.TimeFrame)
	if !okSrc || !okDst || srcMS < dstMS {
		return 0, fmt.Errorf("factor: invalid asof-node timeframe conversion %q -> %q", spec.SourceTimeFrame, spec.TimeFrame)
	}
	if depWarmup <= 0 || srcMS == dstMS {
		return depWarmup, nil
	}
	ratio := (srcMS + dstMS - 1) / dstMS
	if int64(depWarmup) > int64(math.MaxInt32)/ratio {
		return 0, errors.New("factor: sub-timeframe warmup overflows")
	}
	return depWarmup * int(ratio), nil
}

type subStreamRef struct {
	refSymbol string
	source    string
	timeFrame string
}

type subTFScope struct {
	timeFrame string
	tfMS      int64
	streams   []subStreamRef
	retention int
}

type subTFPlanMeta struct {
	scopes      []subTFScope
	nodeScope   []int
	asofStreams map[int][]subStreamRef
}

func compileSubTFMeta(p *Plan) *subTFPlanMeta {
	hasSubTF := false
	for _, n := range p.nodes {
		if n.spec.TimeFrame != p.timeframe || n.spec.Operator == opAsOfNode {
			hasSubTF = true
			break
		}
	}
	if !hasSubTF {
		return nil
	}
	meta := &subTFPlanMeta{
		nodeScope:   make([]int, len(p.nodes)),
		asofStreams: make(map[int][]subStreamRef),
	}
	for i := range meta.nodeScope {
		meta.nodeScope[i] = -1
	}

	// Collect direct leaf streams within the same TimeFrame for each node.
	nodeStreams := make([][]subStreamRef, len(p.nodes))
	hasCSInTF := make([]bool, len(p.nodes))
	for i, n := range p.nodes {
		tf := n.spec.TimeFrame
		var streams []subStreamRef
		if n.spec.Source != "" && n.spec.Operator != opAsOfNode {
			srcTF := n.spec.SourceTimeFrame
			if srcTF == "" {
				srcTF = tf
			}
			if srcTF == tf {
				streams = append(streams, subStreamRef{
					refSymbol: n.spec.RefSymbol,
					source:    n.spec.Source,
					timeFrame: srcTF,
				})
			}
		}
		cs := n.spec.Kind != TS
		for _, dep := range n.inputs {
			if p.nodes[dep].spec.TimeFrame == tf {
				streams = append(streams, nodeStreams[dep]...)
				cs = cs || hasCSInTF[dep]
			}
		}
		nodeStreams[i] = compactSubStreams(streams)
		hasCSInTF[i] = cs
	}

	// If CS/GROUP exists in a sub-timeframe, unify all streams of that sub-timeframe
	// so cross-sectional and downstream TS nodes step synchronously.
	tfUnionStreams := make(map[string][]subStreamRef)
	tfHasCS := make(map[string]bool)
	for i, n := range p.nodes {
		tf := n.spec.TimeFrame
		if tf == p.timeframe {
			continue
		}
		if hasCSInTF[i] {
			tfHasCS[tf] = true
		}
		tfUnionStreams[tf] = compactSubStreams(append(tfUnionStreams[tf], nodeStreams[i]...))
	}

	scopeIndexByKey := make(map[string]int)
	for i, n := range p.nodes {
		if n.spec.Operator == opAsOfNode {
			srcIdx := n.inputs[0]
			meta.asofStreams[i] = slices.Clone(nodeStreams[srcIdx])
		}
		tf := n.spec.TimeFrame
		if tf == p.timeframe {
			continue
		}
		streams := nodeStreams[i]
		if tfHasCS[tf] {
			streams = tfUnionStreams[tf]
		}
		var keyBuilder strings.Builder
		keyBuilder.WriteString(tf)
		if tfHasCS[tf] {
			keyBuilder.WriteString("|cs")
		}
		for _, st := range streams {
			keyBuilder.WriteByte('|')
			keyBuilder.WriteString(st.refSymbol)
			keyBuilder.WriteByte('\x00')
			keyBuilder.WriteString(st.source)
			keyBuilder.WriteByte('\x00')
			keyBuilder.WriteString(st.timeFrame)
		}
		key := keyBuilder.String()
		scopeIdx, exists := scopeIndexByKey[key]
		if !exists {
			tfMS, _ := ParseTimeFrameMS(tf)
			scopeIdx = len(meta.scopes)
			meta.scopes = append(meta.scopes, subTFScope{
				timeFrame: tf,
				tfMS:      tfMS,
				streams:   slices.Clone(streams),
				retention: max(2, n.spec.StateRetention),
			})
			scopeIndexByKey[key] = scopeIdx
		} else {
			meta.scopes[scopeIdx].retention = max(meta.scopes[scopeIdx].retention, n.spec.StateRetention)
		}
		meta.nodeScope[i] = scopeIdx
	}
	return meta
}

func compactSubStreams(streams []subStreamRef) []subStreamRef {
	if len(streams) <= 1 {
		return streams
	}
	sort.Slice(streams, func(i, j int) bool {
		if streams[i].refSymbol != streams[j].refSymbol {
			return streams[i].refSymbol < streams[j].refSymbol
		}
		if streams[i].source != streams[j].source {
			return streams[i].source < streams[j].source
		}
		return streams[i].timeFrame < streams[j].timeFrame
	})
	return slices.Compact(streams)
}

func (p *Plan) isSubTFNode(index int) bool {
	return p.subMeta != nil && p.subMeta.nodeScope[index] >= 0
}

func (p *Plan) buildInputs() ([]InputSpec, []ScopedInputSpec) {
	type accum struct {
		spec            InputSpec
		allAssets       bool
		refSymbols      map[string]bool
		hasDirectStrict bool
		directMinAge    int64
		hasAsOfUnlim    bool
		asofMaxAge      int64
	}
	// Compute max warmup in native bars per sub-timeframe.
	subTFWarmup := make(map[string]int)
	for _, n := range p.nodes {
		if n.spec.TimeFrame != p.timeframe {
			subTFWarmup[n.spec.TimeFrame] = max(subTFWarmup[n.spec.TimeFrame], n.spec.WarmupLength)
		}
	}
	// Propagate AsOfNode MaxAge down to its direct SourceTimeFrame streams.
	streamAsOfUnlim := make(map[string]bool)
	streamAsOfMaxAge := make(map[string]int64)
	if p.subMeta != nil {
		for asofIdx, streams := range p.subMeta.asofStreams {
			age := p.nodes[asofIdx].spec.MaxAge
			for _, st := range streams {
				k := st.source + "\x00" + st.timeFrame
				if age == 0 {
					streamAsOfUnlim[k] = true
				} else if age > streamAsOfMaxAge[k] {
					streamAsOfMaxAge[k] = age
				}
			}
		}
	}

	items := make(map[string]*accum)
	for _, n := range p.nodes {
		if n.spec.Source == "" || n.spec.Operator == opAsOfNode {
			continue
		}
		key := n.spec.Source + "\x00" + n.spec.SourceTimeFrame
		isSubTF := n.spec.TimeFrame != p.timeframe
		asOfLatest := isSubTF || n.spec.AvailabilityPolicy == "asof-latest"
		acc := items[key]
		if acc == nil {
			acc = &accum{
				spec: InputSpec{
					Source:     n.spec.Source,
					TimeFrame:  n.spec.SourceTimeFrame,
					AsOfLatest: asOfLatest,
				},
				refSymbols: make(map[string]bool),
			}
			items[key] = acc
		}
		if !asOfLatest {
			acc.spec.AsOfLatest = false
		}
		field := n.spec.Field
		if field == "" {
			field = n.spec.GroupField
		}
		if field != "" {
			acc.spec.Fields = append(acc.spec.Fields, field)
		}
		if n.spec.RefSymbol != "" {
			acc.refSymbols[n.spec.RefSymbol] = true
		} else {
			acc.allAssets = true
		}
		if isSubTF {
			// Sub-timeframe streams count warmup in their own source bars (+1 so
			// a closed bar precedes the initial warmup grid).
			acc.spec.WarmupLength = max(acc.spec.WarmupLength, subTFWarmup[n.spec.TimeFrame]+1)
		} else {
			acc.spec.WarmupLength = max(acc.spec.WarmupLength, p.warmup)
			if n.spec.AvailabilityPolicy == "asof-latest" {
				if n.spec.MaxAge == 0 {
					acc.hasAsOfUnlim = true
				} else {
					acc.hasDirectStrict = true
					if acc.directMinAge == 0 || n.spec.MaxAge < acc.directMinAge {
						acc.directMinAge = n.spec.MaxAge
					}
				}
			}
		}
	}

	keys := slices.Collect(maps.Keys(items))
	sort.Strings(keys)
	plain := make([]InputSpec, 0, len(keys))
	scoped := make([]ScopedInputSpec, 0, len(keys))
	for _, key := range keys {
		acc := items[key]
		sort.Strings(acc.spec.Fields)
		acc.spec.Fields = slices.Compact(acc.spec.Fields)
		if acc.spec.AsOfLatest {
			switch {
			case acc.hasDirectStrict:
				acc.spec.MaxAge = acc.directMinAge
			case acc.hasAsOfUnlim || streamAsOfUnlim[key]:
				acc.spec.MaxAge = 0
			default:
				acc.spec.MaxAge = streamAsOfMaxAge[key]
			}
		}
		var refs []string
		if len(acc.refSymbols) > 0 {
			refs = slices.Collect(maps.Keys(acc.refSymbols))
			sort.Strings(refs)
		}
		plain = append(plain, acc.spec)
		scoped = append(scoped, ScopedInputSpec{
			InputSpec:  acc.spec,
			AllAssets:  acc.allAssets,
			RefSymbols: refs,
		})
	}
	return plain, scoped
}

// ResolveSymbolSID resolves a symbol name (e.g. "BTC/USDT:USDT") to its SID
// using SIDMap. Returns (0, false) if absent or mapped ambiguously.
func (s SnapshotSpec) ResolveSymbolSID(symbol string) (int32, bool) {
	if symbol == "" || len(s.SIDMap) == 0 {
		return 0, false
	}
	var matched int32
	for sid, sym := range s.SIDMap {
		if sym == symbol {
			if sid <= 0 || matched != 0 {
				return 0, false
			}
			matched = sid
		}
	}
	if matched <= 0 {
		return 0, false
	}
	return matched, true
}

func buildSymbolSIDMap(sidMap map[int32]string) (map[string]int32, error) {
	if len(sidMap) == 0 {
		return nil, nil
	}
	out := make(map[string]int32, len(sidMap))
	for sid, sym := range sidMap {
		if sid <= 0 {
			return nil, fmt.Errorf("factor: invalid non-positive SID %d in SIDMap", sid)
		}
		if sym == "" {
			continue
		}
		if prev, exists := out[sym]; exists && prev != sid {
			return nil, fmt.Errorf("factor: symbol %q ambiguously mapped to SIDs %d and %d", sym, prev, sid)
		}
		out[sym] = sid
	}
	return out, nil
}

// ResolveSymbolSID returns the SID registered for symbol in the frozen Snapshot.
func (s *Snapshot) ResolveSymbolSID(symbol string) (int32, bool) {
	if s == nil || symbol == "" {
		return 0, false
	}
	if s.symbolSIDs != nil {
		sid, ok := s.symbolSIDs[symbol]
		return sid, ok
	}
	return s.spec.ResolveSymbolSID(symbol)
}

// RefNumeric reads a numeric field of a specific named reference symbol.
func (s *Snapshot) RefNumeric(symbol, source, timeframe, field string) Numeric {
	sid, ok := s.ResolveSymbolSID(symbol)
	if !ok {
		return Numeric{math.NaN(), Missing}
	}
	return s.Numeric(sid, source, timeframe, field)
}

func isUnclosedAsOfBar(row VersionRecord, gridTime int64) bool {
	if row.Series.TimeFrame == "" || row.Series.TimeFrame == "event" {
		return false
	}
	if !row.Series.Closed {
		return true
	}
	if row.Series.EndMS > 0 && row.Series.EndMS > gridTime {
		return true
	}
	return false
}

func (p *Plan) validateSnapshotStreams(snapshot *Snapshot, expected map[StreamKey]bool) error {
	var active []int32
	for _, node := range p.nodes {
		if node.spec.Source == "" || node.spec.Operator == opAsOfNode {
			continue
		}
		isSubTF := node.spec.TimeFrame != p.timeframe
		var targetSIDs []int32
		if node.spec.RefSymbol != "" {
			refSID, ok := snapshot.ResolveSymbolSID(node.spec.RefSymbol)
			if !ok {
				return fmt.Errorf("factor: reference symbol %q missing from snapshot SIDMap", node.spec.RefSymbol)
			}
			targetSIDs = []int32{refSID}
		} else {
			if active == nil {
				active = activeFactorSIDs(snapshot.spec)
			}
			targetSIDs = active
		}
		for _, sid := range targetSIDs {
			key := StreamKey{sid, node.spec.Source, node.spec.SourceTimeFrame}
			if !expected[key] {
				return fmt.Errorf("factor: active stream %d/%s/%s absent from declared barrier", sid, node.spec.Source, node.spec.SourceTimeFrame)
			}
			row := snapshot.rows[key]
			if isSubTF {
				if !row.Series.Closed || row.EventTime > snapshot.spec.GridTime || (row.Series.EndMS > 0 && row.Series.EndMS > snapshot.spec.GridTime) {
					return fmt.Errorf("factor: sub-timeframe stream %d/%s/%s lacks closed causal bar", sid, node.spec.Source, node.spec.SourceTimeFrame)
				}
			} else if node.spec.AvailabilityPolicy == "asof-latest" {
				if row.EventTime > snapshot.spec.GridTime || (node.spec.MaxAge > 0 && snapshot.spec.GridTime-row.EventTime > node.spec.MaxAge) {
					return fmt.Errorf("factor: asof stream %d/%s exceeds declared max age", sid, node.spec.Source)
				}
			} else {
				if !row.Series.Closed || row.EventTime != snapshot.spec.GridTime {
					return fmt.Errorf("factor: stream %d/%s lacks current closed event", sid, node.spec.Source)
				}
			}
		}
	}
	return nil
}

// --- Session Sub-Timeframe State & Evaluation ---

type subAssetScopeState struct {
	asset          *assetState
	lastBarTime    int64
	steppedBarTime int64
	cached         map[int]Numeric
}

func cloneAssetState(src *assetState) (*assetState, error) {
	for _, series := range src.env.Items {
		if series.More != nil && series.DupMore == nil {
			if _, immutable := series.More.(float64); !immutable {
				return nil, errors.New("factor: indicator state has no safe clone contract")
			}
		}
	}
	hasExtraState := false
	src.env.Data.Range(func(_, _ any) bool { hasExtraState = true; return false })
	if hasExtraState {
		return nil, errors.New("factor: environment extra state has no safe clone contract")
	}
	env := src.env.Clone()
	for id, series := range env.Items {
		series.ID = id
		series.Time = src.env.Items[id].Time
		series.Data = slices.Clone(series.Data)
	}
	nodes := make([]*ta.Series, len(src.nodes))
	for i, series := range src.nodes {
		if series != nil {
			nodes[i] = env.Items[series.ID]
		}
	}
	technical := make(map[string][]*ta.Series, len(src.technical))
	for id, series := range src.technical {
		cloned := make([]*ta.Series, len(series))
		for i, input := range series {
			cloned[i] = env.Items[input.ID]
		}
		technical[id] = cloned
	}
	return &assetState{env: env, nodes: nodes, technical: technical}, nil
}

func (s *Session) syncSubAssets(allSIDs []int32, sidMap map[int32]string) {
	if s.plan.subMeta == nil {
		return
	}
	if s.subAssets == nil {
		s.subAssets = make(map[int32][]*subAssetScopeState, len(allSIDs))
	}
	for sid := range s.subAssets {
		if !slices.Contains(allSIDs, sid) {
			delete(s.subAssets, sid)
		}
	}
	scopes := s.plan.subMeta.scopes
	for _, sid := range allSIDs {
		states := s.subAssets[sid]
		if states == nil {
			states = make([]*subAssetScopeState, len(scopes))
			for idx, sc := range scopes {
				env := &ta.BarEnv{
					Symbol:    sidMap[sid],
					TimeFrame: sc.timeFrame,
					Items:     make(map[int]*ta.Series),
					MaxCache:  sc.retention,
				}
				nodes := make([]*ta.Series, len(s.plan.nodes))
				for nodeIdx := range nodes {
					if s.plan.subMeta.nodeScope[nodeIdx] == idx {
						nodes[nodeIdx] = env.NewSeries(nil)
					}
				}
				states[idx] = &subAssetScopeState{
					asset:  &assetState{env: env, nodes: nodes},
					cached: make(map[int]Numeric),
				}
			}
			s.subAssets[sid] = states
		}
		for _, st := range states {
			st.steppedBarTime = 0
		}
	}
}

func (s *Session) forkSubAssets() (map[int32][]*subAssetScopeState, error) {
	if len(s.subAssets) == 0 {
		return nil, nil
	}
	out := make(map[int32][]*subAssetScopeState, len(s.subAssets))
	for sid, states := range s.subAssets {
		clonedStates := make([]*subAssetScopeState, len(states))
		for i, st := range states {
			clonedAsset, err := cloneAssetState(st.asset)
			if err != nil {
				return nil, err
			}
			clonedStates[i] = &subAssetScopeState{
				asset:       clonedAsset,
				lastBarTime: st.lastBarTime,
				cached:      maps.Clone(st.cached),
			}
		}
		out[sid] = clonedStates
	}
	return out, nil
}

func (s *Session) retainedSubAssetValues() int {
	total := 0
	for _, states := range s.subAssets {
		for _, st := range states {
			for _, series := range st.asset.env.Items {
				total += len(series.Data)
			}
		}
	}
	return total
}

func (s *Session) trimSubAssets() {
	if s.plan.subMeta == nil {
		return
	}
	scopes := s.plan.subMeta.scopes
	for _, states := range s.subAssets {
		for idx, st := range states {
			limit := scopes[idx].retention
			for _, series := range st.asset.env.Items {
				if len(series.Data) > 2*limit {
					series.Data = slices.Clone(series.Data[len(series.Data)-limit:])
				}
			}
		}
	}
}

// resolveScopeBarTime returns the completed causal source bar timestamp for
// (sid, scope) at snapshot. If streams disagree on bar timestamp (one stream
// has not yet closed the new bar), prevBarTime is retained if > 0.
func resolveScopeBarTime(sc subTFScope, sid int32, snapshot *Snapshot, prevBarTime int64) (int64, bool) {
	if len(sc.streams) == 0 {
		if sc.tfMS <= 0 {
			return 1, true
		}
		return (snapshot.spec.GridTime / sc.tfMS) * sc.tfMS, true
	}
	var minTime, maxTime int64
	for i, st := range sc.streams {
		targetSID := sid
		if st.refSymbol != "" {
			var ok bool
			targetSID, ok = snapshot.ResolveSymbolSID(st.refSymbol)
			if !ok {
				return 0, false
			}
		}
		row, ok := snapshot.rows[StreamKey{targetSID, st.source, st.timeFrame}]
		if !ok || !row.Series.Closed || row.EventTime <= 0 || row.EventTime > snapshot.spec.GridTime || (row.Series.EndMS > 0 && row.Series.EndMS > snapshot.spec.GridTime) {
			return 0, false
		}
		if i == 0 {
			minTime, maxTime = row.EventTime, row.EventTime
		} else {
			minTime = min(minTime, row.EventTime)
			maxTime = max(maxTime, row.EventTime)
		}
	}
	if minTime != maxTime {
		if prevBarTime > 0 && minTime >= prevBarTime {
			return prevBarTime, true
		}
		return 0, false
	}
	return minTime, true
}

func (s *Session) evaluateSubTFNode(index int, sid int32, snapshot *Snapshot, columns []map[int32]Numeric) Numeric {
	scopeIdx := s.plan.subMeta.nodeScope[index]
	sc := s.plan.subMeta.scopes[scopeIdx]
	st := s.subAssets[sid][scopeIdx]
	barTime, ok := resolveScopeBarTime(sc, sid, snapshot, st.lastBarTime)
	if !ok {
		return Numeric{math.NaN(), Missing}
	}
	// Advance BarEnv at most once when a new completed source bar arrives.
	if st.steppedBarTime == 0 && barTime > st.lastBarTime {
		st.asset.env.TimeStart = barTime - 1
		st.asset.env.TimeStop = barTime
		st.asset.env.BarNum++
		st.lastBarTime = barTime
		st.steppedBarTime = barTime
	}
	if st.steppedBarTime == barTime {
		node := s.plan.nodes[index]
		var value Numeric
		if node.spec.Operator == opAsOfNode {
			value = s.evaluateAsOfNode(index, node, sid, snapshot, columns, barTime)
		} else {
			value = s.evaluateTS(node, st.asset, sid, snapshot, columns)
		}
		st.asset.nodes[index].Append(value.Value)
		st.cached[index] = value
		s.updates[node.id]++
		return value
	}
	if cached, exists := st.cached[index]; exists {
		return cached
	}
	return Numeric{math.NaN(), Missing}
}

func (s *Session) appendSubTFCS(index int, sid int32, snapshot *Snapshot, value Numeric) {
	scopeIdx := s.plan.subMeta.nodeScope[index]
	sc := s.plan.subMeta.scopes[scopeIdx]
	st := s.subAssets[sid][scopeIdx]
	barTime, ok := resolveScopeBarTime(sc, sid, snapshot, st.lastBarTime)
	if !ok {
		return
	}
	if st.steppedBarTime == 0 && barTime > st.lastBarTime {
		st.asset.env.TimeStart = barTime - 1
		st.asset.env.TimeStop = barTime
		st.asset.env.BarNum++
		st.lastBarTime = barTime
		st.steppedBarTime = barTime
	}
	if st.steppedBarTime == barTime {
		st.asset.nodes[index].Append(value.Value)
		st.cached[index] = value
	}
}

func (s *Session) evaluateAsOfNode(index int, node compiledNode, sid int32, snapshot *Snapshot, columns []map[int32]Numeric, refTime int64) Numeric {
	val := columns[node.inputs[0]][sid]
	if node.spec.MaxAge > 0 && s.plan.subMeta != nil {
		if !asofStreamsFresh(s.plan.subMeta.asofStreams[index], sid, snapshot, refTime, node.spec.MaxAge) {
			return Numeric{math.NaN(), Missing}
		}
	}
	return val
}

func asofStreamsFresh(streams []subStreamRef, sid int32, snapshot *Snapshot, refTime, maxAge int64) bool {
	for _, st := range streams {
		targetSID := sid
		if st.refSymbol != "" {
			var ok bool
			targetSID, ok = snapshot.ResolveSymbolSID(st.refSymbol)
			if !ok {
				return false
			}
		}
		row, ok := snapshot.rows[StreamKey{targetSID, st.source, st.timeFrame}]
		if !ok || !row.Series.Closed || row.EventTime > refTime || refTime-row.EventTime > maxAge {
			return false
		}
	}
	return true
}

// --- Batch Sub-Timeframe Evaluation ---

func (p *Plan) evaluateBatchSubTFTSSpan(node compiledNode, sp sidSpan, snapshots []*Snapshot, columns [][]map[int32]Numeric, index int) error {
	scopeIdx := p.subMeta.nodeScope[index]
	sc := p.subMeta.scopes[scopeIdx]
	sid := sp.sid
	spanLen := sp.end - sp.start

	var stepTimes []int
	var stepBarTimes []int64
	barIndexAtOffset := make([]int, spanLen)
	var lastBarTime int64
	for offset := 0; offset < spanLen; offset++ {
		t := sp.start + offset
		barTime, ok := resolveScopeBarTime(sc, sid, snapshots[t], lastBarTime)
		if !ok {
			barIndexAtOffset[offset] = -1
			continue
		}
		if barTime > lastBarTime {
			stepTimes = append(stepTimes, t)
			stepBarTimes = append(stepBarTimes, barTime)
			lastBarTime = barTime
		}
		barIndexAtOffset[offset] = len(stepTimes) - 1
	}

	kLen := len(stepTimes)
	if kLen == 0 {
		for offset := 0; offset < spanLen; offset++ {
			columns[index][sp.start+offset][sid] = Numeric{math.NaN(), Missing}
		}
		return nil
	}

	// Build compressed step-level views of snapshots and upstream columns so
	// evaluateBatchTSSpan / evaluateBatchAsOfNode runs strictly once per closed bar.
	if node.spec.Operator == opAsOfNode {
		dep := node.inputs[0]
		compressed := make([]Numeric, kLen)
		for k, t := range stepTimes {
			val := columns[dep][t][sid]
			if node.spec.MaxAge > 0 && !asofStreamsFresh(p.subMeta.asofStreams[index], sid, snapshots[t], stepBarTimes[k], node.spec.MaxAge) {
				val = Numeric{math.NaN(), Missing}
			}
			compressed[k] = val
		}
		for offset := 0; offset < spanLen; offset++ {
			k := barIndexAtOffset[offset]
			if k < 0 {
				columns[index][sp.start+offset][sid] = Numeric{math.NaN(), Missing}
			} else {
				columns[index][sp.start+offset][sid] = compressed[k]
			}
		}
		return nil
	}

	compSnapshots := make([]*Snapshot, kLen)
	compColumns := make([][]map[int32]Numeric, len(p.nodes))
	for _, dep := range node.inputs {
		compColumns[dep] = make([]map[int32]Numeric, kLen)
		for k, t := range stepTimes {
			compColumns[dep][k] = map[int32]Numeric{sid: columns[dep][t][sid]}
		}
	}
	compColumns[index] = make([]map[int32]Numeric, kLen)
	for k, t := range stepTimes {
		compSnapshots[k] = snapshots[t]
		compColumns[index][k] = make(map[int32]Numeric, 1)
	}
	if err := evaluateBatchTSSpanCore(node, sidSpan{sid: sid, start: 0, end: kLen}, compSnapshots, compColumns, index); err != nil {
		return err
	}
	for offset := 0; offset < spanLen; offset++ {
		k := barIndexAtOffset[offset]
		if k < 0 {
			columns[index][sp.start+offset][sid] = Numeric{math.NaN(), Missing}
		} else {
			columns[index][sp.start+offset][sid] = compColumns[index][k][sid]
		}
	}
	return nil
}

func (p *Plan) evaluateBatchAsOfNodeSpan(node compiledNode, sp sidSpan, snapshots []*Snapshot, columns [][]map[int32]Numeric, index int) {
	sid := sp.sid
	dep := node.inputs[0]
	streams := p.subMeta.asofStreams[index]
	for t := sp.start; t < sp.end; t++ {
		val := columns[dep][t][sid]
		if node.spec.MaxAge > 0 && !asofStreamsFresh(streams, sid, snapshots[t], snapshots[t].spec.GridTime, node.spec.MaxAge) {
			val = Numeric{math.NaN(), Missing}
		}
		columns[index][t][sid] = val
	}
}
