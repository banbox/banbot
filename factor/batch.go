package factor

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"sort"

	"github.com/banbox/banta/tav"
)

type sidSpan struct {
	sid   int32
	start int
	end   int
}

// Batch computes one explicitly bounded history from its known starting point
// across topological stages (StageTS per-asset contiguous spans via tav and
// StageCS cross-sectional barriers across the PIT universe at each step).
// For arbitrary time chunks with an EMA continuing from earlier history, reuse
// Session instead of reseeding EMA. Snapshot input archives and returned panels
// are caller-owned, never cached.
func (p *Plan) Batch(snapshots []*Snapshot, maxRows int) ([]Frame, error) {
	if maxRows <= 0 || len(snapshots) > maxRows {
		return nil, errors.New("factor: batch requires explicit row bound")
	}
	if len(snapshots) == 0 {
		return nil, nil
	}
	for _, node := range p.nodes {
		if !node.spec.Batch {
			return nil, fmt.Errorf("factor: operator %s has no batch capability", node.spec.Operator)
		}
		if err := validateNode(node.spec, len(node.inputs), node.evaluate != nil); err != nil {
			return nil, err
		}
	}
	// Validate the same immutable context, time monotonicity, versioned universe,
	// and active SID symbol continuity contract as incremental Session.
	identity := ""
	lastTime := int64(0)
	lastGrid := int64(0)
	universeHash := ""
	universeVersion := ""
	dynamic := false
	activeAt := make([][]int32, len(snapshots))
	for t, snapshot := range snapshots {
		current, err := p.snapshotIdentity(snapshot)
		if err != nil {
			return nil, err
		}
		if identity != "" && identity != current {
			return nil, errors.New("factor: batch context identity changed")
		}
		if snapshot.spec.DecisionTime <= lastTime || snapshot.spec.GridTime <= lastGrid {
			return nil, errors.New("factor: batch times must strictly increase")
		}
		currentUniverse, err := contentHash(snapshot.spec.Universe)
		if err != nil {
			return nil, err
		}
		if universeHash != "" && universeHash != currentUniverse {
			if universeVersion == snapshot.spec.Universe.Version {
				return nil, errors.New("factor: universe changed without a new version")
			}
			dynamic = true
		}
		identity = current
		lastTime = snapshot.spec.DecisionTime
		lastGrid = snapshot.spec.GridTime
		universeHash = currentUniverse
		universeVersion = snapshot.spec.Universe.Version
		activeAt[t] = activeFactorSIDs(snapshot.spec)
		if t > 0 {
			for _, sid := range activeAt[t] {
				if _, stillActive := slices.BinarySearch(activeAt[t-1], sid); stillActive {
					if snapshots[t-1].spec.SIDMap[sid] != snapshot.spec.SIDMap[sid] {
						return nil, fmt.Errorf("factor: SID %d identity changed", sid)
					}
				}
			}
		}
	}

	spans := buildActiveSIDSpans(activeAt, dynamic)
	columns := make([][]map[int32]Numeric, len(p.nodes))
	consumers := make([]int, len(p.nodes))
	for _, node := range p.nodes {
		for _, input := range node.inputs {
			consumers[input]++
		}
	}
	outputsByNode := make([][]string, len(p.nodes))
	for _, name := range p.Outputs() {
		index := p.outputs[name]
		outputsByNode[index] = append(outputsByNode[index], name)
	}
	frames := make([]Frame, len(snapshots))
	for t, snapshot := range snapshots {
		frames[t] = Frame{
			SnapshotID:   snapshot.id,
			PlanHash:     p.hash,
			GridTime:     snapshot.spec.GridTime,
			DecisionTime: snapshot.spec.DecisionTime,
			Values:       make(map[string]map[int32]Numeric, len(p.outputs)),
		}
	}

	for _, stage := range p.stages {
		for _, index := range stage.nodes {
			columns[index] = make([]map[int32]Numeric, len(snapshots))
			for t := range snapshots {
				columns[index][t] = make(map[int32]Numeric, len(activeAt[t]))
			}
		}
		if stage.kind == StageTS {
			for _, sp := range spans {
				for _, index := range stage.nodes {
					if err := evaluateBatchTSSpan(p.nodes[index], sp, snapshots, columns, index); err != nil {
						return nil, err
					}
				}
			}
		} else {
			slice := make([]map[int32]Numeric, len(p.nodes))
			for t, snapshot := range snapshots {
				clear(slice)
				for _, index := range stage.nodes {
					node := p.nodes[index]
					for _, dep := range node.inputs {
						slice[dep] = columns[dep][t]
					}
					res := crossSection(node, snapshot, slice)
					for _, sid := range activeAt[t] {
						if _, exists := res[sid]; !exists {
							res[sid] = Numeric{math.NaN(), Missing}
						}
					}
					columns[index][t] = res
					slice[index] = res
				}
			}
		}
		for _, index := range stage.nodes {
			outNames := outputsByNode[index]
			for i, name := range outNames {
				soleOwner := i == 0 && len(outNames) == 1 && consumers[index] == 0
				for t := range snapshots {
					if soleOwner {
						frames[t].Values[name] = columns[index][t]
					} else {
						frames[t].Values[name] = maps.Clone(columns[index][t])
					}
				}
			}
			for _, dep := range p.nodes[index].inputs {
				consumers[dep]--
				if consumers[dep] == 0 {
					columns[dep] = nil
				}
			}
			if consumers[index] == 0 {
				columns[index] = nil
			}
		}
	}
	return frames, nil
}

func buildActiveSIDSpans(activeAt [][]int32, dynamic bool) []sidSpan {
	if !dynamic {
		spans := make([]sidSpan, len(activeAt[0]))
		for i, sid := range activeAt[0] {
			spans[i] = sidSpan{sid: sid, start: 0, end: len(activeAt)}
		}
		return spans
	}
	openStart := make(map[int32]int, len(activeAt[0]))
	var spans []sidSpan
	for t, sids := range activeAt {
		for sid, start := range openStart {
			if _, present := slices.BinarySearch(sids, sid); !present {
				spans = append(spans, sidSpan{sid: sid, start: start, end: t})
				delete(openStart, sid)
			}
		}
		for _, sid := range sids {
			if _, exists := openStart[sid]; !exists {
				openStart[sid] = t
			}
		}
	}
	for sid, start := range openStart {
		spans = append(spans, sidSpan{sid: sid, start: start, end: len(activeAt)})
	}
	sort.Slice(spans, func(i, j int) bool {
		if spans[i].sid != spans[j].sid {
			return spans[i].sid < spans[j].sid
		}
		return spans[i].start < spans[j].start
	})
	return spans
}

func evaluateBatchTSSpan(node compiledNode, sp sidSpan, snapshots []*Snapshot, columns [][]map[int32]Numeric, index int) error {
	sid := sp.sid
	spanLen := sp.end - sp.start
	if node.spec.Operator == "field" {
		for offset := 0; offset < spanLen; offset++ {
			t := sp.start + offset
			columns[index][t][sid] = snapshots[t].Numeric(sid, node.spec.Source, node.spec.SourceTimeFrame, node.spec.Field)
		}
		return nil
	}
	if node.spec.Operator == "sector-code" {
		for offset := 0; offset < spanLen; offset++ {
			t := sp.start + offset
			columns[index][t][sid] = evaluateSectorCode(snapshots[t], sid)
		}
		return nil
	}
	_, isPointwise := pointwiseArity(node.spec.Operator)
	if node.evaluate != nil || isPointwise || node.spec.Operator == "linear" {
		inputs := make([]Numeric, len(node.inputs))
		for offset := 0; offset < spanLen; offset++ {
			t := sp.start + offset
			for i, dep := range node.inputs {
				inputs[i] = columns[dep][t][sid]
			}
			var value Numeric
			if node.evaluate != nil {
				value = normalized(node.evaluate(inputs))
			} else if isPointwise {
				value, _ = evaluatePointwise(node.spec, inputs)
			} else {
				sum := 0.0
				value = Numeric{0, Valid}
				for i, input := range inputs {
					if input.Validity != Valid {
						value = input
						break
					}
					sum += input.Value * node.spec.Parameters[fmt.Sprintf("weight%d", i)]
				}
				if value.Validity == Valid {
					value = numeric(sum)
				}
			}
			columns[index][t][sid] = value
		}
		return nil
	}

	var values []float64
	_, technical := technicalOperatorArity(node.spec.Operator)
	if technical {
		inputs := make([][]float64, len(node.inputs))
		for i, dep := range node.inputs {
			inputs[i] = make([]float64, spanLen)
			for offset := 0; offset < spanLen; offset++ {
				v := columns[dep][sp.start+offset][sid]
				inputs[i][offset] = math.NaN()
				if v.Validity == Valid {
					inputs[i][offset] = v.Value
				}
			}
		}
		var computed bool
		values, computed = computeTechnicalBatch(node.spec, inputs)
		if !computed {
			return fmt.Errorf("factor: invalid batch dependencies for %s", node.spec.Operator)
		}
	} else {
		data := make([]float64, spanLen)
		if len(node.inputs) > 0 {
			dep := node.inputs[0]
			for offset := 0; offset < spanLen; offset++ {
				data[offset] = columns[dep][sp.start+offset][sid].Value
			}
		}
		period := int(node.spec.Parameters["period"])
		switch node.spec.Operator {
		case "ema":
			// v0.4.1 tav seeds from a contiguous window, while cached EMA seeds
			// from the first period valid observations. The declared skip-invalid
			// policy uses a compact numeric view and restores invalid timestamps.
			compact := make([]float64, 0, spanLen)
			for _, v := range data {
				if !math.IsNaN(v) {
					compact = append(compact, v)
				}
			}
			computed := tav.EMA(compact, period)
			values = make([]float64, spanLen)
			next := 0
			for i, v := range data {
				values[i] = math.NaN()
				if !math.IsNaN(v) {
					values[i] = computed[next]
					next++
				}
			}
		case "stddev":
			values, _ = tav.StdDevBy(data, period, int(node.spec.Parameters["ddof"]))
		case "return":
			values = tav.ROCR(data, period)
			for i := range values {
				values[i] -= 1
			}
		case "lag":
			values = make([]float64, spanLen)
			for i := range values {
				values[i] = math.NaN()
				if i >= period {
					values[i] = data[i-period]
				}
			}
		default:
			return fmt.Errorf("factor: unregistered batch operator %s", node.spec.Operator)
		}
	}

	inputs := make([]Numeric, len(node.inputs))
	for offset := 0; offset < spanLen; offset++ {
		t := sp.start + offset
		for i, dep := range node.inputs {
			inputs[i] = columns[dep][t][sid]
		}
		value := numeric(values[offset])
		if value.Validity != Valid && (!technical || math.IsNaN(values[offset])) {
			value.Validity = Warmup
		}
		if node.spec.Operator != "lag" {
			for _, input := range inputs {
				if input.Validity != Valid {
					value = Numeric{math.NaN(), input.Validity}
					break
				}
			}
		}
		columns[index][t][sid] = value
	}
	return nil
}
