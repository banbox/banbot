package factor

import (
	"errors"
	"fmt"
	"math"

	"github.com/banbox/banta/tav"
)

// Batch computes one explicitly bounded history from its known starting point.
// It calls tav for validated TS operators. For arbitrary time chunks with an
// EMA continuing from earlier history, reuse Session instead of reseeding EMA.
// Snapshot input archives and returned panels are caller-owned, never cached.
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
	// Validate the same immutable context/barrier contract before computation.
	identity := ""
	lastTime := int64(0)
	lastGrid := int64(0)
	universe := ""
	dynamic := false
	for _, snapshot := range snapshots {
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
		identity = current
		lastTime = snapshot.spec.DecisionTime
		lastGrid = snapshot.spec.GridTime
		currentUniverse, err := contentHash(snapshot.spec.Universe)
		if err != nil {
			return nil, err
		}
		if universe != "" && universe != currentUniverse {
			dynamic = true
		}
		universe = currentUniverse
	}
	if dynamic {
		// Explicit cached replay handles tracked asset continuity and removal/
		// re-entry. tav arrays do not encode dynamic membership lifecycle.
		session, err := NewSession(p)
		if err != nil {
			return nil, err
		}
		frames := make([]Frame, 0, len(snapshots))
		for _, snapshot := range snapshots {
			frame, err := session.Evaluate(snapshot)
			if err != nil {
				return nil, err
			}
			frames = append(frames, frame)
		}
		return frames, nil
	}
	sids := activeFactorSIDs(snapshots[0].spec)
	columns := make([][]map[int32]Numeric, len(p.nodes))
	consumers := make([]int, len(p.nodes))
	for _, node := range p.nodes {
		for _, input := range node.inputs {
			consumers[input]++
		}
	}
	frames := make([]Frame, len(snapshots))
	for time, snapshot := range snapshots {
		frames[time] = Frame{SnapshotID: snapshot.id, PlanHash: p.hash, GridTime: snapshot.spec.GridTime, DecisionTime: snapshot.spec.DecisionTime, Values: make(map[string]map[int32]Numeric)}
	}
	for index, node := range p.nodes {
		columns[index] = make([]map[int32]Numeric, len(snapshots))
		for time := range snapshots {
			columns[index][time] = make(map[int32]Numeric, len(sids))
		}
		if node.spec.Kind != TS {
			for time, snapshot := range snapshots {
				slice := make([]map[int32]Numeric, len(p.nodes))
				for _, input := range node.inputs {
					slice[input] = columns[input][time]
				}
				columns[index][time] = crossSection(node, snapshot, slice)
				for _, sid := range sids {
					if _, exists := columns[index][time][sid]; !exists {
						columns[index][time][sid] = Numeric{math.NaN(), Missing}
					}
				}
			}
		} else {
			for _, sid := range sids {
				data := make([]float64, len(snapshots))
				if len(node.inputs) > 0 {
					for time := range snapshots {
						data[time] = columns[node.inputs[0]][time][sid].Value
					}
				}
				var values []float64
				period := int(node.spec.Parameters["period"])
				switch node.spec.Operator {
				case "ema":
					// v0.4.1 tav seeds from a contiguous window, while cached
					// EMA seeds from the first period valid observations. The
					// declared skip-invalid policy uses a compact numeric view
					// and restores invalid timestamps, never filling raw values.
					compact := make([]float64, 0, len(data))
					for _, value := range data {
						if !math.IsNaN(value) {
							compact = append(compact, value)
						}
					}
					computed := tav.EMA(compact, period)
					values = make([]float64, len(data))
					next := 0
					for i, value := range data {
						values[i] = math.NaN()
						if !math.IsNaN(value) {
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
					values = make([]float64, len(data))
					for i := range values {
						values[i] = math.NaN()
						if i >= period {
							values[i] = data[i-period]
						}
					}
				default:
					if _, ok := technicalOperatorArity(node.spec.Operator); ok {
						inputs := make([][]float64, len(node.inputs))
						for i, input := range node.inputs {
							inputs[i] = make([]float64, len(snapshots))
							for time := range snapshots {
								value := columns[input][time][sid]
								inputs[i][time] = math.NaN()
								if value.Validity == Valid {
									inputs[i][time] = value.Value
								}
							}
						}
						var computed bool
						values, computed = computeTechnicalBatch(node.spec, inputs)
						if !computed {
							return nil, fmt.Errorf("factor: invalid batch dependencies for %s", node.spec.Operator)
						}
					}
				}
				for time, snapshot := range snapshots {
					var value Numeric
					if node.spec.Operator == "field" {
						value = snapshot.Numeric(sid, node.spec.Source, node.spec.SourceTimeFrame, node.spec.Field)
					} else {
						inputs := make([]Numeric, len(node.inputs))
						for i, input := range node.inputs {
							inputs[i] = columns[input][time][sid]
						}
						if node.evaluate != nil {
							value = normalized(node.evaluate(inputs))
						} else if pointwise, ok := evaluatePointwise(node.spec, inputs); ok {
							value = pointwise
						} else if node.spec.Operator == "linear" {
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
						} else {
							if len(values) != len(snapshots) {
								return nil, fmt.Errorf("factor: unregistered batch operator %s", node.spec.Operator)
							}
							value = numeric(values[time])
							if value.Validity != Valid {
								_, technical := technicalOperatorArity(node.spec.Operator)
								if !technical || math.IsNaN(values[time]) {
									value.Validity = Warmup
								}
							}
							if node.spec.Operator != "lag" {
								for _, input := range inputs {
									if input.Validity != Valid {
										value = Numeric{math.NaN(), input.Validity}
										break
									}
								}
							}
						}
					}
					columns[index][time][sid] = value
				}
			}
		}
		for name, output := range p.outputs {
			if output == index {
				for time := range snapshots {
					frames[time].Values[name] = columns[index][time]
				}
			}
		}
		for _, input := range node.inputs {
			consumers[input]--
			if consumers[input] == 0 {
				columns[input] = nil
			}
		}
		if consumers[index] == 0 {
			columns[index] = nil
		}
	}
	return frames, nil
}
