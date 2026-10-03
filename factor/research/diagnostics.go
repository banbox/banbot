package research

import (
	"errors"
	"math"
	"sort"

	"github.com/banbox/banbot/factor"
)

type LabelMetrics struct {
	Pairs           int
	IC, RankIC      factor.Numeric
	QuintileReturns [5]factor.Numeric
	Monotonicity    factor.Numeric
}
type ColumnMetrics struct {
	Expected, Valid     int
	Coverage            float64
	Missing             map[factor.Validity]int
	Labels              map[string]LabelMetrics
	TopQuintileTurnover float64
	RiskExposures       map[string]factor.Numeric
}
type Correlation struct {
	Left, Right string
	Pairs       int
	Value       factor.Numeric
}
type Performance struct {
	Label                 string
	Kind                  LabelKind
	Gross, Net            factor.Numeric
	OneWayTurnover, Costs float64
}
type Report struct {
	SnapshotID         string
	DecisionTime, AsOf int64
	Columns            map[string]ColumnMetrics
	Correlations       []Correlation
	Performance        Performance
	Diagnostics        []factor.Diagnostic
}
type EvaluationSpec struct {
	AsOf                            int64
	PrimaryLabel                    string
	CostRate                        float64 // fee+slippage per traded notional / frozen strategy NAV
	CurrentWeights, PreviousWeights map[int32]float64
	PreviousColumns                 map[string]map[int32]factor.Numeric
	Exposures                       map[string]map[int32]factor.Numeric
}
type pair struct {
	sid  int32
	x, y float64
}

func invalid() factor.Numeric { return factor.Numeric{Validity: factor.Missing} }
func correlation(points []pair, rank bool) factor.Numeric {
	if len(points) < 2 {
		return invalid()
	}
	xs, ys := make([]float64, len(points)), make([]float64, len(points))
	for i, p := range points {
		xs[i], ys[i] = p.x, p.y
	}
	if rank {
		xs = ranks(xs)
		ys = ranks(ys)
	}
	mx, my := 0.0, 0.0
	for i := range xs {
		mx += xs[i]
		my += ys[i]
	}
	mx /= float64(len(xs))
	my /= float64(len(ys))
	vx, vy, c := 0.0, 0.0, 0.0
	for i := range xs {
		dx, dy := xs[i]-mx, ys[i]-my
		vx += dx * dx
		vy += dy * dy
		c += dx * dy
	}
	if vx == 0 || vy == 0 {
		return invalid()
	}
	return factor.Numeric{Value: max(-1, min(1, c/math.Sqrt(vx*vy))), Validity: factor.Valid}
}
func ranks(values []float64) []float64 {
	order := make([]int, len(values))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool { return values[order[i]] < values[order[j]] })
	out := make([]float64, len(values))
	for start := 0; start < len(order); {
		end := start + 1
		for end < len(order) && values[order[end]] == values[order[start]] {
			end++
		}
		r := float64(start+end-1) / 2
		for _, i := range order[start:end] {
			out[i] = r
		}
		start = end
	}
	return out
}
func paired(sids []int32, a, b map[int32]factor.Numeric) []pair {
	var rows []pair
	for _, sid := range sids {
		if valid(a[sid]) && valid(b[sid]) {
			rows = append(rows, pair{sid, a[sid].Value, b[sid].Value})
		}
	}
	return rows
}
func topQuintile(sids []int32, values map[int32]factor.Numeric) map[int32]bool {
	var rows []pair
	for _, sid := range sids {
		if valid(values[sid]) {
			rows = append(rows, pair{sid, values[sid].Value, 0})
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].x != rows[j].x {
			return rows[i].x > rows[j].x
		}
		return rows[i].sid < rows[j].sid
	})
	set := make(map[int32]bool)
	for _, row := range rows[:(len(rows)+4)/5] {
		set[row.sid] = true
	}
	return set
}

// Evaluate ranks the frozen Evaluation universe before joining realized labels.
// Missing future labels affect pair counts, never inference ranks or buckets.
func Evaluate(frame factor.Frame, universe factor.Universe, labels []Label, spec EvaluationSpec) (Report, error) {
	if spec.AsOf < frame.DecisionTime || spec.CostRate < 0 || math.IsNaN(spec.CostRate) || math.IsInf(spec.CostRate, 0) {
		return Report{}, errors.New("research: invalid diagnostic as-of/cost")
	}
	sids := uniqueSIDs(universe.Evaluation)
	names := make([]string, 0, len(frame.Values))
	for name := range frame.Values {
		names = append(names, name)
	}
	sort.Strings(names)
	report := Report{SnapshotID: frame.SnapshotID, DecisionTime: frame.DecisionTime, AsOf: spec.AsOf, Columns: make(map[string]ColumnMetrics)}
	if universe.Static {
		report.Diagnostics = append(report.Diagnostics, factor.Diagnostic{Code: "static-universe", Detail: "static membership may contain survivorship bias"})
	}
	byLabel := make(map[string]map[int32]factor.Numeric)
	labelKinds := make(map[string]LabelKind)
	for _, label := range labels {
		if label.Name == "" || label.SID <= 0 || label.DecisionTime <= 0 || label.BeginAt < label.DecisionTime || label.EndAt <= label.BeginAt || label.MatureAt < label.EndAt || label.AvailableAt <= 0 || (label.Kind != ExecutableReturn && label.Kind != CloseToClose) || (label.Kind == ExecutableReturn && label.BeginAt <= label.DecisionTime) || (label.Kind == CloseToClose && label.BeginAt != label.DecisionTime) {
			return Report{}, errors.New("research: invalid diagnostic label chronology")
		}
		if label.Resolved && label.DecisionTime == frame.DecisionTime && label.MatureAt <= spec.AsOf && label.AvailableAt <= spec.AsOf {
			if kind, ok := labelKinds[label.Name]; ok && kind != label.Kind {
				return Report{}, errors.New("research: inconsistent label kind")
			}
			labelKinds[label.Name] = label.Kind
			if byLabel[label.Name] == nil {
				byLabel[label.Name] = make(map[int32]factor.Numeric)
			}
			if _, exists := byLabel[label.Name][label.SID]; exists {
				return Report{}, errors.New("research: duplicate diagnostic label")
			}
			byLabel[label.Name][label.SID] = label.Value
		}
	}
	for _, name := range names {
		values := frame.Values[name]
		m := ColumnMetrics{Expected: len(sids), Missing: make(map[factor.Validity]int), Labels: make(map[string]LabelMetrics), RiskExposures: make(map[string]factor.Numeric)}
		var ordered []pair
		for _, sid := range sids {
			n, exists := values[sid]
			if !exists {
				n.Validity = factor.Missing
			}
			if valid(n) {
				m.Valid++
				ordered = append(ordered, pair{sid, n.Value, 0})
			} else {
				kind := n.Validity
				switch kind {
				case factor.Missing, factor.Null, factor.NonFinite, factor.NotNumeric, factor.Warmup:
				default:
					kind = factor.NotNumeric
				}
				m.Missing[kind]++
			}
		}
		if m.Expected > 0 {
			m.Coverage = float64(m.Valid) / float64(m.Expected)
		}
		sort.Slice(ordered, func(i, j int) bool {
			if ordered[i].x != ordered[j].x {
				return ordered[i].x < ordered[j].x
			}
			return ordered[i].sid < ordered[j].sid
		})
		for label, returns := range byLabel {
			p := paired(sids, values, returns)
			lm := LabelMetrics{Pairs: len(p), IC: correlation(p, false), RankIC: correlation(p, true)}
			for i := range lm.QuintileReturns {
				lm.QuintileReturns[i] = invalid()
			}
			var sums [5]float64
			var counts [5]int
			for i, row := range ordered {
				bucket := i * 5 / len(ordered)
				if n := returns[row.sid]; valid(n) {
					sums[bucket] += n.Value
					counts[bucket]++
				}
			}
			var monotonic []pair
			for i, n := range counts {
				if n > 0 {
					lm.QuintileReturns[i] = factor.Numeric{Value: sums[i] / float64(n), Validity: factor.Valid}
					monotonic = append(monotonic, pair{x: float64(i + 1), y: lm.QuintileReturns[i].Value})
				}
			}
			lm.Monotonicity = correlation(monotonic, false)
			m.Labels[label] = lm
		}
		if previous := spec.PreviousColumns[name]; previous != nil {
			before, after := topQuintile(sids, previous), topQuintile(sids, values)
			overlap := 0
			for sid := range after {
				if before[sid] {
					overlap++
				}
			}
			if size := max(len(before), len(after)); size > 0 {
				m.TopQuintileTurnover = 1 - float64(overlap)/float64(size)
			}
		}
		for exposure, xs := range spec.Exposures {
			m.RiskExposures[exposure] = correlation(paired(sids, values, xs), false)
		}
		report.Columns[name] = m
	}
	for i, left := range names {
		for _, right := range names[i+1:] {
			p := paired(sids, frame.Values[left], frame.Values[right])
			report.Correlations = append(report.Correlations, Correlation{left, right, len(p), correlation(p, false)})
		}
	}
	weights := make(map[int32]bool)
	for sid := range spec.CurrentWeights {
		weights[sid] = true
	}
	for sid := range spec.PreviousWeights {
		weights[sid] = true
	}
	delta := 0.0
	gross := 0.0
	complete := true
	for sid := range weights {
		w, old := spec.CurrentWeights[sid], spec.PreviousWeights[sid]
		if math.IsNaN(w) || math.IsInf(w, 0) || math.IsNaN(old) || math.IsInf(old, 0) {
			return Report{}, errors.New("research: nonfinite performance weight")
		}
		delta += math.Abs(w - old)
		if w != 0 {
			r := byLabel[spec.PrimaryLabel][sid]
			if !valid(r) {
				complete = false
			} else {
				gross += w * r.Value
			}
		}
	}
	report.Performance = Performance{Label: spec.PrimaryLabel, Kind: labelKinds[spec.PrimaryLabel], Gross: invalid(), Net: invalid(), OneWayTurnover: delta / 2, Costs: spec.CostRate * delta}
	if report.Performance.Kind == CloseToClose {
		report.Diagnostics = append(report.Diagnostics, factor.Diagnostic{Code: "statistical-performance", Detail: "close-to-close evaluation is not an executable return claim"})
	}
	if complete {
		report.Performance.Gross = factor.Numeric{Value: gross, Validity: factor.Valid}
		report.Performance.Net = factor.Numeric{Value: gross - report.Performance.Costs, Validity: factor.Valid}
	} else {
		report.Diagnostics = append(report.Diagnostics, factor.Diagnostic{Code: "missing-performance-label", Detail: "a nonzero position has no matured visible realized return"})
	}
	return report, nil
}

type running struct {
	Count    int
	Mean, M2 float64
}

func (s *running) add(x float64) {
	s.Count++
	delta := x - s.Mean
	s.Mean += delta / float64(s.Count)
	s.M2 += delta * (x - s.Mean)
}
func (s running) ir() factor.Numeric {
	if s.Count < 2 || s.M2 <= 0 {
		return invalid()
	}
	return factor.Numeric{Value: s.Mean / math.Sqrt(s.M2/float64(s.Count-1)), Validity: factor.Valid}
}

type SeriesSummary struct {
	Sections           int
	MeanIC, MeanRankIC float64
	ICIR, RankICIR     factor.Numeric
	QuintileMean       [5]factor.Numeric
}
type accumulatorSlot struct {
	ic, rank running
	q        [5]running
}

// Accumulator stores O(columns*label horizons) scalar summaries, no panels.
type Accumulator struct {
	slots        map[string]map[string]*accumulatorSlot
	lastDecision int64
}

func NewAccumulator(columns, labelNames []string) (*Accumulator, error) {
	if len(columns) == 0 || len(labelNames) == 0 {
		return nil, errors.New("research: accumulator dimensions required")
	}
	a := &Accumulator{slots: make(map[string]map[string]*accumulatorSlot)}
	for _, c := range columns {
		if c == "" {
			return nil, errors.New("research: empty report column")
		}
		a.slots[c] = make(map[string]*accumulatorSlot)
		for _, l := range labelNames {
			if l == "" {
				return nil, errors.New("research: empty label name")
			}
			a.slots[c][l] = &accumulatorSlot{}
		}
	}
	return a, nil
}
func (a *Accumulator) Add(r Report) error {
	if r.DecisionTime <= a.lastDecision {
		return errors.New("research: repeated/out-of-order report")
	}
	for name, m := range r.Columns {
		slots, exists := a.slots[name]
		if !exists {
			return errors.New("research: undeclared accumulator column")
		}
		for label := range m.Labels {
			if slots[label] == nil {
				return errors.New("research: undeclared accumulator horizon")
			}
		}
	}
	for name, m := range r.Columns {
		for label, lm := range m.Labels {
			s := a.slots[name][label]
			if valid(lm.IC) {
				s.ic.add(lm.IC.Value)
			}
			if valid(lm.RankIC) {
				s.rank.add(lm.RankIC.Value)
			}
			for i, n := range lm.QuintileReturns {
				if valid(n) {
					s.q[i].add(n.Value)
				}
			}
		}
	}
	a.lastDecision = r.DecisionTime
	return nil
}
func (a *Accumulator) Summary() map[string]map[string]SeriesSummary {
	out := make(map[string]map[string]SeriesSummary)
	for c, labels := range a.slots {
		out[c] = make(map[string]SeriesSummary)
		for name, s := range labels {
			v := SeriesSummary{Sections: s.ic.Count, MeanIC: s.ic.Mean, MeanRankIC: s.rank.Mean, ICIR: s.ic.ir(), RankICIR: s.rank.ir()}
			for i, q := range s.q {
				v.QuintileMean[i] = invalid()
				if q.Count > 0 {
					v.QuintileMean[i] = factor.Numeric{Value: q.Mean, Validity: factor.Valid}
				}
			}
			out[c][name] = v
		}
	}
	return out
}
func (a *Accumulator) Slots() int {
	n := 0
	for _, labels := range a.slots {
		n += len(labels)
	}
	return n
}

// Decay exposes horizon-specific IC without claiming independent samples or
// statistically robust significance for overlapping labels.
func (m ColumnMetrics) Decay() map[string]factor.Numeric {
	out := make(map[string]factor.Numeric)
	for name, l := range m.Labels {
		out[name] = l.IC
	}
	return out
}
