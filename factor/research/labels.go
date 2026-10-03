package research

import (
	"errors"
	"maps"
	"math"
	"sort"

	"github.com/banbox/banbot/factor"
)

type LabelKind string

const (
	ExecutableReturn LabelKind = "executable-return"
	CloseToClose     LabelKind = "close-to-close"
)

type LabelSpec struct {
	Name           string
	Kind           LabelKind
	Horizon        int64
	Overlapping    bool
	PeriodsPerYear float64
}
type Label struct {
	Name                                                string
	Kind                                                LabelKind
	SID                                                 int32
	DecisionTime, BeginAt, EndAt, MatureAt, AvailableAt int64
	Value                                               factor.Numeric
	Resolved                                            bool
}
type LabelObservation struct {
	Label   Label
	Factors map[string]factor.Numeric
}

func validateLabelSpec(s LabelSpec) error {
	if s.Name == "" || (s.Kind != ExecutableReturn && s.Kind != CloseToClose) || s.Horizon <= 0 || s.PeriodsPerYear <= 0 || math.IsNaN(s.PeriodsPerYear) || math.IsInf(s.PeriodsPerYear, 0) {
		return errors.New("research: invalid label definition")
	}
	return nil
}
func ReturnLabel(spec LabelSpec, sid int32, decision, begin, end, available int64, beginPrice, endPrice factor.Numeric) (Label, error) {
	if err := validateLabelSpec(spec); err != nil {
		return Label{}, err
	}
	if sid <= 0 || decision <= 0 || end <= begin || end-begin != spec.Horizon || available <= 0 || (spec.Kind == ExecutableReturn && begin <= decision) || (spec.Kind == CloseToClose && begin != decision) {
		return Label{}, errors.New("research: label execution/statistical time mismatch")
	}
	n := factor.Numeric{Value: math.NaN(), Validity: factor.Missing}
	if valid(beginPrice) && valid(endPrice) && beginPrice.Value > 0 && endPrice.Value > 0 {
		n = factor.Numeric{Value: endPrice.Value/beginPrice.Value - 1, Validity: factor.Valid}
	}
	return Label{Name: spec.Name, Kind: spec.Kind, SID: sid, DecisionTime: decision, BeginAt: begin, EndAt: end, MatureAt: end, AvailableAt: available, Value: n, Resolved: true}, nil
}

// LabelQueue is a bounded, evaluation-only namespace. Factors are copied when
// queued; labels cannot affect an inference universe or its feature values.
type LabelQueue struct {
	specs               map[string]LabelSpec
	maxRows, maxColumns int
	rows                []LabelObservation
	watermark           int64
}

func NewLabelQueue(specs []LabelSpec, maxRows, maxColumns int) (*LabelQueue, error) {
	if maxRows <= 0 || maxColumns <= 0 || len(specs) == 0 {
		return nil, errors.New("research: label queue needs explicit row/column limits")
	}
	q := &LabelQueue{specs: make(map[string]LabelSpec), maxRows: maxRows, maxColumns: maxColumns}
	for _, s := range specs {
		if err := validateLabelSpec(s); err != nil {
			return nil, err
		}
		if _, exists := q.specs[s.Name]; exists {
			return nil, errors.New("research: duplicate label name")
		}
		q.specs[s.Name] = s
	}
	return q, nil
}
func (q *LabelQueue) Add(row LabelObservation) error {
	l := row.Label
	s, exists := q.specs[l.Name]
	if !exists || l.Kind != s.Kind || l.SID <= 0 || l.DecisionTime <= 0 || l.DecisionTime < q.watermark || l.EndAt <= l.BeginAt || l.EndAt-l.BeginAt != s.Horizon || l.MatureAt < l.EndAt || l.AvailableAt <= 0 || (s.Kind == ExecutableReturn && l.BeginAt <= l.DecisionTime) || (s.Kind == CloseToClose && l.BeginAt != l.DecisionTime) {
		return errors.New("research: invalid or late label observation")
	}
	if len(row.Factors) > q.maxColumns || len(q.rows) >= q.maxRows {
		return errors.New("research: label queue capacity exceeded")
	}
	for _, old := range q.rows {
		if old.Label.Name == l.Name && old.Label.SID == l.SID && old.Label.DecisionTime == l.DecisionTime {
			return errors.New("research: duplicate queued label")
		}
	}
	row.Factors = maps.Clone(row.Factors)
	q.rows = append(q.rows, row)
	return nil
}
func (q *LabelQueue) Len() int { return len(q.rows) }

// Schedule freezes an inference cross section without knowing future returns.
func (q *LabelQueue) Schedule(name string, sid int32, decision, begin int64, values map[string]factor.Numeric) error {
	spec, ok := q.specs[name]
	if !ok {
		return errors.New("research: unknown label")
	}
	return q.Add(LabelObservation{Label: Label{Name: name, Kind: spec.Kind, SID: sid, DecisionTime: decision, BeginAt: begin, EndAt: begin + spec.Horizon, MatureAt: begin + spec.Horizon, AvailableAt: begin + spec.Horizon, Value: factor.Numeric{Value: math.NaN(), Validity: factor.Missing}}, Factors: values})
}

// Resolve accepts a realized return (or explicit missing return) for a queued
// horizon; its original features are never replaced with future features.
func (q *LabelQueue) Resolve(label Label) error {
	if !label.Resolved || label.AvailableAt <= 0 || label.MatureAt < label.EndAt {
		return errors.New("research: unresolved/invalid label publication")
	}
	for i, row := range q.rows {
		old := row.Label
		if old.Name == label.Name && old.SID == label.SID && old.DecisionTime == label.DecisionTime {
			if old.Resolved || old.Kind != label.Kind || old.BeginAt != label.BeginAt || old.EndAt != label.EndAt {
				return errors.New("research: conflicting label resolution")
			}
			q.rows[i].Label = label
			return nil
		}
	}
	return errors.New("research: label publication has no queued horizon")
}
func (q *LabelQueue) Drain(asof int64) ([]LabelObservation, error) {
	if asof < q.watermark {
		return nil, errors.New("research: label queue time moved backwards")
	}
	q.watermark = asof
	var ready []LabelObservation
	remaining := q.rows[:0]
	for _, row := range q.rows {
		if row.Label.Resolved && row.Label.MatureAt <= asof && row.Label.AvailableAt <= asof {
			row.Factors = maps.Clone(row.Factors)
			ready = append(ready, row)
		} else {
			remaining = append(remaining, row)
		}
	}
	for i := len(remaining); i < len(q.rows); i++ {
		q.rows[i] = LabelObservation{}
	}
	q.rows = remaining
	sort.Slice(ready, func(i, j int) bool {
		a, b := ready[i].Label, ready[j].Label
		if a.DecisionTime != b.DecisionTime {
			return a.DecisionTime < b.DecisionTime
		}
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		return a.SID < b.SID
	})
	return ready, nil
}
