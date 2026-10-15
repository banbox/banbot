package factor

import (
	"encoding/json"
	"errors"
	"math/big"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

type AllocationBasis string

const (
	NAVFraction      AllocationBasis = "nav-fraction"
	AbsoluteQuantity AllocationBasis = "absolute-quantity"
)

// Allocation quantities are signed standard asset units, never contract counts.
type Allocation struct {
	Basis AllocationBasis `json:"basis"`
	Value string          `json:"value"`
}

const PortfolioTargetVersion = 1

type PortfolioTarget struct {
	spec        PortfolioSpec
	allocations map[int32]Allocation
	id          string
}

var decimalPattern = regexp.MustCompile(`^[+-]?[0-9]+(?:\.[0-9]+)?$`)

func CanonicalDecimal(value string) (string, error) {
	if len(value) > 512 || !decimalPattern.MatchString(value) {
		return "", errors.New("factor: quantity must be a finite plain decimal")
	}
	negative := strings.HasPrefix(value, "-")
	value = strings.TrimLeft(value, "+-")
	parts := strings.SplitN(value, ".", 2)
	whole := strings.TrimLeft(parts[0], "0")
	if whole == "" {
		whole = "0"
	}
	fraction := ""
	if len(parts) == 2 {
		fraction = strings.TrimRight(parts[1], "0")
	}
	value = whole
	if fraction != "" {
		value += "." + fraction
	}
	if negative && value != "0" {
		value = "-" + value
	}
	return value, nil
}
func decimalFloat(value string) (float64, error) {
	canonical, err := CanonicalDecimal(value)
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(canonical, 64)
}
func floatDecimal(value float64) string { return strconv.FormatFloat(value, 'f', -1, 64) }
func NewPortfolioTarget(spec PortfolioSpec, allocations map[int32]Allocation) (*PortfolioTarget, error) {
	// Reuse the legacy identity validation without changing legacy content hashes.
	validation, err := NewTargetPortfolio(spec, nil)
	if err != nil {
		return nil, err
	}
	spec = validation.Spec()
	owned := make(map[int32]Allocation, len(allocations))
	for sid, a := range allocations {
		if sid <= 0 || (a.Basis != NAVFraction && a.Basis != AbsoluteQuantity) {
			return nil, errors.New("factor: invalid allocation identity/basis")
		}
		a.Value, err = CanonicalDecimal(a.Value)
		if err != nil {
			return nil, err
		}
		owned[sid] = a
	}
	p := &PortfolioTarget{spec: spec, allocations: owned}
	p.id, err = contentHash(struct {
		Version     int
		Spec        PortfolioSpec
		Allocations map[int32]Allocation
	}{PortfolioTargetVersion, spec, owned})
	return p, err
}
func (p *PortfolioTarget) Spec() PortfolioSpec {
	s := p.spec
	s.Diagnostics = slices.Clone(s.Diagnostics)
	return s
}
func (p *PortfolioTarget) ID() string { return p.id }
func (p *PortfolioTarget) Allocations() map[int32]Allocation {
	m := make(map[int32]Allocation, len(p.allocations))
	for sid, a := range p.allocations {
		m[sid] = a
	}
	return m
}
func (p *PortfolioTarget) Version() int { return PortfolioTargetVersion }
func PortfolioTargetFromWeights(p *TargetPortfolio) (*PortfolioTarget, error) {
	if p == nil {
		return nil, nil
	}
	m := map[int32]Allocation{}
	for sid, w := range p.Targets() {
		m[sid] = Allocation{NAVFraction, floatDecimal(w)}
	}
	return NewPortfolioTarget(p.Spec(), m)
}
func (p *PortfolioTarget) AsWeightPortfolio() (*TargetPortfolio, error) {
	m := map[int32]float64{}
	for sid, a := range p.allocations {
		if a.Basis != NAVFraction {
			return nil, errors.New("factor: absolute quantity cannot be converted to a weight target")
		}
		v, err := decimalFloat(a.Value)
		if err != nil {
			return nil, err
		}
		m[sid] = v
	}
	return NewTargetPortfolio(p.Spec(), m)
}
func (p *PortfolioTarget) EffectiveAllocations(previous *PortfolioTarget) (map[int32]Allocation, error) {
	m := map[int32]Allocation{}
	if previous != nil {
		if previous.spec.StrategyID != p.spec.StrategyID || previous.spec.AccountID != p.spec.AccountID || previous.spec.Budget.Currency != p.spec.Budget.Currency || previous.spec.PlanSequence >= p.spec.PlanSequence {
			return nil, errors.New("factor: incompatible or stale allocation target")
		}
		for sid, a := range previous.allocations {
			if p.spec.Mode == Full {
				a.Value = "0"
			}
			m[sid] = a
		}
	}
	for sid, a := range p.allocations {
		m[sid] = a
	}
	return m, nil
}
func (p *PortfolioTarget) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Version     int                  `json:"version"`
		Spec        PortfolioSpec        `json:"spec"`
		Allocations map[int32]Allocation `json:"allocations"`
		ID          string               `json:"id"`
	}{PortfolioTargetVersion, p.Spec(), p.Allocations(), p.ID()})
}
func (p *PortfolioTarget) UnmarshalJSON(raw []byte) error {
	var v struct {
		Version     int                  `json:"version"`
		Spec        PortfolioSpec        `json:"spec"`
		Allocations map[int32]Allocation `json:"allocations"`
		ID          string               `json:"id"`
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		return err
	}
	if v.Version != PortfolioTargetVersion {
		return errors.New("factor: unsupported allocation target version")
	}
	n, err := NewPortfolioTarget(v.Spec, v.Allocations)
	if err != nil {
		return err
	}
	if v.ID != "" && v.ID != n.ID() {
		return errors.New("factor: allocation target hash mismatch")
	}
	*p = *n
	return nil
}

// ScaleQuantity floors magnitude to the instrument quantum, using exact arithmetic.
func ScaleQuantity(quantity string, numerator, denominator int, quantum string) (string, error) {
	if numerator < 0 || denominator <= 0 {
		return "", errors.New("factor: invalid quantity scale")
	}
	canonical, err := CanonicalDecimal(quantity)
	if err != nil {
		return "", err
	}
	q, _ := new(big.Rat).SetString(canonical)
	q.Mul(q, new(big.Rat).SetFrac64(int64(numerator), int64(denominator)))
	if quantum == "" {
		quantum = "0.000000000000000001"
	}
	stepText, err := CanonicalDecimal(quantum)
	if err != nil {
		return "", err
	}
	step, _ := new(big.Rat).SetString(stepText)
	if step.Sign() <= 0 {
		return "", errors.New("factor: quantity quantum must be positive")
	}
	steps := new(big.Rat).Quo(q, step)
	integer := new(big.Int).Quo(steps.Num(), steps.Denom())
	q.Mul(new(big.Rat).SetInt(integer), step)
	precision := 0
	if pos := strings.IndexByte(stepText, '.'); pos >= 0 {
		precision = len(stepText) - pos - 1
	}
	return CanonicalDecimal(q.FloatString(precision))
}

// ClampQuantityMagnitude never rounds an exact quantity through float64.
func ClampQuantityMagnitude(quantity string, ceilings ...string) (string, error) {
	result, err := CanonicalDecimal(quantity)
	if err != nil {
		return "", err
	}
	value, _ := new(big.Rat).SetString(result)
	magnitude := new(big.Rat).Abs(value)
	for _, ceiling := range ceilings {
		if ceiling == "" {
			continue
		}
		text, err := CanonicalDecimal(ceiling)
		if err != nil {
			return "", err
		}
		bound, _ := new(big.Rat).SetString(text)
		bound.Abs(bound)
		if magnitude.Cmp(bound) > 0 {
			magnitude.Set(bound)
			if value.Sign() < 0 && text != "0" {
				text = "-" + strings.TrimPrefix(text, "-")
			} else {
				text = strings.TrimPrefix(text, "-")
			}
			result = text
		}
	}
	return result, nil
}
