package runner

import (
	"fmt"
	"math"
	"slices"
)

// Params reads the numeric `params:` of one run_policy item with defaults,
// mirroring the time-series `pol.Param(name, def)`. It works on a private copy,
// so reading never alters the Config or its manifest identity.
//
// Errors (for example a non-integer value for Int) accumulate; check Err once
// after reading everything. Unused lists parameters nobody asked for, which
// catches typos such as `windw: 30` that would otherwise fall back silently.
type Params struct {
	values map[string]float64
	used   map[string]bool
	errs   []error
}

// Params returns an accessor over this Config's parameters.
func (c Config) Params() *Params {
	p := &Params{values: make(map[string]float64, len(c.Manifest.Parameters)), used: map[string]bool{}}
	for k, v := range c.Manifest.Parameters {
		p.values[k] = v
	}
	return p
}

// Float returns the parameter or def when it is absent.
func (p *Params) Float(name string, def float64) float64 {
	p.used[name] = true
	v, ok := p.values[name]
	if !ok {
		return def
	}
	if math.IsNaN(v) || math.IsInf(v, 0) {
		p.errs = append(p.errs, fmt.Errorf("runner: param %q must be finite", name))
		return def
	}
	return v
}

// Int returns an integer parameter; fractional values are an error, not a truncation.
func (p *Params) Int(name string, def int) int {
	v := p.Float(name, float64(def))
	if v != math.Trunc(v) {
		p.errs = append(p.errs, fmt.Errorf("runner: param %q must be an integer, got %v", name, v))
		return def
	}
	return int(v)
}

// Bool reads 0/1 (YAML true/false are decoded as numbers by the loader only
// when written as 0/1); any other value is an error.
func (p *Params) Bool(name string, def bool) bool {
	d := 0.0
	if def {
		d = 1
	}
	switch v := p.Float(name, d); v {
	case 0:
		return false
	case 1:
		return true
	default:
		p.errs = append(p.errs, fmt.Errorf("runner: param %q must be 0 or 1, got %v", name, v))
		return def
	}
}

// Err returns the first accumulated error, if any.
func (p *Params) Err() error {
	if len(p.errs) == 0 {
		return nil
	}
	return p.errs[0]
}

// Unused returns supplied parameter names that were never read, sorted.
// `window` and `k` are consumed by the loader itself and are not reported.
func (p *Params) Unused() []string {
	var out []string
	for name := range p.values {
		if !p.used[name] && name != "window" && name != "k" {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return out
}
