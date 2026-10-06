package expr

import (
	"fmt"
	"maps"
	"math"
	"sort"
	"strconv"
	"strings"
	"text/scanner"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
)

type Binding struct {
	Source    string `json:"source" yaml:"source"`
	TimeFrame string `json:"timeframe" yaml:"timeframe"`
	Sampling  string `json:"sampling" yaml:"sampling"`
	MaxAgeMS  int64  `json:"max_age_ms" yaml:"max_age_ms"`
}
type Spec struct {
	SchemaVersion int                `json:"schema_version" yaml:"schema_version"`
	TimeFrame     string             `json:"timeframe" yaml:"timeframe"`
	Bindings      map[string]Binding `json:"bindings" yaml:"bindings"`
	Params        map[string]float64 `json:"params" yaml:"params"`
	Lets          map[string]string  `json:"lets" yaml:"lets"`
	Outputs       map[string]string  `json:"outputs" yaml:"outputs"`
	Combine       research.ComboSpec `json:"combine" yaml:"combine"`
}

// CloneSpec copies configuration maps and combination columns independently.
func CloneSpec(s Spec) Spec {
	s.Bindings = maps.Clone(s.Bindings)
	s.Params = maps.Clone(s.Params)
	s.Lets = maps.Clone(s.Lets)
	s.Outputs = maps.Clone(s.Outputs)
	s.Combine = research.CloneComboSpec(s.Combine)
	return s
}

type compiler struct {
	spec        Spec
	definitions map[string]*expression
	paths       map[string]string
	nodes       map[string]*factor.Node
	visiting    map[string]bool
	depth       int
	heights     map[*factor.Node]int
}

func names[V any](items map[string]V) []string {
	result := make([]string, 0, len(items))
	for name := range items {
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}
func identifier(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		if !(r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || i > 0 && r >= '0' && r <= '9') {
			return false
		}
	}
	return true
}
func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

// Compile validates every declaration, including unused lets. Only dependencies
// reachable from Outputs are retained in the resulting execution plan.
func Compile(spec Spec) (*factor.Plan, error) {
	if spec.SchemaVersion != 1 {
		return nil, fmt.Errorf("schema_version: expected 1, got %d", spec.SchemaVersion)
	}
	if strings.TrimSpace(spec.TimeFrame) == "" {
		return nil, fmt.Errorf("timeframe: decision timeframe required")
	}
	if len(spec.Outputs) == 0 {
		return nil, fmt.Errorf("outputs: at least one output required")
	}
	if len(spec.Lets)+len(spec.Outputs) > maxDefinitions || len(spec.Bindings) > maxDefinitions || len(spec.Params) > maxDefinitions {
		return nil, fmt.Errorf("spec: declaration count exceeds %d", maxDefinitions)
	}
	for _, name := range names(spec.Bindings) {
		b := spec.Bindings[name]
		if !identifier(name) || name == "factor" || name == "param" || name == "label" || name == "ts" || name == "cs" || name == "group" {
			return nil, fmt.Errorf("bindings.%s: invalid or reserved alias", name)
		}
		if strings.TrimSpace(b.Source) == "" || strings.TrimSpace(b.TimeFrame) == "" {
			return nil, fmt.Errorf("bindings.%s: source and timeframe required", name)
		}
		switch b.Sampling {
		case "", "source-events":
			if b.TimeFrame != spec.TimeFrame || b.MaxAgeMS != 0 {
				return nil, fmt.Errorf("bindings.%s: cross-timeframe data requires asof sampling and positive max_age_ms", name)
			}
		case "asof", "asof-latest":
			if b.MaxAgeMS <= 0 {
				return nil, fmt.Errorf("bindings.%s: asof requires positive max_age_ms", name)
			}
		default:
			return nil, fmt.Errorf("bindings.%s: unsupported sampling %q", name, b.Sampling)
		}
	}
	for _, name := range names(spec.Params) {
		if !identifier(name) || !finite(spec.Params[name]) {
			return nil, fmt.Errorf("params.%s: finite numeric parameter with valid name required", name)
		}
	}
	c := &compiler{spec: spec, definitions: map[string]*expression{}, paths: map[string]string{}, nodes: map[string]*factor.Node{}, visiting: map[string]bool{}, heights: map[*factor.Node]int{}}
	count, total := 0, 0
	for _, section := range []struct {
		name   string
		values map[string]string
	}{{"lets", spec.Lets}, {"outputs", spec.Outputs}} {
		for _, name := range names(section.values) {
			path := section.name + "." + name
			if !identifier(name) {
				return nil, fmt.Errorf("%s: valid factor name required", path)
			}
			if _, exists := c.definitions[name]; exists {
				return nil, fmt.Errorf("%s: duplicate factor name", path)
			}
			source := section.values[name]
			total += len(source)
			if total > maxTotalText {
				return nil, fmt.Errorf("%s: total expression text exceeds %d bytes", path, maxTotalText)
			}
			e, err := parse(path, source, &count)
			if err != nil {
				return nil, err
			}
			c.definitions[name], c.paths[name] = e, path
		}
	}
	// Lower and validate each definition separately so invalid unused nodes cannot
	// escape validation, while keeping them out of the final plan and subscriptions.
	for _, name := range names(c.definitions) {
		n, err := c.named(name)
		if err != nil {
			return nil, err
		}
		if _, err = factor.Compile(map[string]*factor.Node{name: n}); err != nil {
			return nil, c.error(c.paths[name], c.definitions[name], "%s", err)
		}
	}
	outputs := make(map[string]*factor.Node, len(spec.Outputs))
	for _, name := range names(spec.Outputs) {
		outputs[name] = c.nodes[name]
	}
	return factor.Compile(outputs)
}
func (c *compiler) error(path string, e *expression, format string, args ...any) error {
	pos := scanner.Position{Line: 1, Column: 1}
	if e != nil {
		pos = e.pos
	}
	return fmt.Errorf("%s:%d:%d: %s", path, pos.Line, pos.Column, fmt.Sprintf(format, args...))
}
func (c *compiler) named(name string) (*factor.Node, error) {
	if c.visiting[name] {
		return nil, c.error(c.paths[name], c.definitions[name], "cyclic factor reference %q", name)
	}
	if n := c.nodes[name]; n != nil {
		return n, nil
	}
	e, ok := c.definitions[name]
	if !ok {
		return nil, fmt.Errorf("unknown factor %q", name)
	}
	c.visiting[name] = true
	n, err := c.lower(c.paths[name], e)
	delete(c.visiting, name)
	if err == nil && c.height(n) > maxDepth {
		return nil, c.error(c.paths[name], e, "expanded factor DAG exceeds depth %d", maxDepth)
	}
	if err == nil {
		c.nodes[name] = n
	}
	return n, err
}

func (c *compiler) height(n *factor.Node) int {
	if depth, ok := c.heights[n]; ok {
		return depth
	}
	depth := 1
	for _, input := range n.Inputs {
		depth = max(depth, 1+c.height(input))
	}
	c.heights[n] = depth
	return depth
}

func hasCrossSection(n *factor.Node) bool {
	seen := map[*factor.Node]bool{}
	queue := []*factor.Node{n}
	for len(queue) > 0 {
		last := len(queue) - 1
		n = queue[last]
		queue = queue[:last]
		if seen[n] {
			continue
		}
		seen[n] = true
		if n.Spec.Kind == factor.CS || n.Spec.Kind == factor.GROUP {
			return true
		}
		queue = append(queue, n.Inputs...)
	}
	return false
}
func (c *compiler) number(path string, e *expression) (float64, error) {
	if e == nil {
		return 0, c.error(path, e, "numeric constant or parameter required")
	}
	switch e.kind {
	case 'n':
		v, err := strconv.ParseFloat(e.text, 64)
		if err == nil && finite(v) {
			return v, nil
		}
	case 'u':
		v, err := c.number(path, e.args[0])
		if err != nil {
			return 0, err
		}
		if e.text == "-" {
			v = -v
		}
		return v, nil
	case 'r':
		if strings.HasPrefix(e.text, "param.") {
			v, ok := c.spec.Params[strings.TrimPrefix(e.text, "param.")]
			if ok {
				return v, nil
			}
			return 0, c.error(path, e, "unknown parameter %q", e.text)
		}
	}
	return 0, c.error(path, e, "finite numeric literal or param.name required")
}
func (c *compiler) field(path string, e *expression, alias, name string) (*factor.Node, error) {
	b, ok := c.spec.Bindings[alias]
	if !ok {
		return nil, c.error(path, e, "unknown binding %q", alias)
	}
	if name == "" {
		return nil, c.error(path, e, "field name must not be empty")
	}
	if b.Sampling == "asof" || b.Sampling == "asof-latest" {
		return factor.AsOfField(b.Source, name, b.TimeFrame, c.spec.TimeFrame, b.MaxAgeMS), nil
	}
	return factor.Field(b.Source, name, b.TimeFrame), nil
}
func (c *compiler) lower(path string, e *expression) (*factor.Node, error) {
	c.depth++
	defer func() { c.depth-- }()
	if c.depth > maxDepth {
		return nil, c.error(path, e, "expanded factor expression exceeds depth %d", maxDepth)
	}
	switch e.kind {
	case 'n':
		v, err := c.number(path, e)
		if err != nil {
			return nil, err
		}
		return factor.Constant(v, c.spec.TimeFrame), nil
	case 'r':
		parts := strings.Split(e.text, ".")
		if len(parts) != 2 {
			return nil, c.error(path, e, "expected alias.field, factor.name or param.name")
		}
		switch parts[0] {
		case "label":
			return nil, c.error(path, e, "inference cannot reference label namespace")
		case "factor":
			if _, ok := c.definitions[parts[1]]; !ok {
				return nil, c.error(path, e, "unknown factor %q", parts[1])
			}
			return c.named(parts[1])
		case "param":
			v, err := c.number(path, e)
			if err != nil {
				return nil, err
			}
			return factor.Constant(v, c.spec.TimeFrame), nil
		default:
			return c.field(path, e, parts[0], parts[1])
		}
	case 'u':
		n, err := c.lower(path, e.args[0])
		if err != nil {
			return nil, err
		}
		if e.text == "-" {
			return factor.Neg(n), nil
		}
		return n, nil
	case '+', '-', '*', '/':
		a, err := c.lower(path, e.args[0])
		if err != nil {
			return nil, err
		}
		b, err := c.lower(path, e.args[1])
		if err != nil {
			return nil, err
		}
		switch e.kind {
		case '+':
			return factor.Add(a, b), nil
		case '-':
			return factor.Sub(a, b), nil
		case '*':
			return factor.Mul(a, b), nil
		default:
			return factor.Div(a, b), nil
		}
	case 'c':
		return c.call(path, e)
	default:
		return nil, c.error(path, e, "string literals are only allowed in field(alias, name)")
	}
}

func (c *compiler) call(path string, e *expression) (*factor.Node, error) {
	if n, handled, err := c.indicatorCall(path, e); handled {
		return n, err
	}
	arities := map[string]int{"field": 2, "positive": 1, "abs": 1, "log": 1, "sqrt": 1, "pow": 2, "min": 2, "max": 2, "ts.lag": 2, "ts.return": 2, "ts.ema": 2, "ts.std": 3, "cs.rank": 1, "cs.zscore": 1, "cs.robust_zscore": 1, "cs.mad_winsorize": 2, "cs.winsorize": 2, "cs.quantile": 2, "group.residual": 2, "group.demean": 3, "group.zscore": 3}
	if e.text == "group.ols" || e.text == "group.wls" {
		minimum := 2
		if e.text == "group.wls" {
			minimum = 3
		}
		if len(e.args) < minimum {
			return nil, c.error(path, e, "neutralization requires exposures")
		}
		nodes := make([]*factor.Node, len(e.args))
		for i, arg := range e.args {
			var err error
			nodes[i], err = c.lower(path, arg)
			if err != nil {
				return nil, err
			}
		}
		if minimum == 3 {
			return factor.WeightedResidual(nodes[0], nodes[1], nodes[2:]...), nil
		}
		return factor.MultiResidual(nodes[0], nodes[1:]...), nil
	}
	count, ok := arities[e.text]
	if !ok {
		return nil, c.error(path, e, "unknown function %q", e.text)
	}
	if len(e.args) != count {
		return nil, c.error(path, e, "%s expects %d arguments, got %d", e.text, count, len(e.args))
	}
	if e.text == "field" {
		if e.args[0].kind != 's' || e.args[1].kind != 's' {
			return nil, c.error(path, e, "field expects two string literals")
		}
		return c.field(path, e, e.args[0].text, e.args[1].text)
	}
	a, err := c.lower(path, e.args[0])
	if err != nil {
		return nil, err
	}
	if e.text == "group.demean" || e.text == "group.zscore" {
		if e.args[1].kind != 's' || e.args[2].kind != 's' {
			return nil, c.error(path, e, "group source and field must be string literals")
		}
		field, err := c.field(path, e, e.args[1].text, e.args[2].text)
		if err != nil {
			return nil, err
		}
		if e.text == "group.demean" {
			return factor.GroupDemean(a, field.Spec.Source, field.Spec.Field, field.Spec.SourceTimeFrame), nil
		}
		return factor.GroupZScore(a, field.Spec.Source, field.Spec.Field, field.Spec.SourceTimeFrame), nil
	}
	switch e.text {
	case "positive":
		return factor.Positive(a), nil
	case "abs":
		return factor.Abs(a), nil
	case "log":
		return factor.Log(a), nil
	case "sqrt":
		return factor.Sqrt(a), nil
	case "cs.rank":
		return factor.Rank(a), nil
	case "cs.zscore":
		return factor.ZScore(a), nil
	case "cs.robust_zscore":
		return factor.RobustZScore(a), nil
	case "pow", "min", "max", "group.residual":
		b, err := c.lower(path, e.args[1])
		if err != nil {
			return nil, err
		}
		switch e.text {
		case "pow":
			return factor.Pow(a, b), nil
		case "min":
			return factor.Min(a, b), nil
		case "max":
			return factor.Max(a, b), nil
		default:
			return factor.Residual(a, b), nil
		}
	}
	v, err := c.number(path, e.args[1])
	if err != nil {
		return nil, err
	}
	switch e.text {
	case "cs.mad_winsorize":
		if v <= 0 {
			return nil, c.error(path, e.args[1], "MAD multiple must be positive")
		}
		return factor.MADWinsorize(a, v), nil
	case "cs.winsorize":
		if v < 0 || v >= .5 {
			return nil, c.error(path, e.args[1], "winsorize tail must be [0,0.5)")
		}
		return factor.Winsorize(a, v), nil
	case "cs.quantile":
		if v < 0 || v > 1 {
			return nil, c.error(path, e.args[1], "quantile must be [0,1]")
		}
		return factor.Quantile(a, v), nil
	}
	if e.text == "ts.lag" && v == 0 {
		return a, nil
	}
	if hasCrossSection(a) {
		return nil, c.error(path, e, "TS windows over cross-section results are not supported")
	}
	if v < 1 || v > maxWindow || v != math.Trunc(v) {
		return nil, c.error(path, e.args[1], "window must be an integer in [1,%d] (lag also permits zero)", maxWindow)
	}
	period := int(v)
	switch e.text {
	case "ts.lag":
		return factor.Lag(a, period), nil
	case "ts.return":
		return factor.Return(a, period), nil
	case "ts.ema":
		return factor.EMA(a, period), nil
	default:
		ddof, err := c.number(path, e.args[2])
		if err != nil {
			return nil, err
		}
		if ddof < 0 || ddof >= v || ddof != math.Trunc(ddof) {
			return nil, c.error(path, e.args[2], "ddof must be an integer in [0,window)")
		}
		return factor.StdDev(a, period, int(ddof)), nil
	}
}

func (c *compiler) indicatorCall(path string, e *expression) (*factor.Node, bool, error) {
	inputCount, parameterCount := 1, 1
	switch e.text {
	case "ts.sma", "ts.rma", "ts.wma", "ts.rsi", "ts.roc", "ts.mom", "ts.cci", "ts.highest", "ts.lowest":
	case "ts.vwma":
		inputCount = 2
	case "ts.tr":
		inputCount, parameterCount = 3, 0
	case "ts.atr", "ts.stoch", "ts.willr":
		inputCount = 3
	case "ts.obv":
		inputCount, parameterCount = 2, 0
	case "ts.mfi":
		inputCount = 4
	case "ts.macd", "ts.macd_signal", "ts.macd_hist", "ts.bbands_upper", "ts.bbands_middle", "ts.bbands_lower":
		parameterCount = 3
	default:
		return nil, false, nil
	}
	count := inputCount + parameterCount
	if len(e.args) != count {
		return nil, true, c.error(path, e, "%s expects %d arguments, got %d", e.text, count, len(e.args))
	}
	inputs := make([]*factor.Node, inputCount)
	for i := range inputs {
		n, err := c.lower(path, e.args[i])
		if err != nil {
			return nil, true, err
		}
		if hasCrossSection(n) {
			return nil, true, c.error(path, e.args[i], "TS indicators over cross-section results are not supported")
		}
		inputs[i] = n
	}
	parameters := make([]float64, parameterCount)
	for i := range parameters {
		arg := e.args[inputCount+i]
		value, err := c.number(path, arg)
		if err != nil {
			return nil, true, err
		}
		if strings.HasPrefix(e.text, "ts.bbands_") && i > 0 {
			if value < 0 {
				return nil, true, c.error(path, arg, "standard deviation multiplier must be nonnegative")
			}
		} else if value < 1 || value > factor.MaxIndicatorPeriod || value != math.Trunc(value) {
			return nil, true, c.error(path, arg, "window must be an integer in [1,%d]", factor.MaxIndicatorPeriod)
		}
		parameters[i] = value
	}
	a := inputs[0]
	period := 0
	if len(parameters) > 0 {
		period = int(parameters[0])
	}
	var result *factor.Node
	switch e.text {
	case "ts.sma":
		result = factor.SMA(a, period)
	case "ts.rma":
		result = factor.RMA(a, period)
	case "ts.wma":
		result = factor.WMA(a, period)
	case "ts.vwma":
		result = factor.VWMA(a, inputs[1], period)
	case "ts.rsi":
		result = factor.RSI(a, period)
	case "ts.roc":
		result = factor.ROC(a, period)
	case "ts.mom":
		result = factor.MOM(a, period)
	case "ts.tr":
		result = factor.TR(a, inputs[1], inputs[2])
	case "ts.atr":
		result = factor.ATR(a, inputs[1], inputs[2], period)
	case "ts.cci":
		result = factor.CCI(a, period)
	case "ts.stoch":
		result = factor.Stoch(a, inputs[1], inputs[2], period)
	case "ts.willr":
		result = factor.WillR(a, inputs[1], inputs[2], period)
	case "ts.obv":
		result = factor.OBV(a, inputs[1])
	case "ts.mfi":
		result = factor.MFI(a, inputs[1], inputs[2], inputs[3], period)
	case "ts.highest":
		result = factor.Highest(a, period)
	case "ts.lowest":
		result = factor.Lowest(a, period)
	case "ts.macd", "ts.macd_signal", "ts.macd_hist":
		if parameters[0] >= parameters[1] {
			return nil, true, c.error(path, e, "MACD requires fast < slow")
		}
		line, signal, hist := factor.MACD(a, period, int(parameters[1]), int(parameters[2]))
		switch e.text {
		case "ts.macd":
			result = line
		case "ts.macd_signal":
			result = signal
		default:
			result = hist
		}
	case "ts.bbands_upper", "ts.bbands_middle", "ts.bbands_lower":
		upper, middle, lower := factor.BBands(a, period, parameters[1], parameters[2])
		switch e.text {
		case "ts.bbands_upper":
			result = upper
		case "ts.bbands_middle":
			result = middle
		default:
			result = lower
		}
	}
	return result, true, nil
}
