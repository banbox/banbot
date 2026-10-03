package factor

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
)

type NodeKind string

const (
	TS    NodeKind = "TS"
	CS    NodeKind = "CS"
	GROUP NodeKind = "GROUP"
)

type NodeSpec struct {
	Operator                 string
	Version                  string
	Kind                     NodeKind
	Source                   string
	SourceFrequency          string
	Field                    string
	GroupField               string
	Frequency                string
	Parameters               map[string]float64
	OutputType               string
	MissingPolicy            string
	AvailabilityPolicy       string
	SamplingPolicy           string
	MaxAge                   int64
	ReferenceUniverse        string
	UniverseTransitionPolicy string
	WarmupLength             int
	StateRetention           int
	Incremental              bool
	Batch                    bool
}

// Node is a Go builder definition. Compile copies its declaration; mutating
// builder objects afterwards cannot change a compiled plan/cache identity.
type Node struct {
	Spec     NodeSpec
	Inputs   []*Node
	Evaluate func([]Numeric) Numeric
}

func node(operator string, kind NodeKind, inputs ...*Node) *Node {
	frequency := ""
	if len(inputs) > 0 && inputs[0] != nil {
		frequency = inputs[0].Spec.Frequency
	}
	version := "builtin-1/banta-0.4.1"
	if kind == CS || kind == GROUP {
		version = "builtin-2/banta-0.4.1"
	}
	return &Node{Spec: NodeSpec{Operator: operator, Version: version, Kind: kind, Frequency: frequency, Parameters: map[string]float64{}, OutputType: "float64+validity", MissingPolicy: "skip-invalid", AvailabilityPolicy: "closed-visible", SamplingPolicy: "source-events", ReferenceUniverse: "snapshot-reference", UniverseTransitionPolicy: "continue-per-asset", Incremental: true, Batch: true}, Inputs: inputs}
}

func Field(source, field, frequency string) *Node {
	n := node("field", TS)
	n.Spec.Source = source
	n.Spec.Field = field
	n.Spec.Frequency = frequency
	return n
}

// AsOfField explicitly samples a slower/event source on the decision grid.
// Downstream TS periods count decision observations, NOT source publications.
// To compute a native daily indicator, supply its output as a daily source and
// sample that output here; this method never pretends hourly samples are days.
func AsOfField(source, field, sourceFrequency, decisionFrequency string, maxAge int64) *Node {
	n := Field(source, field, decisionFrequency)
	n.Spec.SourceFrequency = sourceFrequency
	n.Spec.AvailabilityPolicy = "asof-latest"
	n.Spec.SamplingPolicy = "decision-grid"
	n.Spec.MaxAge = maxAge
	return n
}
func Lag(input *Node, period int) *Node {
	n := node("lag", TS, input)
	n.Spec.Parameters["period"] = float64(period)
	return n
}
func Return(input *Node, period int) *Node {
	n := node("return", TS, input)
	n.Spec.Parameters["period"] = float64(period)
	return n
}
func EMA(input *Node, period int) *Node {
	n := node("ema", TS, input)
	n.Spec.Parameters["period"] = float64(period)
	return n
}
func StdDev(input *Node, period, ddof int) *Node {
	n := node("stddev", TS, input)
	n.Spec.Parameters["period"] = float64(period)
	n.Spec.Parameters["ddof"] = float64(ddof)
	return n
}
func Rank(input *Node) *Node   { return node("rank", CS, input) }
func ZScore(input *Node) *Node { return node("zscore", CS, input) }
func Winsorize(input *Node, tail float64) *Node {
	n := node("winsorize", CS, input)
	n.Spec.Parameters["tail"] = tail
	return n
}
func Quantile(input *Node, q float64) *Node {
	n := node("quantile", CS, input)
	n.Spec.Parameters["q"] = q
	return n
}
func GroupDemean(input *Node, source, field string, sourceFrequency ...string) *Node {
	n := node("group-demean", GROUP, input)
	n.Spec.Source = source
	n.Spec.GroupField = field
	if len(sourceFrequency) > 0 {
		n.Spec.SourceFrequency = sourceFrequency[0]
	}
	return n
}
func GroupZScore(input *Node, source, field string, sourceFrequency ...string) *Node {
	n := node("group-zscore", GROUP, input)
	n.Spec.Source = source
	n.Spec.GroupField = field
	if len(sourceFrequency) > 0 {
		n.Spec.SourceFrequency = sourceFrequency[0]
	}
	return n
}
func Residual(y, x *Node) *Node { return node("residual", GROUP, y, x) }

// Custom declares a versioned pure pointwise Go evaluator with explicit
// dependencies. Stateful custom indicators require a separate tested contract;
// they must not hide mutable state in this callback.
func Custom(version string, inputs []*Node, evaluate func([]Numeric) Numeric) *Node {
	n := node("custom", TS, inputs...)
	n.Spec.Version = version
	n.Evaluate = evaluate
	return n
}
func Linear(inputs []*Node, weights []float64) *Node {
	n := node("linear", TS, inputs...)
	for i, w := range weights {
		n.Spec.Parameters[fmt.Sprintf("weight%d", i)] = w
	}
	return n
}

type Builder struct{ outputs map[string]*Node }

func New() *Builder                                  { return &Builder{outputs: make(map[string]*Node)} }
func (b *Builder) Add(name string, n *Node) *Builder { b.outputs[name] = n; return b }
func (b *Builder) Compile() (*Plan, error)           { return Compile(b.outputs) }

type compiledNode struct {
	id       string
	spec     NodeSpec
	inputs   []int
	evaluate func([]Numeric) Numeric
}
type Plan struct {
	hash      string
	nodes     []compiledNode
	outputs   map[string]int
	frequency string
	retention int
	warmup    int
}

func (p *Plan) Hash() string        { return p.hash }
func (p *Plan) NodeCount() int      { return len(p.nodes) }
func (p *Plan) WarmupLength() int   { return p.warmup }
func (p *Plan) StateRetention() int { return p.retention }
func (p *Plan) Frequency() string   { return p.frequency }

type InputSpec struct {
	Source       string
	Frequency    string
	Fields       []string
	WarmupLength int
	AsOfLatest   bool
	MaxAge       int64
}

// Inputs exposes the actual raw subscription union without importing data or
// strategies. Fields are sorted and independent copies, suitable for runners.
func (p *Plan) Inputs() []InputSpec {
	inputs := make(map[string]*InputSpec)
	for _, node := range p.nodes {
		if node.spec.Source == "" {
			continue
		}
		key := node.spec.Source + "\x00" + node.spec.SourceFrequency
		input := inputs[key]
		if input == nil {
			input = &InputSpec{Source: node.spec.Source, Frequency: node.spec.SourceFrequency, AsOfLatest: node.spec.Operator != "field" || node.spec.AvailabilityPolicy == "asof-latest", MaxAge: node.spec.MaxAge}
			inputs[key] = input
		}
		field := node.spec.Field
		if field == "" {
			field = node.spec.GroupField
		}
		input.Fields = append(input.Fields, field)
		if node.spec.Operator == "field" {
			input.WarmupLength = max(input.WarmupLength, p.warmup)
			if node.spec.AvailabilityPolicy != "asof-latest" {
				input.AsOfLatest = false
			}
		}
		if input.MaxAge == 0 || (node.spec.MaxAge > 0 && node.spec.MaxAge < input.MaxAge) {
			input.MaxAge = node.spec.MaxAge
		}
	}
	keys := slices.Collect(maps.Keys(inputs))
	sort.Strings(keys)
	result := make([]InputSpec, 0, len(keys))
	for _, key := range keys {
		input := *inputs[key]
		sort.Strings(input.Fields)
		input.Fields = slices.Compact(input.Fields)
		result = append(result, input)
	}
	return result
}
func (p *Plan) Outputs() []string {
	names := slices.Collect(maps.Keys(p.outputs))
	sort.Strings(names)
	return names
}

func Compile(outputs map[string]*Node) (*Plan, error) {
	if len(outputs) == 0 {
		return nil, errors.New("factor: empty inference plan")
	}
	plan := &Plan{outputs: make(map[string]int), retention: 2}
	visiting := make(map[*Node]bool)
	visited := make(map[*Node]int)
	canonicalNodes := make(map[string]int)
	var visitNode func(*Node) (int, error)
	visitNode = func(n *Node) (int, error) {
		if n == nil {
			return 0, errors.New("factor: nil dependency")
		}
		if visiting[n] {
			return 0, errors.New("factor: cyclic dependency")
		}
		if index, ok := visited[n]; ok {
			return index, nil
		}
		visiting[n] = true
		spec := n.Spec
		spec.Parameters = maps.Clone(spec.Parameters)
		if spec.Source != "" && spec.SourceFrequency == "" {
			spec.SourceFrequency = spec.Frequency
		}
		if spec.Operator == "label" {
			return 0, errors.New("factor: inference cannot depend on label namespace")
		}
		if err := validateNode(spec, len(n.Inputs), n.Evaluate != nil); err != nil {
			return 0, err
		}
		if plan.frequency == "" {
			plan.frequency = spec.Frequency
		} else if plan.frequency != spec.Frequency {
			return 0, errors.New("factor: mixed frequencies require explicitly resampled input")
		}
		inputs := make([]int, len(n.Inputs))
		dependencyIDs := make([]string, len(inputs))
		dependencyWarmup := 0
		for i, input := range n.Inputs {
			index, err := visitNode(input)
			if err != nil {
				return 0, err
			}
			inputs[i] = index
			dependencyIDs[i] = plan.nodes[index].id
			dependencyWarmup = max(dependencyWarmup, plan.nodes[index].spec.WarmupLength)
		}
		period := int(spec.Parameters["period"])
		switch spec.Operator {
		case "lag", "return":
			spec.WarmupLength = dependencyWarmup + period
			spec.StateRetention = max(spec.StateRetention, period+1)
		case "ema", "stddev":
			spec.WarmupLength = dependencyWarmup + period - 1
			spec.StateRetention = max(spec.StateRetention, period)
		default:
			spec.WarmupLength = max(spec.WarmupLength, dependencyWarmup)
			spec.StateRetention = max(spec.StateRetention, 1)
		}
		id, err := contentHash(struct {
			Spec         NodeSpec
			Dependencies []string
		}{spec, dependencyIDs})
		if err != nil {
			return 0, err
		}
		if index, exists := canonicalNodes[id]; exists {
			visited[n] = index
			delete(visiting, n)
			return index, nil
		}
		index := len(plan.nodes)
		plan.nodes = append(plan.nodes, compiledNode{id, spec, inputs, n.Evaluate})
		canonicalNodes[id] = index
		visited[n] = index
		delete(visiting, n)
		plan.retention = max(plan.retention, spec.StateRetention)
		plan.warmup = max(plan.warmup, spec.WarmupLength)
		return index, nil
	}
	names := slices.Collect(maps.Keys(outputs))
	sort.Strings(names)
	for _, name := range names {
		if name == "" {
			return nil, errors.New("factor: empty output name")
		}
		index, err := visitNode(outputs[name])
		if err != nil {
			return nil, err
		}
		plan.outputs[name] = index
	}
	ids := make([]string, len(plan.nodes))
	for i, n := range plan.nodes {
		ids[i] = n.id
	}
	var err error
	plan.hash, err = contentHash(struct {
		Nodes   []string
		Outputs map[string]int
	}{ids, plan.outputs})
	return plan, err
}

func validateNode(spec NodeSpec, count int, custom bool) error {
	if spec.Version == "" || spec.Frequency == "" || spec.OutputType != "float64+validity" || spec.MissingPolicy != "skip-invalid" || (spec.AvailabilityPolicy != "closed-visible" && spec.AvailabilityPolicy != "asof-latest") || !spec.Incremental {
		return fmt.Errorf("factor: incomplete or unsupported node contract %s", spec.Operator)
	}
	if spec.SamplingPolicy != "source-events" && spec.SamplingPolicy != "decision-grid" {
		return errors.New("factor: explicit sampling policy required")
	}
	if spec.AvailabilityPolicy == "asof-latest" && (spec.SamplingPolicy != "decision-grid" || spec.MaxAge <= 0) {
		return errors.New("factor: asof numeric source requires decision-grid sampling and positive max age")
	}
	if spec.Operator == "field" && spec.SourceFrequency != spec.Frequency && spec.AvailabilityPolicy != "asof-latest" {
		return errors.New("factor: mixed source frequency requires explicit asof sampling")
	}
	if spec.Kind != TS && spec.Kind != CS && spec.Kind != GROUP {
		return errors.New("factor: invalid node kind")
	}
	if spec.WarmupLength < 0 || spec.StateRetention < 0 {
		return errors.New("factor: negative warmup/retention")
	}
	expectedKind := TS
	switch spec.Operator {
	case "rank", "zscore", "winsorize", "quantile":
		expectedKind = CS
	case "group-demean", "group-zscore", "residual":
		expectedKind = GROUP
	}
	if spec.Kind != expectedKind {
		return errors.New("factor: operator node kind mismatch")
	}
	if spec.Kind != TS && spec.ReferenceUniverse != "snapshot-reference" {
		return errors.New("factor: undeclared reference universe")
	}
	if spec.UniverseTransitionPolicy != "continue-per-asset" {
		return errors.New("factor: unsupported universe transition policy")
	}
	if _, err := contentHash(spec.Parameters); err != nil {
		return err
	}
	for _, value := range spec.Parameters {
		if numeric(value).Validity != Valid {
			return errors.New("factor: non-finite node parameter")
		}
	}
	if custom {
		if count == 0 {
			return errors.New("factor: custom node requires explicit dependencies")
		}
		if spec.Operator != "custom" || spec.Kind != TS {
			return errors.New("factor: custom evaluator must be a declared TS custom node")
		}
		return nil
	}
	if arity, ok := pointwiseArity(spec.Operator); ok {
		if count != arity {
			return fmt.Errorf("factor: %s requires %d dependencies", spec.Operator, arity)
		}
		if spec.Operator == "constant" {
			if _, exists := spec.Parameters["value"]; !exists || len(spec.Parameters) != 1 {
				return errors.New("factor: constant requires one declared value")
			}
		} else if len(spec.Parameters) != 0 {
			return fmt.Errorf("factor: %s does not accept scalar parameters", spec.Operator)
		}
		return nil
	}
	if spec.Operator == "field" {
		if count != 0 || spec.Source == "" || spec.Field == "" {
			return errors.New("factor: field requires source/field and no dependencies")
		}
		return nil
	}
	if spec.Operator == "linear" {
		if count == 0 || len(spec.Parameters) != count {
			return errors.New("factor: linear requires one weight per dependency")
		}
		for i := 0; i < count; i++ {
			if _, exists := spec.Parameters[fmt.Sprintf("weight%d", i)]; !exists {
				return errors.New("factor: missing declared linear weight")
			}
		}
		return nil
	}
	if spec.Operator == "residual" {
		if count != 2 {
			return errors.New("factor: residual requires y and x")
		}
		return nil
	}
	if count != 1 {
		return fmt.Errorf("factor: %s requires one dependency", spec.Operator)
	}
	switch spec.Operator {
	case "lag", "return", "ema", "stddev":
		period := spec.Parameters["period"]
		if period < 1 || period != float64(int(period)) {
			return errors.New("factor: positive integer period required")
		}
		if spec.Operator == "stddev" {
			ddof := spec.Parameters["ddof"]
			if ddof < 0 || ddof >= period || ddof != float64(int(ddof)) {
				return errors.New("factor: invalid ddof")
			}
		}
	case "rank", "zscore":
	case "winsorize":
		if q := spec.Parameters["tail"]; q < 0 || q >= 0.5 {
			return errors.New("factor: winsor tail must be [0,0.5)")
		}
	case "quantile":
		if q := spec.Parameters["q"]; q < 0 || q > 1 {
			return errors.New("factor: quantile must be [0,1]")
		}
	case "group-demean", "group-zscore":
		if spec.Source == "" || spec.GroupField == "" {
			return errors.New("factor: group source/field required")
		}
	default:
		return fmt.Errorf("factor: unregistered operator %s", spec.Operator)
	}
	return nil
}

func MomentumVolatility(source, field, frequency string, window int) (*Plan, error) {
	price := Field(source, field, frequency)
	momentum := Return(price, window)
	volatility := StdDev(Return(price, 1), window, 0)
	zMomentum := ZScore(Winsorize(momentum, 0.01))
	zVolatility := ZScore(Winsorize(volatility, 0.01))
	return New().Add("momentum", momentum).Add("volatility", volatility).Add("score", Linear([]*Node{zMomentum, zVolatility}, []float64{1, -1})).Compile()
}
