package factor

import "math"

// Constant broadcasts a finite value on the plan's explicit decision grid.
// It has no source subscription; the snapshot still supplies the active SIDs.
func Constant(value float64, timeframe string) *Node {
	n := node("constant", TS)
	n.Spec.TimeFrame = timeframe
	n.Spec.Parameters["value"] = value
	return n
}

func Add(a, b *Node) *Node   { return node("add", TS, a, b) }
func Sub(a, b *Node) *Node   { return node("sub", TS, a, b) }
func Mul(a, b *Node) *Node   { return node("mul", TS, a, b) }
func Div(a, b *Node) *Node   { return node("div", TS, a, b) }
func Pow(a, b *Node) *Node   { return node("pow", TS, a, b) }
func Min(a, b *Node) *Node   { return node("min", TS, a, b) }
func Max(a, b *Node) *Node   { return node("max", TS, a, b) }
func Neg(input *Node) *Node  { return node("neg", TS, input) }
func Abs(input *Node) *Node  { return node("abs", TS, input) }
func Log(input *Node) *Node  { return node("log", TS, input) }
func Sqrt(input *Node) *Node { return node("sqrt", TS, input) }

// Positive rejects nonpositive observations before downstream arithmetic.
func Positive(input *Node) *Node { return node("positive", TS, input) }

// Sign maps strictly positive values to 1, negative values to -1, and zero to 0.
func Sign(input *Node) *Node { return node("sign", TS, input) }

// Clip clamps valid observations to [lo, hi] (requires finite lo <= hi).
func Clip(input *Node, lo, hi float64) *Node {
	n := node("clip", TS, input)
	n.Spec.Parameters["lo"] = lo
	n.Spec.Parameters["hi"] = hi
	return n
}

func Gt(a, b *Node) *Node { return node("gt", TS, a, b) }
func Ge(a, b *Node) *Node { return node("ge", TS, a, b) }
func Lt(a, b *Node) *Node { return node("lt", TS, a, b) }
func Le(a, b *Node) *Node { return node("le", TS, a, b) }

// Where selects thenNode when cond is Valid and cond > 0, otherwise elseNode.
// When cond is Valid, only the selected branch needs to be Valid; unselected
// domain errors (such as Log(x) when x <= 0) do not invalidate the result.
func Where(cond, thenNode, elseNode *Node) *Node {
	return node("where", TS, cond, thenNode, elseNode)
}

func pointwiseArity(operator string) (int, bool) {
	switch operator {
	case "constant":
		return 0, true
	case "neg", "abs", "log", "sqrt", "positive", "sign", "clip":
		return 1, true
	case "add", "sub", "mul", "div", "pow", "min", "max", "gt", "ge", "lt", "le":
		return 2, true
	case "where":
		return 3, true
	default:
		return 0, false
	}
}

// evaluatePointwise is shared by the incremental and batch backends. Invalid
// inputs propagate in declared operand order, except Where which short-circuits
// branch validity after validating cond; domain errors and overflow remain NonFinite.
func evaluatePointwise(spec NodeSpec, inputs []Numeric) (Numeric, bool) {
	if _, ok := pointwiseArity(spec.Operator); !ok {
		return Numeric{}, false
	}
	if spec.Operator == "where" {
		if inputs[0].Validity != Valid {
			return normalized(inputs[0]), true
		}
		if inputs[0].Value > 0 {
			return normalized(inputs[1]), true
		}
		return normalized(inputs[2]), true
	}
	for _, input := range inputs {
		if input.Validity != Valid {
			return input, true
		}
	}
	var value float64
	switch spec.Operator {
	case "constant":
		value = spec.Parameters["value"]
	case "add":
		value = inputs[0].Value + inputs[1].Value
	case "sub":
		value = inputs[0].Value - inputs[1].Value
	case "mul":
		value = inputs[0].Value * inputs[1].Value
	case "div":
		value = inputs[0].Value / inputs[1].Value
	case "pow":
		value = math.Pow(inputs[0].Value, inputs[1].Value)
	case "min":
		value = math.Min(inputs[0].Value, inputs[1].Value)
	case "max":
		value = math.Max(inputs[0].Value, inputs[1].Value)
	case "gt":
		if inputs[0].Value > inputs[1].Value {
			value = 1
		}
	case "ge":
		if inputs[0].Value >= inputs[1].Value {
			value = 1
		}
	case "lt":
		if inputs[0].Value < inputs[1].Value {
			value = 1
		}
	case "le":
		if inputs[0].Value <= inputs[1].Value {
			value = 1
		}
	case "neg":
		value = -inputs[0].Value
	case "abs":
		value = math.Abs(inputs[0].Value)
	case "log":
		value = math.Log(inputs[0].Value)
	case "sqrt":
		value = math.Sqrt(inputs[0].Value)
	case "positive":
		value = inputs[0].Value
		if value <= 0 {
			value = math.NaN()
		}
	case "sign":
		if inputs[0].Value > 0 {
			value = 1
		} else if inputs[0].Value < 0 {
			value = -1
		}
	case "clip":
		lo, hi := spec.Parameters["lo"], spec.Parameters["hi"]
		value = inputs[0].Value
		if value < lo {
			value = lo
		} else if value > hi {
			value = hi
		}
		if value == 0 {
			value = 0
		}
	}
	return numeric(value), true
}
