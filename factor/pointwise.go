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

func pointwiseArity(operator string) (int, bool) {
	switch operator {
	case "constant":
		return 0, true
	case "neg", "abs", "log", "sqrt", "positive":
		return 1, true
	case "add", "sub", "mul", "div", "pow", "min", "max":
		return 2, true
	default:
		return 0, false
	}
}

// evaluatePointwise is shared by the incremental and batch backends. Invalid
// inputs propagate in declared operand order, even for mathematically
// commutative operations; domain errors and overflow remain NonFinite.
func evaluatePointwise(spec NodeSpec, inputs []Numeric) (Numeric, bool) {
	if _, ok := pointwiseArity(spec.Operator); !ok {
		return Numeric{}, false
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
	}
	return numeric(value), true
}
