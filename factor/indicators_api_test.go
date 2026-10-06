package factor

import (
	"maps"
	"math"
	"slices"
	"testing"
)

func indicatorAPINodes() map[string]*Node {
	c, h, l, v := Field("prices", "close", "1h"), Field("prices", "high", "1h"), Field("prices", "low", "1h"), Field("prices", "volume", "1h")
	line, signal, hist := MACD(c, 2, 5, 3)
	upper, middle, lower := BBands(c, 3, 2, 1.5)
	return map[string]*Node{
		"sma": SMA(c, 3), "rma": RMA(c, 3), "wma": WMA(c, 3), "vwma": VWMA(c, v, 3),
		"rsi": RSI(c, 3), "roc": ROC(c, 3), "mom": MOM(c, 3), "tr": TR(h, l, c), "atr": ATR(h, l, c, 3),
		"cci": CCI(c, 3), "stoch": Stoch(h, l, c, 3), "willr": WillR(h, l, c, 3), "obv": OBV(c, v),
		"mfi": MFI(h, l, c, v, 3), "highest": Highest(c, 3), "lowest": Lowest(c, 3),
		"macd": line, "macd-signal": signal, "macd-hist": hist,
		"bbands-upper": upper, "bbands-middle": middle, "bbands-lower": lower,
	}
}

func cloneIndicatorNode(n *Node) *Node {
	c := *n
	c.Inputs = slices.Clone(n.Inputs)
	c.Spec.Parameters = maps.Clone(n.Spec.Parameters)
	return &c
}

func TestIndicatorAPIContractsAndLookbacks(t *testing.T) {
	nodes := indicatorAPINodes()
	if len(nodes) != 22 {
		t.Fatalf("scalar operator count = %d", len(nodes))
	}
	for name, n := range nodes {
		t.Run(name, func(t *testing.T) {
			plan, err := Compile(map[string]*Node{"value": n})
			if err != nil {
				t.Fatal(err)
			}
			warmup, retention := 2, 3
			switch name {
			case "rsi", "roc", "mom", "atr":
				warmup, retention = 3, 4
			case "tr":
				warmup, retention = 1, 2
			case "obv":
				warmup, retention = 0, 2
			case "macd":
				warmup, retention = 4, 5
			case "macd-signal", "macd-hist":
				warmup, retention = 6, 7
			}
			if plan.WarmupLength() != warmup || plan.StateRetention() != retention {
				t.Fatalf("warmup/retention = %d/%d, want %d/%d", plan.WarmupLength(), plan.StateRetention(), warmup, retention)
			}
			if n.Spec.Operator != name || !n.Spec.Batch || !n.Spec.Incremental || n.Spec.Version != "indicators-1/banta-0.4.1" {
				t.Fatalf("unexpected contract: %+v", n.Spec)
			}
			if plan.Inputs()[0].WarmupLength != warmup {
				t.Fatal("raw subscription did not inherit lookback")
			}
		})
	}
	c := Field("prices", "close", "1h")
	plan, err := Compile(map[string]*Node{"value": SMA(RSI(c, 3), 4)})
	if err != nil || plan.WarmupLength() != 6 {
		t.Fatalf("nested warmup: plan=%+v err=%v", plan, err)
	}
	if EMA(c, 3).Spec.Version != "builtin-1/banta-0.4.1" || Rank(c).Spec.Version != "builtin-2/banta-0.4.1" {
		t.Fatal("existing operator version changed")
	}
}

func TestIndicatorAPIRejectsMalformedContracts(t *testing.T) {
	for name, n := range indicatorAPINodes() {
		t.Run(name, func(t *testing.T) {
			mutations := map[string]func(*Node){
				"missing dependency":  func(n *Node) { n.Inputs = n.Inputs[:len(n.Inputs)-1] },
				"extra dependency":    func(n *Node) { n.Inputs = append(n.Inputs, n.Inputs[0]) },
				"nil dependency":      func(n *Node) { n.Inputs[0] = nil },
				"unknown parameter":   func(n *Node) { n.Spec.Parameters["unknown"] = 1 },
				"nonfinite parameter": func(n *Node) { n.Spec.Parameters["unknown"] = math.Inf(1) },
				"missing incremental": func(n *Node) { n.Spec.Incremental = false },
				"missing batch":       func(n *Node) { n.Spec.Batch = false },
			}
			for parameter := range n.Spec.Parameters {
				mutations["missing "+parameter] = func(n *Node) { delete(n.Spec.Parameters, parameter) }
				mutations["renamed "+parameter] = func(n *Node) {
					value := n.Spec.Parameters[parameter]
					delete(n.Spec.Parameters, parameter)
					n.Spec.Parameters["typo"] = value
				}
				mutations["nan "+parameter] = func(n *Node) { n.Spec.Parameters[parameter] = math.NaN() }
				mutations["infinite "+parameter] = func(n *Node) { n.Spec.Parameters[parameter] = math.Inf(-1) }
				mutations["negative "+parameter] = func(n *Node) { n.Spec.Parameters[parameter] = -1 }
				if parameter == "std_up" || parameter == "std_down" {
					continue
				}
				mutations["zero "+parameter] = func(n *Node) { n.Spec.Parameters[parameter] = 0 }
				mutations["fractional "+parameter] = func(n *Node) { n.Spec.Parameters[parameter] = 1.5 }
				mutations["huge "+parameter] = func(n *Node) { n.Spec.Parameters[parameter] = math.MaxFloat64 }
			}
			for mutation, apply := range mutations {
				t.Run(mutation, func(t *testing.T) {
					copy := cloneIndicatorNode(n)
					apply(copy)
					if _, err := Compile(map[string]*Node{"value": copy}); err == nil {
						t.Fatal("accepted malformed indicator")
					}
				})
			}
		})
	}
}

func TestIndicatorAPIBoundariesAndDependencyIdentity(t *testing.T) {
	c, v := Field("prices", "close", "1h"), Field("prices", "volume", "1h")
	upper, middle, lower := BBands(c, 1, 0, 0)
	line, signal, hist := MACD(c, 1, 2, 1)
	for _, n := range []*Node{SMA(c, 1), RSI(c, 1), SMA(c, MaxIndicatorPeriod), upper, middle, lower, line, signal, hist} {
		if _, err := Compile(map[string]*Node{"value": n}); err != nil {
			t.Fatal(err)
		}
	}
	for _, args := range [][3]int{{3, 3, 1}, {4, 3, 1}} {
		line, _, _ := MACD(c, args[0], args[1], args[2])
		if _, err := Compile(map[string]*Node{"value": line}); err == nil {
			t.Fatal("accepted MACD fast >= slow")
		}
	}
	for _, n := range []*Node{VWMA(c, Field("prices", "volume", "1d"), 3), ATR(c, v, Field("prices", "close", "1d"), 3), MFI(c, c, c, Field("prices", "volume", "1d"), 3)} {
		if _, err := Compile(map[string]*Node{"value": n}); err == nil {
			t.Fatal("accepted mixed-timeframe technical inputs")
		}
	}
	compileHash := func(n *Node) string {
		t.Helper()
		plan, err := Compile(map[string]*Node{"value": n})
		if err != nil {
			t.Fatal(err)
		}
		return plan.Hash()
	}
	base := VWMA(c, v, 3)
	baseHash := compileHash(base)
	for _, n := range []*Node{VWMA(c, Field("prices", "other-volume", "1h"), 3), VWMA(v, c, 3), VWMA(c, v, 4), VWMA(c, Field("alternate", "volume", "1h"), 3)} {
		if compileHash(n) == baseHash {
			t.Fatal("secondary dependency/order/parameter omitted from identity")
		}
	}
	copy := VWMA(Field("prices", "close", "1h"), Field("prices", "volume", "1h"), 3)
	if compileHash(copy) != baseHash {
		t.Fatal("equivalent technical DAGs differ")
	}
	copy.Spec.Version += "/changed"
	if compileHash(copy) == baseHash {
		t.Fatal("indicator version omitted from identity")
	}
	plan, err := Compile(map[string]*Node{"a": base, "b": VWMA(c, v, 3)})
	if err != nil || plan.NodeCount() != 3 {
		t.Fatalf("CSE failed: plan=%+v err=%v", plan, err)
	}
	hash := plan.Hash()
	base.Spec.Parameters["period"] = 50
	v.Spec.Field = "changed"
	if plan.Hash() != hash || plan.Inputs()[0].Fields[1] != "volume" {
		t.Fatal("builder mutation changed compiled indicator")
	}
}
