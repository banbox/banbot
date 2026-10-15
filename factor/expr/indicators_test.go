package expr

import (
	"fmt"
	"strings"
	"testing"

	"github.com/banbox/banbot/factor"
)

type indicatorExpressionCase struct {
	name string
	args []string
	node *factor.Node
}

func indicatorExpressionCases() []indicatorExpressionCase {
	c, h, l, v := factor.Field("prices", "close", "1h"), factor.Field("prices", "high", "1h"), factor.Field("prices", "low", "1h"), factor.Field("prices", "volume", "1h")
	line, signal, hist := factor.MACD(c, 2, 5, 3)
	upper, middle, lower := factor.BBands(c, 3, 2, 1.5)
	return []indicatorExpressionCase{
		{"sma", []string{"kline.close", "3"}, factor.SMA(c, 3)},
		{"rma", []string{"kline.close", "3"}, factor.RMA(c, 3)},
		{"wma", []string{"kline.close", "3"}, factor.WMA(c, 3)},
		{"vwma", []string{"kline.close", "kline.volume", "3"}, factor.VWMA(c, v, 3)},
		{"rsi", []string{"kline.close", "3"}, factor.RSI(c, 3)},
		{"roc", []string{"kline.close", "3"}, factor.ROC(c, 3)},
		{"mom", []string{"kline.close", "3"}, factor.MOM(c, 3)},
		{"tr", []string{"kline.high", "kline.low", "kline.close"}, factor.TR(h, l, c)},
		{"atr", []string{"kline.high", "kline.low", "kline.close", "3"}, factor.ATR(h, l, c, 3)},
		{"cci", []string{"kline.close", "3"}, factor.CCI(c, 3)},
		{"stoch", []string{"kline.high", "kline.low", "kline.close", "3"}, factor.Stoch(h, l, c, 3)},
		{"willr", []string{"kline.high", "kline.low", "kline.close", "3"}, factor.WillR(h, l, c, 3)},
		{"obv", []string{"kline.close", "kline.volume"}, factor.OBV(c, v)},
		{"mfi", []string{"kline.high", "kline.low", "kline.close", "kline.volume", "3"}, factor.MFI(h, l, c, v, 3)},
		{"highest", []string{"kline.high", "3"}, factor.Highest(h, 3)},
		{"lowest", []string{"kline.low", "3"}, factor.Lowest(l, 3)},
		{"macd", []string{"kline.close", "2", "5", "3"}, line},
		{"macd_signal", []string{"kline.close", "2", "5", "3"}, signal},
		{"macd_hist", []string{"kline.close", "2", "5", "3"}, hist},
		{"bbands_upper", []string{"kline.close", "3", "2", "1.5"}, upper},
		{"bbands_middle", []string{"kline.close", "3", "2", "1.5"}, middle},
		{"bbands_lower", []string{"kline.close", "3", "2", "1.5"}, lower},
	}
}

func TestIndicatorDSLGoEquivalence(t *testing.T) {
	for _, test := range indicatorExpressionCases() {
		t.Run(test.name, func(t *testing.T) {
			formula := "ts." + test.name + "(" + strings.Join(test.args, ",") + ")"
			dsl := mustCompile(t, specFor(formula))
			goPlan, err := factor.Compile(map[string]*factor.Node{"score": test.node})
			if err != nil {
				t.Fatal(err)
			}
			if dsl.Hash() != goPlan.Hash() || dsl.WarmupLength() != goPlan.WarmupLength() || dsl.StateRetention() != goPlan.StateRetention() {
				t.Fatalf("DSL/Go declarations differ for %s", formula)
			}
			spec := specFor(formula)
			spec.Params = map[string]float64{"period": 3, "fast": 2, "slow": 5, "std_up": 2, "std_down": 1.5}
			replacer := strings.NewReplacer(",3", ",param.period", ",2", ",param.fast", ",5", ",param.slow", ",1.5", ",param.std_down")
			spec.Outputs["score"] = replacer.Replace(formula)
			if mustCompile(t, spec).Hash() != dsl.Hash() {
				t.Fatal("bound scalar parameters changed indicator identity")
			}
		})
	}
}

func TestIndicatorDSLRejectsArityParametersAndCrossSectionInputs(t *testing.T) {
	for _, test := range indicatorExpressionCases() {
		t.Run(test.name, func(t *testing.T) {
			formulas := []string{
				"ts." + test.name + "(" + strings.Join(test.args[:len(test.args)-1], ",") + ")",
				"ts." + test.name + "(" + strings.Join(test.args, ",") + ",1)",
			}
			for i, arg := range test.args {
				args := append([]string(nil), test.args...)
				if strings.HasPrefix(arg, "kline.") {
					args[i] = "abs(cs.rank(" + arg + "))"
					formulas = append(formulas, "ts."+test.name+"("+strings.Join(args, ",")+")")
					continue
				}
				for _, bad := range []string{"-1", "kline.close", "param.unknown", "param.window + 1", "1e100"} {
					args[i] = bad
					formulas = append(formulas, "ts."+test.name+"("+strings.Join(args, ",")+")")
				}
				if strings.HasPrefix(test.name, "bbands_") && i > 1 {
					// Deviation multipliers are finite scalars, not bounded periods.
					formulas = formulas[:len(formulas)-1]
				} else {
					for _, bad := range []string{"0", "1.5", "10001"} {
						args[i] = bad
						formulas = append(formulas, "ts."+test.name+"("+strings.Join(args, ",")+")")
					}
				}
			}
			for _, formula := range formulas {
				if _, err := Compile(specFor(formula)); err == nil {
					t.Fatalf("accepted %s", formula)
				}
			}
		})
	}
	for _, formula := range []string{"ts.macd(kline.close,5,2,3)", "ts.macd_signal(kline.close,2,2,3)", "ts.macd_hist(kline.close,2,2,3)", "ts.bbands(kline.close,3,2,2)", "ts.macd(kline.close,2,5)", "ts.atr(kline.close,3)"} {
		if _, err := Compile(specFor(formula)); err == nil {
			t.Fatalf("accepted %s", formula)
		}
	}
}

func TestIndicatorDSLMultiInputSamplingAndPriceExpressions(t *testing.T) {
	spec := specFor("ts.vwma(kline.close,daily.volume,3)")
	spec.Bindings["daily"] = Binding{Source: "daily-volumes", TimeFrame: "1d"}
	if _, err := Compile(spec); err == nil {
		t.Fatal("accepted implicit multi-input cross-timeframe sampling")
	}
	spec.Bindings["daily"] = Binding{Source: "daily-volumes", TimeFrame: "1d", Sampling: "asof", MaxAgeMS: 86_400_000}
	dsl := mustCompile(t, spec)
	goPlan, err := factor.Compile(map[string]*factor.Node{"score": factor.VWMA(factor.Field("prices", "close", "1h"), factor.AsOfField("daily-volumes", "volume", "1d", "1h", 86_400_000), 3)})
	if err != nil || dsl.Hash() != goPlan.Hash() {
		t.Fatalf("multi-input asof equivalence: %v", err)
	}
	inputs := dsl.Inputs()
	if len(inputs) != 2 || inputs[0].TimeFrame != "1d" || !inputs[0].AsOfLatest {
		t.Fatalf("missing explicit daily subscription: %+v", inputs)
	}
	p := mustCompile(t, specFor("cs.zscore(ts.cci((kline.high+kline.low+kline.close)/3,3))"))
	if p.WarmupLength() != 2 || strings.Join(p.Inputs()[0].Fields, ",") != "close,high,low" {
		t.Fatal("CCI expression lost declared fields/lookback")
	}
	for _, name := range []string{"tr", "obv", "atr", "mfi"} {
		for _, test := range indicatorExpressionCases() {
			if test.name != name {
				continue
			}
			spec := specFor("kline.close")
			spec.Lets = map[string]string{"unused": fmt.Sprintf("ts.%s(%s)", name, strings.Join(test.args[:len(test.args)-1], ","))}
			if _, err := Compile(spec); err == nil {
				t.Fatalf("accepted invalid unused %s declaration", name)
			}
		}
	}
}
