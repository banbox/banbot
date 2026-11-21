package expr

import (
	"math"
	"strings"

	"github.com/banbox/banbot/factor"
)

func (c *compiler) currentTF() string {
	if c.tfOverride != "" {
		return c.tfOverride
	}
	return c.spec.TimeFrame
}

func (c *compiler) scopedFactorKey(name string) string {
	if c.tfOverride == "" || c.tfOverride == c.spec.TimeFrame {
		return name
	}
	return name + "@" + c.tfOverride
}

func isDefaultKlineField(name string) bool {
	switch name {
	case "open", "high", "low", "close", "volume", "buy_volume":
		return true
	default:
		return false
	}
}

func (c *compiler) mtfOrRefCall(path string, e *expression) (*factor.Node, bool, error) {
	switch e.text {
	case "mtf.asof", "asof_tf":
		if len(e.args) != 2 && len(e.args) != 3 {
			return nil, true, c.error(path, e, "%s expects 2 or 3 arguments, got %d", e.text, len(e.args))
		}
		if e.args[0].kind != 's' {
			return nil, true, c.error(path, e.args[0], "%s expects a source timeframe string literal as first argument", e.text)
		}
		srcTF := strings.TrimSpace(e.args[0].text)
		targetTF := c.currentTF()
		srcMS, okSrc := factor.ParseTimeFrameMS(srcTF)
		dstMS, okDst := factor.ParseTimeFrameMS(targetTF)
		if !okSrc || !okDst {
			return nil, true, c.error(path, e.args[0], "invalid bar timeframe %q (target %q)", srcTF, targetTF)
		}
		if srcMS < dstMS {
			return nil, true, c.error(path, e.args[0], "sub-timeframe %q must not be finer than target timeframe %q", srcTF, targetTF)
		}
		var maxAge int64
		if len(e.args) == 3 {
			v, err := c.number(path, e.args[2])
			if err != nil {
				return nil, true, err
			}
			if v < 0 || v != math.Trunc(v) || v > float64(math.MaxInt64/2) {
				return nil, true, c.error(path, e.args[2], "max_stale_ms must be a nonnegative integer")
			}
			maxAge = int64(v)
		}
		prevTF := c.tfOverride
		c.tfOverride = srcTF
		inner, err := c.lower(path, e.args[1])
		c.tfOverride = prevTF
		if err != nil {
			return nil, true, err
		}
		return factor.AsOfNode(inner, targetTF, maxAge), true, nil

	case "ref.field":
		if len(e.args) != 2 && len(e.args) != 3 {
			return nil, true, c.error(path, e, "ref.field expects 2 or 3 string literal arguments, got %d", len(e.args))
		}
		for i, arg := range e.args {
			if arg.kind != 's' {
				return nil, true, c.error(path, arg, "ref.field argument %d must be a string literal", i+1)
			}
		}
		symbol := strings.TrimSpace(e.args[0].text)
		if symbol == "" {
			return nil, true, c.error(path, e.args[0], "ref.field symbol must not be empty")
		}
		activeTF := c.currentTF()
		if len(e.args) == 2 {
			fieldName := e.args[1].text
			if fieldName == "" {
				return nil, true, c.error(path, e.args[1], "ref.field field name must not be empty")
			}
			source := "kline"
			if b, ok := c.spec.Bindings["kline"]; ok {
				source = b.Source
				if b.Sampling == "asof" || b.Sampling == "asof-latest" {
					return factor.AsOfRefAssetField(symbol, source, fieldName, b.TimeFrame, activeTF, b.MaxAgeMS), true, nil
				}
			}
			return factor.RefAssetField(symbol, source, fieldName, activeTF), true, nil
		}
		sourceOrAlias := strings.TrimSpace(e.args[1].text)
		fieldName := e.args[2].text
		if sourceOrAlias == "" || fieldName == "" {
			return nil, true, c.error(path, e, "ref.field source and field must not be empty")
		}
		if b, ok := c.spec.Bindings[sourceOrAlias]; ok {
			if b.Sampling == "asof" || b.Sampling == "asof-latest" {
				return factor.AsOfRefAssetField(symbol, b.Source, fieldName, b.TimeFrame, activeTF, b.MaxAgeMS), true, nil
			}
			tf := activeTF
			if c.tfOverride == "" {
				tf = b.TimeFrame
			}
			return factor.RefAssetField(symbol, b.Source, fieldName, tf), true, nil
		}
		return factor.RefAssetField(symbol, sourceOrAlias, fieldName, activeTF), true, nil

	default:
		return nil, false, nil
	}
}
