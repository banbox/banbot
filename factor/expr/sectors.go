package expr

import (
	"math"

	"github.com/banbox/banbot/factor"
)

func (c *compiler) sectorOrQuantileCall(path string, e *expression) (*factor.Node, bool, error) {
	switch e.text {
	case "sector_code":
		if len(e.args) != 0 {
			return nil, true, c.error(path, e, "sector_code expects 0 arguments, got %d", len(e.args))
		}
		return factor.SectorCode(c.spec.TimeFrame), true, nil

	case "cs.sector_demean", "cs.sector_zscore", "group.sector_demean", "group.sector_zscore":
		if len(e.args) != 1 {
			return nil, true, c.error(path, e, "%s expects 1 argument, got %d", e.text, len(e.args))
		}
		a, err := c.lower(path, e.args[0])
		if err != nil {
			return nil, true, err
		}
		if e.text == "cs.sector_demean" || e.text == "group.sector_demean" {
			return factor.SectorDemean(a), true, nil
		}
		return factor.SectorZScore(a), true, nil

	case "group.demean", "group.zscore":
		if len(e.args) != 2 {
			return nil, false, nil
		}
		a, err := c.lower(path, e.args[0])
		if err != nil {
			return nil, true, err
		}
		arg := e.args[1]
		isBuiltInSector := (arg.kind == 'r' && arg.text == "sector") ||
			(arg.kind == 's' && arg.text == "sector") ||
			(arg.kind == 'c' && arg.text == "sector_code" && len(arg.args) == 0)
		if !isBuiltInSector {
			return nil, true, c.error(path, arg, "%s with 2 arguments expects built-in sector; for raw classification fields pass (x, \"alias\", \"field\")", e.text)
		}
		if e.text == "group.demean" {
			return factor.SectorDemean(a), true, nil
		}
		return factor.SectorZScore(a), true, nil

	case "group.quantile_demean", "group.quantile_zscore", "group.quantile_rank", "group.quantile_pct_rank":
		if len(e.args) != 3 {
			return nil, true, c.error(path, e, "%s expects 3 arguments, got %d", e.text, len(e.args))
		}
		x, err := c.lower(path, e.args[0])
		if err != nil {
			return nil, true, err
		}
		metric, err := c.lower(path, e.args[1])
		if err != nil {
			return nil, true, err
		}
		v, err := c.number(path, e.args[2])
		if err != nil {
			return nil, true, err
		}
		if v < 2 || v > factor.MaxQuantileBuckets || v != math.Trunc(v) {
			return nil, true, c.error(path, e.args[2], "n_buckets must be an integer in [2,%d]", factor.MaxQuantileBuckets)
		}
		buckets := int(v)
		switch e.text {
		case "group.quantile_demean":
			return factor.QuantileGroupDemean(x, metric, buckets), true, nil
		case "group.quantile_zscore":
			return factor.QuantileGroupZScore(x, metric, buckets), true, nil
		case "group.quantile_rank":
			return factor.QuantileGroupRank(x, metric, buckets), true, nil
		default:
			return factor.QuantileGroupPctRank(x, metric, buckets), true, nil
		}
	default:
		return nil, false, nil
	}
}
