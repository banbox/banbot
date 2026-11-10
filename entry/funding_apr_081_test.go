package entry

import (
	"testing"

	"github.com/banbox/banbot/factor/research"
)

// 081: manifest.costs.funding_apr is a known, finite non-negative key and decodes into CostSpec.FundingApr.
func TestManifestCostsFundingApr081(t *testing.T) {
	var costs research.CostSpec
	if err := decodeFactorFields(map[string]any{"fee_rate": 0.0005, "slippage_rate": 0.0005, "funding_apr": 10.0, "funding_policy": "explicit-zero"}, &costs); err != nil {
		t.Fatal(err)
	}
	if costs.FundingApr != 10 || costs.SlippageRate != 0.0005 || costs.FundingPolicy != "explicit-zero" {
		t.Fatalf("%+v", costs)
	}
}
