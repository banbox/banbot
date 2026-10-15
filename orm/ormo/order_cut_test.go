package ormo

import (
	"math"
	"testing"
)

func TestExOrderCutPartPreservesFilledFees(t *testing.T) {
	tests := []struct {
		name           string
		amount, filled float64
		rate           float64
		fill           bool
		wantPartFilled float64
		wantPartFee    float64
	}{
		{name: "filled", amount: 10, filled: 10, rate: 0.5, fill: true, wantPartFilled: 5, wantPartFee: 3},
		{name: "partially filled", amount: 10, filled: 2, rate: 0.5, fill: true, wantPartFilled: 2, wantPartFee: 6},
		{name: "unfilled", amount: 10, rate: 0.5, fill: true},
		{name: "zero rate", amount: 10, filled: 10, fill: true},
		{name: "move filled remainder", amount: 10, filled: 8, rate: 0.5, wantPartFilled: 3, wantPartFee: 2.25},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			od := &ExOrder{Amount: tc.amount, Filled: tc.filled, Fee: 6, FeeQuote: 12}
			part := od.CutPart(tc.rate, tc.fill)

			assertFloat(t, "part filled", part.Filled, tc.wantPartFilled)
			assertFloat(t, "part fee", part.Fee, tc.wantPartFee)
			assertFloat(t, "filled total", od.Filled+part.Filled, tc.filled)
			assertFloat(t, "fee total", od.Fee+part.Fee, 6)
			assertFloat(t, "quote fee total", od.FeeQuote+part.FeeQuote, 12)
		})
	}
}

func TestExOrderCutPartPreservesFeesAcrossRepeatedSplits(t *testing.T) {
	od := &ExOrder{Amount: 8, Filled: 8, Fee: 8, FeeQuote: 16}
	first := od.CutPart(0.25, true)
	second := od.CutPart(1.0/3, true)

	assertFloat(t, "filled total", od.Filled+first.Filled+second.Filled, 8)
	assertFloat(t, "fee total", od.Fee+first.Fee+second.Fee, 8)
	assertFloat(t, "quote fee total", od.FeeQuote+first.FeeQuote+second.FeeQuote, 16)
}

func assertFloat(t *testing.T, name string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-12 {
		t.Fatalf("%s = %.12f, want %.12f", name, got, want)
	}
}
