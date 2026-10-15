package runner

import (
	"github.com/banbox/banbot/execution"
	"github.com/shopspring/decimal"
)

// Compatibility names; the simulated venue belongs to the execution domain.
type PaperAdapter = execution.PaperAdapter
type PaperMetrics = execution.PaperMetrics

func NewPaperAdapter(cash, fee, slip decimal.Decimal) (*PaperAdapter, error) {
	return execution.NewPaperAdapter(cash, fee, slip)
}
