package biz

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

// State a strategy attaches to a Hub order view. The view is rebuilt from the
// journal on every poll, so anything not derivable from the journal has to be
// kept next to the lot: strategy-written Info keys, the running profit extremes
// and the scope of the last explicit exit (see hubProjectedLot).

type hubInfoWrite struct {
	Val     any
	Deleted bool
}

// hubSystemInfoKeys are written by the projection or the order engine itself;
// a strategy value never replaces them.
var hubSystemInfoKeys = map[string]bool{
	"exit_tags":                true,
	ormo.OdInfoArchiveKey:      true,
	ormo.OdInfoLegalCost:       true,
	ormo.OdInfoStopAfter:       true,
	ormo.OdInfoStopBars:        true,
	ormo.OdInfoStopLoss:        true,
	ormo.OdInfoCallbackPct:     true,
	ormo.OdInfoActivePrice:     true,
	ormo.OdInfoTrailingBest:    true,
	ormo.OdInfoTrailingID:      true,
	ormo.OdInfoLocalTrailing:   true,
	ormo.OdInfoTakeProfit:      true,
	ormo.OdInfoTakeProfits:     true,
	ormo.OdInfoOCOGroup:        true,
	ormo.OdInfoLocalStopLoss:   true,
	ormo.OdInfoLocalTakeProfit: true,
	ormo.OdInfoClientID:        true,
	ormo.OdInfoRetry:           true,
	ormo.OdInfoSLTP:            true,
}

func hubSystemInfoKey(key string) bool {
	return hubSystemInfoKeys[key] || strings.HasPrefix(key, "shared_")
}

// infoHook returns the SetInfo observer installed on every view of one order.
// It runs on the strategy's goroutine and only takes infoMu (never m.mu), so it
// is safe inside callbacks and while the manager is busy. Values that cannot be
// journaled are skipped with a single warning.
func (m *HubOrderMgr) infoHook(orderID int64) func(*ormo.InOutOrder, string, interface{}) {
	return func(od *ormo.InOutOrder, key string, val interface{}) {
		if hubSystemInfoKey(key) {
			return
		}
		write := hubInfoWrite{Deleted: val == nil}
		if val != nil {
			body, err := json.Marshal(val)
			if err != nil {
				m.infoMu.Lock()
				first := !m.infoWarned[key]
				if m.infoWarned == nil {
					m.infoWarned = map[string]bool{}
				}
				m.infoWarned[key] = true
				m.infoMu.Unlock()
				if first {
					m.deps.Logger().Warn("hub order Info value is not JSON-serialisable; it is kept on this object only and lost at the next view refresh",
						zap.Int64("order_id", orderID), zap.String("key", key), zap.Error(err))
				}
				return
			}
			var copied any
			if json.Unmarshal(body, &copied) != nil {
				return
			}
			// The journal keeps the JSON form: that is what a restart restores,
			// so a live view and a restored one agree.
			write.Val = copied
		}
		m.infoMu.Lock()
		if m.infoStage == nil {
			m.infoStage = map[int64]map[string]hubInfoWrite{}
		}
		if m.infoStage[orderID] == nil {
			m.infoStage[orderID] = map[string]hubInfoWrite{}
		}
		m.infoStage[orderID][key] = write
		m.infoMu.Unlock()
	}
}

// drainStratInfoLocked moves staged strategy writes into the lot (journaled by
// the next saveLocked).
func (m *HubOrderMgr) drainStratInfoLocked(lot *hubProjectedLot, orderID int64, source map[string]any) {
	m.infoMu.Lock()
	staged := m.infoStage[orderID]
	delete(m.infoStage, orderID)
	m.infoMu.Unlock()
	for key, w := range staged {
		if w.Deleted {
			if _, inSource := source[key]; inSource {
				if lot.StratInfo == nil {
					lot.StratInfo = map[string]any{}
				}
				lot.StratInfo[key] = nil // tombstone hides the entry-request value
			} else {
				delete(lot.StratInfo, key)
			}
			continue
		}
		if lot.StratInfo == nil {
			lot.StratInfo = map[string]any{}
		}
		lot.StratInfo[key] = w.Val
	}
}

// overlayStratInfo applies the journaled strategy keys to a rebuilt view.
func overlayStratInfo(od *ormo.InOutOrder, lot *hubProjectedLot) {
	for key, val := range lot.StratInfo {
		if hubSystemInfoKey(key) {
			continue
		}
		if val == nil {
			delete(od.Info, key)
			continue
		}
		od.Info[key] = cloneJSONValue(val)
	}
}

func cloneJSONValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = cloneJSONValue(e)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = cloneJSONValue(e)
		}
		return out
	}
	return v
}

// markPriceLocked is the latest price this manager has for a symbol: the last
// bar it was driven with, else the runtime's last bar price. 0 means unknown.
func (m *HubOrderMgr) markPriceLocked(symbol string) float64 {
	if p := m.marks[symbol]; p > 0 {
		return p
	}
	if m.deps.Market != nil && m.deps.Market.Prices != nil {
		if p := m.deps.Market.Prices.GetLastBarPriceAt(symbol); p > 0 {
			return p
		}
	}
	return 0
}

// applyProfitsLocked gives a view the profit fields a backtest order carries.
// ormo.InOutOrder.UpdateProfits cannot be used as is: it values the full
// Enter.Filled at the mark and ignores what a partial exit already realized.
//   - open (also partially exited): realized + funding + unrealized on the
//     remaining quantity - fees, at the mark price; unchanged (realized only)
//     while no price is known.
//   - fully exited: the ledger result (realized + funding - fees).
//
// ProfitRate is Profit / (Enter.Average * Enter.Filled) like the archive row.
// MaxPftRate / MaxDrawDown are running extremes kept on the lot, updated with
// UpdateProfits' own rules.
func (m *HubOrderMgr) applyProfitsLocked(lot *hubProjectedLot, od *ormo.InOutOrder, instrument execution.Instrument) {
	od.MaxPftRate, od.MaxDrawDown = lot.MaxPftRate, lot.MaxDrawDown
	if lot.Entered <= 0 || od.Status == ormo.InOutStatusDelete || od.Enter == nil || od.Enter.Average <= 0 || od.Enter.Filled <= 0 {
		return
	}
	if od.Status != ormo.InOutStatusFullExit {
		mark := m.markPriceLocked(od.Symbol)
		if mark <= 0 {
			return
		}
		held := decimal.NewFromInt(lot.Entered - lot.Exited).Mul(instrument.QuantityStep).Mul(instrument.ContractSize)
		unrealized := held.Mul(decimal.NewFromFloat(mark).Sub(decimal.NewFromFloat(od.Enter.Average)))
		if od.Short {
			unrealized = unrealized.Neg()
		}
		od.Profit = lot.Realized.Add(lot.Funding).Add(unrealized).Sub(lot.Fees).InexactFloat64()
	}
	od.ProfitRate = od.Profit / (od.Enter.Average * od.Enter.Filled)
	if math.IsNaN(od.ProfitRate) || math.IsInf(od.ProfitRate, 0) {
		od.ProfitRate = 0
		return
	}
	if od.ProfitRate > lot.MaxPftRate {
		lot.MaxPftRate = od.ProfitRate
	} else if m.deps.Orders.LegacyOrderMetrics() {
		if lot.MaxPftRate > 0 {
			lot.MaxDrawDown = (lot.MaxPftRate - od.ProfitRate) / lot.MaxPftRate
		} else {
			lot.MaxDrawDown = -od.ProfitRate
		}
	} else {
		lot.MaxDrawDown = min(lot.MaxDrawDown, od.Profit, 0)
	}
	od.MaxPftRate, od.MaxDrawDown = lot.MaxPftRate, lot.MaxDrawDown
}

// captureExitScopeLocked records, when a new explicit reduction command shows
// up in the lot metadata, whether it covered the whole position held at that
// moment. The metadata keeps the command's size after it filled, so the fact
// cannot be re-derived later from held quantity.
func (m *HubOrderMgr) captureExitScopeLocked(lot *hubProjectedLot) {
	md := lot.Record.Metadata
	if md.ExitQuantitySteps <= 0 {
		return
	}
	sig := fmt.Sprintf("%s/%d", md.ExitTag, md.ExitQuantitySteps)
	if lot.ExitSig == sig {
		return
	}
	lot.ExitSig = sig
	lot.ExitFull = md.ExitQuantitySteps >= lot.Entered-lot.Exited
}
