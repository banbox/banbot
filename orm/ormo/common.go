package ormo

import (
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	utils2 "github.com/banbox/banexg/utils"
	"go.uber.org/zap"
	"math"
)

func DumpOrdersGob(path string) *errs.Error {
	return DumpOrdersGobItems(path, HistODs)
}

// FilterUnfilledHistoricalOrders keeps the legacy facade behavior while
// routing the mutation through its registry owner.
func FilterUnfilledHistoricalOrders() {
	legacyStateView().FilterUnfilledHistoricalOrders()
}

// HistoricalOrdersSnapshot returns a stable snapshot of the package-level
// compatibility registry. Explicit runtimes should use OrderState directly.
func HistoricalOrdersSnapshot() []*InOutOrder {
	return legacyStateView().HistoricalOrders()
}

// DumpOrdersGobItems serializes an explicit order snapshot. Runtime-owned
// backtests use this entry point so report output does not copy orders into
// the legacy process-wide history registry.
func DumpOrdersGobItems(path string, orders []*InOutOrder) *errs.Error {
	for _, od := range orders {
		_, _ = od.GetInfoText()
		od.Info = nil
	}
	return utils.EncodeGob(path, orders)
}

func LoadOrdersGob(path string) ([]*InOutOrder, *errs.Error) {
	var data []*InOutOrder
	err := utils.DecodeGobFile(path, &data)
	for _, od := range data {
		od.loadInfo()
	}
	return data, err
}

func decodeIOrderInfo(text string) map[string]interface{} {
	result := make(map[string]interface{})
	if text == "" {
		return result
	}
	err_ := utils2.UnmarshalString(text, &result, utils2.JsonNumDefault)
	if err_ != nil {
		log.Error("unmarshal ioder info fail", zap.String("info", text), zap.Error(err_))
	} else {
		for key, val := range result {
			if key == OdInfoStopAfter {
				if floatVal, ok := val.(float64); ok {
					result[key] = int64(math.Round(floatVal))
				}
			} else if key == OdInfoStopLoss || key == OdInfoTakeProfit {
				if mapVal, ok := val.(map[string]interface{}); ok {
					state := decodeTriggerState(mapVal)
					if state == nil {
						delete(result, key)
					} else {
						result[key] = state
					}
				}
			} else if key == OdInfoTakeProfits {
				if mapVal, ok := val.(map[string]interface{}); ok {
					ladder := decodeTPLadderState(mapVal)
					if ladder == nil {
						delete(result, key)
					} else {
						result[key] = ladder
					}
				}
			}
		}
	}
	return result
}

func decodeTPLadderState(data map[string]interface{}) *TPLadderState {
	if len(data) == 0 {
		return nil
	}
	st := &TPLadderState{}
	if v, ok := data["remaining_share"].(float64); ok {
		st.RemainingShare = v
	}
	if v, ok := data["resolved"].(bool); ok {
		st.Resolved = v
	}
	if rawList, ok := data["rungs"].([]interface{}); ok {
		st.Rungs = make([]*TPLadderRung, 0, len(rawList))
		for _, item := range rawList {
			m, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			r := &TPLadderRung{}
			if v, ok := m["price"].(float64); ok {
				r.Price = v
			}
			if v, ok := m["val"].(float64); ok {
				r.Val = v
			}
			if v, ok := m["rate"].(float64); ok {
				r.Rate = v
			}
			if v, ok := m["limit"].(float64); ok {
				r.Limit = v
			}
			if v, ok := m["exit_rate"].(float64); ok {
				r.ExitRate = v
			}
			if v, ok := m["move_sl_to_breakeven"].(bool); ok {
				r.MoveSLToBreakeven = v
			}
			if v, ok := m["tag"].(string); ok {
				r.Tag = v
			}
			if v, ok := m["done"].(bool); ok {
				r.Done = v
			}
			st.Rungs = append(st.Rungs, r)
		}
	}
	if len(st.Rungs) == 0 {
		return nil
	}
	return st
}

func decodeTriggerState(data map[string]interface{}) *TriggerState {
	if data == nil || len(data) == 0 {
		return nil
	}
	ts := &TriggerState{
		ExitTrigger: decodeTrigger(data),
	}
	if v, ok := data["range"].(float64); ok {
		ts.Range = v
	}
	if v, ok := data["hit"].(bool); ok {
		ts.Hit = v
	}
	if v, ok := data["order_id"].(string); ok {
		ts.OrderId = v
	}
	if v, ok := data["client_id"].(string); ok {
		ts.ClientId = v
	}

	// 处理嵌套的Old字段
	if oldData, ok := data["old"].(map[string]interface{}); ok {
		ts.Old = decodeTrigger(oldData)
	}

	return ts
}

func decodeTrigger(data map[string]interface{}) *ExitTrigger {
	ts := &ExitTrigger{}
	if v, ok := data["price"].(float64); ok {
		ts.Price = v
	}
	if v, ok := data["limit"].(float64); ok {
		ts.Limit = v
	}
	if v, ok := data["rate"].(float64); ok {
		ts.Rate = v
	}
	if v, ok := data["tag"].(string); ok {
		ts.Tag = v
	}
	return ts
}
