package biz

import (
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/rpc"
)

// NewOrderMessage builds the rpc entry/exit notification payload for a filled
// order leg. ok is false when the leg is missing, not closed or has no fill,
// which are the cases the order callbacks skip. live.sendOrderMsgWithRuntime
// and the execution-backed runtimes share this builder.
func NewOrderMessage(od *ormo.InOutOrder, isEnter bool, account string) (msg map[string]interface{}, ok bool) {
	if od == nil {
		return nil, false
	}
	msgType := rpc.MsgTypeExit
	subOd := od.Exit
	action := "Close Long"
	if od.Short {
		action = "Close Short"
	}
	if isEnter {
		msgType = rpc.MsgTypeEntry
		subOd = od.Enter
		action = "Open Long"
		if od.Short {
			action = "Open Short"
		}
	}
	if subOd == nil || subOd.Status != ormo.OdStatusClosed || subOd.Filled == 0 {
		return nil, false
	}
	filled, price := subOd.Filled, subOd.Average
	return map[string]interface{}{
		"type":          msgType,
		"account":       account,
		"action":        action,
		"enter_tag":     od.EnterTag,
		"exit_tag":      od.ExitTag,
		"side":          subOd.Side,
		"short":         od.Short,
		"leverage":      od.Leverage,
		"amount":        filled,
		"price":         price,
		"value":         filled * price,
		"cost":          filled * price / od.Leverage,
		"strategy":      od.Strategy,
		"pair":          od.Symbol,
		"timeframe":     od.Timeframe,
		"profit":        od.Profit,
		"profit_rate":   od.ProfitRate,
		"max_pft_rate":  od.MaxPftRate,
		"max_draw_down": od.MaxDrawDown,
	}, true
}
