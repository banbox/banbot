package biz

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/shopspring/decimal"
)

func (m *HubOrderMgr) EditOrder(od *ormo.InOutOrder, action string) {
	if od == nil {
		return
	}
	m.mu.Lock()
	m.started = true
	err := m.editLocked(od, action)
	m.mu.Unlock()
	if err == nil {
		err = m.poll(false)
	}
	if err != nil {
		m.fail(err)
	}
}
func (m *HubOrderMgr) editLocked(od *ormo.InOutOrder, action string) error {
	_, b, err := m.strategy(od.Strategy)
	if err != nil {
		return err
	}
	lotID, ok := od.Info["shared_lot"].(string)
	if !ok {
		return errors.New("biz: Hub edit missing lot")
	}
	key := hubLotKey(b.ID, execution.VirtualLotID(lotID))
	lot := m.journal.Lots[key]
	if lot == nil {
		return errors.New("biz: Hub edit lot is unmapped")
	}
	// Re-read the authoritative revision; an asynchronous trigger may have changed it.
	definitions, err := m.config.Client.PendingOrders(m.ctx, m.config.Account)
	if err != nil {
		return err
	}
	var p *execution.PendingDefinition
	for i := range definitions {
		if definitions[i].Order.Strategy == b.ID && string(definitions[i].Order.Lot) == lotID {
			p = &definitions[i]
			break
		}
	}
	if p == nil {
		return errors.New("biz: Hub edit pending definition missing")
	}
	id, epoch, seq, err := m.nextLocked(b.ID)
	if err != nil {
		return err
	}
	command := execution.PendingCommand{ID: id, Strategy: b.ID, StrategyEpoch: epoch, StrategySequence: seq, Lot: execution.VirtualLotID(lotID), ExpectedRevision: p.CommandRevision}
	var payload TimeSeriesEntryPayload
	if err = json.Unmarshal(lot.Record.Metadata.Payload, &payload); err != nil {
		return err
	}
	switch action {
	case ormo.OdActionLimitEnter:
		if od.Enter == nil || od.Enter.Price <= 0 {
			return errors.New("biz: missing edited Hub entry price")
		}
		command.Action = execution.PendingEditLimit
		command.Limit = decimal.NewFromFloat(od.Enter.Price)
		payload.Request.Limit = od.Enter.Price
	case ormo.OdActionLimitExit:
		if od.Exit == nil || od.Exit.Price <= 0 {
			return errors.New("biz: missing edited Hub exit price")
		}
		command.Action = execution.PendingEditExitLimit
		command.Limit = decimal.NewFromFloat(od.Exit.Price)
	case ormo.OdInfoStopLoss:
		payload.Request.StopLossVal = 0
		payload.Request.StopLoss, payload.Request.StopLossLimit, payload.Request.StopLossRate, payload.Request.StopLossTag = 0, 0, 0, ""
		if t := od.GetStopLoss(); t != nil && t.ExitTrigger != nil {
			payload.Request.StopLoss, payload.Request.StopLossLimit, payload.Request.StopLossRate, payload.Request.StopLossTag = t.Price, t.Limit, t.Rate, t.Tag
		}
		command.Action = execution.PendingSetProtections
	case ormo.OdInfoTakeProfit:
		payload.Request.TakeProfitVal = 0
		payload.Request.TakeProfit, payload.Request.TakeProfitLimit, payload.Request.TakeProfitRate, payload.Request.TakeProfitTag = 0, 0, 0, ""
		if t := od.GetTakeProfit(); t != nil && t.ExitTrigger != nil {
			payload.Request.TakeProfit, payload.Request.TakeProfitLimit, payload.Request.TakeProfitRate, payload.Request.TakeProfitTag = t.Price, t.Limit, t.Rate, t.Tag
		}
		command.Action = execution.PendingSetProtections
	case ormo.OdActionTrailing:
		payload.Request.CallbackPct = od.GetInfoFloat64(ormo.OdInfoCallbackPct)
		payload.Request.ActivationPrice = od.GetInfoFloat64(ormo.OdInfoActivePrice)
		command.Action = execution.PendingSetProtections
	default:
		return fmt.Errorf("biz: unsupported Hub edit %s", action)
	}
	if err = validateLegacyNumbers(&payload.Request); err != nil {
		return err
	}
	if command.Action == execution.PendingSetProtections {
		command.Protections = sharedProtectionSpecs(payload.Request)
	}
	metadata := lot.Record.Metadata
	metadata.Payload, err = json.Marshal(payload)
	if err != nil {
		return err
	}
	command.Metadata = &metadata
	frozen := &hubFrozenCommand{Pending: &command, OrderID: od.ID}
	m.journal.Commands[string(b.ID)+"/edit/"+id] = frozen
	if err = m.saveLocked(); err != nil {
		return err
	}
	return m.sendLocked(frozen)
}
