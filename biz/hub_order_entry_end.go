package biz

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"go.uber.org/zap"
)

// Entries that end without ever filling.
//
// LocalOrderMgr tells the strategy about every accepted entry that dies
// unfilled with OdChgExitFill, a Delete-status order and an ExitTag naming the
// cause (ent_expire, oco_cancel, force_exit/fatal_err, the strategy's own exit
// tag). The Hub path mirrors that in two places:
//
//   - an entry the owner accepted and then terminated: the projection detects it
//     once the page is fully replayed (markEntryEndLocked), journals the cause on
//     the lot and the delivery in PendingDeliveries in one save (exactly once
//     across restart/replay, like fill deliveries);
//   - an entry the manager itself drops before or at submission (size, ladder/OCO
//     not expressible, owner refusal): a synthesized Delete order goes through
//     the persisted EntryDrops outbox (deliverDrops).
//
// Requests LocalOrderMgr refuses silently at admission (max open orders, no-enter
// switch, loss-stop pause) stay silent here too.

// Reason codes of entries dropped by the manager (Info status_msg / reject_code).
const (
	hubDropSize   = "size_rejected"
	hubDropLadder = "ladder_unsupported"
)

// hubDropTag maps a refusal code to the LocalOrderMgr tag of the analogous case:
// a request whose size cannot be traded is the zero-amount case (fatal_err); any
// other refusal by the owner is the insufficient-funds/risk case (force_exit).
func hubDropTag(code string) string {
	if code == hubDropSize || code == hubDropLadder || code == "venue_minimum" {
		return core.ExitTagFatalErr
	}
	return core.ExitTagForceExit
}

// captureEntryEndLocked records why an unfilled lot ended when the event that
// ended it is replayed (the owner's pending definition alone cannot tell
// cancel, OCO and isolation apart, and may be dropped afterwards).
func (m *HubOrderMgr) captureEntryEndLocked(lot *hubProjectedLot, accepted execution.StrategyAcceptedEvent) {
	if lot.Entered != 0 || lot.Exited != 0 || lot.EndNotified {
		return
	}
	switch {
	case accepted.Rejected != "":
		// Terminated by the owner: OCO sibling lost, or the plan was refused.
		lot.EndTag, lot.EndReason, lot.EndCancel, lot.EndMS = core.ExitTagForceExit, accepted.Rejected, false, accepted.AtMS
		if strings.HasPrefix(accepted.CommandID, "oco-") {
			lot.EndTag = ExitTagOCOCancel
		}
	case accepted.Kind == execution.ExitIntent && !hubFollowupID(accepted.CommandID) && !hubEditID(accepted.CommandID):
		// The strategy's own exit request reached a still unfilled entry.
		tag := ""
		for _, c := range m.journal.Commands {
			if c.Exit != nil && c.Exit.ID == accepted.CommandID {
				tag = c.Exit.ExitTag
				break
			}
		}
		if tag == "" && accepted.Metadata != nil {
			tag = accepted.Metadata.ExitTag
		}
		if tag == "" {
			tag = core.ExitTagForceExit
		}
		lot.EndTag, lot.EndReason, lot.EndCancel, lot.EndMS = tag, "", true, accepted.AtMS
	case accepted.Kind == execution.EntryIntent && strings.HasSuffix(accepted.CommandID, hubFollowupOCO):
		lot.EndTag, lot.EndReason, lot.EndCancel, lot.EndMS = ExitTagOCOCancel, "", false, accepted.AtMS
	}
}

// entryEndedLocked reports whether the lot's entry is over without any fill,
// judged on the latest owner definition (k is the lot key).
func (m *HubOrderMgr) entryEndedLocked(k string, lot *hubProjectedLot, held int64) bool {
	if lot.Entered != 0 || lot.Exited != 0 || held != 0 {
		return false
	}
	p, active := m.pending[k]
	if !active {
		return true
	}
	o := p.Order
	return o.Entry.FilledSteps == 0 && (o.Canceled || o.Entry.State == execution.Canceled)
}

// markEntryEndLocked resolves the cause the first time the lot is seen ended
// and sets EndNotified; it returns true exactly once per lot.
func (m *HubOrderMgr) markEntryEndLocked(k string, lot *hubProjectedLot, held int64) bool {
	if lot.EndNotified || !m.entryEndedLocked(k, lot, held) {
		return false
	}
	p, active := m.pending[k]
	if lot.EndTag == "" {
		var payload TimeSeriesEntryPayload
		_ = json.Unmarshal(lot.Record.Metadata.Payload, &payload)
		switch plan := m.journal.Plans[k]; {
		case active && p.Order.Entry.State == execution.Expired:
			lot.EndTag, lot.EndReason = core.ExitTagEntExp, "reach StopEnterBars"
		case plan != nil && plan.OCOGroup != "" && plan.OCOCancelled:
			lot.EndTag = ExitTagOCOCancel
		case payload.Request.StopBars > 0:
			lot.EndTag, lot.EndReason = core.ExitTagEntExp, "reach StopEnterBars"
		default:
			lot.EndTag, lot.EndReason = core.ExitTagForceExit, "entry ended"
		}
	}
	if lot.EndTag == core.ExitTagEntExp && lot.EndReason == "" {
		lot.EndReason = "reach StopEnterBars"
	}
	if lot.EndMS == 0 && m.deps.Clock != nil {
		lot.EndMS = m.deps.Clock.TimeMS()
	}
	lot.EndNotified = true
	return true
}

// applyEntryEndLocked labels the Delete view of an ended lot like the backtest
// order (exit tag, an unfilled exit leg, status message).
func (m *HubOrderMgr) applyEntryEndLocked(lot *hubProjectedLot, od *ormo.InOutOrder) {
	if od.Status != ormo.InOutStatusDelete || lot.EndTag == "" {
		return
	}
	od.ExitTag = lot.EndTag
	if lot.EndMS > 0 {
		od.ExitAt = lot.EndMS
	}
	if od.Exit == nil {
		od.Exit = &ormo.ExOrder{Symbol: od.Symbol, Price: od.InitPrice, OrderType: banexg.OdTypeMarket, Side: banexg.OdSideSell, Status: ormo.OdStatusClosed, UpdateAt: lot.EndMS}
		if od.Short {
			od.Exit.Side = banexg.OdSideBuy
		}
	}
	if lot.EndReason != "" {
		od.Info[ormo.KeyStatusMsg] = lot.EndReason
	}
}

// dropOrderLocked builds the Delete-status order shown to the strategy for an
// entry request the manager dropped: the request's identity and requested size.
func (m *HubOrderMgr) dropOrderLocked(strategy, symbol, tf string, sid int32, req *strat.EnterReq, orderID, atMS int64, initPrice float64, code, msg string) *ormo.InOutOrder {
	if atMS == 0 && m.deps.Clock != nil {
		atMS = m.deps.Clock.TimeMS()
	}
	tag := hubDropTag(code)
	enter := &ormo.ExOrder{Enter: true, Symbol: symbol, Amount: req.Amount, Price: req.Limit, Side: banexg.OdSideBuy, CreateAt: atMS, UpdateAt: atMS}
	if req.OrderType >= 0 && req.OrderType < len(core.OrderTypeEnums) {
		enter.OrderType = core.OrderTypeEnums[req.OrderType]
	}
	exit := &ormo.ExOrder{Symbol: symbol, Price: initPrice, OrderType: banexg.OdTypeMarket, Side: banexg.OdSideSell, Status: ormo.OdStatusClosed, CreateAt: atMS, UpdateAt: atMS}
	if req.Short {
		enter.Side, exit.Side = banexg.OdSideSell, banexg.OdSideBuy
	}
	od := &ormo.InOutOrder{
		IOrder: &ormo.IOrder{ID: orderID, TaskID: m.deps.Orders.GetTaskID(m.deps.DefaultAccount), Symbol: symbol, Sid: int64(sid), Timeframe: tf,
			Short: req.Short, Strategy: strategy, EnterTag: req.Tag, ExitTag: tag, Leverage: req.Leverage, InitPrice: initPrice,
			EnterAt: atMS, ExitAt: atMS, Stop: req.Stop, Status: ormo.InOutStatusDelete},
		Enter: enter, Exit: exit, Info: map[string]any{},
	}
	for k, v := range req.Infos {
		od.Info[k] = v
	}
	if req.LegalCost > 0 {
		od.Info[ormo.OdInfoLegalCost] = req.LegalCost
	}
	od.Info[ormo.KeyStatusMsg] = msg
	od.Info["reject_code"] = code
	return od
}

func (m *HubOrderMgr) queueDropLocked(id string, od *ormo.InOutOrder) {
	for _, d := range m.journal.EntryDrops {
		if d.ID == id {
			return
		}
	}
	m.journal.EntryDrops = append(m.journal.EntryDrops, hubOrderDelivery{Order: od, Kind: strat.OdChgExitFill, ID: id})
}

// queueRequestDropLocked records the drop of an entry request that never became
// a frozen command and persists it. The id is the request's stable command ID
// when it has one, so a replayed request queues one callback.
func (m *HubOrderMgr) queueRequestDropLocked(strategy, symbol, tf string, sid int32, req *strat.EnterReq, orderID int64, code, msg string) error {
	id := req.CommandID
	if id == "" {
		id, _ = hubRandomID()
	}
	m.queueDropLocked("drop/"+id, m.dropOrderLocked(strategy, symbol, tf, sid, req, orderID, 0, 0, code, msg))
	return m.saveLocked()
}

// queueCommandDropLocked records the drop of a frozen entry command the owner
// refused (the caller persists the journal right after).
func (m *HubOrderMgr) queueCommandDropLocked(command *hubFrozenCommand, code string) {
	if command == nil || command.Pending == nil || command.Pending.Action != execution.PendingCreate || command.Pending.Metadata == nil {
		return
	}
	// max_open_orders / max_simul_open refusals are silent in LocalOrderMgr (no
	// order, no callback); the owner's account-wide limit is the same gate.
	if code == execution.RejectAdmissionLimit {
		return
	}
	md := command.Pending.Metadata
	var payload TimeSeriesEntryPayload
	if err := json.Unmarshal(md.Payload, &payload); err != nil {
		m.deps.Logger().Warn("hub dropped entry callback skipped: payload unreadable", zap.Int64("order_id", md.OrderID), zap.Error(err))
		return
	}
	msg := "entry rejected: " + code
	m.queueDropLocked("drop/"+command.Pending.ID, m.dropOrderLocked(md.StrategyName, md.Symbol, md.TimeFrame, payload.SID, &payload.Request, md.OrderID, md.CreatedMS, md.InitPrice.InexactFloat64(), code, msg))
}

// deliverDrops delivers queued dropped-entry callbacks outside m.mu, oldest
// first, removing each only after its delivery. Drains never nest: a drop
// raised from inside a callback is appended and picked up by the running drain
// (TryLock), so callback-queued requests are flushed by deliver without
// recursion. inPoll marks calls made under poll's exclusivity so a callback
// that submits an order does not wait for the poll that is running it.
func (m *HubOrderMgr) deliverDrops(inPoll bool) {
	if !m.dropMu.TryLock() {
		return
	}
	defer m.dropMu.Unlock()
	if inPoll {
		m.mu.Lock()
		m.delivering = true
		m.mu.Unlock()
		defer func() { m.mu.Lock(); m.delivering = false; m.mu.Unlock() }()
	}
	for {
		m.mu.Lock()
		if len(m.journal.EntryDrops) == 0 {
			m.mu.Unlock()
			return
		}
		d := m.journal.EntryDrops[0]
		m.mu.Unlock()
		m.deliver(d)
		m.mu.Lock()
		if len(m.journal.EntryDrops) > 0 && m.journal.EntryDrops[0].ID == d.ID {
			m.journal.EntryDrops = m.journal.EntryDrops[1:]
		}
		err := m.saveLocked()
		m.mu.Unlock()
		if err != nil {
			m.deps.Logger().Error("hub could not persist the dropped-entry outbox", zap.Error(err))
			return
		}
	}
}

func hubEndDeliveryID(lot *hubProjectedLot) string {
	return fmt.Sprintf("end/%s/%s", lot.Record.Strategy, lot.Record.Lot)
}
