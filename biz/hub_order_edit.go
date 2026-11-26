package biz

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"strings"
)

// hubFollowupEdit ends the command ID of an edit (limit, protections, exit
// limit). The owner reports every accepted command as a StrategyAccepted event
// with the entry/exit intent kind; the suffix lets the projection tell an edit
// from a created entry or a requested exit, also after a restart or replay.
const hubFollowupEdit = "/edt"

// hubEditAttempts bounds the sends of one frozen edit. An edit is advisory: a
// stale or refused one is dropped, never resent forever.
const hubEditAttempts = 3

// errHubEditUnconfirmed: an edit was frozen and sent but the owner has not
// acknowledged it (and did not refuse it). It stays frozen and is resent, up to
// hubEditAttempts sends in total, then dropped.
var errHubEditUnconfirmed = errors.New("biz: Hub edit not confirmed")

func hubEditID(commandID string) bool { return strings.HasSuffix(commandID, hubFollowupEdit) }

// errHubEditSkipped marks an edit that was not applied and needs no recovery:
// the lot is gone, the definition is missing, the Hub was unreachable before
// anything was frozen, or the edit itself is invalid. It never stops the robot.
type errHubEditSkipped struct{ err error }

func (e *errHubEditSkipped) Error() string { return e.err.Error() }
func (e *errHubEditSkipped) Unwrap() error { return e.err }

func hubEditSkip(err error) error { return &errHubEditSkipped{err} }

func (m *HubOrderMgr) EditOrder(od *ormo.InOutOrder, action string) {
	if od == nil {
		return
	}
	m.mu.Lock()
	m.started = true
	err := m.editLocked(od, action)
	m.mu.Unlock()
	var skipped *errHubEditSkipped
	switch {
	case errors.Is(err, errSharedEntryRejected):
		return
	case errors.As(err, &skipped):
		m.deps.Logger().Warn("hub order edit not applied", zap.Int64("order_id", od.ID), zap.String("action", action), zap.Error(skipped.err))
		// A closed lot is refreshed by the projection; a failed poll is fatal as always.
		err = m.poll(false)
	case errors.Is(err, errHubEditUnconfirmed):
		m.deps.Logger().Warn("hub order edit not confirmed by the owner; it is retried", zap.Int64("order_id", od.ID), zap.String("action", action), zap.Error(err))
		err = nil
	case err == nil:
		err = m.poll(false)
	}
	if err != nil {
		m.fail(err)
	}
}

func (m *HubOrderMgr) editLocked(od *ormo.InOutOrder, action string) error {
	_, b, err := m.strategy(od.Strategy)
	if err != nil {
		return hubEditSkip(err)
	}
	lotID, ok := od.Info["shared_lot"].(string)
	if !ok {
		return hubEditSkip(errors.New("biz: Hub edit missing lot"))
	}
	key := hubLotKey(b.ID, execution.VirtualLotID(lotID))
	lot := m.journal.Lots[key]
	if lot == nil {
		return hubEditSkip(errors.New("biz: Hub edit lot is unmapped"))
	}
	// Re-read the authoritative revision; an asynchronous trigger may have changed it.
	definitions, err := m.config.Client.PendingOrders(m.ctx, m.config.Account)
	if err != nil {
		return hubEditSkip(err)
	}
	var p *execution.PendingDefinition
	for i := range definitions {
		if definitions[i].Order.Strategy == b.ID && string(definitions[i].Order.Lot) == lotID {
			p = &definitions[i]
			break
		}
	}
	if p == nil {
		return hubEditSkip(errors.New("biz: Hub edit pending definition missing"))
	}
	plan := m.journal.Plans[key]
	if plan != nil && plan.Ladder != nil && action == ormo.OdInfoTakeProfit {
		// The ladder owns the lot's take-profit; a single-level edit would
		// silently replace the remaining rungs.
		m.deps.Logger().Warn("take-profit edit rejected: the lot has a take-profit ladder", zap.Int64("order_id", od.ID))
		return errSharedEntryRejected
	}
	command := execution.PendingCommand{Strategy: b.ID, Lot: execution.VirtualLotID(lotID), ExpectedRevision: p.CommandRevision}
	var payload TimeSeriesEntryPayload
	if err = json.Unmarshal(lot.Record.Metadata.Payload, &payload); err != nil {
		return hubEditSkip(err)
	}
	tick := m.config.Bridge.Instruments[od.Symbol].PriceTick
	switch action {
	case ormo.OdActionLimitEnter:
		if od.Enter == nil || od.Enter.Price <= 0 {
			return hubEditSkip(errors.New("biz: missing edited Hub entry price"))
		}
		command.Action = execution.PendingEditLimit
		command.Limit = decimal.NewFromFloat(roundToTick(od.Enter.Price, tick))
		payload.Request.Limit = od.Enter.Price
	case ormo.OdActionLimitExit:
		if od.Exit == nil || od.Exit.Price <= 0 {
			return hubEditSkip(errors.New("biz: missing edited Hub exit price"))
		}
		command.Action = execution.PendingEditExitLimit
		command.Limit = decimal.NewFromFloat(roundToTick(od.Exit.Price, tick))
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
		return hubEditSkip(fmt.Errorf("biz: unsupported Hub edit %s", action))
	}
	if err = validateLegacyNumbers(&payload.Request); err != nil {
		return hubEditSkip(err)
	}
	if command.Action == execution.PendingSetProtections {
		// Order prices (protection limits) sit on the tick like the entry path's;
		// trigger levels are thresholds and stay as given.
		payload.Request.StopLossLimit = roundToTick(payload.Request.StopLossLimit, tick)
		payload.Request.TakeProfitLimit = roundToTick(payload.Request.TakeProfitLimit, tick)
		command.Protections = sharedProtectionSpecs(payload.Request)
		if plan != nil && plan.Ladder != nil {
			// set_protections replaces the whole set: keep the ladder's active
			// rung (never the single TakeProfit fields) and settle a finished
			// rung first so it is not re-armed.
			snap, e := m.config.Client.Snapshot(m.ctx, m.config.Account)
			if e != nil {
				return hubEditSkip(e)
			}
			var held int64
			for _, l := range snap.Lots {
				if l.Strategy == b.ID && string(l.ID) == lotID {
					held = l.SignedSteps
				}
			}
			next, _ := m.ladderStep(*plan.Ladder, lot, *p, held)
			command.Protections = m.ladderSpecs(next, command.Protections, nil, m.lotAverage(lot))
			// BEPending is deliberately kept: the owner may refuse this command (it is
			// then dropped), so ladderStep clears it once the owner's stop is tight.
			*plan.Ladder = next
		}
	}
	metadata := lot.Record.Metadata
	metadata.Payload, err = json.Marshal(payload)
	if err != nil {
		return hubEditSkip(err)
	}
	command.Metadata = &metadata
	// Allocated last, so a skipped edit never consumes a command identity. If the
	// allocation resent older frozen commands and moved the revision, the owner
	// refuses this edit as stale and it is dropped (see sendLocked).
	id, epoch, seq, err := m.nextLocked(b.ID)
	if err != nil {
		return err
	}
	command.ID, command.StrategyEpoch, command.StrategySequence = id+hubFollowupEdit, epoch, seq
	frozen := &hubFrozenCommand{Pending: &command, OrderID: od.ID}
	m.journal.Commands[string(b.ID)+"/edit/"+command.ID] = frozen
	if err = m.saveLocked(); err != nil {
		return err
	}
	return m.sendLocked(frozen)
}
