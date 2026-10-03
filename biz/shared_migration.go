package biz

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

// ImportLegacyMigration is called only by a composition root which has actually
// stopped and joined the legacy runtime. checkStopped rechecks that lifecycle
// and the absence of its old order manager at both atomic import gates.
// Accounting and native order highwaters must already be explicitly attributed;
// they are never inferred from symbol netting or an order's display price.
func ImportLegacyMigration(b *SharedAccountBorrow, request execution.LegacyMigration, config *SharedOrderBridgeConfig, checkStopped func() error) (bool, error) {
	if err := config.Validate(); err != nil {
		return false, err
	}
	checkpoint, err := legacyProjectionCheckpoint(b.AccountKey(), request, config)
	if err != nil {
		return false, err
	}
	for _, c := range request.Checkpoints {
		if c.Strategy == "__legacy_bridge" && c.Name == "legacy-ts" {
			return false, errors.New("biz: caller must not replace generated legacy projection checkpoint")
		}
	}
	request.Checkpoints = append(request.Checkpoints, execution.LegacyCheckpoint{Strategy: "__legacy_bridge", Name: "legacy-ts", Payload: checkpoint})
	return b.ImportLegacyMigration(request, checkStopped)
}
func legacyProjectionCheckpoint(account execution.AccountKey, request execution.LegacyMigration, config *SharedOrderBridgeConfig) (json.RawMessage, error) {
	state := sharedTSCheckpoint{Version: config.Version, Orders: map[string]*sharedTSOrder{}}
	lots := map[string]execution.VirtualLot{}
	for _, l := range request.Lots {
		lots[string(l.Strategy)+"/"+string(l.ID)] = l
	}
	for _, mapping := range request.RawLegacyMap {
		var row ormo.InOutOrder
		if err := json.Unmarshal(mapping.RawJSON, &row); err != nil {
			return nil, err
		}
		if row.IOrder == nil || row.Enter == nil || row.ID != mapping.IOrderID || row.TaskID != mapping.TaskID || row.ID <= 0 {
			return nil, errors.New("biz: invalid legacy source order identity")
		}
		binding, ok := config.Strategies[row.Strategy]
		if !ok || binding.ID != mapping.Strategy {
			return nil, errors.New("biz: unmapped legacy strategy")
		}
		instrument, ok := config.Instruments[row.Symbol]
		if !ok {
			return nil, errors.New("biz: unmapped legacy instrument")
		}
		lot, ok := lots[string(mapping.Strategy)+"/"+string(mapping.Lot)]
		if !ok || lot.Instrument.ID != instrument.ID {
			return nil, errors.New("biz: missing attributed legacy lot")
		}
		steps, rem := decimal.NewFromFloat(row.Enter.Amount).QuoRem(instrument.QuantityStep, 0)
		filled, fillRem := decimal.NewFromFloat(row.Enter.Filled).QuoRem(instrument.QuantityStep, 0)
		if !rem.IsZero() || !fillRem.IsZero() || steps.IsNegative() || filled.IsNegative() || filled.GreaterThan(steps) {
			return nil, errors.New("biz: legacy amount is outside unit lattice")
		}
		// Raw JSON has generic maps; restore known typed trigger metadata before
		// using the legacy getters. Custom values and explicit NULL remain intact.
		row.GetInfoInt64(ormo.OdInfoStopBars)
		for _, key := range []string{ormo.OdInfoStopLoss, ormo.OdInfoTakeProfit} {
			if value := row.Info[key]; value != nil {
				body, err := json.Marshal(value)
				if err != nil {
					return nil, err
				}
				var trigger ormo.TriggerState
				if err := json.Unmarshal(body, &trigger); err != nil {
					return nil, err
				}
				row.Info[key] = &trigger
			}
		}
		stopBars, stopAfter := row.GetInfoInt64(ormo.OdInfoStopBars), row.GetInfoInt64(ormo.OdInfoStopAfter)
		if stopBars < 0 || stopAfter < 0 || stopBars > 0 && stopAfter == 0 && filled.LessThan(steps) {
			return nil, errors.New("biz: legacy pending expiry needs its original absolute deadline")
		}
		req := strat.EnterReq{StratName: row.Strategy, Tag: row.EnterTag, Short: row.Short, Amount: row.Enter.Amount, Leverage: row.Leverage, Limit: row.Enter.Price, Stop: row.Stop, StopBars: int(stopBars), ClientID: row.GetInfoString(ormo.OdInfoClientID)}
		switch row.Enter.OrderType {
		case "":
		case "market":
			req.OrderType = core.OrderTypeMarket
		case "limit":
			req.OrderType = core.OrderTypeLimit
		default:
			return nil, errors.New("biz: unsupported legacy entry order style")
		}
		if tg := row.GetStopLoss(); tg != nil && tg.ExitTrigger != nil {
			req.StopLoss, req.StopLossLimit, req.StopLossRate, req.StopLossTag = tg.Price, tg.Limit, tg.Rate, tg.Tag
		}
		if tg := row.GetTakeProfit(); tg != nil && tg.ExitTrigger != nil {
			req.TakeProfit, req.TakeProfitLimit, req.TakeProfitRate, req.TakeProfitTag = tg.Price, tg.Limit, tg.Rate, tg.Tag
		}
		req.CallbackPct = row.GetInfoFloat64(ormo.OdInfoCallbackPct)
		req.ActivationPrice = row.GetInfoFloat64(ormo.OdInfoActivePrice)
		trailingBest := float64(0)
		if value := row.Info[ormo.OdInfoTrailingBest]; value != nil {
			body, err := json.Marshal(value)
			if err != nil {
				return nil, err
			}
			if err := json.Unmarshal(body, &trailingBest); err != nil {
				return nil, err
			}
		}
		if math.IsNaN(trailingBest) || math.IsInf(trailingBest, 0) || trailingBest < 0 {
			return nil, errors.New("biz: invalid legacy trailing anchor")
		}
		if err := validateLegacyNumbers(&req); err != nil {
			return nil, err
		}
		if req.StopLossRate > 1 || req.TakeProfitRate > 1 || req.CallbackPct >= 100 {
			return nil, errors.New("biz: invalid legacy protection rate")
		}
		side := execution.Buy
		if row.Short {
			side = execution.Sell
		}
		entry := execution.EligibleIntent{ID: execution.VirtualIntentID(fmt.Sprintf("legacy-entry/%s/%d", binding.ID, row.ID)), Account: account, Strategy: binding.ID, Lot: mapping.Lot, Instrument: instrument.ID, Side: side, Kind: execution.EntryIntent, QuantitySteps: steps.IntPart(), FilledSteps: filled.IntPart(), Triggered: filled.IsPositive(), Conditions: execution.IntentConditions{Limit: decimal.NewFromFloat(row.Enter.Price), Stop: decimal.NewFromFloat(row.Stop), ExpiresAtMS: stopAfter}}
		entry.State = execution.Partial
		if filled.Equal(steps) {
			entry.State = execution.Filled
		}
		desired := lot.SignedSteps
		canceled := row.Status >= ormo.InOutStatusPartExit
		if canceled {
			desired = lot.SignedSteps
			entry.State = execution.Canceled
		}
		if err := entry.Validate(); err != nil {
			return nil, err
		}
		key := sharedOrderKey(binding.ID, row.ID)
		if state.Orders[key] != nil {
			return nil, errors.New("biz: duplicate legacy source identity")
		}
		state.Orders[key] = &sharedTSOrder{ID: row.ID, StrategyName: row.Strategy, Strategy: binding.ID, Lot: mapping.Lot, Symbol: row.Symbol, SID: int32(row.Sid), TimeFrame: row.Timeframe, Request: req, Entry: entry, Desired: desired, Canceled: canceled, ExitTag: row.ExitTag, Protections: map[string]execution.EligibleIntent{}, ProtectionDone: map[string]bool{}}
		record := state.Orders[key]
		initialEntrySteps := filled.IntPart()
		record.SourceEntrySteps = &initialEntrySteps
		record.SourceLotFees = lot.Fees
		if err := restoreLegacyExit(record, row.Exit, request, instrument, account); err != nil {
			return nil, err
		}
		record.ProtectionLevels = map[string]decimal.Decimal{}
		for _, kind := range []string{"stop_loss", "take_profit", "trailing"} {
			if !filled.IsPositive() {
				continue
			}
			conditions := execution.IntentConditions{}
			latched := false
			rate := float64(1)
			if kind == "trailing" {
				if req.CallbackPct == 0 {
					continue
				}
				conditions.TrailingPercent, conditions.ActivationPrice = decimal.NewFromFloat(req.CallbackPct), decimal.NewFromFloat(req.ActivationPrice)
			} else {
				tg := row.GetStopLoss()
				if kind == "take_profit" {
					tg = row.GetTakeProfit()
				}
				if tg == nil || tg.ExitTrigger == nil || tg.Price == 0 {
					continue
				}
				latched = tg.Hit
				conditions.Limit = decimal.NewFromFloat(tg.Limit)
				if tg.Rate > 0 {
					rate = tg.Rate
				}
				if kind == "stop_loss" {
					conditions.Stop = decimal.NewFromFloat(tg.Price)
				} else {
					record.ProtectionLevels[kind] = decimal.NewFromFloat(tg.Price)
				}
			}
			quantity, _ := execution.QuantitySteps(filled.Mul(decimal.NewFromFloat(rate)), decimal.NewFromInt(1))
			if quantity == 0 {
				continue
			}
			exitSide := execution.Sell
			if row.Short {
				exitSide = execution.Buy
			}
			protection := execution.EligibleIntent{ID: execution.VirtualIntentID(fmt.Sprintf("legacy-%s/%s", kind, mapping.Lot)), Account: account, Strategy: binding.ID, Lot: mapping.Lot, Instrument: instrument.ID, Side: exitSide, Kind: execution.ExitIntent, QuantitySteps: quantity, Conditions: conditions, Triggered: latched}
			if kind == "trailing" {
				protection.TrailingAnchor = decimal.NewFromFloat(trailingBest)
				protection.TrailingActive = req.ActivationPrice == 0 && protection.TrailingAnchor.IsPositive()
			}
			if err := protection.Validate(); err != nil {
				return nil, err
			}
			record.Protections[kind] = protection
		}
		state.Orders[key].SourceTaskID, state.Orders[key].SourceInfo, state.Orders[key].CreatedMS, state.Orders[key].InitPrice = row.TaskID, row.Info, row.EnterAt, row.InitPrice
		state.Serial = max(state.Serial, row.ID)
	}
	if len(state.Orders) != len(request.Lots) {
		return nil, errors.New("biz: every imported virtual lot needs an original legacy order")
	}
	return json.Marshal(state)
}

// Restore the source reduction before this lot becomes managed by the bridge.
// Confirmed venue allocation highwaters must describe the same original exit.
func restoreLegacyExit(record *sharedTSOrder, source *ormo.ExOrder, request execution.LegacyMigration, instrument execution.Instrument, account execution.AccountKey) error {
	var mapped []execution.StoredOrder
	for _, order := range request.Orders {
		for _, allocation := range order.Intent.Allocations {
			// A source exit cancels its pending legacy entry. The bridge does
			// not retain that native entry's cancel linkage, so accepting both
			// would leave an unmanaged counter-order and mixed source exit fees.
			if source != nil && allocation.Kind == execution.EntryIntent && allocation.Strategy == record.Strategy && allocation.Lot == record.Lot && (order.State == execution.OrderAcknowledged || order.State == execution.OrderPartial) {
				return errors.New("biz: overlapping legacy entry/exit native orders cannot preserve source entry cancellation")
			}
			if allocation.Kind == execution.ExitIntent && allocation.Strategy == record.Strategy && allocation.Lot == record.Lot {
				mapped = append(mapped, order)
				break
			}
		}
	}
	if source == nil {
		if len(mapped) > 0 {
			return errors.New("biz: native legacy exit lacks original source definition")
		}
		return nil
	}
	amount, rem := decimal.NewFromFloat(source.Amount).QuoRem(instrument.QuantityStep, 0)
	filled, fillRem := decimal.NewFromFloat(source.Filled).QuoRem(instrument.QuantityStep, 0)
	if source.Amount <= 0 || !rem.IsZero() || !fillRem.IsZero() || filled.IsNegative() || filled.GreaterThan(amount) || source.Price < 0 || source.Symbol != record.Symbol || source.Enter || source.Status < ormo.OdStatusInit || source.Status > ormo.OdStatusClosed {
		return errors.New("biz: invalid legacy exit quantities or instrument")
	}
	side := execution.Sell
	if record.Request.Short {
		side = execution.Buy
	}
	if execution.OrderSide(source.Side) != side || source.OrderType != "" && source.OrderType != "market" && source.OrderType != "limit" || source.OrderType == "limit" && source.Price <= 0 {
		return errors.New("biz: unsupported legacy exit side/order style")
	}
	held := record.Desired
	if held < 0 {
		held = -held
	}
	if record.Entry.FilledSteps-filled.IntPart() != held || amount.IntPart()-filled.IntPart() > held {
		return errors.New("biz: legacy exit highwater does not reconcile attributed lot")
	}
	limit := decimal.Zero
	if source.OrderType == "limit" {
		limit = decimal.NewFromFloat(source.Price)
	}
	exit := execution.EligibleIntent{ID: execution.VirtualIntentID(fmt.Sprintf("legacy-exit/%s", record.Lot)), Account: account, Strategy: record.Strategy, Lot: record.Lot, Instrument: instrument.ID, Kind: execution.ExitIntent, Side: side, QuantitySteps: amount.IntPart(), FilledSteps: filled.IntPart(), Conditions: execution.IntentConditions{Limit: limit}}
	var allocated, reported int64
	var linked execution.VirtualIntentID
	for _, order := range mapped {
		if source.Status == ormo.OdStatusClosed || source.OrderID != order.ExchangeID || order.Intent.Side != side || !order.Intent.Limit.Equal(limit) {
			return errors.New("biz: legacy source exit differs from native order")
		}
		for _, allocation := range order.Intent.Allocations {
			if allocation.Kind != execution.ExitIntent || allocation.Strategy != record.Strategy || allocation.Lot != record.Lot {
				continue
			}
			if linked != "" && linked != allocation.IntentID {
				return errors.New("biz: legacy source exit has ambiguous native intents")
			}
			linked = allocation.IntentID
			allocated += allocation.Steps
			reported += order.AllocationFilled[allocation.ID]
		}
		record.NativeExitOrders = append(record.NativeExitOrders, order.Intent.ID)
	}
	if len(mapped) > 0 {
		if allocated != amount.IntPart() || reported != filled.IntPart() {
			return errors.New("biz: legacy exit allocation/highwater differs from original")
		}
		found := false
		for _, plan := range request.Plans {
			for _, intent := range plan.Intents {
				if intent.ID != linked {
					continue
				}
				if intent.QuantitySteps != amount.IntPart() || intent.Kind != execution.ExitIntent || intent.Side != side || !intent.Conditions.Limit.Equal(limit) || intent.FilledSteps > reported {
					return errors.New("biz: legacy exit mapped intent contract differs")
				}
				exit, found = intent, true
			}
		}
		if !found {
			return errors.New("biz: legacy exit mapped intent missing")
		}
		// Reservation projection belongs to the authoritative native allocations,
		// not to the bridge's local eligibility copy.
		exit.ReservedSteps = 0
		exit.FilledSteps = reported
	} else if source.OrderID != "" && source.Status != ormo.OdStatusClosed {
		return errors.New("biz: legacy source exit lacks native allocation")
	}
	if source.Status == ormo.OdStatusClosed {
		exit.State = execution.Canceled
		if filled.Equal(amount) {
			exit.State = execution.Filled
		}
	} else {
		record.Desired = held - (amount.IntPart() - filled.IntPart())
		if record.Request.Short {
			record.Desired = -record.Desired
		}
	}
	if err := exit.Validate(); err != nil {
		return err
	}
	copy := *source
	record.SourceExit, record.Exit, record.Canceled, record.Entry.State = &copy, &exit, true, execution.Canceled
	return nil
}
