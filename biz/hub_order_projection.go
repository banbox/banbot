package biz

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg"
	"github.com/shopspring/decimal"
)

type hubProjectedLot struct {
	Terminal                                               bool
	Record                                                 execution.TimeSeriesProjectionRecord
	Entered, Exited                                        int64
	EntryCost, ExitCost, Fees, ExitFees, Realized, Funding decimal.Decimal
	EntryMS, ExitMS                                        int64
}

func (m *HubOrderMgr) projectionName() string { return "hub-ts/" + m.journal.Session }
func hubLotKey(strategy execution.StrategyID, lot execution.VirtualLotID) string {
	return string(strategy) + "\x00" + string(lot)
}

// poll folds committed facts in order. Snapshot hydration never supplies a
// historical callback's fill totals, prices, fees, or status. The journal is
// fsynced before ACK; callbacks carry stable IDs for crash-time deduplication.
func (m *HubOrderMgr) poll(restore bool) error {
	m.mu.Lock()
	if m.polling {
		done, reentrant := m.pollDone, m.delivering
		m.mu.Unlock()
		if reentrant {
			return nil
		}
		select {
		case <-done:
			return m.poll(restore)
		case <-m.ctx.Done():
			return m.ctx.Err()
		}
	}
	m.polling = true
	m.pollDone = make(chan struct{})
	m.mu.Unlock()
	defer func() { m.mu.Lock(); m.polling = false; close(m.pollDone); m.mu.Unlock() }()
	if err := m.deliverPending(); err != nil {
		return err
	}
	for {
		m.mu.Lock()
		keys := make([]execution.TimeSeriesLotKey, 0, len(m.journal.Lots))
		for _, l := range m.journal.Lots {
			if !l.Terminal {
				keys = append(keys, l.Record.TimeSeriesLotKey)
			}
		}
		name := m.projectionName()
		m.mu.Unlock()
		page, err := m.config.Client.TimeSeriesProjection(m.ctx, m.config.Account, execution.TimeSeriesProjectionRequest{Name: name, Limit: 512, Lots: keys})
		if err != nil {
			return err
		}
		m.mu.Lock()
		if page.Cursor > m.journal.Cursor {
			m.mu.Unlock()
			return errors.New("biz: Hub projection cursor ahead of durable robot state")
		}
		m.pending = map[string]execution.PendingDefinition{}
		for _, p := range page.Pending {
			m.pending[hubLotKey(p.Order.Strategy, p.Order.Lot)] = p
		}
		records := map[string]execution.TimeSeriesProjectionRecord{}
		for _, r := range page.Records {
			if b, ok := m.config.Bridge.Strategies[r.Metadata.StrategyName]; ok && b.ID == r.Strategy {
				records[hubLotKey(r.Strategy, r.Lot)] = r
			}
		}
		previous := page.Cursor
		for _, event := range page.Events {
			if event.ID == "" || event.Checkpoint <= previous || event.Checkpoint > page.Snapshot.Checkpoint {
				m.mu.Unlock()
				return errors.New("biz: invalid Hub projection event sequence")
			}
			previous = event.Checkpoint
		}
		if len(page.Events) == 0 && m.journal.Cursor < page.Snapshot.Checkpoint {
			m.mu.Unlock()
			return errors.New("biz: Hub projection event gap")
		}
		m.mu.Unlock()
		for _, event := range page.Events {
			m.mu.Lock()
			if event.Checkpoint <= m.journal.Cursor {
				m.mu.Unlock()
				continue
			}
			var accepted execution.StrategyAcceptedEvent
			if event.Kind == "StrategyAccepted" {
				if err = json.Unmarshal(event.Payload, &accepted); err != nil {
					m.mu.Unlock()
					return err
				}
			}
			affected, err := timeSeriesEventLots(event)
			if err != nil {
				m.mu.Unlock()
				return err
			}
			var fillOrderID string
			if event.Kind == "OrderState" {
				var changed execution.OrderStateEvent
				if err = json.Unmarshal(event.Payload, &changed); err != nil {
					m.mu.Unlock()
					return err
				}
				kinds := map[string]execution.IntentKind{}
				for _, a := range changed.Allocations {
					kinds[hubLotKey(a.Strategy, a.Lot)] = a.Kind
				}
				m.journal.OrderKinds[changed.OrderID] = kinds
			}
			if event.Kind == "ExchangeFill" {
				var fill execution.FillReport
				if err = json.Unmarshal(event.Payload, &fill); err != nil {
					m.mu.Unlock()
					return err
				}
				fillOrderID = fill.OrderID
			}
			costs, err := m.eventCostsLocked(event, records)
			if err != nil {
				m.mu.Unlock()
				return err
			}
			var deliveries []hubOrderDelivery
			orderedKeys := make([]execution.TimeSeriesLotKey, 0, len(affected))
			for key := range affected {
				orderedKeys = append(orderedKeys, key)
			}
			sort.Slice(orderedKeys, func(i, j int) bool {
				if orderedKeys[i].Strategy != orderedKeys[j].Strategy {
					return orderedKeys[i].Strategy < orderedKeys[j].Strategy
				}
				return orderedKeys[i].Lot < orderedKeys[j].Lot
			})
			for _, key := range orderedKeys {
				k := hubLotKey(key.Strategy, key.Lot)
				record, ok := records[k]
				if !ok {
					continue
				}
				if accepted.Metadata != nil && accepted.Strategy == key.Strategy && accepted.Lot == key.Lot {
					record.Metadata = *accepted.Metadata
				}
				lot := m.journal.Lots[k]
				if lot == nil {
					lot = &hubProjectedLot{Record: record}
					m.journal.Lots[k] = lot
				} else if event.Kind == "StrategyAccepted" {
					lot.Record = record
				}
				for _, entry := range event.Ledger {
					if entry.Strategy != key.Strategy || entry.Lot != key.Lot {
						continue
					}
					lot.Fees = lot.Fees.Add(entry.Fee)
					lot.Realized = lot.Realized.Add(entry.RealizedPnL)
					if entry.Kind == "Funding" {
						lot.Funding = lot.Funding.Add(entry.CashDelta)
					}
					if entry.QuantityDelta == 0 {
						if entry.Kind == "FeeCorrection" && m.journal.OrderKinds[fillOrderID][k] == execution.ExitIntent {
							lot.ExitFees = lot.ExitFees.Add(entry.Fee)
						}
						continue
					}
					steps := entry.QuantityDelta
					if steps < 0 {
						steps = -steps
					}
					isEnter := entry.QuantityDelta > 0 != lot.Record.Metadata.Short
					notional := costs[entry.ID]
					if isEnter {
						lot.Entered += steps
						lot.EntryCost = lot.EntryCost.Add(notional)
						lot.EntryMS = entry.AtMS
					} else {
						lot.Exited += steps
						lot.ExitCost = lot.ExitCost.Add(notional)
						lot.ExitFees = lot.ExitFees.Add(entry.Fee)
						lot.ExitMS = entry.AtMS
					}
					visible, e := m.viewLocked(lot, false)
					if e != nil {
						m.mu.Unlock()
						return e
					}
					visible.Info["shared_event_id"] = event.ID
					visible.Info["shared_event_fee"] = entry.Fee.String()
					kind := strat.OdChgEnterFill
					if !isEnter {
						kind = strat.OdChgExitFill
					}
					deliveries = append(deliveries, hubOrderDelivery{visible, kind, fmt.Sprintf("%s/%d", event.ID, entry.ID), true, isEnter})
				}
				if event.Kind == "StrategyAccepted" {
					visible, e := m.viewLocked(lot, false)
					if e != nil {
						m.mu.Unlock()
						return e
					}
					kind := strat.OdChgEnter
					if accepted.Kind == execution.ExitIntent {
						kind = strat.OdChgExit
					}
					visible.Info["shared_event_id"] = event.ID
					deliveries = append(deliveries, hubOrderDelivery{Order: visible, Kind: kind, ID: event.ID})
				}
				if event.Kind == "OrderState" {
					visible, e := m.viewLocked(lot, false)
					if e != nil {
						m.mu.Unlock()
						return e
					}
					visible.Info["shared_event_id"] = event.ID
					deliveries = append(deliveries, hubOrderDelivery{Order: visible, Kind: strat.OdChgOrderChanged, ID: event.ID + "/" + string(key.Lot)})
				}
			}
			m.journal.Cursor = event.Checkpoint
			if !restore {
				m.journal.PendingDeliveries = deliveries
			}
			err = m.saveLocked()
			m.mu.Unlock()
			if err != nil {
				return err
			}
			if err = m.deliverPending(); err != nil {
				return err
			}
			if err = m.config.Client.AdvanceProjection(m.ctx, m.config.Account, name, event.Checkpoint); err != nil {
				return err
			}
		}
		m.mu.Lock()
		// Latest metadata/protection definitions are presentation only, installed
		// after event replay so later exits cannot leak into earlier entry callbacks.
		for k, r := range records {
			if l := m.journal.Lots[k]; l != nil && m.journal.Cursor >= page.Snapshot.Checkpoint {
				l.Record = r
			}
		}
		held := map[string]int64{}
		for _, l := range page.Snapshot.Lots {
			held[hubLotKey(l.Strategy, l.ID)] = l.SignedSteps
		}
		for key, lot := range m.journal.Lots {
			_, active := m.pending[key]
			lot.Terminal = !active && held[key] == 0
			od, e := m.viewLocked(lot, true)
			if e != nil {
				m.mu.Unlock()
				return e
			}
			ods, lock := m.deps.Orders.GetOpenODs(m.deps.DefaultAccount)
			lock.Lock()
			ods[od.ID] = od
			lock.Unlock()
		}
		m.publishJobsLocked()
		if page.Reconciled {
			m.deps.Orders.SetSyncStamp(m.deps.DefaultAccount, max(int64(1), m.deps.Clock.TimeMS()))
		}
		err = m.saveLocked()
		cursor := m.journal.Cursor
		m.mu.Unlock()
		if err != nil {
			return err
		}
		if cursor > page.Cursor {
			if err := m.config.Client.AdvanceProjection(m.ctx, m.config.Account, name, cursor); err != nil {
				return err
			}
		}
		if len(page.Events) == 0 || cursor >= page.Snapshot.Checkpoint {
			return nil
		}
	}
}

type hubFillTotal struct {
	Steps int64
	Cost  decimal.Decimal
}

// Reconstruct the same exact notional allocation used by the account ledger.
// Cumulative reports are differences of frozen venue highwaters, not a new
// execution at the lifetime average price.
func (m *HubOrderMgr) eventCostsLocked(event execution.CommittedEvent, records map[string]execution.TimeSeriesProjectionRecord) (map[int64]decimal.Decimal, error) {
	costs := map[int64]decimal.Decimal{}
	var instrument execution.Instrument
	for _, e := range event.Ledger {
		if e.Strategy == "" || e.Lot == "" {
			continue
		}
		if r, ok := records[hubLotKey(e.Strategy, e.Lot)]; ok {
			instrument = m.config.Bridge.Instruments[r.Metadata.Symbol]
			break
		}
	}
	if instrument.ID == "" {
		return costs, nil
	}
	if event.Kind == "InternalFill" {
		var match execution.InternalMatch
		if err := json.Unmarshal(event.Payload, &match); err != nil {
			return nil, err
		}
		units, _ := match.Quote.Bid.Add(match.Quote.Ask).Div(decimal.NewFromInt(2)).QuoRem(instrument.PriceTick, 0)
		price := units.Mul(instrument.PriceTick)
		for _, e := range event.Ledger {
			if e.Strategy == "" || e.Lot == "" {
				continue
			}
			n := e.QuantityDelta
			if n < 0 {
				n = -n
			}
			costs[e.ID] = instrument.Notional(n, price)
		}
		return costs, nil
	}
	if event.Kind == "TerminalSettlement" || event.Kind == "OptionExpiry" {
		var price decimal.Decimal
		if event.Kind == "TerminalSettlement" {
			var e execution.TerminalSettlement
			if err := json.Unmarshal(event.Payload, &e); err != nil {
				return nil, err
			}
			price = e.CashValue
		} else {
			var e execution.OptionExpiry
			if err := json.Unmarshal(event.Payload, &e); err != nil {
				return nil, err
			}
			price = instrument.OptionPayoff(e.Fixing)
		}
		for _, e := range event.Ledger {
			if e.Strategy != "" && e.Lot != "" {
				n := e.QuantityDelta
				if n < 0 {
					n = -n
				}
				costs[e.ID] = instrument.Notional(n, price)
			}
		}
		return costs, nil
	}
	if event.Kind != "ExchangeFill" {
		return costs, nil
	}
	var fill execution.FillReport
	if err := json.Unmarshal(event.Payload, &fill); err != nil {
		return nil, err
	}
	old := m.journal.FillTotals[fill.OrderID]
	cost := fill.Cost
	if cost.IsZero() {
		cost = instrument.Notional(fill.Steps, fill.Price)
	}
	if fill.Cumulative {
		m.journal.FillTotals[fill.OrderID] = hubFillTotal{fill.Steps, cost}
		cost = cost.Sub(old.Cost)
	} else {
		m.journal.FillTotals[fill.OrderID] = hubFillTotal{old.Steps + fill.Steps, old.Cost.Add(cost)}
	}
	var steps int64
	for _, e := range event.Ledger {
		if e.Strategy == "" || e.Lot == "" {
			continue
		}
		n := e.QuantityDelta
		if n < 0 {
			n = -n
		}
		steps += n
	}
	if steps == 0 {
		return costs, nil
	}
	residual := cost
	last := int64(0)
	for _, e := range event.Ledger {
		if e.Strategy == "" || e.Lot == "" {
			continue
		}
		n := e.QuantityDelta
		if n < 0 {
			n = -n
		}
		if n == 0 {
			continue
		}
		part, _ := cost.Mul(decimal.NewFromInt(n)).QuoRem(decimal.NewFromInt(steps), instrument.MoneyScale)
		costs[e.ID] = part
		residual = residual.Sub(part)
		last = e.ID
	}
	unit := decimal.New(1, -instrument.MoneyScale)
	for _, e := range event.Ledger {
		if e.Strategy == "" || e.Lot == "" {
			continue
		}
		if e.QuantityDelta == 0 {
			continue
		}
		if residual.GreaterThanOrEqual(unit) {
			costs[e.ID] = costs[e.ID].Add(unit)
			residual = residual.Sub(unit)
		}
		if e.ID == last {
			costs[e.ID] = costs[e.ID].Add(residual)
		}
	}
	return costs, nil
}

type hubOrderDelivery struct {
	Order         *ormo.InOutOrder
	Kind          int
	ID            string
	Fill, IsEnter bool
}

func (m *HubOrderMgr) deliver(d hubOrderDelivery) {
	d.Order = d.Order.Clone()
	d.Order.BindState(m.deps.Orders)
	for _, key := range []string{ormo.OdInfoStopLoss, ormo.OdInfoTakeProfit} {
		if value, ok := d.Order.Info[key]; ok {
			if typed, ok := value.(*ormo.TriggerState); ok {
				d.Order.Info[key] = typed.Clone()
			} else {
				body, err := json.Marshal(value)
				if err == nil {
					var trigger ormo.TriggerState
					if json.Unmarshal(body, &trigger) == nil {
						d.Order.Info[key] = &trigger
					}
				}
			}
		}
	}

	m.mu.Lock()
	job := m.jobs[sharedJobIdentity{d.Order.Strategy, d.Order.Symbol, d.Order.Timeframe}]
	m.mu.Unlock()
	fn := func() {
		if d.Fill && m.callback != nil {
			m.callback(d.Order, d.IsEnter)
		}
		strat.FireOdChangeWithState(m.deps.Strategies, m.deps.DefaultAccount, d.Order, d.Kind)
		if job != nil && job.Strat.OnOrderChange != nil {
			job.Strat.OnOrderChange(job, d.Order, d.Kind)
		}
	}
	if job != nil {
		job.WithOrderEvent(d.ID, fn)
	} else {
		fn()
	}
}
func (m *HubOrderMgr) viewLocked(lot *hubProjectedLot, latest bool) (*ormo.InOutOrder, error) {
	r := lot.Record.Metadata
	var payload TimeSeriesEntryPayload
	if err := json.Unmarshal(r.Payload, &payload); err != nil {
		return nil, err
	}
	req := payload.Request
	instrument, ok := m.config.Bridge.Instruments[r.Symbol]
	if !ok {
		return nil, errors.New("biz: projected Hub instrument is undeclared")
	}
	p := m.pending[hubLotKey(lot.Record.Strategy, lot.Record.Lot)]
	amount := req.Amount
	if payload.QuantitySteps > 0 {
		amount = decimal.NewFromInt(payload.QuantitySteps).Mul(instrument.QuantityStep).InexactFloat64()
	}
	if payload.QuantitySteps == 0 && p.Order.Entry.QuantitySteps > 0 {
		amount = decimal.NewFromInt(p.Order.Entry.QuantitySteps).Mul(instrument.QuantityStep).InexactFloat64()
	}
	if amount == 0 {
		amount = decimal.NewFromInt(lot.Entered).Mul(instrument.QuantityStep).InexactFloat64()
	}
	quantity := decimal.NewFromInt(lot.Entered).Mul(instrument.QuantityStep)
	od := &ormo.InOutOrder{IOrder: &ormo.IOrder{ID: r.OrderID, TaskID: m.deps.Orders.GetTaskID(m.deps.DefaultAccount), Symbol: r.Symbol, Sid: int64(payload.SID), Timeframe: r.TimeFrame, Short: r.Short, Strategy: r.StrategyName, EnterTag: r.EnterTag, ExitTag: r.ExitTag, Leverage: req.Leverage, Profit: lot.Realized.Add(lot.Funding).Sub(lot.Fees).InexactFloat64()}, Enter: &ormo.ExOrder{Enter: true, Symbol: r.Symbol, Amount: amount, Filled: quantity.InexactFloat64(), Price: req.Limit, Fee: lot.Fees.Sub(lot.ExitFees).InexactFloat64(), FeeQuote: lot.Fees.Sub(lot.ExitFees).InexactFloat64(), CreateAt: r.CreatedMS, UpdateAt: lot.EntryMS}, Info: map[string]any{}}
	for k, v := range payload.SourceInfo {
		od.Info[k] = v
	}
	od.Info["shared_lot"], od.Info["shared_checkpoint"] = string(lot.Record.Lot), m.journal.Cursor
	od.InitPrice, od.EnterAt, od.Stop = r.InitPrice.InexactFloat64(), r.CreatedMS, req.Stop
	if req.OrderType < 0 || req.OrderType >= len(core.OrderTypeEnums) {
		return nil, errors.New("biz: invalid projected Hub order style")
	}
	od.Enter.OrderType = core.OrderTypeEnums[req.OrderType]
	od.Enter.Side = banexg.OdSideBuy
	if r.Short {
		od.Enter.Side = banexg.OdSideSell
	}
	if quantity.IsPositive() {
		od.Enter.Status = ormo.OdStatusClosed
		od.Enter.Average = lot.EntryCost.Div(quantity.Mul(instrument.ContractSize)).InexactFloat64()
		od.Status = ormo.InOutStatusFullEnter
		if quantity.LessThan(decimal.NewFromFloat(amount)) {
			od.Status = ormo.InOutStatusPartEnter
			od.Enter.Status = ormo.OdStatusPartOK
		}
	}
	if lot.Exited > 0 || r.ExitQuantitySteps > 0 || latest && p.Order.Exit != nil {
		od.ExitAt = lot.ExitMS
		od.Exit = &ormo.ExOrder{Symbol: r.Symbol, Amount: quantity.InexactFloat64(), Filled: decimal.NewFromInt(lot.Exited).Mul(instrument.QuantityStep).InexactFloat64(), UpdateAt: lot.ExitMS, Fee: lot.ExitFees.InexactFloat64(), FeeQuote: lot.ExitFees.InexactFloat64(), OrderType: banexg.OdTypeMarket, Side: banexg.OdSideSell}
		if r.Short {
			od.Exit.Side = banexg.OdSideBuy
		}
		if r.ExitQuantitySteps > 0 {
			od.Exit.Amount = decimal.NewFromInt(r.ExitQuantitySteps).Mul(instrument.QuantityStep).InexactFloat64()
		}
		if r.ExitConditions != nil {
			od.Exit.Price = r.ExitConditions.Limit.InexactFloat64()
			if od.Exit.Price > 0 {
				od.Exit.OrderType = banexg.OdTypeLimit
			}
			if r.ExitConditions.PostOnly {
				od.Exit.OrderType = banexg.OdTypeLimitMaker
			}
		}

		if lot.Exited > 0 {
			od.Exit.Average = lot.ExitCost.Div(decimal.NewFromInt(lot.Exited).Mul(instrument.QuantityStep).Mul(instrument.ContractSize)).InexactFloat64()
			od.Status = ormo.InOutStatusPartExit
			if lot.Exited >= lot.Entered {
				od.Status = ormo.InOutStatusFullExit
				od.Exit.Status = ormo.OdStatusClosed
			}
		}
		if latest && p.Order.Exit != nil {
			od.Exit.Price = p.Order.Exit.Conditions.Limit.InexactFloat64()
			if od.Exit.Price > 0 {
				od.Exit.OrderType = banexg.OdTypeLimit
			}
		}
	}
	if latest && (p.Order.Canceled || lot.Terminal) && lot.Entered == 0 {
		od.Status = ormo.InOutStatusDelete
	}
	for _, kind := range []string{"stop_loss", "take_profit"} {
		price, limit, rate, tag, key := req.StopLoss, req.StopLossLimit, req.StopLossRate, req.StopLossTag, ormo.OdInfoStopLoss
		if kind == "take_profit" {
			price, limit, rate, tag, key = req.TakeProfit, req.TakeProfitLimit, req.TakeProfitRate, req.TakeProfitTag, ormo.OdInfoTakeProfit
		}
		if latest {
			if trigger, ok := p.Order.Protections[kind]; ok {
				limit = trigger.Conditions.Limit.InexactFloat64()
				if level, ok := p.Order.ProtectionLevels[kind]; ok {
					price = level.InexactFloat64()
				} else if trigger.Conditions.Stop.IsPositive() {
					price = trigger.Conditions.Stop.InexactFloat64()
				}
			}
		}
		if price > 0 {
			od.Info[key] = &ormo.TriggerState{ExitTrigger: &ormo.ExitTrigger{Price: price, Limit: limit, Rate: rate, Tag: tag}, Hit: latest && (p.Order.ProtectionDone[kind] || p.Order.Protections[kind].Triggered)}
		}
	}
	od.Info[ormo.OdInfoCallbackPct], od.Info[ormo.OdInfoActivePrice] = req.CallbackPct, req.ActivationPrice
	od.BindState(m.deps.Orders)
	return od, nil
}
func (m *HubOrderMgr) publishJobsLocked() {
	ods, lock := m.deps.Orders.GetOpenODs(m.deps.DefaultAccount)
	lock.Lock()
	copy := make([]*ormo.InOutOrder, 0, len(ods))
	for _, od := range ods {
		copy = append(copy, od)
	}
	lock.Unlock()
	sort.Slice(copy, func(i, j int) bool { return copy[i].ID < copy[j].ID })
	for _, job := range m.jobs {
		var long, short []*ormo.InOutOrder
		for _, od := range copy {
			if od.Strategy == job.Strat.Name && od.Symbol == job.Symbol.Symbol && od.Timeframe == job.TimeFrame && od.Status < ormo.InOutStatusFullExit {
				if od.Short {
					short = append(short, od)
				} else {
					long = append(long, od)
				}
			}
		}
		job.SetProjectedOrders(long, short)
	}
}

func initHubOrderMgr(deps RuntimeDeps, callback FnOdCb) {
	if existing := deps.Trading.OrderManager(deps.DefaultAccount); existing != nil {
		if _, ok := existing.(*HubOrderMgr); !ok {
			panic("biz: local manager must stop before Hub cutover")
		}
		return
	}
	manager, err := NewHubOrderMgr(deps, deps.HubOrderBridge, callback)
	if err != nil {
		panic(err)
	}
	deps.Trading.SetOrderManager(deps.DefaultAccount, manager)
	deps.Orders.SetSoftwareEditListener(manager.EditOrder)
}

func (m *HubOrderMgr) deliverPending() error {
	m.mu.Lock()
	pending := append([]hubOrderDelivery(nil), m.journal.PendingDeliveries...)
	m.mu.Unlock()
	m.mu.Lock()
	m.delivering = true
	m.mu.Unlock()
	for _, d := range pending {
		m.deliver(d)
	}
	m.mu.Lock()
	m.delivering = false
	m.mu.Unlock()
	if len(pending) == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.journal.PendingDeliveries = nil
	return m.saveLocked()
}

// Restart hydration is deliberately callback-free. New committed events and a
// durable callback outbox are consumed only after the runtime binds its jobs.
func (m *HubOrderMgr) restoreViewsLocked() error {
	orders, lock := m.deps.Orders.GetOpenODs(m.deps.DefaultAccount)
	for _, lot := range m.journal.Lots {
		od, err := m.viewLocked(lot, true)
		if err != nil {
			return err
		}
		lock.Lock()
		orders[od.ID] = od
		lock.Unlock()
	}
	m.publishJobsLocked()
	return nil
}
