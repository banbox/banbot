package biz

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/shopspring/decimal"
)

const sharedCheckpointStrategy execution.StrategyID = "__legacy_bridge"

func sharedCommandName(key string) string {
	hash := sha256.Sum256([]byte(key))
	return "legacy-ts/command:" + hex.EncodeToString(hash[:])
}

func sharedLotName(strategy execution.StrategyID, lot execution.VirtualLotID) string {
	body, _ := json.Marshal([]string{string(strategy), string(lot)})
	hash := sha256.Sum256(body)
	return "legacy-ts/order:lot:" + hex.EncodeToString(hash[:])
}

// Old checkpoints are imported in memory and split by the next accepted
// mutation. New checkpoints retain only active keys; individual compatibility
// records remain point-readable after their lots close or callbacks lag.
func loadSharedCheckpoint(store *execution.Store, ctx context.Context, version string) (sharedTSCheckpoint, error) {
	state := sharedTSCheckpoint{Version: version, Orders: map[string]*sharedTSOrder{}, Commands: map[string]sharedTSCommand{}, store: store, ctx: ctx}
	body, err := store.StrategyCheckpoint(ctx, sharedCheckpointStrategy, "legacy-ts")
	if errors.Is(err, sql.ErrNoRows) {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	if err := json.Unmarshal(body, &state); err != nil {
		return state, err
	}
	if state.Version != version || state.StorageVersion > 1 {
		return state, errors.New("biz: shared legacy declaration or checkpoint version changed; explicit migration required")
	}
	if state.Orders == nil {
		state.Orders = map[string]*sharedTSOrder{}
	}
	if state.Commands == nil {
		state.Commands = map[string]sharedTSCommand{}
	}
	for _, key := range state.ActiveOrders {
		if _, err := state.loadOrder(key); err != nil {
			return state, err
		}
	}
	return state, nil
}

func (state *sharedTSCheckpoint) loadOrder(key string) (*sharedTSOrder, error) {
	if order := state.Orders[key]; order != nil {
		return order, nil
	}
	body, err := state.store.StrategyCheckpoint(state.ctx, sharedCheckpointStrategy, "legacy-ts/order:id:"+key)
	if err != nil {
		return nil, err
	}
	var order sharedTSOrder
	if err := json.Unmarshal(body, &order); err != nil {
		return nil, err
	}
	if sharedOrderKey(order.Strategy, order.ID) != key {
		return nil, errors.New("biz: shared order checkpoint identity mismatch")
	}
	state.Orders[key] = &order
	return &order, nil
}

func (state *sharedTSCheckpoint) loadLot(strategy execution.StrategyID, lot execution.VirtualLotID) error {
	for _, order := range state.Orders {
		if order.Strategy == strategy && order.Lot == lot {
			return nil
		}
	}
	body, err := state.store.StrategyCheckpoint(state.ctx, sharedCheckpointStrategy, sharedLotName(strategy, lot))
	if err != nil {
		return err
	}
	var key string
	if err := json.Unmarshal(body, &key); err != nil {
		return err
	}
	order, err := state.loadOrder(key)
	if err != nil {
		return err
	}
	if order.Strategy != strategy || order.Lot != lot {
		return errors.New("biz: shared lot checkpoint identity mismatch")
	}
	return nil
}

type sharedCommandRecord struct {
	Key string
	sharedTSCommand
}

func (state *sharedTSCheckpoint) loadCommand(key string) (sharedTSCommand, bool, error) {
	if command, ok := state.Commands[key]; ok {
		return command, true, nil
	}
	if state.store == nil {
		return sharedTSCommand{}, false, nil
	}
	body, err := state.store.StrategyCheckpoint(state.ctx, sharedCheckpointStrategy, sharedCommandName(key))
	if errors.Is(err, sql.ErrNoRows) {
		return sharedTSCommand{}, false, nil
	}
	if err != nil {
		return sharedTSCommand{}, false, err
	}
	var record sharedCommandRecord
	if err := json.Unmarshal(body, &record); err != nil {
		return sharedTSCommand{}, false, err
	}
	if record.Key != key {
		return sharedTSCommand{}, false, errors.New("biz: shared command checkpoint identity mismatch")
	}
	state.Commands[key] = record.sharedTSCommand
	return record.sharedTSCommand, true, nil
}

func splitSharedCheckpoint(state sharedTSCheckpoint, snapshot execution.AccountSnapshot) (execution.StrategyCheckpoint, error) {
	checkpoint := execution.StrategyCheckpoint{Strategy: sharedCheckpointStrategy, Name: "legacy-ts", Events: state.Accepted}
	active := map[string]bool{}
	for _, lot := range snapshot.Lots {
		if lot.SignedSteps != 0 {
			active[sharedLotName(lot.Strategy, lot.ID)] = true
		}
	}
	for _, order := range snapshot.Orders {
		for _, allocation := range order.Intent.Allocations {
			active[sharedLotName(allocation.Strategy, allocation.Lot)] = true
		}
	}
	state.ActiveOrders = nil
	keys := make([]string, 0, len(state.Orders))
	for key := range state.Orders {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	add := func(name string, value any) error {
		body, err := json.Marshal(value)
		if err != nil {
			return err
		}
		checkpoint.Records = append(checkpoint.Records, execution.StrategyCheckpoint{Strategy: sharedCheckpointStrategy, Name: name, Payload: body})
		return nil
	}
	for _, key := range keys {
		order := state.Orders[key]
		if err := add("legacy-ts/order:id:"+key, order); err != nil {
			return checkpoint, err
		}
		if err := add(sharedLotName(order.Strategy, order.Lot), key); err != nil {
			return checkpoint, err
		}
		terminalEntry := order.Canceled || order.Entry.State == execution.Filled || order.Entry.State == execution.Canceled || order.Entry.State == execution.Expired
		if active[sharedLotName(order.Strategy, order.Lot)] || !terminalEntry || order.Desired != 0 || order.ResumeEntrySteps != 0 {
			state.ActiveOrders = append(state.ActiveOrders, key)
		}
	}
	for key, command := range state.Commands {
		if err := add(sharedCommandName(key), sharedCommandRecord{key, command}); err != nil {
			return checkpoint, err
		}
	}
	state.StorageVersion = 1
	state.Orders, state.Commands = nil, nil
	body, err := json.Marshal(state)
	if err != nil {
		return checkpoint, fmt.Errorf("biz: shared active checkpoint: %w", err)
	}
	checkpoint.Payload = body
	return checkpoint, nil
}

func (m *SharedOrderMgr) loadProjectionOrders(state *sharedTSCheckpoint, event execution.CommittedEvent) error {
	load := func(strategy execution.StrategyID, lot execution.VirtualLotID) error {
		for _, binding := range m.config.Strategies {
			if binding.ID == strategy {
				err := state.loadLot(strategy, lot)
				if errors.Is(err, sql.ErrNoRows) {
					return nil // manually owned or pre-bridge lots have no TS facade
				}
				return err
			}
		}
		return nil
	}
	for _, posting := range event.Ledger {
		if posting.Lot != "" {
			if err := load(posting.Strategy, posting.Lot); err != nil {
				return err
			}
		}
	}
	switch event.Kind {
	case "StrategyAccepted":
		var accepted execution.StrategyAcceptedEvent
		if err := json.Unmarshal(event.Payload, &accepted); err != nil {
			return err
		}
		return load(accepted.Strategy, accepted.Lot)
	case "OrderState":
		var changed execution.OrderStateEvent
		if err := json.Unmarshal(event.Payload, &changed); err != nil {
			return err
		}
		for _, allocation := range changed.Allocations {
			if err := load(allocation.Strategy, allocation.Lot); err != nil {
				return err
			}
		}
	}
	return nil
}

// Cold-enabled simulations keep closed compatibility rows in indexed history.
// A command can still return its row, and callbacks retain their own values,
// without the runtime registry holding every settled order.
func (m *SharedOrderMgr) pruneClosedFacades() {
	if !m.account.Service().Store().HasMemoryHistory() {
		return
	}
	orders, lock := m.deps.Orders.GetOpenODs(m.deps.DefaultAccount)
	lock.Lock()
	for id, order := range orders {
		if order != nil && order.Status >= ormo.InOutStatusFullExit {
			delete(orders, id)
		}
	}
	lock.Unlock()
}

// Open lot quantity is a net balance. Entry quantity is an immutable fill
// total, including entries followed by partial exits and resumed pending legs.
// Imported genesis predates typed postings and remains explicit source evidence.
func sharedEnteredSteps(store *execution.Store, ctx context.Context, record *sharedTSOrder, instrument execution.Instrument) (int64, error) {
	side := execution.Buy
	if record.Request.Short {
		side = execution.Sell
	}
	steps, err := store.LotEntrySteps(ctx, record.Strategy, record.Lot, side)
	if err != nil {
		return 0, err
	}
	if record.SourceEntrySteps == nil && record.SourceTaskID != 0 {
		source, err := store.LegacySource(ctx, record.Strategy, record.Lot)
		if err != nil {
			return 0, err
		}
		var original ormo.InOutOrder
		if err := json.Unmarshal(source.RawJSON, &original); err != nil {
			return 0, err
		}
		if original.Enter == nil {
			return 0, errors.New("biz: legacy entry quantity evidence missing")
		}
		quantity, err := execution.QuantitySteps(decimal.NewFromFloat(original.Enter.Filled), instrument.QuantityStep)
		if err != nil {
			return 0, err
		}
		record.SourceEntrySteps = &quantity
	}
	if record.SourceEntrySteps != nil {
		initial := *record.SourceEntrySteps
		if initial < 0 || steps > math.MaxInt64-initial {
			return 0, errors.New("biz: legacy entry quantity overflow")
		}
		steps += initial
	}
	return max(record.Entry.FilledSteps, steps), nil
}

// VisitOrderViews reads compatibility report rows in bounded metadata pages.
// Call after input/decision producers have stopped and joined for a stable
// final report. The visitor runs outside account admission and owns its row.
func (m *SharedOrderMgr) VisitOrderViews(ctx context.Context, visit func(*ormo.InOutOrder) error) error {
	if visit == nil {
		return errors.New("biz: order report visitor is required")
	}
	emit := func(id int64) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := m.projectSnapshot(false, id); err != nil {
			return err
		}
		orders, lock := m.deps.Orders.GetOpenODs(m.deps.DefaultAccount)
		lock.Lock()
		row := orders[id].Clone()
		lock.Unlock()
		m.pruneClosedFacades()
		if row == nil {
			return errors.New("biz: historical compatibility projection missing")
		}
		return visit(row)
	}
	var old sharedTSCheckpoint
	if err := m.account.WithState(func(s *SharedAccount) error {
		var err error
		old, err = loadSharedCheckpoint(s.Store(), ctx, m.config.Version)
		return err
	}); err != nil {
		return err
	}
	if old.StorageVersion == 0 {
		keys := make([]string, 0, len(old.Orders))
		for key := range old.Orders {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if err := emit(old.Orders[key].ID); err != nil {
				return err
			}
		}
		return nil
	}
	const prefix = "legacy-ts/order:id:"
	after := ""
	for {
		var page []execution.StrategyCheckpoint
		if err := m.account.WithState(func(s *SharedAccount) error {
			var err error
			page, err = s.Store().StrategyCheckpointsAfter(ctx, sharedCheckpointStrategy, prefix, after, 64)
			return err
		}); err != nil {
			return err
		}
		if len(page) == 0 {
			return nil
		}
		for _, item := range page {
			var record sharedTSOrder
			if err := json.Unmarshal(item.Payload, &record); err != nil {
				return err
			}
			if sharedOrderKey(record.Strategy, record.ID) != strings.TrimPrefix(item.Name, prefix) {
				return errors.New("biz: historical order identity mismatch")
			}
			if err := emit(record.ID); err != nil {
				return err
			}
		}
		after = page[len(page)-1].Name
	}
}
