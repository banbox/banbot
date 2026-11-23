package biz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/banbox/banbot/execution"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

// One-time import of an embedded account's SharedOrderMgr state into a new
// HubOrderMgr journal (embedded production robot, first start, no journal yet).
//
// The legacy facade keeps its per-account metadata in the reserved
// __legacy_bridge strategy checkpoints; the Hub facade only renders lots that
// carry time-series metadata in the account owner. Both facades share the
// owner's pending book and ledger, so nothing about quantities, protections or
// cash moves. The import
//
//  1. reads the legacy checkpoint with loadSharedCheckpoint (never writes it),
//  2. validates EVERY active legacy order and refuses the whole import, before
//     anything is written, when one cannot be mapped faithfully,
//  3. binds the metadata in the owner (BindTimeSeriesLegacy, idempotent),
//  4. seeds the new journal (lots, admission, ladder/OCO plans, commands) in
//     memory; NewHubOrderMgr writes it atomically afterwards.
//
// A crash anywhere before the journal write leaves only idempotent owner
// binds, so the next start repeats the import and converges. Fill totals are
// deliberately not seeded: the fresh journal replays the ledger from the start,
// which rebuilds them exactly as for any lot of a lost journal.
const hubLegacyMaxCommands = 200000

type hubLegacyMigration struct {
	AtMS     int64
	Orders   []int64
	Commands int
}

type hubLegacyLot struct {
	key      string
	order    *sharedTSOrder
	metadata execution.TimeSeriesMetadata
	plan     *hubLotPlan
	ocoSeq   int64
	clock    *execution.TimeSeriesLegacyEntryClock
}

func (m *HubOrderMgr) importLegacyCheckpoint(ctx context.Context) error {
	borrow := m.deps.SharedExecution
	if !m.config.LocalAccount || borrow == nil || borrow.AccountKey() != m.config.Account {
		return nil
	}
	var state sharedTSCheckpoint
	var records []sharedCommandRecord
	if err := borrow.WithState(func(s *SharedAccount) error {
		var err error
		if state, err = loadSharedCheckpoint(s.Store(), ctx, m.config.Bridge.Version); err != nil {
			return err
		}
		if state.StorageVersion == 0 {
			for key, command := range state.Commands {
				records = append(records, sharedCommandRecord{Key: key, sharedTSCommand: command})
			}
			return nil
		}
		const prefix = "legacy-ts/command:"
		after := ""
		for {
			page, err := s.Store().StrategyCheckpointsAfter(ctx, sharedCheckpointStrategy, prefix, after, 256)
			if err != nil {
				return err
			}
			for _, item := range page {
				var record sharedCommandRecord
				if err := json.Unmarshal(item.Payload, &record); err != nil {
					return fmt.Errorf("biz: legacy command checkpoint %s: %w", item.Name, err)
				}
				if records = append(records, record); len(records) > hubLegacyMaxCommands {
					return errors.New("biz: legacy checkpoint holds too many commands to import")
				}
			}
			if len(page) < 256 {
				return nil
			}
			after = page[len(page)-1].Name
		}
	}); err != nil {
		return fmt.Errorf("biz: legacy checkpoint import: %w", err)
	}
	snapshot, err := borrow.Snapshot(ctx)
	if err != nil {
		return err
	}
	definitions, err := borrow.PendingOrders(ctx)
	if err != nil {
		return err
	}
	lots, err := m.planLegacyImport(state, snapshot, definitions)
	if err != nil {
		return err
	}
	if len(lots) == 0 && len(records) == 0 {
		return nil
	}
	for _, l := range lots {
		binding := execution.TimeSeriesLegacyBinding{Strategy: l.order.Strategy, Lot: l.order.Lot, Metadata: l.metadata, OCOGroup: l.order.Request.OCOGroup, OCOSeq: l.ocoSeq, Entry: l.clock}
		if err := borrow.BindTimeSeriesLegacy(ctx, binding); err != nil {
			return fmt.Errorf("biz: legacy order %d (%s %s) cannot be bound: %w", l.order.ID, l.order.StrategyName, l.order.Symbol, err)
		}
	}
	migrated := hubLegacyMigration{AtMS: m.deps.Clock.TimeMS()}
	for _, l := range lots {
		m.journal.Lots[l.key] = &hubProjectedLot{Record: execution.TimeSeriesProjectionRecord{TimeSeriesLotKey: execution.TimeSeriesLotKey{Strategy: l.order.Strategy, Lot: l.order.Lot}, Metadata: l.metadata}}
		if l.plan != nil {
			if m.journal.Plans == nil {
				m.journal.Plans = map[string]*hubLotPlan{}
			}
			m.journal.Plans[l.key] = l.plan
		}
		migrated.Orders = append(migrated.Orders, l.order.ID)
		m.deps.Logger().Info("hub facade migrated legacy order", zap.Int64("order_id", l.order.ID), zap.String("strategy", l.order.StrategyName), zap.String("symbol", l.order.Symbol),
			zap.String("timeframe", l.order.TimeFrame), zap.String("tag", l.order.Request.Tag), zap.Bool("short", l.order.Request.Short), zap.String("lot", string(l.order.Lot)), zap.Bool("ladder", l.plan != nil && l.plan.Ladder != nil), zap.String("oco_group", l.order.Request.OCOGroup))
	}
	m.journal.Admission = state.Admission
	migrated.Commands = m.importLegacyCommands(records)
	m.journal.Migrated = &migrated
	m.deps.Logger().Info("hub facade migrated the legacy shared checkpoint", zap.Int("orders", len(lots)), zap.Int("commands", migrated.Commands))
	return nil
}

// legacyActive mirrors the activity definition of splitSharedCheckpoint.
func legacyActive(order *sharedTSOrder, active map[string]bool) bool {
	terminalEntry := order.Canceled || order.Entry.State == execution.Filled || order.Entry.State == execution.Canceled || order.Entry.State == execution.Expired
	return active[sharedLotName(order.Strategy, order.Lot)] || !terminalEntry || order.Desired != 0 || order.ResumeEntrySteps != 0
}

func (m *HubOrderMgr) planLegacyImport(state sharedTSCheckpoint, snapshot execution.AccountSnapshot, definitions []execution.PendingDefinition) ([]hubLegacyLot, error) {
	active := map[string]bool{}
	held := map[string]int64{}
	for _, lot := range snapshot.Lots {
		if lot.SignedSteps != 0 {
			active[sharedLotName(lot.Strategy, lot.ID)] = true
			held[hubLotKey(lot.Strategy, lot.ID)] = lot.SignedSteps
		}
	}
	for _, order := range snapshot.Orders {
		for _, allocation := range order.Intent.Allocations {
			active[sharedLotName(allocation.Strategy, allocation.Lot)] = true
		}
	}
	pending := map[string]execution.PendingDefinition{}
	for _, d := range definitions {
		pending[hubLotKey(d.Order.Strategy, d.Order.Lot)] = d
	}
	keys := make([]string, 0, len(state.Orders))
	for key, order := range state.Orders {
		if order != nil && legacyActive(order, active) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	if len(keys) > 0 && state.StorageVersion == 0 {
		return nil, errors.New("biz: legacy checkpoint predates split storage; the shared facade was removed, so start it once with release v0.7.9 (which converts it) and then upgrade")
	}
	var lots []hubLegacyLot
	for _, key := range keys {
		l, err := m.planLegacyOrder(state.Orders[key], pending, held)
		if err != nil {
			return nil, fmt.Errorf("biz: legacy order %d (strategy %s, symbol %s) cannot be migrated faithfully: %w", state.Orders[key].ID, state.Orders[key].StrategyName, state.Orders[key].Symbol, err)
		}
		lots = append(lots, l)
	}
	return lots, nil
}

func (m *HubOrderMgr) planLegacyOrder(order *sharedTSOrder, pending map[string]execution.PendingDefinition, held map[string]int64) (hubLegacyLot, error) {
	l := hubLegacyLot{key: hubLotKey(order.Strategy, order.Lot), order: order}
	binding, ok := m.config.Bridge.Strategies[order.StrategyName]
	if !ok || binding.ID != order.Strategy {
		return l, errors.New("strategy is not declared by the Hub bridge")
	}
	instrument, ok := m.config.Bridge.Instruments[order.Symbol]
	if !ok || order.Entry.Instrument != instrument.ID {
		return l, errors.New("instrument is not declared by the Hub bridge")
	}
	if order.ID <= 0 || order.TimeFrame == "" || order.SID <= 0 || order.CreatedMS < 0 || order.InitPrice < 0 {
		return l, errors.New("incomplete order identity")
	}
	if order.SourceTaskID != 0 || order.SourceEntrySteps != nil {
		return l, errors.New("lot was imported from a pre-ledger order; its entry history cannot be rebuilt from the ledger")
	}
	if order.Request.Short != (order.Entry.Side == execution.Sell) {
		return l, errors.New("request direction differs from the entry side")
	}
	def, hasDef := pending[l.key]
	if !hasDef && held[l.key] == 0 {
		return l, errors.New("lot has neither a pending definition nor a held position")
	}
	payload, err := json.Marshal(TimeSeriesEntryPayload{QuantitySteps: order.Entry.QuantitySteps, Request: order.Request, SourceInfo: order.SourceInfo, SID: order.SID})
	if err != nil {
		return l, err
	}
	l.metadata = execution.TimeSeriesMetadata{OrderID: order.ID, StrategyName: order.StrategyName, Symbol: order.Symbol, TimeFrame: order.TimeFrame, EnterTag: order.Request.Tag, Short: order.Request.Short, CreatedMS: order.CreatedMS, InitPrice: decimal.NewFromFloat(order.InitPrice), Payload: payload}
	if order.Exit != nil && order.LastProtection == "" {
		l.metadata.ExitTag = order.ExitTag // explicit strategy exit in flight
	}
	ladder, err := legacyLadder(order, def, hasDef)
	if err != nil {
		return l, err
	}
	if order.Request.OCOGroup != "" {
		l.ocoSeq = order.ID // legacy order IDs are allocated in creation order
	}
	if hasDef && def.Order.Entry.Conditions.StopBars > 0 && def.Order.EntryFeedExpiry == nil && !def.Order.Canceled &&
		def.Order.Entry.State != execution.Filled && def.Order.Entry.State != execution.Canceled && def.Order.Entry.State != execution.Expired {
		if l.clock, err = m.legacyEntryClock(order, def); err != nil {
			return l, err
		}
	}
	if ladder != nil || order.Request.OCOGroup != "" {
		l.plan = &hubLotPlan{OCOGroup: order.Request.OCOGroup, OCOCancelled: order.Request.OCOGroup != "" && order.Canceled, Ladder: ladder}
	}
	return l, nil
}

// legacyLadder maps TPLadderIndex/TPRemainingShare/TPResolved onto hubLadder.
// Legacy mutates Request.TakeProfits in place (resolved prices, nearest
// first), so a resolved ladder is carried as is. Anything inconsistent with
// the owner's take-profit protection is refused instead of guessed.
func legacyLadder(order *sharedTSOrder, def execution.PendingDefinition, hasDef bool) (*hubLadder, error) {
	rungs := order.Request.TakeProfits
	if len(rungs) == 0 {
		if order.TPLadderIndex != 0 || order.TPRemainingShare != 0 || order.TPResolved {
			return nil, errors.New("ladder progress without ladder rungs")
		}
		return nil, nil
	}
	l := &hubLadder{Raw: slices.Clone(rungs), Remaining: 1, Short: order.Request.Short}
	if !order.TPResolved {
		if order.TPLadderIndex != 0 || order.TPRemainingShare != 0 {
			return nil, errors.New("unresolved ladder has progress")
		}
		mid := order.InitPrice
		if !l.resolve(hubLadderReference(&order.Request, mid)) {
			return nil, errors.New("take-profit ladder rung resolves to a non-positive price")
		}
		return l, nil
	}
	if order.TPLadderIndex < 0 || order.TPLadderIndex > len(rungs) || order.TPRemainingShare < 0 || order.TPRemainingShare > 1+1e-9 {
		return nil, errors.New("ladder progress out of range")
	}
	for _, r := range rungs {
		if r.Price <= 0 {
			return nil, errors.New("resolved ladder rung without a price")
		}
	}
	l.Rungs, l.Resolved, l.Index, l.Remaining = slices.Clone(rungs), true, order.TPLadderIndex, order.TPRemainingShare
	for i := 0; i < l.Index; i++ {
		if l.Rungs[i].MoveSLToBreakeven {
			// Legacy already tightened the stop in the request; the Hub clears
			// this flag at once when the owner's stop is at the entry average.
			l.BEPending = true
		}
	}
	if !hasDef || l.Index >= len(l.Rungs) || l.Remaining <= 1e-9 {
		return l, nil
	}
	if def.Order.ProtectionDone[string(execution.TakeProfitProtection)] && def.Order.Exit != nil && def.Order.Exit.State != execution.Filled {
		return nil, errors.New("a take-profit rung reduction is in flight; retry after it settles")
	}
	owner := protectionSpec(def.Specs, execution.TakeProfitProtection)
	if !owner.Level.IsPositive() || sameTPSpec(owner, l.tpSpec()) {
		return l, nil
	}
	if l.Index > 0 {
		prev := *l
		prev.Index--
		prev.Remaining += l.Rungs[prev.Index].ExitRate
		if sameTPSpec(owner, prev.tpSpec()) {
			return l, nil // settled rung not re-armed yet; the Hub installs the next one
		}
	}
	return nil, errors.New("the owner's take-profit protection does not match the legacy ladder progress")
}

// importLegacyCommands records every legacy command ID as a finished, Imported
// command. A strategy retrying a pre-migration CommandID gets the original
// order(s) back (idempotent replay, as on the legacy facade) without a hash
// comparison and without anything being sent. (The two facades hash different
// encodings of the request; the legacy hash cannot be replayed.)
func (m *HubOrderMgr) importLegacyCommands(records []sharedCommandRecord) int {
	byName := map[string]execution.StrategyID{}
	ids := map[string]bool{}
	for name, b := range m.config.Bridge.Strategies {
		byName[name] = b.ID
		ids[string(b.ID)] = true
	}
	imported := 0
	for _, record := range records {
		key, ok := m.legacyCommandKey(record.Key, byName, ids)
		if !ok || m.journal.Commands[key] != nil {
			continue
		}
		frozen := &hubFrozenCommand{Hash: "legacy-import:" + record.Hash, Done: true, Imported: true, OrderIDs: slices.Clone(record.Orders)}
		if len(record.Orders) > 0 {
			frozen.OrderID = record.Orders[0]
		}
		m.journal.Commands[key] = frozen
		imported++
	}
	return imported
}

func (m *HubOrderMgr) legacyCommandKey(key string, byName map[string]execution.StrategyID, ids map[string]bool) (string, bool) {
	for _, kind := range []string{"/entry/", "/exit/"} {
		i := strings.Index(key, kind)
		if i < 0 {
			continue
		}
		prefix, rest := key[:i], key[i+len(kind):]
		switch {
		case ids[prefix]:
			return key, true
		case byName[prefix] != "":
			return string(byName[prefix]) + kind + rest, true
		case prefix == "" && len(byName) == 1:
			for _, id := range byName {
				return string(id) + kind + rest, true
			}
		}
		return "", false
	}
	return "", false
}

// legacyEntryClock carries the remaining StopBars lifetime of a resting entry
// created on a quote-bar clock (CreatedBar/StopBars; legacy live quotes with
// Bar 0 are already feed-bound and never reach here). The hub path's quotes
// carry Bar 0, so the lifetime is rebound to the entry's feed: the bars
// consumed so far are read from the quote bar clock that is still visible now,
// and StopBars-consumed further closed bars remain. When no bar clock is
// visible the consumed bars are unknowable and the migration is refused.
func (m *HubOrderMgr) legacyEntryClock(order *sharedTSOrder, def execution.PendingDefinition) (*execution.TimeSeriesLegacyEntryClock, error) {
	var quote execution.VisibleQuote
	var err error
	now := m.deps.Clock.TimeMS()
	if m.config.Bridge.Quote != nil {
		quote, err = m.config.Bridge.Quote(order.Symbol, now)
	}
	if (err != nil || quote.Bar <= 0) && m.config.Client != nil {
		var q execution.VisibleQuote
		if q, err = m.config.Client.VisibleQuote(m.ctx, m.config.Account, m.config.Bridge.Instruments[order.Symbol].ID); err == nil {
			quote = q
		}
	}
	if err != nil {
		return nil, fmt.Errorf("the entry's StopBars lifetime needs the current bar clock: %w", err)
	}
	created, stop := def.Order.Entry.Conditions.CreatedBar, def.Order.Entry.Conditions.StopBars
	if quote.Bar <= 0 {
		return nil, fmt.Errorf("the entry expires after %d bars counted on a quote bar clock (created at bar %d) but no bar clock is visible to compute the bars already consumed; let it fill or expire under release v0.7.9 (the last release with the shared facade), then upgrade", stop, created)
	}
	if quote.Bar < created {
		return nil, fmt.Errorf("the visible bar clock %d is before the entry's creation bar %d", quote.Bar, created)
	}
	return &execution.TimeSeriesLegacyEntryClock{Feed: execution.PendingFeedID(sharedFeedKey(order.SID, order.TimeFrame)), RemainingBars: max(0, stop-(quote.Bar-created))}, nil
}
