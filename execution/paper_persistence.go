package execution

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

const paperJournalStrategy StrategyID = "execution-paper-venue"
const paperJournalHeadName = "journal-v1"

type paperJournalConfig struct {
	Cash, Fee, Slippage decimal.Decimal
	Model               PaperFillModel
}
type paperJournalHead struct {
	Version  int
	Config   paperJournalConfig
	Sequence int64
}
type paperJournal struct {
	head    paperJournalHead
	store   *Store
	failure error
}
type paperSubmitOperation struct {
	Intent OrderIntent
	Client string
	// OrderIntent intentionally excludes this owner-assigned clock from JSON.
	SubmitAtMS int64
}
type paperQuoteOperation struct {
	Instrument string
	Quote      VisibleQuote
	NowMS      int64
}
type paperOperation struct {
	Kind     string
	Submit   *paperSubmitOperation `json:",omitempty"`
	Quote    *paperQuoteOperation  `json:",omitempty"`
	Cancel   string                `json:",omitempty"`
	Cash     *decimal.Decimal      `json:",omitempty"`
	Option   *OptionExpiry         `json:",omitempty"`
	Terminal *TerminalSettlement   `json:",omitempty"`
	Orders   []string
	Proof    [32]byte
}

// NewDurablePaperAdapter opts into a venue journal in the account's own SQLite
// transaction domain. BindStore must complete before the venue can be used.
// Ordinary paper/backtest constructors remain entirely memory-only.
func NewDurablePaperAdapter(cash, fee, slip decimal.Decimal, model PaperFillModel) (*PaperAdapter, error) {
	adapter, err := NewPaperAdapterWithFillModel(cash, fee, slip, model)
	if err != nil {
		return nil, err
	}
	adapter.journal = &paperJournal{head: paperJournalHead{Version: 1, Config: paperJournalConfig{cash, fee, slip, model}}}
	return adapter, nil
}

func paperJournalOperationName(sequence int64) string {
	return fmt.Sprintf("operation:%020d", sequence)
}

func (a *PaperAdapter) durableReadyLocked() error {
	if a.journal == nil {
		return nil
	}
	if a.journal.failure != nil {
		return a.journal.failure
	}
	if a.journal.store == nil {
		return errors.New("execution: durable paper venue is not bound")
	}
	return nil
}

func (a *PaperAdapter) bindJournalLocked(store *Store) error {
	if store == nil || store.memory != nil {
		return errors.New("execution: durable paper venue requires the account SQLite store")
	}
	if a.journal.store != nil {
		if a.journal.store != store {
			return errors.New("execution: durable paper venue already belongs to another store")
		}
		return a.journal.failure
	}
	ctx := context.Background()
	body, err := store.StrategyCheckpoint(ctx, paperJournalStrategy, paperJournalHeadName)
	if errors.Is(err, sql.ErrNoRows) {
		snapshot, readErr := store.Snapshot(ctx)
		if readErr != nil {
			return readErr
		}
		if snapshot.Checkpoint != 0 || len(snapshot.Orders) != 0 || len(snapshot.Lots) != 0 || !snapshot.AccountSettledCash.IsZero() {
			return errors.New("execution: existing ledger lacks its paper venue journal; refusing inferred reconstruction")
		}
		body, err = json.Marshal(a.journal.head)
		if err == nil {
			err = store.SaveStrategyCheckpoint(ctx, paperJournalStrategy, paperJournalHeadName, body)
		}
		if err != nil {
			return err
		}
		a.journal.store = store
		return nil
	}
	if err != nil {
		return err
	}
	var head paperJournalHead
	if err := json.Unmarshal(body, &head); err != nil {
		return err
	}
	expected, _ := json.Marshal(a.journal.head.Config)
	actual, _ := json.Marshal(head.Config)
	if head.Version != 1 || head.Sequence < 0 || string(expected) != string(actual) {
		return errors.New("execution: durable paper venue configuration/version mismatch")
	}
	// Replay only venue facts committed before acknowledgement. Ledger positions
	// are deliberately not consulted to synthesize receipts or cost bases.
	for sequence := int64(1); sequence <= head.Sequence; sequence++ {
		body, err := store.StrategyCheckpoint(ctx, paperJournalStrategy, paperJournalOperationName(sequence))
		if err != nil {
			return fmt.Errorf("execution: missing paper venue operation %d: %w", sequence, err)
		}
		var operation paperOperation
		if err := json.Unmarshal(body, &operation); err != nil {
			return err
		}
		if err := a.replayPaperOperationLocked(ctx, operation); err != nil {
			return fmt.Errorf("execution: replay paper venue operation %d: %w", sequence, err)
		}
		proof, err := a.paperProofLocked(operation.Orders)
		if err != nil || proof != operation.Proof {
			return errors.New("execution: paper venue replay diverged from its committed proof")
		}
	}
	a.journal.head = head
	a.journal.store = store
	return nil
}

func (a *PaperAdapter) replayPaperOperationLocked(ctx context.Context, operation paperOperation) error {
	switch operation.Kind {
	case "submit":
		if operation.Submit == nil {
			return errors.New("execution: paper submit journal payload missing")
		}
		intent := operation.Submit.Intent
		intent.SubmitAtMS = operation.Submit.SubmitAtMS
		_, err := a.submitLocked(ctx, intent, operation.Submit.Client)
		return err
	case "quote":
		if operation.Quote == nil {
			return errors.New("execution: paper quote journal payload missing")
		}
		_, err := a.advanceQuoteLocked(ctx, operation.Quote.Instrument, operation.Quote.Quote, operation.Quote.NowMS)
		return err
	case "cancel":
		_, err := a.cancelLocked(ctx, operation.Cancel)
		return err
	case "cash":
		if operation.Cash == nil {
			return errors.New("execution: paper cash journal payload missing")
		}
		a.cash = a.cash.Add(*operation.Cash)
		return nil
	case "option":
		if operation.Option == nil {
			return errors.New("execution: paper option journal payload missing")
		}
		if err := operation.Option.validate(); err != nil {
			return err
		}
		if err := a.validateOptionExpiryLocked(*operation.Option); err != nil {
			return err
		}
		a.applyOptionExpiryLocked(*operation.Option)
		return nil
	case "terminal":
		if operation.Terminal == nil {
			return errors.New("execution: paper settlement journal payload missing")
		}
		if err := operation.Terminal.validate(); err != nil {
			return err
		}
		if err := a.validateTerminalSettlementLocked(*operation.Terminal); err != nil {
			return err
		}
		_, err := a.applyTerminalSettlementLocked(*operation.Terminal)
		return err
	default:
		return errors.New("execution: unknown paper venue journal operation")
	}
}

func (a *PaperAdapter) touchedPaperOrders(operation paperOperation) []string {
	var ids []string
	switch operation.Kind {
	case "submit":
		ids = append(ids, operation.Submit.Intent.ID)
	case "cancel":
		ids = append(ids, strings.TrimPrefix(operation.Cancel, "paper:"))
	case "quote":
		for id, order := range a.pending {
			if order.intent.Instrument.ID == operation.Quote.Instrument {
				ids = append(ids, id)
			}
		}
	}
	sort.Strings(ids)
	return ids
}

// The proof covers financial state and touched receipts, not a full historical
// snapshot written after success. Old orders remain in the append-only journal.
func (a *PaperAdapter) paperProofLocked(ids []string) ([32]byte, error) {
	type position struct {
		Steps      int64
		Basis      decimal.Decimal
		Instrument Instrument
	}
	type totals struct {
		Slippage, Turnover decimal.Decimal
		Fills              int
	}
	proof := struct {
		Cash                                decimal.Decimal
		Fills                               int
		LastFill                            FillReport
		LastSourceAt                        int64
		Positions                           map[string]position
		Totals                              map[StrategyID]totals
		Quotes                              map[string]VisibleQuote
		Orders                              map[string]*archivedPaperOrder
		OptionExpiries, TerminalSettlements map[string]string
		OrderCount, ClientCount             int
	}{Cash: a.cash, Fills: a.fills, LastFill: a.lastFill, LastSourceAt: a.lastSourceAt, Positions: map[string]position{}, Totals: map[StrategyID]totals{}, Quotes: a.quotes, Orders: map[string]*archivedPaperOrder{}, OptionExpiries: a.optionExpiries, TerminalSettlements: a.terminalSettlements, OrderCount: len(a.orders), ClientCount: len(a.clients)}
	for id, p := range a.positions {
		proof.Positions[id] = position{p.steps, p.costBasis, p.instrument}
	}
	for id, t := range a.totals {
		proof.Totals[id] = totals{t.slippage, t.turnover, t.fills}
	}
	for _, id := range ids {
		if order := a.orders[id]; order != nil {
			proof.Orders[id] = &archivedPaperOrder{order.intent, order.client, order.result}
		} else {
			proof.Orders[id] = nil
		}
	}
	body, err := json.Marshal(proof)
	return sha256.Sum256(body), err
}

type paperUndo struct {
	cash                                decimal.Decimal
	fills                               int
	lastFill                            FillReport
	lastSource                          int64
	positions                           map[string]paperPosition
	totals                              map[StrategyID]paperTotals
	quotes                              map[string]VisibleQuote
	orders                              map[string]*paperOrder
	pending                             map[string]*paperOrder
	optionExpiries, terminalSettlements map[string]string
	client, oldClient                   string
	clientExisted                       bool
}

func (a *PaperAdapter) paperUndoLocked(operation paperOperation) paperUndo {
	undo := paperUndo{cash: a.cash, fills: a.fills, lastFill: a.lastFill, lastSource: a.lastSourceAt, positions: clonePaperMap(a.positions), totals: clonePaperMap(a.totals), quotes: clonePaperMap(a.quotes), orders: map[string]*paperOrder{}, pending: clonePaperMap(a.pending), optionExpiries: clonePaperMap(a.optionExpiries), terminalSettlements: clonePaperMap(a.terminalSettlements)}
	for _, id := range operation.Orders {
		if order := a.orders[id]; order != nil {
			copy := *order
			undo.orders[id] = &copy
			if undo.pending[id] != nil {
				undo.pending[id] = &copy
			}
		} else {
			undo.orders[id] = nil
		}
	}
	if operation.Submit != nil {
		undo.client = operation.Submit.Client
		undo.oldClient, undo.clientExisted = a.clients[undo.client]
	}
	return undo
}
func clonePaperMap[K comparable, V any](input map[K]V) map[K]V {
	if input == nil {
		return nil
	}
	output := make(map[K]V, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}
func (u paperUndo) restore(a *PaperAdapter) {
	a.cash, a.fills, a.lastFill, a.lastSourceAt = u.cash, u.fills, u.lastFill, u.lastSource
	a.positions, a.totals, a.quotes, a.pending = u.positions, u.totals, u.quotes, u.pending
	a.optionExpiries, a.terminalSettlements = u.optionExpiries, u.terminalSettlements
	for id, order := range u.orders {
		if order == nil {
			delete(a.orders, id)
		} else {
			a.orders[id] = order
		}
	}
	if u.client != "" {
		if u.clientExisted {
			a.clients[u.client] = u.oldClient
		} else {
			delete(a.clients, u.client)
		}
	}
}

// mutatePaperLocked owns the outer transaction, including any account posting
// made by funding or settlement callbacks. Receipt publication follows commit.
func (a *PaperAdapter) mutatePaperLocked(ctx context.Context, operation paperOperation, mutate func(context.Context) error) error {
	if a.journal == nil {
		return mutate(ctx)
	}
	if err := a.durableReadyLocked(); err != nil {
		return err
	}
	if _, nested := ctx.Value(storeTxContextKey{}).(scopedStoreTx); nested {
		return errors.New("execution: durable paper mutation must own its commit boundary")
	}
	if a.journal.head.Sequence == math.MaxInt64 {
		return errors.New("execution: paper journal sequence exhausted")
	}
	operation.Orders = a.touchedPaperOrders(operation)
	undo := a.paperUndoLocked(operation)
	next := a.journal.head
	next.Sequence++
	var operationBody []byte
	err := a.journal.store.atomically(ctx, func(txCtx context.Context) error {
		if err := mutate(txCtx); err != nil {
			return err
		}
		var err error
		operation.Proof, err = a.paperProofLocked(operation.Orders)
		if err != nil {
			return err
		}
		operationBody, err = json.Marshal(operation)
		if err != nil {
			return err
		}
		if err := a.journal.store.SaveStrategyCheckpoint(txCtx, paperJournalStrategy, paperJournalOperationName(next.Sequence), operationBody); err != nil {
			return err
		}
		headBody, err := json.Marshal(next)
		if err != nil {
			return err
		}
		return a.journal.store.SaveStrategyCheckpoint(txCtx, paperJournalStrategy, paperJournalHeadName, headBody)
	})
	if err == nil {
		a.journal.head = next
		return nil
	}
	// A commit error is not proof of absence. Re-read the authoritative head
	// before rolling back volatile state, or fail closed until restart/replay.
	checkCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	body, readErr := a.journal.store.StrategyCheckpoint(checkCtx, paperJournalStrategy, paperJournalHeadName)
	var head paperJournalHead
	if readErr == nil {
		readErr = json.Unmarshal(body, &head)
	}
	if readErr == nil && head.Sequence == a.journal.head.Sequence {
		undo.restore(a)
		return err
	}
	if readErr == nil && head.Sequence == next.Sequence {
		committed, checkErr := a.journal.store.StrategyCheckpoint(checkCtx, paperJournalStrategy, paperJournalOperationName(next.Sequence))
		if checkErr == nil && string(committed) == string(operationBody) {
			a.journal.head = next
			return err
		}
		readErr = errors.Join(checkErr, errors.New("execution: uncertain paper journal commit identity"))
	}
	undo.restore(a)
	a.journal.failure = errors.Join(err, readErr, errors.New("execution: paper journal requires restart and replay"))
	return a.journal.failure
}
