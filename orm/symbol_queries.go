package orm

import (
	"context"
	"fmt"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/jackc/pgx/v5"
)

// SymbolQueries binds symbol metadata operations to one SymbolState without
// extending the sqlc-generated Queries type. Ordinary Queries remain exactly
// the generated, allocation-free database handle; this adapter is used only
// at the low-frequency symbol/catalog boundary.
type SymbolQueries struct {
	*Queries
	symbols *SymbolState
}

// NewSymbolQueries binds an existing generated query handle to explicit
// symbol state. The database handle is not wrapped, so regular query calls
// retain their existing dispatch and cost.
func NewSymbolQueries(q *Queries, symbols *SymbolState) *SymbolQueries {
	if q == nil {
		q = New(nil)
	}
	return &SymbolQueries{Queries: q, symbols: symbols}
}

// WithSymbolState returns a new adapter sharing the same generated query
// handle and using the supplied symbol state.
func (q *Queries) WithSymbolState(symbols *SymbolState) *SymbolQueries {
	return NewSymbolQueries(q, symbols)
}

func (q *SymbolQueries) symbolState() *SymbolState {
	if q == nil {
		return loadDefaultSymbolState()
	}
	if q.symbols == nil {
		if q.Queries != nil && q.Queries.storage != nil && !q.Queries.storage.legacy {
			return nil
		}
		return loadDefaultSymbolState()
	}
	return q.symbols
}

// resolveQuerySymbolState resolves the catalog for an operation that accepts
// both a query handle and an optional state. A state explicitly supplied by a
// caller wins; otherwise a state already bound to the query is used. Only a
// query on the package-level legacy facade may fall back to the default
// catalog. Explicit Storage handles must fail closed when neither owner is
// present, otherwise a SID can be resolved against a different database's
// catalog.
func resolveQuerySymbolState(q *Queries, state *SymbolState) (*SymbolState, error) {
	if state != nil {
		return state, nil
	}
	if q != nil {
		if q.symbols != nil {
			return q.symbols, nil
		}
		if q.storage != nil && !q.storage.legacy {
			return nil, fmt.Errorf("explicit storage requires an explicit symbol state")
		}
	}
	return loadDefaultSymbolState(), nil
}

// EnsureListDates keeps listing-date discovery on the state bound to this
// adapter. The package-level EnsureListDates function remains the legacy
// facade for callers that still use a generated *Queries directly.
func (q *SymbolQueries) EnsureListDates(exchange banexg.BanExchange, exsMap map[int32]*ExSymbol, exsList []*ExSymbol) *errs.Error {
	if q == nil {
		return errs.NewMsg(core.ErrBadConfig, "symbol query is required")
	}
	state := q.symbolState()
	if state == nil {
		return errs.NewMsg(core.ErrBadConfig, "explicit storage requires an explicit symbol state")
	}
	return EnsureListDatesWithState(q.Queries, state, exchange, exsMap, exsList)
}

// WithTx preserves the explicit symbol state across a transaction handle.
func (q *SymbolQueries) WithTx(tx pgx.Tx) *SymbolQueries {
	if q == nil {
		return NewSymbolQueries(nil, nil).WithTx(tx)
	}
	return NewSymbolQueries(q.Queries.WithTx(tx), q.symbols)
}

// NewTx preserves the explicit symbol state across the transaction helper
// implemented by the generated query handle's owner.
func (q *SymbolQueries) NewTx(ctx context.Context) (*Tx, *SymbolQueries, *errs.Error) {
	if q == nil {
		return nil, nil, errs.NewMsg(core.ErrRunTime, "nil symbol query")
	}
	tx, next, err := q.Queries.NewTx(ctx)
	if err != nil {
		return nil, nil, err
	}
	return tx, NewSymbolQueries(next, q.symbols), nil
}

func symbolStateOrDefault(state *SymbolState) *SymbolState {
	if state == nil {
		return loadDefaultSymbolState()
	}
	return state
}
