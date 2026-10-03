package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

type archivedPaperOrder struct {
	Intent OrderIntent
	Client string
	Result QueryResult
}

// BindStore enables cold receipt read-through only when the account explicitly
// selected indexed history. File-free paper sessions retain their existing path.
func (a *PaperAdapter) BindStore(store *Store) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.history != nil && a.history != store {
		return errors.New("execution: paper venue already belongs to another history store")
	}
	if store != nil && store.memory != nil && store.memory.history != nil {
		a.history = store
	}
	return nil
}

func (a *PaperAdapter) archiveSettled(ctx context.Context) error {
	if a.history == nil {
		return nil
	}
	for id, paper := range a.orders {
		if _, pending := a.pending[id]; pending {
			continue
		}
		order, err := a.history.Order(ctx, id)
		if err != nil {
			return err
		}
		if !terminalOrder(string(order.State)) {
			continue // account acknowledgement/fill has not committed yet
		}
		body, err := json.Marshal(archivedPaperOrder{paper.intent, paper.client, paper.result})
		if err != nil {
			return err
		}
		if err := a.history.commit(ctx, func(tx *storeTxn) error {
			return tx.putRecord(recordsPaperOrder, memoryRecord{Account: a.history.accountID, ID: id, ClientId: paper.client, Payload: string(body)}, insertRecord, nil)
		}); err != nil {
			return err
		}
		delete(a.orders, id)
		delete(a.clients, paper.client)
	}
	return nil
}

func (a *PaperAdapter) archivedOrder(ctx context.Context, id, client string) (*paperOrder, error) {
	if a.history == nil {
		return nil, nil
	}
	var record archivedPaperOrder
	err := a.history.readRecord(ctx, func(tx *storeTxn) error {
		if id == "" {
			id = tx.historyIndex(recordsPaperOrder, "client", client)
		}
		if tx.recordErr != nil {
			return tx.recordErr
		}
		if id == "" {
			return sql.ErrNoRows
		}
		r, found := tx.memoryRecord(recordsPaperOrder, memoryKey{a.history.accountID, id, ""})
		if tx.recordErr != nil {
			return tx.recordErr
		}
		if !found {
			return sql.ErrNoRows
		}
		return json.Unmarshal([]byte(r.Payload), &record)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &paperOrder{intent: record.Intent, client: record.Client, result: record.Result}, nil
}
