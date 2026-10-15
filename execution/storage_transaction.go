package execution

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
)

// storeTxn is the small shared commit boundary for execution records. Domain
// methods use named operations; SQLite and memory own their commit mechanics.
type storeTxn struct {
	sql    *sql.Tx
	reader interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	}
	memory    *memoryState
	ctx       context.Context
	undo      []func()
	changed   map[historyRecordKey]bool
	recordErr error
}

// readRecord retains the serialized, allocation-consistent view used by Order
// without adding a SQLite transaction to this previously direct read path.
func (s *Store) readRecord(ctx context.Context, fn func(*storeTxn) error) error {
	if scope, ok := ctx.Value(storeTxContextKey{}).(scopedStoreTx); ok {
		if scope.store != s {
			return errors.New("execution: transaction belongs to another account store")
		}
		return fn(scope.tx)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("execution: store closed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := fn(&storeTxn{ctx: ctx, reader: s.db, memory: s.memory}); err != nil {
		return err
	}
	return ctx.Err()
}

func (s *Store) commit(ctx context.Context, fn func(*storeTxn) error) error {
	if scope, ok := ctx.Value(storeTxContextKey{}).(scopedStoreTx); ok {
		if scope.store != s {
			return errors.New("execution: transaction belongs to another account store")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return fn(scope.tx)
	}
	if s.memory == nil {
		return s.transaction(ctx, func(tx *sql.Tx) error { return fn(&storeTxn{sql: tx, ctx: ctx}) })
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("execution: store closed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	tx := &storeTxn{memory: s.memory, ctx: ctx}
	committed := false
	defer func() {
		if !committed {
			for n := len(tx.undo) - 1; n >= 0; n-- {
				tx.undo[n]()
			}
		}
	}()
	if err := fn(tx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := tx.saveHistory(); err != nil {
		return err
	}
	committed = true
	s.memory.version++
	s.memory.evictHistory()
	return nil
}

func (tx *storeTxn) Exec(operation storageOperation, args ...any) (sql.Result, error) {
	if err := tx.ctx.Err(); err != nil {
		return nil, err
	}
	if tx.sql != nil {
		return tx.sql.ExecContext(tx.ctx, sqliteStatements[operation], args...)
	}
	err := tx.memoryWrite(operation, args)
	if err == nil {
		err = tx.recordErr
	}
	return memoryResult(1), err
}

func (tx *storeTxn) Query(operation storageOperation, args ...any) (*storeRows, error) {
	if err := tx.ctx.Err(); err != nil {
		return nil, err
	}
	if tx.sql != nil {
		// modernc v1.39.1 can lose the newly created driver rows when context
		// cancellation races stmt.query's return. CloseV2 then leaves a zombie
		// statement/file handle. Join these fixed SQL reads and observe caller
		// cancellation through the rows wrapper after closing the statement.
		rows, err := tx.sql.QueryContext(context.WithoutCancel(tx.ctx), sqliteStatements[operation], args...)
		if err != nil {
			return nil, err
		}
		return &storeRows{sql: rows, ctx: tx.ctx}, nil
	}
	rows, err := tx.memoryRead(operation, args)
	if err == nil {
		err = tx.recordErr
	}
	return &storeRows{values: rows, index: -1}, err
}

func (tx *storeTxn) QueryRow(operation storageOperation, args ...any) *storeRow {
	if err := tx.ctx.Err(); err != nil {
		return &storeRow{err: err}
	}
	if tx.sql != nil {
		return &storeRow{sql: tx.sql.QueryRowContext(context.WithoutCancel(tx.ctx), sqliteStatements[operation], args...), ctx: tx.ctx}
	}
	if tx.memory == nil && tx.reader != nil {
		return &storeRow{sql: tx.reader.QueryRowContext(context.WithoutCancel(tx.ctx), sqliteStatements[operation], args...), ctx: tx.ctx}
	}
	rows, err := tx.memoryRead(operation, args)
	if err == nil {
		err = tx.recordErr
	}
	if err != nil {
		return &storeRow{err: err}
	}
	if len(rows) == 0 {
		return &storeRow{err: sql.ErrNoRows}
	}
	return &storeRow{values: rows[0]}
}

func (tx *storeTxn) QueryRowContext(ctx context.Context, operation storageOperation, args ...any) *storeRow {
	if err := ctx.Err(); err != nil {
		return &storeRow{err: err}
	}
	return tx.QueryRow(operation, args...)
}

type storeRow struct {
	sql    *sql.Row
	values []any
	err    error
	ctx    context.Context
}

func (r *storeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if r.sql != nil {
		if err := r.sql.Scan(dest...); err != nil {
			return err
		}
		return r.ctx.Err()
	}
	return scanMemory(r.values, dest)
}

type storeRows struct {
	sql    *sql.Rows
	values [][]any
	index  int
	closed bool
	ctx    context.Context
}

func (r *storeRows) Next() bool {
	if r.sql != nil {
		if r.ctx.Err() != nil {
			r.sql.Close()
			return false
		}
		return r.sql.Next()
	}
	if r.closed {
		return false
	}
	r.index++
	return r.index < len(r.values)
}
func (r *storeRows) Scan(dest ...any) error {
	if r.sql != nil {
		if err := r.sql.Scan(dest...); err != nil {
			return err
		}
		return r.ctx.Err()
	}
	if r.closed || r.index < 0 || r.index >= len(r.values) {
		return errors.New("execution: no current record")
	}
	return scanMemory(r.values[r.index], dest)
}
func (r *storeRows) Err() error {
	if r.sql != nil {
		return errors.Join(r.sql.Err(), r.ctx.Err())
	}
	return nil
}
func (r *storeRows) Close() error {
	if r.sql != nil {
		return r.sql.Close()
	}
	r.closed = true
	return nil
}

func scanMemory(values []any, dest []any) error {
	if len(values) != len(dest) {
		return errors.New("execution: record projection mismatch")
	}
	for n, value := range values {
		if scanner, ok := dest[n].(sql.Scanner); ok {
			if err := scanner.Scan(value); err != nil {
				return err
			}
			continue
		}
		ptr := reflect.ValueOf(dest[n])
		if ptr.Kind() != reflect.Pointer || ptr.IsNil() {
			return errors.New("execution: invalid record destination")
		}
		target := ptr.Elem()
		source := reflect.ValueOf(value)
		if !source.IsValid() || !source.Type().ConvertibleTo(target.Type()) {
			return fmt.Errorf("execution: incompatible record field %T", value)
		}
		target.Set(source.Convert(target.Type()))
	}
	return nil
}

type memoryResult int64

func (r memoryResult) LastInsertId() (int64, error) { return 0, nil }
func (r memoryResult) RowsAffected() (int64, error) { return int64(r), nil }
