package execution

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestSQLiteCanceledTransactionCloseReleasesFile(t *testing.T) {
	for range 100 {
		path := filepath.Join(t.TempDir(), "canceled.db")
		store, err := OpenStoreWithLeaseDir(path, testIntent(Buy).Account, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		err = store.commit(ctx, func(tx *storeTxn) error {
			var checkpoint int64
			if err := tx.QueryRow(opReadAccountCheckpoint, store.accountID).Scan(&checkpoint); err != nil {
				return err
			}
			cancel()
			return nil
		})
		cancel()
		if err == nil {
			t.Fatal("canceled transaction committed")
		}
		if !errors.Is(err, context.Canceled) && err.Error() != "sql: transaction has already been committed or rolled back" {
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		if stats := store.db.Stats(); stats.InUse != 0 || stats.OpenConnections != 0 {
			t.Fatal("closed pool retained connections", stats)
		}
		if err := os.Remove(path); err != nil {
			t.Fatal("closed execution database remains locked", err)
		}
	}
}

func TestSQLiteCanceledReadsFinalizeBeforeClose(t *testing.T) {
	for _, kind := range []string{"rows", "row"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "reads.db")
			store, err := OpenStoreWithLeaseDir(path, testIntent(Buy).Account, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			fundStrategies(t, store)
			ctx, cancel := context.WithCancel(context.Background())
			err = store.commit(ctx, func(tx *storeTxn) error {
				if kind == "rows" {
					rows, err := tx.Query(opListStrategyCash, store.accountID)
					if err != nil {
						return err
					}
					defer rows.Close()
					cancel()
					if rows.Next() {
						t.Fatal("canceled rows continued reading")
					}
					return rows.Err()
				}
				row := tx.QueryRow(opReadAccountCheckpoint, store.accountID)
				cancel()
				var checkpoint int64
				return row.Scan(&checkpoint)
			})
			cancel()
			if !errors.Is(err, context.Canceled) {
				t.Fatal("read cancellation lost", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if stats := store.db.Stats(); stats.InUse != 0 || stats.OpenConnections != 0 {
				t.Fatal("closed pool retained read", stats)
			}
			if err := os.Remove(path); err != nil {
				t.Fatal("read statement leaked Windows handle", err)
			}
		})
	}
}
