package orm

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

type failedRewriteCleanupDB struct{ fail bool }

func (db *failedRewriteCleanupDB) Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error) {
	if db.fail {
		return pgconn.CommandTag{}, errors.New("temporary cleanup failed")
	}
	return pgconn.CommandTag{}, nil
}

func TestAbortQuestRewritePreservesIntentUntilCleanupSucceeds(t *testing.T) {
	for _, kind := range []string{"table", "compact"} {
		t.Run(kind, func(t *testing.T) {
			store := &memoryQuestRewriteIntentStore{intent: &questRewriteSwapIntent{Source: "source", Temp: "tmp", Kind: kind}}
			installMemoryQuestRewriteIntentStore(t, store)
			db := &failedRewriteCleanupDB{fail: true}
			cause := errors.New("rename source to backup failed")
			err := abortQuestRewriteSwap(context.Background(), db, "source", "tmp", cause)
			if !errors.Is(err, cause) || !strings.Contains(err.Error(), "temporary cleanup failed") || store.intent == nil || store.removes != 0 {
				t.Fatalf("failed cleanup lost recovery state: %v, %+v", err, store)
			}
			db.fail = false
			err = abortQuestRewriteSwap(context.Background(), db, "source", "tmp", cause)
			if !errors.Is(err, cause) || store.intent != nil || store.removes != 1 {
				t.Fatalf("successful cleanup retained recovery state: %v, %+v", err, store)
			}
		})
	}
}
