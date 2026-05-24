package orm

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestDelInsKlineQuestDBUsesCurrentSchema(t *testing.T) {
	oldQuestDB := IsQuestDB
	IsQuestDB = true
	defer func() {
		IsQuestDB = oldQuestDB
	}()

	db := &repairDBStub{}
	q := New(db)
	err := q.DelInsKline(context.Background(), 1, "1m", time.Unix(0, 0).UTC())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(db.execSQL) != 1 {
		t.Fatalf("expected one insert statement, got %d", len(db.execSQL))
	}
	sql := db.execSQL[0]
	if !strings.Contains(sql, "INSERT INTO ins_kline_q") {
		t.Fatalf("unexpected SQL: %s", sql)
	}
	if strings.Contains(sql, "deleted_at") {
		t.Fatalf("unexpected deleted_at reference in SQL: %s", sql)
	}
}
