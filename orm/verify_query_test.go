package orm

import (
	"reflect"
	"strings"
	"testing"
)

func TestListSRangeSidsQueryUsesPostgresSchema(t *testing.T) {
	sqlText, args := listSRangeSidsQuery([]string{"kline", "mark"}, false)
	for _, unexpected := range []string{"sranges_q", "LATEST BY", "is_deleted"} {
		if strings.Contains(sqlText, unexpected) {
			t.Fatalf("PostgreSQL query contains QuestDB syntax %q: %s", unexpected, sqlText)
		}
	}
	if !strings.Contains(sqlText, "FROM sranges") {
		t.Fatalf("PostgreSQL query does not use sranges: %s", sqlText)
	}
	wantArgs := []any{"kline_.*", "mark_.*"}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("unexpected query args: got %#v want %#v", args, wantArgs)
	}
}

func TestListSRangeSidsQueryKeepsQuestDBLatestRows(t *testing.T) {
	sqlText, args := listSRangeSidsQuery([]string{"kline"}, true)
	for _, want := range []string{"FROM sranges_q", "LATEST BY sid, tbl, timeframe, start_ms", "coalesce(is_deleted, false) = false", "tbl ~ $1"} {
		if !strings.Contains(sqlText, want) {
			t.Fatalf("QuestDB query is missing %q: %s", want, sqlText)
		}
	}
	if !reflect.DeepEqual(args, []any{"kline_.*"}) {
		t.Fatalf("unexpected query args: %#v", args)
	}
}
