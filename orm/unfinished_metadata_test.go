package orm

import (
	"strings"
	"testing"
	"time"

	"github.com/banbox/banexg"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestQuestUnfinishedBarKeepsEventTimeSeparateFromVersion(t *testing.T) {
	installMetadataVersionTestRoot(t)
	oldQuest := IsQuestDB
	IsQuestDB = true
	t.Cleanup(func() { IsQuestDB = oldQuest })
	bar := &banexg.Kline{Time: 1704067200000, Open: 1, High: 3, Low: 1, Close: 2, Volume: 4}
	writes := 0
	db := &visibilityDBStub{exec: func(sql string, args ...any) (pgconn.CommandTag, error) {
		writes++
		if !strings.Contains(sql, "bar_ts") {
			t.Errorf("unfinished write has no independent event timestamp: %s", sql)
		}
		var foundEvent, foundVersion bool
		for _, arg := range args {
			if timestamp, ok := arg.(time.Time); ok {
				foundEvent = foundEvent || timestamp.UnixMilli() == bar.Time
				foundVersion = foundVersion || timestamp.UnixMilli() > bar.Time
			}
		}
		if !foundEvent || !foundVersion {
			t.Errorf("event/version time not separated: %#v", args)
		}
		return pgconn.CommandTag{}, nil
	}}
	q := New(db).WithKlineRuntimeOptions(KlineRuntimeOptions{NowMS: bar.Time + 60000, ClockValid: true})
	if err := q.SetUnfinish(17, "1m", bar.Time+30000, bar); err != nil {
		t.Fatal(err)
	}
	if writes != 1 {
		t.Fatalf("writes = %d", writes)
	}
}

func TestQuestUnfinishedBarCanBeRecreatedAfterDeleteWithEarlierEventTime(t *testing.T) {
	installMetadataVersionTestRoot(t)
	previous := IsQuestDB
	IsQuestDB = true
	t.Cleanup(func() { IsQuestDB = previous })
	eventMS := int64(1704067200000)
	var lastVersion time.Time
	var current *banexg.Kline
	db := &visibilityDBStub{
		exec: func(sql string, args ...any) (pgconn.CommandTag, error) {
			version := args[2].(time.Time)
			if !version.After(lastVersion) {
				t.Fatalf("unfinished revision failed to advance: %s <= %s", version, lastVersion)
			}
			lastVersion = version
			if strings.Contains(sql, ", true)") {
				current = nil
			} else {
				current = &banexg.Kline{Time: args[13].(time.Time).UnixMilli()}
			}
			return pgconn.CommandTag{}, nil
		},
		queryRow: func(_ string, _ ...any) pgx.Row {
			if current == nil {
				return scriptedRow{err: pgx.ErrNoRows}
			}
			return visibilityRowStub{scan: func(dest ...any) error {
				*dest[0].(*int64) = current.Time
				for i := 1; i <= 7; i++ {
					*dest[i].(*float64) = float64(i)
				}
				*dest[8].(*int64) = 10
				*dest[9].(*int64) = eventMS + 30000
				*dest[10].(*int64) = eventMS + 60000
				return nil
			}}
		},
	}
	q := New(db).WithKlineRuntimeOptions(KlineRuntimeOptions{NowMS: eventMS + 60000, ClockValid: true})
	if err := q.SetUnfinish(17, "1m", eventMS+30000, &banexg.Kline{Time: eventMS}); err != nil {
		t.Fatal(err)
	}
	if err := q.DelKLineUn(17, "1m"); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := q.queryUnfinish(17, "1m", eventMS); err != pgx.ErrNoRows {
		t.Fatalf("deleted unfinished bar remained visible: %v", err)
	}
	// The new revision remains latest even if its event clock predates both
	// the deleted bar and the deletion's wall-clock metadata timestamp.
	eventMS -= 60000
	if err := q.SetUnfinish(17, "1m", eventMS+30000, &banexg.Kline{Time: eventMS}); err != nil {
		t.Fatal(err)
	}
	bar, _, _, err := q.queryUnfinish(17, "1m", eventMS)
	if err != nil || bar.Time != eventMS {
		t.Fatalf("recreated unfinished bar=%+v err=%v", bar, err)
	}
}

func TestQuestUnfinishedBarReadsLegacyNullEventTimestamp(t *testing.T) {
	oldQuest := IsQuestDB
	IsQuestDB = true
	t.Cleanup(func() { IsQuestDB = oldQuest })
	eventMS := int64(1704067200000)
	db := &visibilityDBStub{queryRow: func(sql string, args ...any) pgx.Row {
		if !strings.Contains(sql, "coalesce(bar_ts, ts)") {
			t.Errorf("read does not fall back for legacy NULL bar_ts: %s", sql)
		}
		return visibilityRowStub{scan: func(dest ...any) error {
			*dest[0].(*int64) = eventMS
			for i := 1; i <= 7; i++ {
				*dest[i].(*float64) = float64(i)
			}
			*dest[8].(*int64) = 10
			*dest[9].(*int64) = eventMS + 30000
			*dest[10].(*int64) = eventMS + 60000
			return nil
		}}
	}}
	bar, _, _, err := New(db).queryUnfinish(17, "1m", eventMS)
	if err != nil || bar.Time != eventMS {
		t.Fatalf("legacy unfinished bar=%+v error=%v", bar, err)
	}
}

func TestQuestUnfinishedMigrationAddsNullableTimestampWithoutRebuilding(t *testing.T) {
	if !strings.Contains(ddlQdbMigrations, "ALTER TABLE kline_un_q ADD COLUMN bar_ts TIMESTAMP") {
		t.Fatal("missing additive unfinished-event timestamp migration")
	}
	if strings.Contains(ddlQdbMigrations, "DROP TABLE kline_un_q") {
		t.Fatal("unfinished migration destroys old rows")
	}
}
