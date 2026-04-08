package orm

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/exg"
)

func TestSeriesRepoTimescaleRoundTrip(t *testing.T) {
	initSeriesRepoTestApp(t, mustFindSeriesRepoConfig(t, "config.local.yml"))
	runSeriesRepoRoundTrip(t, "timescale")
}

func TestSeriesRepoQuestDBRoundTrip(t *testing.T) {
	initSeriesRepoTestApp(t, mustFindSeriesRepoConfig(t, "config.yml"))
	runSeriesRepoRoundTrip(t, "quest")
}

func mustFindSeriesRepoConfig(t *testing.T, name string) string {
	t.Helper()
	candidates := []string{
		filepath.Join("..", "..", "data", name),
		filepath.Join("..", "data", name),
		filepath.Join("..", "biz", name),
		name,
	}
	for _, candidate := range candidates {
		abs, err := filepath.Abs(candidate)
		if err != nil {
			continue
		}
		if _, err := os.Stat(abs); err == nil {
			return abs
		}
	}
	t.Fatalf("series repository test config %q not found in candidates: %v", name, candidates)
	return ""
}

func runSeriesRepoRoundTrip(t *testing.T, backend string) {
	t.Helper()
	repo := DefaultSeriesRepo()
	tableName := fmt.Sprintf("series_repo_%s_%d", backend, time.Now().UnixNano())
	info := &SeriesInfo{
		Name:      "macro_test",
		TimeFrame: "1d",
		Binding: SeriesBinding{
			Table:      tableName,
			TimeColumn: "ts",
			EndColumn:  "end_ms",
			SIDColumn:  "sid",
			Fields: []SeriesField{
				{Name: "value", Type: "float", Role: "value"},
				{Name: "label", Type: "string", Role: "custom"},
				{Name: "payload", Type: "json", Role: "custom"},
			},
		},
	}
	ctx := context.Background()
	if err := repo.EnsureSeriesTable(ctx, info); err != nil {
		t.Fatalf("EnsureSeriesTable failed: %v", err)
	}
	sid := int32(time.Now().UnixNano() % 1_000_000)
	startMS := int64(1_700_000_000_000)
	rows := []*DataRecord{
		{
			Sid:    sid,
			TimeMS: startMS,
			EndMS:  startMS + 86_400_000,
			Closed: true,
			Values: map[string]any{
				"value":   12.5,
				"label":   "fred",
				"payload": map[string]any{"source": backend},
			},
		},
		{
			Sid:    sid,
			TimeMS: startMS + 86_400_000,
			EndMS:  startMS + 172_800_000,
			Closed: true,
			Values: map[string]any{
				"value":   13.5,
				"label":   "wind",
				"payload": `{"source":"alt"}`,
			},
		},
	}
	if err := repo.InsertSeriesBatch(ctx, info, rows); err != nil {
		t.Fatalf("InsertSeriesBatch failed: %v", err)
	}
	if err := repo.UpdateSeriesRange(ctx, info, sid, rows[0].TimeMS, rows[len(rows)-1].EndMS); err != nil {
		t.Fatalf("UpdateSeriesRange failed: %v", err)
	}
	got, err := repo.QuerySeriesRange(ctx, info, sid, rows[0].TimeMS, rows[len(rows)-1].EndMS, 10)
	if err != nil {
		t.Fatalf("QuerySeriesRange failed: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(got))
	}
	if got[0].Sid != sid || got[0].Values["label"] != "fred" {
		t.Fatalf("unexpected first row: %+v", got[0])
	}
	if got[1].Values["label"] != "wind" {
		t.Fatalf("unexpected second row: %+v", got[1])
	}
	if start, stop, err := repo.GetSeriesRange(ctx, info, sid); err != nil {
		t.Fatalf("GetSeriesRange failed: %v", err)
	} else if start != rows[0].TimeMS || stop != rows[len(rows)-1].EndMS {
		t.Fatalf("unexpected series range: start=%d stop=%d", start, stop)
	}
	cleanupSeriesRepoTestTable(t, info)
}

func initSeriesRepoTestApp(t *testing.T, cfgPath string) {
	t.Helper()
	dataDir := filepath.Dir(cfgPath)
	t.Setenv("BanDataDir", dataDir)
	config.Loaded = false
	config.DataDir = ""
	config.Args = nil
	keySymbolMap = map[string]*ExSymbol{}
	idSymbolMap = map[int32]*ExSymbol{}
	marketMap = map[string]int{}
	pairsMap = map[string]*ExSymbol{}
	hourPairsMap = map[string]*ExSymbol{}
	maxSid = 0
	var args config.CmdArgs
	args.NoDefault = true
	args.Configs = []string{cfgPath}
	if err := config.LoadConfig(&args); err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}
	if err := exg.Setup(); err != nil {
		t.Fatalf("exg.Setup failed: %v", err)
	}
	if err := Setup(); err != nil {
		t.Fatalf("Setup failed: %v", err)
	}
}

func cleanupSeriesRepoTestTable(t *testing.T, info *SeriesInfo) {
	t.Helper()
	ctx := context.Background()
	q, conn, err := Conn(ctx)
	if err != nil {
		t.Fatalf("Conn failed during cleanup: %v", err)
	}
	defer conn.Release()
	if _, err_ := q.db.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdent(info.Binding.Table))); err_ != nil {
		t.Fatalf("drop table failed: %v", err_)
	}
	if !IsQuestDB {
		if _, err_ := q.db.Exec(ctx, `DELETE FROM sranges WHERE tbl = $1 AND timeframe = $2`, info.Binding.Table, info.TimeFrame); err_ != nil {
			t.Fatalf("cleanup sranges failed: %v", err_)
		}
	}
}

func TestSeriesRepoRejectsInvalidInfo(t *testing.T) {
	repo := DefaultSeriesRepo()
	err := repo.EnsureSeriesTable(context.Background(), &SeriesInfo{
		Name:      "bad",
		TimeFrame: "1d",
		Binding: SeriesBinding{
			Table:      "bad_series",
			TimeColumn: "ts",
			EndColumn:  "end_ms",
			SIDColumn:  "sid",
			Fields: []SeriesField{
				{Name: "payload", Type: "unknown"},
			},
		},
	})
	if err == nil {
		t.Fatalf("expected invalid field type to fail")
	}
}

func TestValidateSeriesInfoDefaultsSIDColumn(t *testing.T) {
	info := &SeriesInfo{
		Name:      "macro_default_sid",
		TimeFrame: "1d",
		Binding: SeriesBinding{
			Table:      "macro_default_sid",
			TimeColumn: "ts",
			EndColumn:  "end_ms",
			Fields: []SeriesField{
				{Name: "value", Type: "float"},
			},
		},
	}
	if err := ValidateSeriesInfo(info); err != nil {
		t.Fatalf("expected empty SIDColumn to default to sid, got error: %v", err)
	}
	if got := normalizedSeriesBinding(info.Binding).SIDColumn; got != "sid" {
		t.Fatalf("expected default sid column, got %q", got)
	}
}

func TestMain(m *testing.M) {
	code := m.Run()
	if pool != nil {
		pool.Close()
		pool = nil
	}
	os.Exit(code)
}
