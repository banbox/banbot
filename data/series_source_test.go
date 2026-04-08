package data

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
)

type stubSeriesSource struct {
	info       *orm.SeriesInfo
	fetchCount int
	rows       []*orm.DataRecord
}

func (s *stubSeriesSource) Info() *orm.SeriesInfo {
	return s.info
}

func (s *stubSeriesSource) FetchHistory(ctx context.Context, sub *strat.DataSub, startMS, endMS int64) ([]*orm.DataRecord, error) {
	s.fetchCount++
	return append([]*orm.DataRecord(nil), s.rows...), nil
}

func (s *stubSeriesSource) SubscribeLive(ctx context.Context, subs []*strat.DataSub, sink DataSink) error {
	return nil
}

func TestRegisterDataSourceRejectsDuplicates(t *testing.T) {
	old := dataSources
	dataSources = make(map[string]DataSource)
	t.Cleanup(func() {
		dataSources = old
	})
	src := &stubSeriesSource{
		info: &orm.SeriesInfo{
			Name:      "macro_dup_test",
			TimeFrame: "1d",
			Binding: orm.SeriesBinding{
				Table:      "macro_dup_test",
				TimeColumn: "ts",
				EndColumn:  "end_ms",
				SIDColumn:  "sid",
				Fields: []orm.SeriesField{
					{Name: "value", Type: "float", Role: "value"},
				},
			},
		},
	}
	if err := RegisterDataSource(src); err != nil {
		t.Fatalf("RegisterDataSource failed: %v", err)
	}
	if err := RegisterDataSource(src); err == nil {
		t.Fatalf("expected duplicate data source registration to fail")
	}
}

func TestEnsureSeriesRangeTimescale(t *testing.T) {
	initSeriesSourceTestApp(t, mustFindSeriesSourceConfig(t, "config.local.yml"))
	runEnsureSeriesRangeTest(t, "timescale")
}

func TestEnsureSeriesRangeQuestDB(t *testing.T) {
	initSeriesSourceTestApp(t, mustFindSeriesSourceConfig(t, "config.yml"))
	runEnsureSeriesRangeTest(t, "quest")
}

func mustFindSeriesSourceConfig(t *testing.T, name string) string {
	t.Helper()
	candidates := []string{
		filepath.Join("..", "..", "data", name),
		filepath.Join("..", name),
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
	t.Fatalf("series source test config %q not found in candidates: %v", name, candidates)
	return ""
}

func runEnsureSeriesRangeTest(t *testing.T, backend string) {
	t.Helper()
	now := time.Now().UnixNano()
	info := &orm.SeriesInfo{
		Name:      fmt.Sprintf("macro_%s_%d", backend, now),
		TimeFrame: "1d",
		Binding: orm.SeriesBinding{
			Table:      fmt.Sprintf("series_source_%s_%d", backend, now),
			TimeColumn: "ts",
			EndColumn:  "end_ms",
			SIDColumn:  "sid",
			Fields: []orm.SeriesField{
				{Name: "value", Type: "float", Role: "value"},
				{Name: "label", Type: "string", Role: "custom"},
			},
		},
	}
	startMS := int64(1_700_000_000_000)
	rows := []*orm.DataRecord{
		{
			TimeMS: startMS,
			EndMS:  startMS + 86_400_000,
			Values: map[string]any{"value": 12.5, "label": "fred"},
		},
		{
			TimeMS: startMS + 86_400_000,
			EndMS:  startMS + 172_800_000,
			Values: map[string]any{"value": 13.5, "label": "wind"},
		},
	}
	src := &stubSeriesSource{info: info, rows: rows}
	sub := &strat.DataSub{
		Source:    info.Name,
		ExSymbol:  &orm.ExSymbol{ID: int32(now % 1_000_000)},
		TimeFrame: info.TimeFrame,
	}
	ctx := context.Background()
	if err := EnsureSeriesRange(ctx, src, sub, startMS, startMS+172_800_000); err != nil {
		t.Fatalf("EnsureSeriesRange failed: %v", err)
	}
	got, err := orm.DefaultSeriesRepo().QuerySeriesRange(ctx, info, sub.ExSymbol.ID, startMS, startMS+172_800_000, 10)
	if err != nil {
		t.Fatalf("QuerySeriesRange failed: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(got))
	}
	if src.fetchCount != 1 {
		t.Fatalf("expected initial fetch count to be 1, got %d", src.fetchCount)
	}
	if err := EnsureSeriesRange(ctx, src, sub, startMS, startMS+172_800_000); err != nil {
		t.Fatalf("EnsureSeriesRange second pass failed: %v", err)
	}
	if src.fetchCount != 1 {
		t.Fatalf("expected coverage metadata to skip duplicate fetch, got %d fetches", src.fetchCount)
	}
}

func initSeriesSourceTestApp(t *testing.T, cfgPath string) {
	t.Helper()
	config.Loaded = false
	config.DataDir = ""
	config.Args = nil
	var args config.CmdArgs
	args.NoDefault = true
	args.Configs = []string{cfgPath}
	if err := config.LoadConfig(&args); err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}
	if err := exg.Setup(); err != nil {
		t.Fatalf("exg.Setup failed: %v", err)
	}
	if err := orm.Setup(); err != nil {
		t.Fatalf("Setup failed: %v", err)
	}
}
