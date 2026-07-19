package entry

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
)

type entrySeriesSource struct {
	info    *orm.SeriesInfo
	fetches [][2]int64
}

func (s *entrySeriesSource) Info() *orm.SeriesInfo { return s.info }

func (s *entrySeriesSource) FetchHistory(_ context.Context, _ *strat.DataSub, startMS, endMS int64) ([]*orm.DataRecord, error) {
	s.fetches = append(s.fetches, [2]int64{startMS, endMS})
	return nil, nil
}

func (s *entrySeriesSource) SubscribeLive(context.Context, []*strat.DataSub, data.DataSink) error {
	return nil
}

type entrySeriesRepo struct {
	coverage [][2]int64
}

func (r *entrySeriesRepo) EnsureSeriesTable(context.Context, *orm.SeriesInfo) *errs.Error {
	return nil
}

func (r *entrySeriesRepo) InsertSeriesBatch(context.Context, *orm.SeriesInfo, []*orm.DataRecord) *errs.Error {
	return nil
}

func (r *entrySeriesRepo) QuerySeriesRange(context.Context, *orm.SeriesInfo, int32, int64, int64, int) ([]*orm.DataRecord, *errs.Error) {
	return nil, nil
}

func (r *entrySeriesRepo) DeleteSeriesRange(context.Context, *orm.SeriesInfo, int32, int64, int64) *errs.Error {
	return nil
}

func (r *entrySeriesRepo) UpdateSeriesRange(context.Context, *orm.SeriesInfo, int32, int64, int64) *errs.Error {
	return nil
}

func (r *entrySeriesRepo) UpdateSeriesCoverage(_ context.Context, _ *orm.SeriesInfo, _ int32, startMS, endMS int64, _ []*orm.DataRecord) *errs.Error {
	r.coverage = append(r.coverage, [2]int64{startMS, endMS})
	return nil
}

func (r *entrySeriesRepo) GetSeriesRange(context.Context, *orm.SeriesInfo, int32) (int64, int64, *errs.Error) {
	return 0, 0, nil
}

func (r *entrySeriesRepo) MissingSeriesRanges(_ context.Context, _ *orm.SeriesInfo, _ int32, startMS, endMS int64) ([]orm.MSRange, *errs.Error) {
	return []orm.MSRange{{Start: startMS, Stop: endMS}}, nil
}

func TestSeriesCommandsExposeExpectedFlags(t *testing.T) {
	root := NewRootCommand()
	down, _, err := root.Find([]string{"series", "down"})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"pairs", "tables", "timerange", "timestart", "timeend"} {
		if down.Flags().Lookup(name) == nil {
			t.Fatalf("series down missing --%s", name)
		}
	}
	list, _, err := root.Find([]string{"series", "list"})
	if err != nil || list.Name() != "list" {
		t.Fatalf("series list command missing: command=%v err=%v", list, err)
	}
}

func TestWriteSeriesDefinitionsUsesStableJSONSchema(t *testing.T) {
	source := &entrySeriesSource{info: orm.NewSeriesInfo("funding_rate", "1h", []orm.SeriesField{
		{Name: "rate", Type: "float", Role: "value"},
	})}
	var output bytes.Buffer
	if err := writeSeriesDefinitions(&output, []data.DataSource{source}); err != nil {
		t.Fatal(err)
	}
	var got []map[string]any
	if err := json.Unmarshal(output.Bytes(), &got); err != nil {
		t.Fatalf("invalid JSON %q: %v", output.String(), err)
	}
	if len(got) != 1 || got[0]["name"] != "funding_rate" || got[0]["timeframe"] != "1h" || got[0]["table"] != "funding_rate_1h" {
		t.Fatalf("unexpected series definition: %+v", got)
	}
	fields, ok := got[0]["fields"].([]any)
	if !ok || len(fields) != 1 || fields[0].(map[string]any)["name"] != "rate" {
		t.Fatalf("unexpected field schema: %+v", got[0]["fields"])
	}
}

func TestEnsureSeriesRangesClampsToLastClosedIntervalForEveryPair(t *testing.T) {
	oldQuest := orm.IsQuestDB
	orm.IsQuestDB = false
	defer func() { orm.IsQuestDB = oldQuest }()

	const hour = int64(3_600_000)
	nowMS := int64(1_700_000_123_000)
	closedEnd := nowMS / hour * hour
	startMS := closedEnd - 2*hour
	source := &entrySeriesSource{info: orm.NewSeriesInfo("funding_rate", "1h", []orm.SeriesField{{Name: "rate", Type: "float"}})}
	repo := &entrySeriesRepo{}
	targets := []*orm.ExSymbol{{ID: 1, Symbol: "BTC/USDT"}, {ID: 2, Symbol: "ETH/USDT"}}

	if err := ensureSeriesRanges(context.Background(), repo, []data.DataSource{source}, targets, startMS, nowMS+hour, nowMS); err != nil {
		t.Fatal(err)
	}
	if len(source.fetches) != 2 || len(repo.coverage) != 2 {
		t.Fatalf("expected one ensure per pair, fetches=%v coverage=%v", source.fetches, repo.coverage)
	}
	for _, fetch := range source.fetches {
		if fetch != ([2]int64{startMS, closedEnd}) {
			t.Fatalf("fetch included an open interval: got=%v want=[%d %d]", fetch, startMS, closedEnd)
		}
	}
}

func TestLastClosedSeriesEndRejectsInvalidTimeframe(t *testing.T) {
	_, err := lastClosedSeriesEnd("bad", 1_700_000_000_000, 1_700_000_000_000)
	if err == nil || !strings.Contains(err.Error(), "invalid timeframe") {
		t.Fatalf("expected invalid timeframe error, got %v", err)
	}
}
