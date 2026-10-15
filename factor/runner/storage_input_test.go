package runner

import (
	"context"
	"errors"
	"io"
	"math"
	"reflect"
	"sort"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/orm"
)

type strictFixtureFactory struct {
	rows   []factor.VersionRecord
	closed *bool
}

func (s strictFixtureFactory) Identity() string { return "immutable-test-revisions" }
func (s strictFixtureFactory) Open(context.Context, StorageStream, int64, int64, bool) (VersionPageReader, error) {
	return &strictFixtureReader{rows: s.rows, closed: s.closed}, nil
}

type strictFixtureReader struct {
	rows   []factor.VersionRecord
	at     int
	closed *bool
}

func (s *strictFixtureReader) NextPage(ctx context.Context, limit int) ([]factor.VersionRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.at == len(s.rows) {
		return nil, io.EOF
	}
	to := min(s.at+limit, len(s.rows))
	rows := s.rows[s.at:to]
	s.at = to
	return rows, nil
}
func (s *strictFixtureReader) Close() error {
	if s.closed != nil {
		*s.closed = true
	}
	return nil
}

func TestStorageStrictRevisionsDelayAndBoundedOwnership(t *testing.T) {
	record := func(event, available, ingested int64, revision uint64, value int64) factor.VersionRecord {
		return factor.VersionRecord{Series: orm.DataSeries{Source: "source", TimeFrame: "event", Sid: 1, TimeMS: event, EndMS: event, Closed: true, Values: map[string]any{"value": value, "null": nil}}, EventTime: event, AvailableAt: available, IngestedAt: ingested, Revision: revision, SourceVersion: "v1"}
	}
	rows := []factor.VersionRecord{record(100, 100, 200, 1, 9007199254740993), record(150, 150, 250, 1, 2), record(100, 160, 260, 2, 3), record(180, 180, 270, 1, 4)}
	closed := false
	options := StorageInputOptions{Namespace: "strict", PITPolicy: "strict", FromMS: 100, ToMS: 300, PageRows: 1, PrefetchRows: 1, Streams: []StorageStream{{Subscription: orm.Subscription{Source: "source", TimeFrame: "event", ExSymbol: &orm.ExSymbol{ID: 1}}, SourceVersion: "v1", SchemaHash: "s1"}}, VersionPages: strictFixtureFactory{rows: rows, closed: &closed}}
	factory, err := NewStorageInputFactory(options)
	if err != nil {
		t.Fatal(err)
	}
	c := Config{MaxRecords: 5, Snapshot: factor.SnapshotSpec{VisibilityPolicy: "strict", ReplayTime: 1, SourceVersions: map[string]string{"source": "v1"}, Schemas: map[string]string{"source": "s1"}}}
	input, err := factory.Open(context.Background(), c, factory.Ranges()[0])
	if err != nil {
		t.Fatal(err)
	}
	batch, err := input.Next(context.Background())
	if err != nil || batch.AtMS != 200 {
		t.Fatalf("reception not respected %+v %v", batch, err)
	}
	visible, err := input.Visible(context.Background(), 100, 200, 200)
	if err != nil || len(visible) != 1 || visible[0].Series.Values["value"] != int64(9007199254740993) {
		t.Fatalf("strict initial cutoff lost %+v %v", visible, err)
	}
	visible[0].Series.Values["value"] = int64(8)
	batch.Records[0].Series.Values["value"] = int64(9)
	visible, err = input.Visible(context.Background(), 100, 200, 200)
	if err != nil || visible[0].Series.Values["value"] != int64(9007199254740993) {
		t.Fatal("caller mutated stored Values")
	}
	for _, at := range []int64{250, 260, 270} {
		batch, err = input.Next(context.Background())
		if err != nil || batch.AtMS != at {
			t.Fatalf("bad next %+v %v", batch, err)
		}
		visible, err = input.Visible(context.Background(), 150, at, at)
		if err != nil || len(visible) != 1 || visible[0].EventTime != 150 || visible[0].Series.Values["value"] != int64(2) {
			t.Fatal("future event or old revision leaked into delayed grid")
		}
	}
	if _, err = input.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatal("final EOF omitted")
	}
	if err = input.Close(); err != nil || !closed {
		t.Fatal("strict reader not closed")
	}
	options.VersionPages = strictFixtureFactory{rows: []factor.VersionRecord{record(100, 100, 100, 1, 1), record(101, 101, 101, 1, 2), record(102, 102, 102, 1, 3), record(103, 103, 103, 1, 4)}}
	factory, err = NewStorageInputFactory(options)
	if err != nil {
		t.Fatal(err)
	}
	c.MaxRecords = 2
	input, err = factory.Open(context.Background(), c, factory.Ranges()[0])
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	for i := 0; i < 4; i++ {
		_, err = input.Next(context.Background())
		if err != nil {
			break
		}
	}
	if err == nil {
		t.Fatal("decision gap permitted unbounded raw retention")
	}
}

func TestReplayInputDelayedDecisionAndMaxTimestamp(t *testing.T) {
	input := &archiveInput{batches: []HistoricalBatch{{AtMS: 100}, {AtMS: 103}, {AtMS: 110}}}
	driver := newReplayInput(input, Config{DecisionInterval: 10, DecisionDelayMS: 3}, Chunk{From: 100, To: 115})
	var times []int64
	for {
		batch, err := driver.Next(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		times = append(times, batch.AtMS)
	}
	if !reflect.DeepEqual(times, []int64{100, 103, 110, 113}) {
		t.Fatalf("wrong delayed timeline %v", times)
	}
	driver = newReplayInput(&archiveInput{}, Config{DecisionInterval: 10, DecisionDelayMS: 3}, Chunk{From: math.MaxInt64 - 1, To: math.MaxInt64})
	if _, err := driver.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatal("decision overflow")
	}
}

func storageFixture(t *testing.T, c Config, page int) (HistoricalInputFactory, int) {
	t.Helper()
	store, err := factor.OpenVersionStore(c.Chunks[0].Path, c.MaxRecords)
	if err != nil {
		t.Fatal(err)
	}
	records, err := store.Records()
	if err != nil {
		t.Fatal(err)
	}
	rows := map[string][]*orm.DataSeries{}
	streams := map[string]StorageStream{}
	for _, r := range records {
		sub := orm.Subscription{Source: r.Series.Source, TimeFrame: r.Series.TimeFrame, ExSymbol: &orm.ExSymbol{ID: r.Series.Sid, Symbol: c.Snapshot.SIDMap[r.Series.Sid]}}
		key := sub.Key().String()
		copySeries := r.Series
		rows[key] = append(rows[key], &copySeries)
		streams[key] = StorageStream{Subscription: sub, SourceVersion: r.SourceVersion, SchemaHash: c.Snapshot.Schemas[r.Series.Source]}
	}
	list := make([]StorageStream, 0, len(streams))
	for _, s := range streams {
		list = append(list, s)
	}
	calls := 0
	factory, err := NewStorageInputFactory(StorageInputOptions{Namespace: "fixture", PITPolicy: "static-approximation", FromMS: c.Chunks[0].From, ToMS: c.Chunks[0].To, PageRows: page, Streams: list, QueryPage: func(ctx context.Context, sub orm.Subscription, start, end int64, limit int) ([]*orm.DataSeries, error) {
		calls++
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var result []*orm.DataSeries
		for _, r := range rows[sub.Key().String()] {
			if r.TimeMS >= start && r.TimeMS < end {
				result = append(result, r)
				if len(result) == limit {
					break
				}
			}
		}
		return result, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	_ = calls
	return factory, len(records)
}

func TestStorageReplayMatchesFixedArchiveAndBounds(t *testing.T) {
	c := archiveConfig(t, false)
	archive := &capture{targets: map[int64]map[int32]float64{}}
	expected, err := Run(context.Background(), c, nil, archive)
	if err != nil {
		t.Fatal(err)
	}
	factory, total := storageFixture(t, c, 3)
	c.HistoricalInput = factory
	c.Chunks = nil
	c.Snapshot.VisibilityPolicy = "static-approximation"
	actual := &capture{targets: map[int64]map[int32]float64{}}
	result, err := Run(context.Background(), c, nil, actual)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(archive.targets, actual.targets) || !reflect.DeepEqual(archive.executed, actual.executed) || !reflect.DeepEqual(expected.Book, result.Book) || expected.Decisions != result.Decisions || expected.Unresolved != result.Unresolved || !reflect.DeepEqual(expected.Summary, result.Summary) {
		t.Fatalf("paged replay differs: targets=%v execution=%v book=%v decisions=%d/%d unresolved=%d/%d summary=%v\\narchive=%+v\\nstorage=%+v", reflect.DeepEqual(archive.targets, actual.targets), reflect.DeepEqual(archive.executed, actual.executed), reflect.DeepEqual(expected.Book, result.Book), expected.Decisions, result.Decisions, expected.Unresolved, result.Unresolved, reflect.DeepEqual(expected.Summary, result.Summary), expected.Book, result.Book)
	}
	if result.MaxRawRecords >= total || result.MaxRawRecords > c.MaxRecords {
		t.Fatalf("unbounded retention %d/%d", result.MaxRawRecords, total)
	}
	if result.Manifest.Snapshots[0].ID != factory.Identity() {
		t.Fatal("storage lineage omitted")
	}
	if result.Manifest.Snapshots[0].ContentDigest == "" || result.Manifest.Snapshots[0].ContentDigest == factory.Identity() {
		t.Fatal("storage content fingerprint is missing or contains only factory metadata")
	}
}

func TestStorageContentLineageTracksConsumedTypesNullsAndPageIndependence(t *testing.T) {
	c := archiveConfig(t, false)
	factory, _ := storageFixture(t, c, 3)
	options := factory.(*storageInputFactory).options
	query := options.QueryPage
	c.Chunks = nil
	c.Snapshot.VisibilityPolicy = "static-approximation"
	run := func(value any, present bool, page int) (string, string) {
		t.Helper()
		opts := options
		opts.PageRows = page
		opts.QueryPage = func(ctx context.Context, sub orm.Subscription, from, to int64, limit int) ([]*orm.DataSeries, error) {
			rows, err := query(ctx, sub, from, to, limit)
			for _, row := range rows {
				if present {
					row.Values["lineage_custom"] = value
				} else {
					delete(row.Values, "lineage_custom")
				}
			}
			return rows, err
		}
		input, err := NewStorageInputFactory(opts)
		if err != nil {
			t.Fatal(err)
		}
		cfg := c
		cfg.HistoricalInput = input
		result, err := Run(context.Background(), cfg, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		return result.Manifest.Snapshots[0].ContentDigest, input.Identity()
	}
	integer, identity := run(int64(9007199254740993), true, 3)
	same, _ := run(int64(9007199254740993), true, 1)
	floating, sameIdentity := run(float64(9007199254740993), true, 3)
	null, _ := run(nil, true, 3)
	missing, _ := run(nil, false, 3)
	if integer != same || integer == floating || null == missing || identity != sameIdentity {
		t.Fatal("content lineage ignores values/types/NULL semantics or depends on page boundaries")
	}
	failed := c
	failed.HistoricalInput = factory
	failure := errors.New("observer stopped replay")
	failed.ObserveBatch = func(context.Context, HistoricalBatch) error { return failure }
	result, err := Run(context.Background(), failed, nil, nil)
	if !errors.Is(err, failure) || len(result.Manifest.Snapshots) != 1 || result.Manifest.Snapshots[0].ID != factory.Identity() || result.Manifest.Snapshots[0].ContentDigest != "" || result.ManifestID != "" {
		t.Fatalf("partial replay claimed complete content lineage: result=%+v err=%v", result.Manifest, err)
	}
}

func TestStorageWarmupAdvancesIndicatorsWithoutEarlyTargetsOrFills(t *testing.T) {
	c := archiveConfig(t, false)
	c.Chunks[0].From = 6 * 3600000
	c.Factor.Window = 2
	factory, _ := storageFixture(t, c, 3)
	c.HistoricalInput = factory
	c.Chunks = nil
	c.Snapshot.VisibilityPolicy = "static-approximation"
	o := &capture{targets: map[int64]map[int32]float64{}}
	result, err := Run(context.Background(), c, nil, o)
	if err != nil {
		t.Fatal(err)
	}
	if len(o.targets[6*3600000]) == 0 {
		t.Fatal("pre-range observations did not warm the first decision")
	}
	for at := range o.targets {
		if at < 6*3600000 {
			t.Fatal("warmup produced a portfolio")
		}
	}
	for _, at := range o.executed {
		if at <= 6*3600000 {
			t.Fatal("warmup or same-grid price executed a trade")
		}
	}
	if result.Decisions != 23 || result.Executions == 0 {
		t.Fatalf("range changed by warmup: %+v", result)
	}
}

func TestStoragePolicyIdentityCancellationAndFinalPage(t *testing.T) {
	c := archiveConfig(t, false)
	factory, total := storageFixture(t, c, 2)
	c.Chunks = nil
	c.Snapshot.VisibilityPolicy = "static-approximation"
	input, err := factory.Open(context.Background(), c, factory.Ranges()[0])
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	last := int64(-1)
	for {
		batch, err := input.Next(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if batch.AtMS <= last {
			t.Fatal("unordered batches")
		}
		last = batch.AtMS
		seen += len(batch.Records)
		if _, err = input.Visible(context.Background(), batch.AtMS, batch.AtMS, 0); err != nil {
			t.Fatal(err)
		}
	}
	if seen != total || last != factory.Ranges()[0].To {
		t.Fatalf("last page lost %d/%d at %d", seen, total, last)
	}
	if err = input.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = factory.Open(ctx, c, factory.Ranges()[0]); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	bad := factory.(*storageInputFactory).options
	bad.PITPolicy = "strict"
	if _, err = NewStorageInputFactory(bad); err == nil {
		t.Fatal("strict PIT fabricated from latest values")
	}
	other := factory.(*storageInputFactory).options
	other.Namespace = "other"
	second, err := NewStorageInputFactory(other)
	if err != nil {
		t.Fatal(err)
	}
	if second.Identity() == factory.Identity() {
		t.Fatal("namespace absent from identity")
	}
	old := factory.Identity()
	view := factory.Ranges()
	view[0].From++
	if factory.Identity() != old || factory.Ranges()[0].From == view[0].From {
		t.Fatal("mutable range identity")
	}
}

func TestArchiveObserveBatchIncludesAllSourcesOnce(t *testing.T) {
	c := archiveConfig(t, false)
	count := 0
	seen := map[string]int{}
	c.ObserveBatch = func(_ context.Context, b HistoricalBatch) error {
		count++
		for _, r := range b.Records {
			seen[r.Series.Source]++
		}
		return nil
	}
	if _, err := Run(context.Background(), c, nil, nil); err != nil {
		t.Fatal(err)
	}
	if seen["kline"] != 24*28 || count != 28*3 {
		t.Fatalf("observer omitted or repeated source rows: %v batches=%d", seen, count)
	}
	fail := errors.New("observer fail")
	c.ObserveBatch = func(context.Context, HistoricalBatch) error { return fail }
	if _, err := Run(context.Background(), c, nil, nil); !errors.Is(err, fail) {
		t.Fatal("observer failure ignored")
	}
}

func TestStorageRejectsUnorderedForeignAndOverBudgetPages(t *testing.T) {
	c := archiveConfig(t, false)
	factory, _ := storageFixture(t, c, 2)
	base := factory.(*storageInputFactory).options
	c.Chunks = nil
	c.Snapshot.VisibilityPolicy = "static-approximation"
	for _, kind := range []string{"foreign", "unordered", "oversize"} {
		t.Run(kind, func(t *testing.T) {
			options := base
			query := options.QueryPage
			options.QueryPage = func(ctx context.Context, sub orm.Subscription, start, end int64, limit int) ([]*orm.DataSeries, error) {
				rows, err := query(ctx, sub, start, end, limit)
				if err != nil || len(rows) == 0 {
					return rows, err
				}
				cloned := make([]*orm.DataSeries, len(rows))
				for i, r := range rows {
					cp := *r
					cloned[i] = &cp
				}
				switch kind {
				case "foreign":
					cloned[0].Sid = 999
				case "unordered":
					sort.Slice(cloned, func(i, j int) bool { return cloned[i].TimeMS > cloned[j].TimeMS })
				case "oversize":
					cloned = append(cloned, cloned[0])
				}
				return cloned, nil
			}
			f, err := NewStorageInputFactory(options)
			if err != nil {
				t.Fatal(err)
			}
			input, err := f.Open(context.Background(), c, f.Ranges()[0])
			if err != nil {
				t.Fatal(err)
			}
			defer input.Close()
			if _, err = input.Next(context.Background()); err == nil {
				t.Fatal("invalid page accepted")
			}
		})
	}
}
