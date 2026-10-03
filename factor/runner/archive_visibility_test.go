package runner

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/orm"
)

func archiveVisibilityFixture(t *testing.T, reception bool) (HistoricalInput, *factor.VersionStore) {
	t.Helper()
	store, err := factor.NewVersionStore(20)
	if err != nil {
		t.Fatal(err)
	}
	// Include early publication of a future event, delayed reception, and a
	// revision to an older event after a newer event has already been selected.
	for _, item := range []struct {
		event, available, ingested int64
		revision                   uint64
		frequency                  string
	}{
		{10, 10, 12, 1, "event"}, {20, 21, 25, 1, "event"},
		{10, 30, 31, 2, "event"}, {20, 32, 33, 2, "event"},
		{50, 14, 15, 1, "event"}, {20, 22, 23, 1, "1h"},
	} {
		row := factor.VersionRecord{Series: orm.DataSeries{
			Source: "series", Sid: 1, TimeFrame: item.frequency,
			TimeMS: item.event, EndMS: item.event, Closed: true,
			Values: map[string]any{"integer": int64(9007199254740993), "null": nil, "nested": map[string]any{"flag": true}, "revision": item.revision},
		}, EventTime: item.event, AvailableAt: item.available, IngestedAt: item.ingested, Revision: item.revision, SourceVersion: "v1"}
		if err = store.Put(row); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(t.TempDir(), "visibility.gob")
	if _, err = store.Export(path); err != nil {
		t.Fatal(err)
	}
	c := Config{MaxRecords: 20, ObserveBatch: func(context.Context, HistoricalBatch) error { return nil }}
	if reception {
		c.Snapshot.ReplayTime = 1
	}
	input, err := openArchiveInput(context.Background(), c, Chunk{Path: path, From: 1, To: 100})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := input.Close(); err != nil {
			t.Error(err)
		}
	})
	return input, store
}

func TestArchiveVisibilityMatchesFrozenRawHistoryAtEveryCutoff(t *testing.T) {
	for _, reception := range []bool{false, true} {
		t.Run(map[bool]string{false: "publication", true: "reception"}[reception], func(t *testing.T) {
			input, store := archiveVisibilityFixture(t, reception)
			for _, cutoff := range [][3]int64{
				{10, 10, 10}, {10, 12, 12}, {12, 20, 20}, {20, 23, 23},
				{20, 25, 25}, {20, 31, 31}, {20, 33, 33}, {50, 55, 55},
				{10, 12, 12}, // Backward cutoff must return the earlier immutable view.
				{20, 33, 24}, // Publication and reception cutoffs can differ.
				{50, 55, 55},
			} {
				grid, now, replay := cutoff[0], cutoff[1], cutoff[2]
				if !reception {
					replay = 0
				}
				got, err := input.Visible(context.Background(), grid, now, replay)
				if err != nil {
					t.Fatal(err)
				}
				// RoundBarrier also gates local reception at now even when explicit
				// reception replay is disabled. Freeze alone does not apply that gate.
				admittedReplay := now
				if replay != 0 {
					admittedReplay = min(now, replay)
				}
				want, err := store.Visible(0, grid, now, admittedReplay)
				if err != nil {
					t.Fatal(err)
				}
				spec := factor.SnapshotSpec{GridTime: grid, DecisionTime: now, ReplayTime: replay,
					Universe: factor.Universe{Version: "u1", Investable: []int32{1}}, SIDMap: map[int32]string{1: "one"},
					Schemas: map[string]string{"series": "s1"}, SourceVersions: map[string]string{"series": "v1"}, VisibilityPolicy: "pit"}
				actual, err := factor.Freeze(spec, got, nil)
				if err != nil {
					t.Fatal(err)
				}
				expected, err := factor.Freeze(spec, want, nil)
				if err != nil {
					t.Fatal(err)
				}
				if actual.ID() != expected.ID() {
					t.Fatalf("cutoff %v changed snapshot: %s != %s", cutoff, actual.ID(), expected.ID())
				}
				// Consumer writes may never change the next snapshot or raw batch.
				for _, record := range got {
					record.Series.Values["integer"] = int64(0)
					record.Series.Values["nested"].(map[string]any)["flag"] = false
				}
			}
		})
	}
}

func TestArchiveBatchesKeepEveryRevisionAndIsolateConsumerWrites(t *testing.T) {
	input, store := archiveVisibilityFixture(t, true)
	want, err := store.Records()
	if err != nil {
		t.Fatal(err)
	}
	var got []factor.VersionRecord
	for {
		batch, err := input.Next(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range batch.Records {
			if batch.AtMS != max(row.EventTime, row.AvailableAt, row.IngestedAt) {
				t.Fatalf("incorrect visibility timestamp: %+v", row)
			}
			clone, err := factor.CloneVersionRecord(row)
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, clone)
			row.Series.Values["nested"].(map[string]any)["flag"] = false
		}
	}
	sortHistoricalRecords(got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("raw revision stream changed: got %v, want %v", got, want)
	}
	visible, err := input.Visible(context.Background(), 100, 100, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range visible {
		if row.Series.Values["integer"] != int64(9007199254740993) || row.Series.Values["null"] != nil || !row.Series.Values["nested"].(map[string]any)["flag"].(bool) {
			t.Fatalf("raw field type/NULL/ownership changed: %v", row.Series.Values)
		}
	}
}

func TestArchiveVisibilityCursorAdvancesOnceAndBoundsDecisionRowsByStreams(t *testing.T) {
	reader, _ := archiveVisibilityFixture(t, true)
	input := reader.(*archiveInput)
	visible, err := input.Visible(context.Background(), 20, 25, 25)
	if err != nil {
		t.Fatal(err)
	}
	if len(visible) != 2 || input.visibleIndex != 4 || input.pending.Len() != 1 {
		t.Fatalf("cursor did not separate publication from event time: rows=%d progressed=%d future=%d", len(visible), input.visibleIndex, input.pending.Len())
	}
	visible, err = input.Visible(context.Background(), 50, 100, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(visible) != 2 || len(input.latest) != 2 || input.visibleIndex != len(input.rows) || input.pending.Len() != 0 {
		t.Fatal("decision view retained raw history or failed to drain the cursor")
	}
	// A historical query uses the immutable raw fallback and never resets the
	// advancing cursor. Returning to the newest cutoff does no source work.
	if _, err = input.Visible(context.Background(), 10, 12, 12); err != nil {
		t.Fatal(err)
	}
	if input.visibleIndex != len(input.rows) || input.grid != 50 || input.now != 100 {
		t.Fatal("backward query rewound the replay cursor")
	}
	if err = input.Close(); err != nil {
		t.Fatal(err)
	}
	if input.rows != nil || input.pending.rows != nil || input.latest != nil || input.batches != nil {
		t.Fatal("closed archive reader retained chunk state")
	}
	if _, err = input.Next(context.Background()); err == nil {
		t.Fatal("closed archive reader accepted another batch read")
	}
	if _, err = input.Visible(context.Background(), 50, 100, 100); err == nil {
		t.Fatal("closed archive reader accepted another decision read")
	}
}

func TestArchiveLatestPreservesBarrierReceptionInPublicationReplay(t *testing.T) {
	input, store := archiveVisibilityFixture(t, false)
	spec := factor.SnapshotSpec{GridTime: 20, DecisionTime: 23,
		Universe: factor.Universe{Version: "u1", Investable: []int32{1}}, SIDMap: map[int32]string{1: "one"},
		Schemas: map[string]string{"series": "s1"}, SourceVersions: map[string]string{"series": "v1"}, VisibilityPolicy: "publication"}
	needs := []factor.Requirement{{SID: 1, Source: "series", Frequency: "event", EventTime: 20, AsOfLatest: true}}
	freeze := func(rows []factor.VersionRecord) *factor.Snapshot {
		t.Helper()
		var barrier factor.RoundBarrier
		defer func() { barrier.Stop(); barrier.Join() }()
		token, err := barrier.Begin("plan", spec, needs, 100)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			if row.Series.TimeFrame == "event" {
				if err = barrier.Observe(token, row, 23); err != nil {
					t.Fatal(err)
				}
			}
		}
		snapshot, err := barrier.Freeze(token, 23)
		if err != nil {
			t.Fatal(err)
		}
		return snapshot
	}
	raw, err := store.Visible(0, 20, 23, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := freeze(raw)
	latest, err := input.Visible(context.Background(), 20, 23, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := freeze(latest)
	if got.ID() != want.ID() {
		t.Fatal("latest preselection changed the actual round snapshot")
	}
}

func TestArchiveRunAsOfKeepsReceivedOlderEventWhenLatestReceptionIsFuture(t *testing.T) {
	c := archiveConfig(t, false)
	c.Mode = Research
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	c.Manifest.Portfolio.K = 1
	c.Snapshot.Universe = factor.Universe{Version: "delayed", Static: true, Investable: []int32{1, 2}, Reference: []int32{1, 2}, Tradable: []int32{1, 2}, Evaluation: []int32{1, 2}}
	const hour int64 = 3600000
	var err error
	c.Plan, err = factor.New().Add("close", factor.AsOfField("kline", "close", "event", "1h", 2*hour)).Compile()
	if err != nil {
		t.Fatal(err)
	}
	c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"close"}, Weights: map[string]float64{"close": 1}}
	store, err := factor.NewVersionStore(4)
	if err != nil {
		t.Fatal(err)
	}
	for _, sid := range []int32{1, 2} {
		for _, event := range []int64{hour, 2 * hour} {
			ingested := event
			if sid == 1 && event == 2*hour {
				ingested = 100 * hour
			}
			row := factor.VersionRecord{Series: orm.DataSeries{Source: "kline", Sid: sid, TimeFrame: "event", TimeMS: event, EndMS: event, Closed: true, Values: map[string]any{"close": float64(sid) * float64(event/hour)}}, EventTime: event, AvailableAt: event, IngestedAt: ingested, Revision: 1, SourceVersion: "v1"}
			if err = store.Put(row); err != nil {
				t.Fatal(err)
			}
		}
	}
	path := filepath.Join(t.TempDir(), "delayed-reception.gob")
	if _, err = store.Export(path); err != nil {
		t.Fatal(err)
	}
	c.Chunks = []Chunk{{Path: path, From: hour, To: 2 * hour}}
	output := &parityOutput{capture: capture{targets: map[int64]map[int32]float64{}}}
	result, err := Run(context.Background(), c, nil, output)
	if err != nil {
		t.Fatal(err)
	}
	if result.Decisions != 2 || result.Incomplete != 0 || len(output.frames) != 2 || output.frames[1].Values["close"][1].Value != 1 || output.frames[1].Values["close"][2].Value != 4 {
		t.Fatalf("delayed reception changed actual replay: result=%+v frames=%+v", result, output.frames)
	}
}
