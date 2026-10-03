package runner

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
)

func TestPaperColdHistoryPreservesReplayAndRetainsOnlyHotState(t *testing.T) {
	ctx := context.Background()
	c := paperConfig(t, archiveConfig(t, false))
	c.Mode = Events
	c.Execution.StorePath, c.Execution.SenderLeaseDir = "", ""
	baseline, err := Run(ctx, c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.Execution.HistoryPath = filepath.Join(t.TempDir(), "history.sqlite")
	sink, cleanup, err := NewPaperSink(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	got, err := Run(ctx, c, sink, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Book, baseline.Book) || got.TargetsAccepted != baseline.TargetsAccepted || got.Fills != baseline.Fills || got.ManifestID != baseline.ManifestID || got.StrategyHash != baseline.StrategyHash {
		t.Fatalf("cold output changed replay: cold=%+v memory=%+v", got, baseline)
	}
	stats, err := sink.Account.Service().Store().MemoryHistoryStats(ctx)
	if err != nil || stats.ColdRecords <= int64(stats.HotRecords)*10 {
		t.Fatalf("history was not spilled: %+v %v", stats, err)
	}
	// A consumer can still export every earlier committed event and posting.
	manifest, err := sink.Account.ArchiveCommittedEvents(ctx, filepath.Join(t.TempDir(), "events"), 0, 7)
	if err != nil || manifest.Status != "complete" || len(manifest.Files) < 2 {
		t.Fatalf("cold read-through archive incomplete: %+v %v", manifest, err)
	}
}
