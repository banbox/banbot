package execution

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func readArchiveManifest(t *testing.T, directory string) CommittedArchiveManifest {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(directory, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest CommittedArchiveManifest
	if err := json.Unmarshal(body, &manifest); err != nil {
		t.Fatal(err)
	}
	return manifest
}

func TestCommittedArchiveChunksPreserveTypedEventsLedgerAndChecksum(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore"} {
		t.Run(backend, func(t *testing.T) {
			store, _, owner, _ := testStore(t)
			ctx := context.Background()
			fundStrategies(t, store)
			order := planOrder(t, store, "archive", 1, Buy, EntryIntent, map[string]int64{"a": 3})
			if err := executorFor(store, owner, &fakeExecutionAdapter{}).Send(order.ID, 11); err != nil {
				t.Fatal(err)
			}
			if _, err := store.ApplyFill(ctx, FillReport{EventID: "fill", OrderID: order.ID, Steps: 3, Price: intentPrice("100"), Fee: intentPrice("0.05"), AtMS: 12}); err != nil {
				t.Fatal(err)
			}
			before, err := store.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			directory := filepath.Join(t.TempDir(), "execution")
			manifest, err := store.ArchiveCommittedEvents(ctx, directory, 0, 2)
			if err != nil {
				t.Fatal(err)
			}
			if manifest.Status != "complete" || manifest.ExportedThrough != before.Checkpoint || manifest.ThroughInclusive != before.Checkpoint || manifest.SchemaVersion != CommittedArchiveVersion || len(manifest.Files) < 2 {
				t.Fatal(manifest)
			}
			onDisk := readArchiveManifest(t, directory)
			a, _ := json.Marshal(manifest)
			b, _ := json.Marshal(onDisk)
			if !bytes.Equal(a, b) {
				t.Fatal("manifest publication differed")
			}
			var exported []CommittedEvent
			for _, file := range manifest.Files {
				body, err := os.ReadFile(filepath.Join(directory, file.Name))
				if err != nil {
					t.Fatal(err)
				}
				sum := sha256.Sum256(body)
				if hex.EncodeToString(sum[:]) != file.SHA256 {
					t.Fatal("chunk checksum mismatch")
				}
				var chunk CommittedArchiveChunk
				if err := gob.NewDecoder(bytes.NewReader(body)).Decode(&chunk); err != nil {
					t.Fatal(err)
				}
				if chunk.SchemaVersion != manifest.SchemaVersion || chunk.Account != store.key || chunk.FromExclusive != file.FromExclusive || chunk.ThroughInclusive != file.ThroughInclusive || len(chunk.Events) > 2 {
					t.Fatal("chunk schema/range mismatch", chunk, file)
				}
				postings := 0
				for _, event := range chunk.Events {
					postings += len(event.Ledger)
				}
				if postings != file.Postings {
					t.Fatal("ledger count mismatch")
				}
				exported = append(exported, chunk.Events...)
			}
			retained, err := store.EventsAfter(ctx, 0, 10000)
			if err != nil {
				t.Fatal(err)
			}
			a, _ = json.Marshal(exported)
			b, _ = json.Marshal(retained)
			if !bytes.Equal(a, b) {
				t.Fatal("archive changed event or attribution facts")
			}
			if _, err := store.ArchiveCommittedEvents(ctx, directory, 0, 2); err == nil {
				t.Fatal("existing artifact overwritten")
			}
			after, err := store.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			a, _ = json.Marshal(before)
			b, _ = json.Marshal(after)
			if !bytes.Equal(a, b) {
				t.Fatal("archiving pruned or mutated account")
			}
		})
	}
}

func TestCommittedArchiveBoundsHighwaterDespiteLaterCommits(t *testing.T) {
	events := []CommittedEvent{{ID: "a", Kind: "Audit", Checkpoint: 1, Payload: []byte(`{}`)}, {ID: "b", Kind: "Audit", Checkpoint: 2, Payload: []byte(`{}`)}, {ID: "later", Kind: "Audit", Checkpoint: 3, Payload: []byte(`{}`)}}
	requested := 0
	manifest, err := writeCommittedArchive(context.Background(), filepath.Join(t.TempDir(), "archive"), testIntent(Buy).Account, 0, 2, 3, func(_ context.Context, after int64, limit int) ([]CommittedEvent, error) {
		requested++
		if limit != 3 || after != 0 {
			t.Fatal(after, limit)
		}
		return events, nil
	})
	if err != nil || manifest.ExportedThrough != 2 || len(manifest.Files) != 1 || manifest.Files[0].Events != 2 || requested != 1 {
		t.Fatal(manifest, err)
	}
}

func TestCommittedArchiveCancellationAndFailuresRemainIncomplete(t *testing.T) {
	for _, scenario := range []string{"canceled", "read-failure", "write-failure", "missing-checkpoint"} {
		t.Run(scenario, func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), "archive")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			failure := errors.New("reader failed")
			reads := 0
			manifest, err := writeCommittedArchive(ctx, directory, testIntent(Buy).Account, 0, 3, 1, func(_ context.Context, after int64, _ int) ([]CommittedEvent, error) {
				reads++
				if reads == 2 {
					switch scenario {
					case "canceled":
						cancel()
						return nil, ctx.Err()
					case "read-failure":
						return nil, failure
					case "missing-checkpoint":
						return nil, nil
					}
				}
				if scenario == "write-failure" {
					if err := os.Mkdir(filepath.Join(directory, "chunk-000001.gob"), 0o700); err != nil {
						t.Fatal(err)
					}
				}
				return []CommittedEvent{{ID: "event", Kind: "Audit", Checkpoint: after + 1, Payload: []byte(`{}`)}}, nil
			})
			if err == nil || manifest.Status != "incomplete" || manifest.Reason == "" {
				t.Fatal("failure marked complete", manifest, err)
			}
			if scenario == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if scenario == "read-failure" && !errors.Is(err, failure) {
				t.Fatal(err)
			}
			disk := readArchiveManifest(t, directory)
			if disk.Status != "incomplete" || disk.Reason == "" || disk.ExportedThrough != manifest.ExportedThrough {
				t.Fatal("failure status not published", disk, manifest)
			}
			if scenario != "write-failure" && (len(disk.Files) != 1 || disk.ExportedThrough != 1) {
				t.Fatal("published chunk lost", disk)
			}
			entries, err := os.ReadDir(directory)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if strings.HasSuffix(entry.Name(), ".tmp") {
					t.Fatal("temporary chunk leaked", entry.Name())
				}
			}
		})
	}
	store, _, _, _ := testStore(t)
	fundStrategies(t, store)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	directory := filepath.Join(t.TempDir(), "canceled-store")
	manifest, err := store.ArchiveCommittedEvents(ctx, directory, 0, 2)
	if !errors.Is(err, context.Canceled) || manifest.Status != "incomplete" || readArchiveManifest(t, directory).Status != "incomplete" {
		t.Fatal("canceled run lacked incomplete artifact", manifest, err)
	}
}
