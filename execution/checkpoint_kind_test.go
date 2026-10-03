package execution

import (
	"context"
	"errors"
	"testing"
)

func TestCheckpointAPIsKeepProjectionAndStrategySeparate(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore"} {
		t.Run(backend, func(t *testing.T) {
			store, _, _, _ := testStore(t)
			ctx := context.Background()
			fundStrategies(t, store)
			snapshot, err := store.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			body := []byte(`{"checkpoint":999,"nullable":null}`)
			if err := store.SaveStrategyCheckpoint(ctx, "strategy", "same", body); err != nil {
				t.Fatal(err)
			}
			if err := store.AdvanceProjection(ctx, "same", snapshot.Checkpoint); err != nil {
				t.Fatal(err)
			}
			payload, err := store.StrategyCheckpoint(ctx, "strategy", "same")
			if err != nil || string(payload) != string(body) {
				t.Fatal(string(payload), err)
			}
			cursor, err := store.ProjectionCursor(ctx, "same")
			if err != nil || cursor != snapshot.Checkpoint {
				t.Fatal(cursor, err)
			}
			if err := store.AdvanceProjection(ctx, "same", snapshot.Checkpoint+1); err == nil {
				t.Fatal("projection advanced beyond committed highwater")
			}
			if err := store.AdvanceProjection(ctx, "same", 0); err == nil {
				t.Fatal("projection regressed")
			}
			failure := errors.New("rollback")
			err = store.atomically(ctx, func(scope context.Context) error {
				if err := store.SaveStrategyCheckpoint(scope, "strategy", "same", []byte(`{}`)); err != nil {
					return err
				}
				if err := store.AdvanceProjection(scope, "new", snapshot.Checkpoint); err != nil {
					return err
				}
				return failure
			})
			if !errors.Is(err, failure) {
				t.Fatal(err)
			}
			payload, err = store.StrategyCheckpoint(ctx, "strategy", "same")
			if err != nil || string(payload) != string(body) {
				t.Fatal("rollback lost strategy checkpoint", string(payload), err)
			}
			cursor, err = store.ProjectionCursor(ctx, "new")
			if err != nil || cursor != 0 {
				t.Fatal("rollback leaked projection", cursor, err)
			}
		})
	}
}

func TestCheckpointMetadataBatchIsAtomicAndPointReadable(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		t.Run(backend, func(t *testing.T) {
			store, _, _, _ := testStore(t)
			ctx := context.Background()
			checkpoint := StrategyCheckpoint{Strategy: "ts", Name: "legacy-ts", Payload: []byte(`{"serial":1}`), Records: []StrategyCheckpoint{
				{Strategy: "ts", Name: "legacy-ts/order:first", Payload: []byte(`{"tag":"entry"}`)},
				{Strategy: "ts", Name: "legacy-ts/command:first", Payload: []byte(`{"order":1}`)},
			}}
			if err := store.saveAcceptedCheckpoint(ctx, checkpoint); err != nil {
				t.Fatal(err)
			}
			assertOriginal := func() {
				t.Helper()
				for _, record := range append([]StrategyCheckpoint{checkpoint}, checkpoint.Records...) {
					body, err := store.StrategyCheckpoint(ctx, record.Strategy, record.Name)
					if err != nil || string(body) != string(record.Payload) {
						t.Fatal("metadata commit/rollback lost point record", record.Name, string(body), err)
					}
				}
			}
			assertOriginal()
			bad := checkpoint
			bad.Records = append([]StrategyCheckpoint(nil), checkpoint.Records...)
			bad.Payload = []byte(`{"serial":2}`)
			bad.Records[0].Payload = []byte(`{"tag":"changed"}`)
			bad.Events = []StrategyAcceptedEvent{{CommandID: "invalid"}}
			if err := store.saveAcceptedCheckpoint(ctx, bad); err == nil {
				t.Fatal("invalid acceptance event committed metadata")
			}
			assertOriginal()
			bad.Events = nil
			bad.Records[0].Records = []StrategyCheckpoint{{}}
			if err := store.saveAcceptedCheckpoint(ctx, bad); err == nil {
				t.Fatal("nested metadata batch admitted")
			}
			assertOriginal()
		})
	}
}
