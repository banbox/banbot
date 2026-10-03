package execution

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestCheckpointPagesKeepScopeOrderAndStagedChanges(t *testing.T) {
	for _, backend := range []string{"SQLite", "MemoryStore", "MemoryHistory"} {
		t.Run(backend, func(t *testing.T) {
			store, _, _, _ := testStore(t)
			ctx := context.Background()
			const prefix = "legacy-ts/order:id:"
			for n := 19; n >= 0; n-- {
				if err := store.SaveStrategyCheckpoint(ctx, "ts", fmt.Sprintf("%s%02d", prefix, n), []byte(`{"value":1}`)); err != nil {
					t.Fatal(err)
				}
			}
			for _, item := range []StrategyCheckpoint{
				{Strategy: "other", Name: prefix + "01", Payload: []byte(`{}`)},
				{Strategy: "ts", Name: "legacy-ts/order:lot:01", Payload: []byte(`{}`)},
				{Strategy: "ts", Name: "Legacy-ts/order:id:01", Payload: []byte(`{}`)},
			} {
				if err := store.SaveStrategyCheckpoint(ctx, item.Strategy, item.Name, item.Payload); err != nil {
					t.Fatal(err)
				}
			}
			after, count := "", 0
			for {
				page, err := store.StrategyCheckpointsAfter(ctx, "ts", prefix, after, 3)
				if err != nil || len(page) > 3 {
					t.Fatal(page, err)
				}
				if len(page) == 0 {
					break
				}
				for _, item := range page {
					if item.Name != fmt.Sprintf("%s%02d", prefix, count) || item.Strategy != "ts" {
						t.Fatal("page lost scope/order", count, item)
					}
					count++
				}
				after = page[len(page)-1].Name
			}
			if count != 20 {
				t.Fatal("page lost records", count)
			}
			err := store.atomically(ctx, func(scope context.Context) error {
				if err := store.SaveStrategyCheckpoint(scope, "ts", prefix+"01", []byte(`{"value":2}`)); err != nil {
					return err
				}
				page, err := store.StrategyCheckpointsAfter(scope, "ts", prefix, "", 3)
				if err == nil && (len(page) != 3 || string(page[1].Payload) != `{"value":2}`) {
					t.Fatal("cold row shadowed staged metadata", page)
				}
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if _, err := store.StrategyCheckpointsAfter(canceled, "ts", prefix, "", 3); !errors.Is(err, context.Canceled) {
				t.Fatal("checkpoint page ignored cancellation", err)
			}
		})
	}
}
