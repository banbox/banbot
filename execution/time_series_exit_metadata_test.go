package execution

import (
	"context"
	"encoding/json"
	"testing"
)

func TestTimeSeriesExitMetadataFreezesAcceptedConditionsAcrossEdit(t *testing.T) {
	b, _ := strategyService(t, true)
	ctx := context.Background()
	create := tsCreate("entry", "lot", 1, true)
	if _, err := b.ApplyPendingCommand(ctx, create, 12); err != nil {
		t.Fatal(err)
	}
	results, err := b.ApplyTimeSeriesExit(ctx, TimeSeriesExitCommand{ID: "exit", Strategy: "a", Force: true, Amount: intentPrice("0.5"), Conditions: IntentConditions{Limit: intentPrice("110")}, ExitTag: "close"}, 13)
	if err != nil || len(results) != 1 {
		t.Fatal(results, err)
	}
	page, err := b.TimeSeriesProjection(ctx, TimeSeriesProjectionRequest{Name: "metadata-review", Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	var metadata TimeSeriesMetadata
	for _, r := range page.Records {
		if r.Lot == "lot" {
			metadata = r.Metadata
		}
	}
	if metadata.ExitQuantitySteps != 5 || metadata.ExitConditions == nil || !metadata.ExitConditions.Limit.Equal(intentPrice("110")) {
		t.Fatal("exit acceptance lacks immutable conditions", metadata)
	}
	edit := PendingCommand{ID: "edit", Strategy: "a", Lot: "lot", ExpectedRevision: results[0].Revision, Action: PendingEditExitLimit, Limit: intentPrice("120"), Metadata: &metadata}
	if _, err = b.ApplyPendingCommand(ctx, edit, 14); err != nil {
		t.Fatal(err)
	}
	page, err = b.TimeSeriesProjection(ctx, TimeSeriesProjectionRequest{Name: "metadata-review", Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	var old, updated bool
	for _, event := range page.Events {
		if event.Kind != "StrategyAccepted" {
			continue
		}
		var accepted StrategyAcceptedEvent
		if err = json.Unmarshal(event.Payload, &accepted); err != nil {
			t.Fatal(err)
		}
		if accepted.CommandID == "exit" {
			old = accepted.Metadata != nil && accepted.Metadata.ExitQuantitySteps == 5 && accepted.Metadata.ExitConditions != nil && accepted.Metadata.ExitConditions.Limit.Equal(intentPrice("110"))
		}
		if accepted.CommandID == "edit" {
			updated = accepted.Metadata != nil && accepted.Metadata.ExitConditions != nil && accepted.Metadata.ExitConditions.Limit.Equal(intentPrice("120"))
		}
	}
	if !old || !updated {
		t.Fatal("exit metadata rewrote historical acceptance or ignored edit", old, updated)
	}
}
