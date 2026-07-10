package opt

import (
	"testing"

	"github.com/banbox/banbot/config"
)

func TestRollBtOptSyncsRuntimeTimeRange(t *testing.T) {
	previous := config.TimeRange
	defer func() { config.TimeRange = previous }()

	original := &config.TimeTuple{StartMS: 1_000, EndMS: 10_000}
	task := &rollBtOpt{
		curMs:       4_000,
		allEndMs:    10_000,
		dateRange:   original.Clone(),
		runMSecs:    3_000,
		reviewMSecs: 2_000,
	}
	config.TimeRange = original

	task.setReviewRange()
	assertRuntimeTimeRange(t, task.dateRange, 2_000, 4_000)

	task.setRunRange()
	assertRuntimeTimeRange(t, task.dateRange, 4_000, 7_000)

	task.curMs = 9_000
	task.setRunRange()
	assertRuntimeTimeRange(t, task.dateRange, 9_000, 10_000)

	if original.StartMS != 1_000 || original.EndMS != 10_000 {
		t.Fatalf("original config range was mutated: %+v", original)
	}
}

func assertRuntimeTimeRange(t *testing.T, wantPtr *config.TimeTuple, wantStart, wantEnd int64) {
	t.Helper()
	if config.TimeRange != wantPtr {
		t.Fatal("runtime time range does not reference the rolling range")
	}
	if config.TimeRange.StartMS != wantStart || config.TimeRange.EndMS != wantEnd {
		t.Fatalf("runtime time range = %+v, want [%d, %d]", config.TimeRange, wantStart, wantEnd)
	}
}
