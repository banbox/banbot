package opt

import (
	"testing"

	"github.com/banbox/banbot/config"
)

func TestRollBtOptKeepsRollingTimeRangeTaskLocal(t *testing.T) {
	original := &config.TimeTuple{StartMS: 1_000, EndMS: 10_000}
	task := &rollBtOpt{
		curMs:       4_000,
		allEndMs:    10_000,
		dateRange:   original.Clone(),
		runMSecs:    3_000,
		reviewMSecs: 2_000,
	}
	task.setReviewRange()
	assertTaskTimeRange(t, task.dateRange, 2_000, 4_000)

	task.setRunRange()
	assertTaskTimeRange(t, task.dateRange, 4_000, 7_000)

	task.curMs = 9_000
	task.setRunRange()
	assertTaskTimeRange(t, task.dateRange, 9_000, 10_000)

	if original.StartMS != 1_000 || original.EndMS != 10_000 {
		t.Fatalf("original config range was mutated: %+v", original)
	}
}

func assertTaskTimeRange(t *testing.T, got *config.TimeTuple, wantStart, wantEnd int64) {
	t.Helper()
	if got == nil || got.StartMS != wantStart || got.EndMS != wantEnd {
		t.Fatalf("task time range = %+v, want [%d, %d]", got, wantStart, wantEnd)
	}
}
