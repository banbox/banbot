package data

import (
	"testing"

	"github.com/banbox/banexg"
)

func TestTickWorkersKeepInvocationStateIsolated(t *testing.T) {
	first, second := NewTickWorker(0), NewTickWorker(3)
	first.TimeMSMin = 100
	first.symKLines["first"] = []*banexg.Kline{{Time: 1}}
	second.TimeMSMax = 200
	second.symKLines["second"] = []*banexg.Kline{{Time: 2}}
	if first.ConcurNum != 5 || second.ConcurNum != 3 || first.TimeMSMax != 0 || second.TimeMSMin != 0 {
		t.Fatalf("worker options leaked: first=%+v second=%+v", first, second)
	}
	if len(first.symKLines) != 1 || first.symKLines["second"] != nil || len(second.symKLines) != 1 || second.symKLines["first"] != nil {
		t.Fatalf("worker candle buffers leaked: first=%v second=%v", first.symKLines, second.symKLines)
	}
}
