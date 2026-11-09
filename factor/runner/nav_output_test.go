package runner

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"testing"
)

// Weights-mode runs emit a mark-to-market "nav" event at every decision, not only at executed rebalances.
func TestJSONOutputEmitsNavEveryDecision(t *testing.T) {
	cfg := archiveConfig(t, false)
	var buf bytes.Buffer
	out := &JSONOutput{Writer: &buf, SIDs: cfg.Snapshot.Universe.Evaluation}
	res, err := Run(context.Background(), cfg, nil, out)
	if err != nil {
		t.Fatal(err)
	}
	var navs, accepted int
	var last int64
	sc := bufio.NewScanner(&buf)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	for sc.Scan() {
		var ev struct {
			Kind string
			AtMS int64
			NAV  float64
		}
		if json.Unmarshal(sc.Bytes(), &ev) != nil {
			continue
		}
		switch ev.Kind {
		case "nav":
			navs++
			if ev.NAV <= 0 || ev.AtMS <= last {
				t.Fatalf("bad nav event %+v after %d", ev, last)
			}
			last = ev.AtMS
		case "target-accepted":
			accepted++
		}
	}
	if navs == 0 || navs < res.Decisions || navs <= accepted && res.Decisions > accepted {
		t.Fatalf("nav events %d, decisions %d, accepted %d", navs, res.Decisions, accepted)
	}
}
