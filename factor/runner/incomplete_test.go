package runner

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/orm"
)

func TestReplayReportsMissingAndStaleStreamsWithoutAdmittingPartialFrames(t *testing.T) {
	c := archiveConfig(t, false)
	c.Mode = Research
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	c.Manifest.Portfolio.K = 1
	c.FundingSource = ""
	c.Prices = PriceStream{"kline", "1h", "close"}
	c.Snapshot.Universe = factor.Universe{Version: "two-assets", Static: true, Reference: []int32{1, 2}, Investable: []int32{1, 2}, Tradable: []int32{1, 2}}
	var err error
	c.Plan, err = factor.New().Add("close", factor.Field("kline", "close", "1h")).Compile()
	if err != nil {
		t.Fatal(err)
	}
	c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"close"}, Weights: map[string]float64{"close": 1}}
	store, err := factor.NewVersionStore(8)
	if err != nil {
		t.Fatal(err)
	}
	const hour int64 = 3600000
	for _, item := range []struct {
		sid int32
		bar int64
	}{{1, 1}, {2, 2}, {2, 3}, {1, 4}, {2, 4}} {
		at := item.bar * hour
		err = store.Put(factor.VersionRecord{Series: orm.DataSeries{Source: "kline", TimeFrame: "1h", Sid: item.sid, TimeMS: at - hour, EndMS: at, Closed: true, Values: map[string]any{"close": float64(item.sid)}}, EventTime: at, AvailableAt: at, IngestedAt: at, Revision: 1, SourceVersion: "v1"})
		if err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(t.TempDir(), "sparse.gob")
	if _, err = store.Export(path); err != nil {
		t.Fatal(err)
	}
	c.Chunks = []Chunk{{Path: path, From: hour, To: 4*hour + 2}}
	result, err := Run(context.Background(), c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Incomplete != 3 || result.Decisions != 1 {
		t.Fatalf("partial rounds were admitted or recovery failed: incomplete=%d decisions=%d", result.Incomplete, result.Decisions)
	}
	payload, err := json.Marshal(makeRunArtifact(result, nil))
	if err != nil {
		t.Fatal(err)
	}
	var artifact struct {
		Result struct {
			IncompleteStreams []struct {
				factor.StreamKey
				Missing, Invalid            int
				FirstGridTime, LastGridTime int64
			}
		} `json:"result"`
	}
	if err = json.Unmarshal(payload, &artifact); err != nil {
		t.Fatal(err)
	}
	issues := artifact.Result.IncompleteStreams
	if len(issues) != 2 {
		t.Fatalf("artifact lacks per-stream diagnostics: %s", payload)
	}
	if issues[0].SID != 1 || issues[0].Source != "kline" || issues[0].TimeFrame != "1h" || issues[0].Missing != 0 || issues[0].Invalid != 2 || issues[0].FirstGridTime != 2*hour || issues[0].LastGridTime != 3*hour {
		t.Fatalf("stale stream was not distinguished: %+v", issues[0])
	}
	if issues[1].SID != 2 || issues[1].Missing != 1 || issues[1].Invalid != 0 || issues[1].FirstGridTime != hour || issues[1].LastGridTime != hour {
		t.Fatalf("missing stream was not distinguished: %+v", issues[1])
	}
}
