package orm

import (
	"strings"
	"testing"
)

func TestBuildSeriesRangeSummaryWhere(t *testing.T) {
	hasData := true
	where, params := buildSeriesRangeSummaryWhere(ListSeriesRangeSummariesArgs{
		Source:    "kline",
		TimeFrame: "1h",
		Sid:       12,
		HasData:   &hasData,
	})
	if !strings.Contains(where, "tbl LIKE 'kline_%'") || !strings.Contains(where, "sid = $1") || !strings.Contains(where, "timeframe = $2") || !strings.Contains(where, "has_data = $3") {
		t.Fatalf("unexpected kline where clause: %s", where)
	}
	if len(params) != 3 || params[0] != int32(12) || params[1] != "1h" || params[2] != true {
		t.Fatalf("unexpected params: %#v", params)
	}

	where, params = buildSeriesRangeSummaryWhere(ListSeriesRangeSummariesArgs{Source: "funding_rate"})
	if where != "tbl = $1" || len(params) != 1 || params[0] != "funding_rate" {
		t.Fatalf("unexpected custom source filter where=%q params=%#v", where, params)
	}
}
