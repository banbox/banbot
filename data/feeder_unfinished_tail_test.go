package data

import (
	"testing"

	"github.com/banbox/banbot/orm"
)

func TestDropUnfinishedTail(t *testing.T) {
	rows := []*orm.DataSeries{{TimeMS: 0, EndMS: 3600}, {TimeMS: 3600, EndMS: 7200}, {TimeMS: 7200, EndMS: 10800}}
	if got := dropUnfinishedTail(rows, 7300); len(got) != 2 || got[1].EndMS != 7200 {
		t.Fatalf("unfinished hour not dropped: %d rows", len(got))
	}
	if got := dropUnfinishedTail(rows, 10800); len(got) != 3 {
		t.Fatalf("closed bar dropped: %d rows", len(got))
	}
	if got := dropUnfinishedTail(rows, 0); len(got) != 0 {
		t.Fatalf("expected all dropped, got %d", len(got))
	}
}
