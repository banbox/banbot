package runner

import (
	"context"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/orm"
	"io"
	"path/filepath"
	"testing"
)

func TestTerminalArchiveSourcePublicationReceiptAndIdentity(t *testing.T) {
	r := factor.VersionRecord{Series: orm.DataSeries{Source: "terminal", TimeFrame: "event", Sid: 1, TimeMS: 10, EndMS: 10, Closed: true, Values: map[string]any{"terminal_cash_value": "0"}}, EventTime: 10, AvailableAt: 20, IngestedAt: 30, Revision: 1, SourceVersion: "v1"}
	if _, err := terminalRecord(r, 29); err == nil {
		t.Fatal("future receipt accepted")
	}
	if _, err := terminalRecord(r, 30); err != nil {
		t.Fatal(err)
	}
	c := Config{MaxRecords: 10, TerminalSettlementSource: "terminal", Prices: PriceStream{Source: "prices"}}
	store, _ := factor.NewVersionStore(10)
	if err := store.Put(r); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "terminal.gob")
	if _, err := store.Export(path); err != nil {
		t.Fatal(err)
	}
	input, err := openArchiveInput(context.Background(), c, Chunk{Path: path, From: 1, To: 40})
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	batch, err := input.Next(context.Background())
	if err != nil || batch.AtMS != 30 || len(batch.Records) != 1 {
		t.Fatal("receipt chronology", batch, err)
	}
	if _, err := input.Next(context.Background()); err != io.EOF {
		t.Fatal(err)
	}
	a, err := runAssumptions(c)
	if err != nil {
		t.Fatal(err)
	}
	c.TerminalSettlementSource = "other"
	b, err := runAssumptions(c)
	if err != nil || a == b {
		t.Fatal("terminal source absent from run identity", err)
	}
	for _, value := range []any{nil, 0., "-1", "NaN"} {
		r.Series.Values["terminal_cash_value"] = value
		if _, err := terminalRecord(r, 30); err == nil {
			t.Fatal("inferred/invalid value accepted", value)
		}
	}
}
