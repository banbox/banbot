package runner

import (
	"context"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/orm"
)

type settlementClockFactory struct {
	row       factor.VersionRecord
	reception *bool
}

func (f settlementClockFactory) Identity() string { return "received-settlement-clock-test" }
func (f settlementClockFactory) Open(_ context.Context, _ StorageStream, _, _ int64, reception bool) (VersionPageReader, error) {
	*f.reception = reception
	return &strictFixtureReader{rows: []factor.VersionRecord{f.row}}, nil
}

func TestSettlementSourcesScheduleReceivedPagedRows(t *testing.T) {
	for _, option := range []bool{false, true} {
		ctx := context.Background()
		row := factor.VersionRecord{Series: orm.DataSeries{Source: "settlement", TimeFrame: "event", Sid: 1, TimeMS: 100, EndMS: 100, Closed: true, Values: map[string]any{"fixing": "110", "terminal_cash_value": "0", "null": nil}}, EventTime: 100, AvailableAt: 110, IngestedAt: 200, Revision: 1, SourceVersion: "v1"}
		reception := false
		factory, err := NewStorageInputFactory(StorageInputOptions{Namespace: "settlement", PITPolicy: "strict", FromMS: 100, ToMS: 300, PageRows: 1, Streams: []StorageStream{{Subscription: orm.Subscription{Source: "settlement", TimeFrame: "event", ExSymbol: &orm.ExSymbol{ID: 1}}, SourceVersion: "v1", SchemaHash: "s1"}}, VersionPages: settlementClockFactory{row, &reception}})
		if err != nil {
			t.Fatal(err)
		}
		c := Config{MaxRecords: 10, Snapshot: factor.SnapshotSpec{VisibilityPolicy: "strict", SourceVersions: map[string]string{"settlement": "v1"}, Schemas: map[string]string{"settlement": "s1"}}}
		if option {
			c.OptionExpirySource = "settlement"
		} else {
			c.TerminalSettlementSource = "settlement"
		}
		input, err := factory.Open(ctx, c, factory.Ranges()[0])
		if err != nil {
			t.Fatal(err)
		}
		batch, err := input.Next(ctx)
		closed := input.Close()
		if err != nil || closed != nil || !reception || batch.AtMS != 200 || len(batch.Records) != 1 || batch.Records[0].AvailableAt != 110 || batch.Records[0].IngestedAt != 200 {
			t.Fatal("default publication replay lost settlement receipt", option, reception, batch, err, closed)
		}
	}
}
