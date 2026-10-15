package data

import (
	"strings"
	"testing"

	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
)

func TestLegacyHistoricalSeriesSubscriptionRejectsUnknownSourceBeforeInstall(t *testing.T) {
	old := &HistSeriesFeeder{}
	p := &HistProvider{series: map[string]*HistSeriesFeeder{"previous": old}}
	err := p.SetSeriesSubs([]*strat.DataSub{{Source: "events", ExSymbol: &orm.ExSymbol{ID: 1, Symbol: "BTC"}, TimeFrame: "event", Frequency: orm.FrequencyEvent}})
	if err == nil || !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("unknown source was not rejected explicitly: %v", err)
	}
	if len(p.series) != 1 || p.series["previous"] != old {
		t.Fatal("failed installation changed the active subscription plan")
	}
}
