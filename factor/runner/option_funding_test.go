package runner

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/orm"
	"github.com/shopspring/decimal"
)

func TestOptionFundingRejectedWithoutCashMutation(t *testing.T) {
	ctx := context.Background()
	c := paperConfig(t, archiveConfig(t, false))
	c.Mode = Events
	i := c.Execution.Instruments[1]
	i.Valuation = "cash_european_option"
	i.Option = &execution.EuropeanOption{UnderlyingID: "underlying", Right: "call", Strike: decimal.NewFromInt(100), ExpiryMS: 1000, UnderlyingStressMax: decimal.NewFromInt(200)}
	c.Execution.Instruments[1] = i
	sink, done, err := NewPaperSink(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	before, err := sink.Account.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = sink.ObserveFunding(ctx, backtest.Funding{SID: 1, AtMS: 12, AvailableAt: 12, Rate: .001}, 12); err == nil {
		t.Fatal("cash option funding accepted")
	}
	sink.AuthoritativeFunding = true
	if err = sink.ObserveFundingRecord(ctx, factor.VersionRecord{Series: orm.DataSeries{Sid: 1}}, 12); err == nil {
		t.Fatal("authoritative cash option funding accepted")
	}
	after, err := sink.Account.Snapshot(ctx)
	if err != nil || before.Checkpoint != after.Checkpoint || !before.AccountSettledCash.Equal(after.AccountSettledCash) {
		t.Fatal("rejected funding mutated account", err)
	}
	if _, err = openArchiveInput(ctx, c, c.Chunks[0]); err == nil {
		t.Fatal("archived option perpetual funding stream accepted")
	}
}

func TestOptionFundingCompletenessOnlyRequiresPerpetualSIDs(t *testing.T) {
	ctx := context.Background()
	c := paperConfig(t, archiveConfig(t, false))
	store, err := factor.OpenVersionStore(c.Chunks[0].Path, c.MaxRecords)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := store.Records()
	if err != nil {
		t.Fatal(err)
	}
	clean, err := factor.NewVersionStore(c.MaxRecords)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if r.Series.Source != c.FundingSource {
			if err = clean.Put(r); err != nil {
				t.Fatal(err)
			}
		}
	}
	path := filepath.Join(t.TempDir(), "option-only.gob")
	if _, err = clean.Export(path); err != nil {
		t.Fatal(err)
	}
	c.Chunks[0].Path = path
	for sid, i := range c.Execution.Instruments {
		i.Valuation = "cash_european_option"
		i.Option = &execution.EuropeanOption{UnderlyingID: "underlying", Right: "put", Strike: decimal.NewFromInt(100), ExpiryMS: 1000000000, UnderlyingStressMax: decimal.NewFromInt(200)}
		c.Execution.Instruments[sid] = i
	}
	input, err := openArchiveInput(ctx, c, c.Chunks[0])
	if err != nil {
		t.Fatal("option-only book required perpetual payments", err)
	}
	_ = input.Close()
	i := c.Execution.Instruments[2]
	i.Valuation = "linear_perpetual"
	i.Option = nil
	c.Execution.Instruments[2] = i
	if _, err = openArchiveInput(ctx, c, c.Chunks[0]); err == nil {
		t.Fatal("mixed book lost actual perpetual completeness requirement")
	}
}

func TestLivePreflightRejectsAllOptionEntrySurfaces(t *testing.T) {
	c := archiveConfig(t, false)
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	c.Manifest.Labels = nil
	c.Plan, _ = factor.New().Add("close", factor.Field("kline", "close", "1h")).Compile()
	c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"close"}, Weights: map[string]float64{"close": 1}}
	if err := ValidateLiveConfig(c); err != nil {
		t.Fatal("ordinary live baseline", err)
	}
	for _, mutation := range []func(*Config){
		func(c *Config) { c.OptionExpirySource = "option-expiry" },
		func(c *Config) {
			c.Execution.Instruments = map[int32]execution.Instrument{1: {Valuation: "cash_european_option"}}
		},
		func(c *Config) {
			c.Execution.Instruments = map[int32]execution.Instrument{1: {Option: &execution.EuropeanOption{}}}
		},
	} {
		owned, err := CloneConfig(c)
		if err != nil {
			t.Fatal(err)
		}
		mutation(&owned)
		if err = ValidateLiveConfig(owned); err == nil {
			t.Fatal("unsupported live options passed preflight")
		}
		if _, _, err = compileLiveDecision(owned); err == nil {
			t.Fatal("live constructor compile accepted option")
		}
	}
}
