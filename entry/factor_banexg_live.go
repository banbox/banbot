package entry

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

// The production binding consumes SDK capabilities, never venue-name branches.
func newBanexgFactorLiveBinding(ctx context.Context, exchange banexg.BanExchange, snapshot *config.Snapshot, c runner.Config) (FactorLiveBinding, error) {
	var binding FactorLiveBinding
	if snapshot == nil || snapshot.View() == nil || snapshot.View().Exchange == nil || exchange == nil {
		return binding, errors.New("factor: live exchange configuration required")
	}
	cfg := snapshot.View()
	if cfg.Env != "prod" {
		return binding, errors.New("factor: banexg live binding requires env: prod")
	}
	binding.Account = execution.AccountKey{VenueSessionIdentity: fmt.Sprintf("%s/%s/%s", cfg.Exchange.Name, cfg.MarketType, cfg.Env), Account: c.AccountID, SettlementDomain: c.Manifest.Currency}
	binding.Transport = execution.NewBanexgTransport()
	binding.BootstrapCapital = true
	binding.Symbols = map[int32]*orm.ExSymbol{}
	for sid, symbol := range c.Snapshot.SIDMap {
		market, err := exchange.GetMarket(symbol)
		if err != nil {
			return binding, err
		}
		binding.Symbols[sid] = &orm.ExSymbol{ID: sid, Symbol: symbol, Exchange: cfg.Exchange.Name, Market: cfg.MarketType, Combined: market.Combined}
		if unit, ok := c.Execution.Instruments[sid]; ok {
			if !market.Linear || !market.Swap || market.Settle != unit.SettlementCurrency || market.Symbol != symbol {
				return binding, fmt.Errorf("factor: SID %d requires matching normalized linear perpetual metadata", sid)
			}
			expected, err := execution.InstrumentFromBanexgMarket(unit.ID, market, exchange.GetExg().CurrenciesByCode[market.Settle])
			if err != nil {
				return binding, err
			}
			if !unit.QuantityStep.Equal(expected.QuantityStep) || !unit.PriceTick.Equal(expected.PriceTick) || !unit.ContractSize.Equal(expected.ContractSize) || unit.MoneyScale != expected.MoneyScale || unit.MinSteps < expected.MinSteps || unit.MinNotional.LessThan(expected.MinNotional) {
				return binding, fmt.Errorf("factor: SID %d configured units or minima disagree with venue metadata", sid)
			}
		}
	}
	funding, ok := factorFundingCapability(exchange)
	if !ok || c.Manifest.Costs.FundingPolicy != "required-stream" || c.FundingSource == "" {
		return binding, errors.New("factor: linear perpetual live execution requires authoritative funding cash capability and required-stream policy")
	}
	binding.VerifyFunding = func(ctx context.Context, policy string) (string, error) {
		if policy != "required-stream" {
			return "", errors.New("factor: perpetual funding cannot use explicit-zero")
		}
		now := time.Now().UnixMilli()
		_, err := funding.FetchFundingCash(ctx, c.AccountID, c.Manifest.Currency, now-1, now)
		if err != nil {
			return "", err
		}
		return "banexg-funding-cash-v1", nil
	}
	source, err := newFactorFundingSource(funding, c)
	if err != nil {
		return binding, err
	}
	binding.Sources = []data.DataSource{source}
	binding.Record = func(series *orm.DataSeries, received int64) (factor.VersionRecord, error) {
		if series == nil || series.TimeMS < 0 || received < 0 {
			return factor.VersionRecord{}, errors.New("factor: invalid live observation")
		}
		event := series.EndMS
		if event == 0 || series.TimeFrame == "event" {
			event = series.TimeMS
		}
		version := c.Snapshot.SourceVersions[series.Source]
		if version == "" || event > received {
			return factor.VersionRecord{}, errors.New("factor: missing source lineage or future live observation")
		}
		// Preserve every field and concrete type, including NULL; reception is
		// conservative publication evidence for live observations and warmup.
		return factor.CloneVersionRecord(factor.VersionRecord{Series: *series, EventTime: event, Revision: uint64(received) + 1, AvailableAt: received, IngestedAt: received, SourceVersion: version})
	}
	return binding, ctx.Err()
}

func factorFundingCapability(exchange banexg.BanExchange) (banexg.FundingCashCapability, bool) {
	if capability, ok := exchange.(banexg.FundingCashCapability); ok {
		return capability, true
	}
	if wrapped, ok := exchange.(interface{ UnderlyingExchange() banexg.BanExchange }); ok {
		return factorFundingCapability(wrapped.UnderlyingExchange())
	}
	return nil, false
}

// factorFundingPollInterval spaces live funding polls. Venues settle funding
// every one to eight hours and the income endpoint is heavily rate limited
// (Binance: two weight-30 calls per window), so one account-wide poll per
// minute covers every subscribed instrument.
const factorFundingPollInterval = time.Minute

func newFactorFundingSource(capability banexg.FundingCashCapability, c runner.Config) (*data.FuncDataSource, error) {
	info := orm.NewSeriesInfo(c.FundingSource, "event", []orm.SeriesField{{Name: "settlement_id", Type: "string"}, {Name: "account_amount", Type: "string"}, {Name: "rate", Type: "string"}, {Name: "mark", Type: "string"}})
	// fetchAll returns the account's settlements in [start, end) for every
	// instrument; the venue reports them account-wide.
	fetchAll := func(ctx context.Context, start, end int64) ([]banexg.FundingCash, error) {
		var all []banexg.FundingCash
		for start < end {
			until := min(end-1, start+7*24*time.Hour.Milliseconds())
			rows, err := capability.FetchFundingCash(ctx, c.AccountID, c.Manifest.Currency, start, until)
			if err != nil {
				return nil, err
			}
			all = append(all, rows...)
			start = until + 1
		}
		return all, nil
	}
	records := func(sub *orm.Subscription, rows []banexg.FundingCash) []*orm.DataRecord {
		var out []*orm.DataRecord
		for _, row := range rows {
			if row.Symbol == sub.ExSymbol.Symbol {
				out = append(out, &orm.DataRecord{Sid: sub.ExSymbol.ID, TimeMS: row.AtMS, EndMS: row.AtMS, Closed: true, Values: map[string]any{"settlement_id": row.ID, "account_amount": row.Amount, "rate": row.Rate, "mark": row.Mark}})
			}
		}
		return out
	}
	fetch := func(ctx context.Context, sub *orm.Subscription, start, end int64) ([]*orm.DataRecord, error) {
		rows, err := fetchAll(ctx, start, end)
		if err != nil {
			return nil, err
		}
		return records(sub, rows), nil
	}
	return data.NewFuncDataSource(info, fetch, func(ctx context.Context, subs []*orm.Subscription, sink data.DataSink) error {
		cursor := time.Now().UnixMilli()
		ticker := time.NewTicker(factorFundingPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				end := time.Now().UnixMilli() + 1
				rows, err := fetchAll(ctx, max(0, cursor-60000), end)
				if err != nil {
					if ctx.Err() == nil && execution.TransientVenueReadError(err) {
						// Keep the cursor: the next poll covers this window again.
						log.Warn("factor funding poll failed transiently; retrying", zap.Error(err))
						continue
					}
					return err
				}
				for _, sub := range subs {
					if out := records(sub, rows); len(out) > 0 {
						if err := sink.Emit(sub, out); err != nil {
							return err
						}
					}
				}
				cursor = end - 1
			}
		}
	})
}
