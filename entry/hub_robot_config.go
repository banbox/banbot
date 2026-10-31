package entry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
	"github.com/shopspring/decimal"
)

func robotHubSocket(spec *config.RunSpec, account, dataDir string) (string, error) {
	fields := accountExecutionFields(spec.Config(), account)
	if _, ok := fields["hub_socket"]; ok {
		field := "execution.hub_socket"
		if _, ok := spec.Config().AccountExecution[account]["hub_socket"]; ok {
			field = "accounts." + account + ".hub_socket"
		}
		return spec.ResolvePath(field)
	}
	path := filepath.Join(dataDir, "execution", "hub.sock")
	if !filepath.IsAbs(path) {
		return "", errors.New("hub: absolute data directory or hub_socket required")
	}
	return path, nil
}

func robotPublicSnapshot(snapshot *config.Snapshot) (*config.Snapshot, error) {
	if snapshot == nil || snapshot.View() == nil || snapshot.View().Exchange == nil {
		return nil, errors.New("hub robot: exchange configuration required")
	}
	public := snapshot.Clone()
	public.View().Accounts = nil
	public.View().Exchange.Items = nil
	return public, nil
}

func ensureRobotPublicExchange(session *explicitEntrySession, public *config.Snapshot) error {
	session.exchangeMu.Lock()
	defer session.exchangeMu.Unlock()
	if session.exchange != nil {
		return errors.New("hub robot: public exchange already initialized")
	}
	exchange, err := exg.NewForRuntime(public, session.netDisable)
	if err != nil {
		return err
	}
	if _, err := exchange.LoadMarkets(false, map[string]any{banexg.ParamContext: session.ctx}); err != nil {
		_ = exchange.Close()
		return err
	}
	session.exchange = exchange
	return nil
}

func robotPublicBinding(ctx context.Context, exchange banexg.BanExchange, snapshot *config.Snapshot, c runner.Config) (FactorLiveBinding, error) {
	var binding FactorLiveBinding
	if snapshot == nil || snapshot.View() == nil || snapshot.View().Exchange == nil || exchange == nil {
		return binding, errors.New("hub robot: public exchange required")
	}
	cfg := snapshot.View()
	if (cfg.Env != core.RunEnvProd && cfg.Env != core.RunEnvDryRun) || cfg.MarketType != banexg.MarketLinear {
		return binding, errors.New("hub robot: prod/dry_run linear market required")
	}
	binding.Account = execution.AccountKey{VenueSessionIdentity: cfg.Exchange.Name + "/linear/" + cfg.Env, Account: c.AccountID, SettlementDomain: c.Manifest.Currency}
	binding.Symbols = map[int32]*orm.ExSymbol{}
	for sid, symbol := range c.Snapshot.SIDMap {
		market, err := exchange.GetMarket(symbol)
		if err != nil {
			return binding, err
		}
		if market == nil {
			return binding, errors.New("hub robot: missing normalized public market")
		}
		binding.Symbols[sid] = &orm.ExSymbol{ID: sid, Symbol: symbol, Exchange: cfg.Exchange.Name, Market: cfg.MarketType, Combined: market.Combined}
		if unit, ok := c.Execution.Instruments[sid]; ok {
			expected, err := execution.InstrumentFromBanexgMarket(unit.ID, market, exchange.GetExg().CurrenciesByCode[market.Settle])
			if err != nil {
				return binding, err
			}
			if !hubInstrumentMatches(unit, expected) {
				return binding, fmt.Errorf("hub robot: SID %d instrument disagrees with public market", sid)
			}
		}
	}
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
		return factor.CloneVersionRecord(factor.VersionRecord{Series: *series, EventTime: event, Revision: uint64(received) + 1, AvailableAt: received, IngestedAt: received, SourceVersion: version})
	}
	return binding, ctx.Err()
}

func registerRobotHubAccount(ctx context.Context, client *execution.HubClient, snapshot *config.Snapshot, configs []runner.Config, binding FactorLiveBinding) (execution.AccountKey, error) {
	key := binding.Account
	if len(configs) == 0 || snapshot == nil || snapshot.View() == nil {
		return key, errors.New("hub robot: account configuration required")
	}
	cfg := snapshot.View()
	first := configs[0]
	e := first.Execution
	request := HubAccountConfig{Exchange: cfg.Exchange.Name, Capital: map[execution.StrategyID]decimal.Decimal{}, Risk: execution.PortfolioRisk{MarginRate: e.MarginRate, MaxAccountMargin: e.MaxAccountMargin, MaxVirtualGross: e.MaxVirtualGross, StrategyGrossLimits: map[execution.StrategyID]decimal.Decimal{}}}
	units := map[string]execution.BanexgInstrument{}
	for _, c := range configs {
		if c.AccountID != key.Account || c.Manifest.Currency != key.SettlementDomain {
			return key, errors.New("hub robot: inconsistent account configuration")
		}
		if c.StrategyID != "" {
			id := execution.StrategyID(c.StrategyID)
			request.Capital[id] = decimal.NewFromFloat(c.InitialNAV)
			request.Risk.StrategyGrossLimits[id] = c.Execution.StrategyGrossLimit
		}
		for sid, unit := range c.Execution.Instruments {
			symbol := c.Snapshot.SIDMap[sid]
			units[unit.ID] = execution.BanexgInstrument{Symbol: symbol, Instrument: unit}
		}
	}
	for _, unit := range binding.AccountInstruments {
		units[unit.Instrument.ID] = unit
	}
	if binding.Legacy != nil {
		for id, capital := range binding.Legacy.capital {
			request.Capital[id] = capital
			request.Risk.StrategyGrossLimits[id] = binding.Legacy.Bridge.Risk.StrategyGrossLimits[id]
		}
	}
	for _, unit := range units {
		request.Instruments = append(request.Instruments, unit)
	}
	sort.Slice(request.Instruments, func(i, j int) bool {
		return request.Instruments[i].Instrument.ID < request.Instruments[j].Instrument.ID
	})
	if cfg.Env == core.RunEnvProd {
		request.Credential = cfg.Accounts[key.Account]
		var err error
		var bridge = binding.Legacy
		if bridge != nil {
			request.Takeover, err = executionTakeoverOwner(cfg, bridge.Bridge)
		} else {
			request.Takeover, err = executionTakeoverOwner(cfg, nil)
		}
		if err != nil {
			return key, err
		}
	} else {
		nav := first.AccountInitialNAV
		if nav <= 0 {
			nav = first.InitialNAV
		}
		request.PaperCash = decimal.NewFromFloat(nav)
		request.PaperFee = decimal.NewFromFloat(first.Manifest.Costs.FeeRate)
		request.PaperSlippage = decimal.NewFromFloat(first.Manifest.Costs.SlippageRate)
		for _, c := range configs {
			if c.Manifest.Costs.FeeRate != first.Manifest.Costs.FeeRate || c.Manifest.Costs.SlippageRate != first.Manifest.Costs.SlippageRate {
				return key, errors.New("hub robot: inconsistent account paper costs")
			}
		}
	}
	body, err := json.Marshal(request)
	if err != nil {
		return key, err
	}
	err = client.Register(ctx, execution.HubAccountRegistration{Account: key, Env: cfg.Env, Configuration: body})
	return key, err
}
