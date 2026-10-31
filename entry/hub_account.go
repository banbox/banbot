package entry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banexg"
	"github.com/shopspring/decimal"
)

// HubAccountConfig is sent privately by a robot. Hub YAML accounts are never
// consulted. Store paths are chosen by the host, not supplied over RPC.
type HubAccountConfig struct {
	Exchange      string
	Credential    *config.AccountConfig
	Instruments   []execution.BanexgInstrument
	Capital       map[execution.StrategyID]decimal.Decimal
	Risk          execution.PortfolioRisk
	Takeover      execution.StrategyID
	PaperCash     decimal.Decimal
	PaperFee      decimal.Decimal
	PaperSlippage decimal.Decimal
}

func decodeHubAccountConfig(request execution.HubAccountRegistration) (HubAccountConfig, error) {
	var c HubAccountConfig
	d := json.NewDecoder(bytes.NewReader(request.Configuration))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, errors.New("hub: invalid account configuration")
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return c, errors.New("hub: account configuration must contain one object")
	}
	if request.Env != core.RunEnvProd && request.Env != core.RunEnvDryRun {
		return c, errors.New("hub: only prod and dry_run accounts are supported")
	}
	if err := request.Account.Validate(); err != nil {
		return c, err
	}
	if c.Exchange == "" || request.Account.VenueSessionIdentity != c.Exchange+"/linear/"+request.Env || len(c.Instruments) == 0 || len(c.Capital) == 0 {
		return c, errors.New("hub: explicit linear venue identity, instruments and strategy capital required")
	}
	if request.Env == core.RunEnvProd {
		if c.Credential == nil || c.Credential.NoTrade || !c.PaperCash.IsZero() || !c.PaperFee.IsZero() || !c.PaperSlippage.IsZero() {
			return c, errors.New("hub: production requires trading credentials and no paper settings")
		}
		secret := c.Credential.GetApiSecretFor(c.Exchange, core.RunEnvProd)
		if secret.APIKey == "" || secret.APISecret == "" {
			return c, errors.New("hub: missing production credentials")
		}
	} else if c.Credential != nil || c.Takeover != "" || !c.PaperCash.IsPositive() {
		return c, errors.New("hub: dry_run requires positive simulated cash and forbids credentials or external takeover")
	}
	symbols, ids := map[string]bool{}, map[string]bool{}
	for _, unit := range c.Instruments {
		if err := unit.Instrument.Validate(); err != nil {
			return c, err
		}
		if unit.Symbol == "" || symbols[unit.Symbol] || ids[unit.Instrument.ID] || unit.Instrument.Valuation != "linear_perpetual" || unit.Instrument.SettlementCurrency != request.Account.SettlementDomain {
			return c, errors.New("hub: unique matching linear perpetual instruments required")
		}
		symbols[unit.Symbol], ids[unit.Instrument.ID] = true, true
	}
	if !c.Risk.MarginRate.IsPositive() || c.Risk.MarginRate.GreaterThan(decimal.NewFromInt(1)) || !c.Risk.MaxAccountMargin.IsPositive() || !c.Risk.MaxVirtualGross.IsPositive() || len(c.Risk.StrategyGrossLimits) != len(c.Capital) {
		return c, errors.New("hub: explicit positive account and strategy risk limits required")
	}
	for id, capital := range c.Capital {
		if id == "" || !capital.IsPositive() || !c.Risk.StrategyGrossLimits[id].IsPositive() {
			return c, errors.New("hub: positive strategy capital and gross limit required")
		}
	}
	return c, nil
}

// newHubAccountFactory creates no private clients until the first registration.
func newHubAccountFactory(root, leaseDir string, netDisabled bool) execution.HubAccountFactory {
	return func(ctx context.Context, request execution.HubAccountRegistration) (binding execution.HubAccountBinding, result error) {
		c, err := decodeHubAccountConfig(request)
		if err != nil {
			return binding, err
		}
		if request.Env == core.RunEnvProd && !filepath.IsAbs(leaseDir) {
			return binding, errors.New("hub: production requires an explicit absolute sender lease directory shared with the stopped robot owner")
		}
		c, err = loadOrCreateHubHostConfig(ctx, root, request, c)
		if err != nil {
			return binding, err
		}
		cfg := &config.Config{Env: request.Env, Exchange: &config.ExchangeConfig{Name: c.Exchange}, MarketType: banexg.MarketLinear, ContractType: banexg.MarketSwap, StakeCurrency: []string{request.Account.SettlementDomain}}
		if c.Credential != nil {
			cfg.Accounts = map[string]*config.AccountConfig{request.Account.Account: c.Credential}
		}
		snapshot := config.NewSnapshotWithDirs(cfg, "", "")
		exchange, exErr := exg.NewForRuntime(snapshot, netDisabled)
		if exErr != nil {
			return binding, exErr
		}
		var registry execution.AccountRegistry
		var account *execution.SharedAccount
		var market *execution.AccountMarketStream
		var once sync.Once
		var closeErr error
		shutdown := func() error {
			once.Do(func() {
				if market != nil {
					closeErr = errors.Join(closeErr, market.Close())
				}
				registry.Stop()
				registry.Join()
				if account != nil {
					closeErr = errors.Join(closeErr, account.Close())
				}
				if err := exchange.Close(); err != nil {
					closeErr = errors.Join(closeErr, err)
				}
			})
			return closeErr
		}
		defer func() {
			if result != nil {
				result = errors.Join(result, shutdown())
			}
		}()
		if _, err := exchange.LoadMarkets(false, map[string]any{banexg.ParamContext: ctx}); err != nil {
			return binding, err
		}
		for _, unit := range c.Instruments {
			m, marketErr := exchange.GetMarket(unit.Symbol)
			if marketErr != nil {
				return binding, marketErr
			}
			if m == nil {
				return binding, errors.New("hub: normalized exchange market is missing")
			}
			expected, err := execution.InstrumentFromBanexgMarket(unit.Instrument.ID, m, exchange.GetExg().CurrenciesByCode[m.Settle])
			if err != nil || !hubInstrumentMatches(unit.Instrument, expected) {
				return binding, errors.New("hub: declared instrument disagrees with normalized exchange metadata")
			}
		}
		owner, err := registry.Acquire(request.Account)
		if err != nil {
			return binding, err
		}
		var adapter execution.ExecutionAdapter
		options := execution.SharedExecutionOptions{AuthoritativeSnapshot: true, TakeoverStrategy: c.Takeover}
		directory := hubAccountDirectory(root, request.Account)
		if mkdirErr := os.MkdirAll(directory, 0700); mkdirErr != nil {
			return binding, mkdirErr
		}
		options.StorePath = filepath.Join(directory, "ledger.db")
		options.SenderLeaseDir = leaseDir
		if request.Env == core.RunEnvDryRun && options.SenderLeaseDir == "" {
			options.SenderLeaseDir = filepath.Join(root, "sender-leases")
		}
		if mkdirErr := os.MkdirAll(options.SenderLeaseDir, 0700); mkdirErr != nil {
			return binding, mkdirErr
		}
		if request.Env == core.RunEnvDryRun {
			adapter, err = execution.NewDurablePaperAdapter(c.PaperCash, c.PaperFee, c.PaperSlippage, execution.PaperBidAsk)
		} else {
			adapter, err = execution.NewBanexgAdapter(ctx, exchange, execution.BanexgAdapterConfig{Account: request.Account, Instruments: c.Instruments, Transport: execution.NewBanexgTransport()})
		}
		if err != nil {
			return binding, err
		}
		options.Adapter = adapter
		account, err = execution.NewSharedAccount(owner, options)
		if err != nil {
			return binding, err
		}
		borrow := account.Borrow()
		defer borrow.Release()
		if err := borrow.RegisterRiskPolicy("hub-v1", c.Risk); err != nil {
			return binding, err
		}
		// RegisterAccountQuotes is installed by the market stream below. Startup
		// inventory recovery does not synthesize prices or adopt raw positions.
		if err := borrow.BootstrapCapital(ctx, c.Capital, time.Now().UnixMilli()); err != nil {
			return binding, err
		}
		if err := borrow.RecoverPersisted(ctx); err != nil {
			return binding, err
		}
		if live, ok := adapter.(*execution.BanexgAdapter); ok {
			if err := live.RecoverCash(ctx, borrow); err != nil {
				return binding, err
			}
		}
		if err := borrow.Reconcile("hub-startup", time.Now().UnixMilli()); err != nil {
			return binding, err
		}
		strategies := map[execution.StrategyID]bool{}
		for id := range c.Capital {
			strategies[id] = true
		}
		market, err = execution.StartBanexgMarketStream(ctx, account, exchange, c.Instruments, 5*time.Second, func(account *execution.SharedAccountBorrow) error {
			return account.ValidateAccountBindings(strategies)
		}, func(account *execution.SharedAccountBorrow) error {
			if request.Env == core.RunEnvProd {
				return account.StartReportsContext(ctx)
			}
			return nil
		})
		if err != nil {
			return binding, err
		}
		additional := hubAdditionalRegistration(account, c)
		if err := additional(ctx, request); err != nil {
			return binding, err
		}
		return execution.HubAccountBinding{Account: account, Shutdown: shutdown, RegisterAdditional: additional}, nil
	}
}

func hubInstrumentMatches(declared, expected execution.Instrument) bool {
	return declared.ID == expected.ID && declared.Valuation == expected.Valuation && declared.SettlementCurrency == expected.SettlementCurrency && declared.QuantityStep.Equal(expected.QuantityStep) && declared.PriceTick.Equal(expected.PriceTick) && declared.ContractSize.Equal(expected.ContractSize) && declared.MoneyScale == expected.MoneyScale && declared.MinSteps >= expected.MinSteps && !declared.MinNotional.LessThan(expected.MinNotional)
}
