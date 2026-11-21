package strat

import (
	"math"
	"reflect"
	"strings"
	"sync"

	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

// AccountMetrics holds the real-time equity, available balance/margin, and
// observed peak equity of one account in the runtime's pricing (fiat) currency.
// Valid reports whether an initialized wallet or execution ledger supplied the reading.
type AccountMetrics struct {
	Equity    float64
	Available float64
	Peak      float64
	Valid     bool
}

// FuncAccountMetrics reads the current account metrics for a job's account.
type FuncAccountMetrics = func(account string) AccountMetrics

var (
	legacyAccountMetricsMu sync.RWMutex
	legacyAccountMetrics   FuncAccountMetrics
)

// RegisterLegacyAccountMetrics installs the compatibility account reader used
// only by legacy jobs that are not bound to an explicit Runtime.
func RegisterLegacyAccountMetrics(reader FuncAccountMetrics) {
	legacyAccountMetricsMu.Lock()
	legacyAccountMetrics = reader
	legacyAccountMetricsMu.Unlock()
}

func getLegacyAccountMetrics() FuncAccountMetrics {
	legacyAccountMetricsMu.RLock()
	reader := legacyAccountMetrics
	legacyAccountMetricsMu.RUnlock()
	return reader
}

// ObserveLegacyAccountEquity updates the High-Water Mark on the package-level
// compatibility state.
func ObserveLegacyAccountEquity(account string, equity float64) float64 {
	return legacyState.ObserveAccountEquity(normalizeAccountPeakKey(account), equity)
}

// ResetLegacyAccountPeaks clears High-Water Marks stored on the package-level
// compatibility state when a legacy wallet is reinitialized.
func ResetLegacyAccountPeaks() {
	legacyState.accountMetricsMu.Lock()
	legacyState.accountPeakEquities = make(map[string]float64)
	legacyState.accountMetricsMu.Unlock()
}

// BindAccountMetrics installs the runtime-owned account metrics reader once
// during composition, mirroring BindOrderProcessor.
func (s *State) BindAccountMetrics(owner any, account string, reader FuncAccountMetrics) *errs.Error {
	value := reflect.ValueOf(owner)
	if !value.IsValid() || value.Kind() != reflect.Pointer || value.IsNil() || reader == nil {
		return errs.NewMsg(core.ErrBadConfig, "account metrics owner and callback are required")
	}
	s.runtimeBindMu.Lock()
	defer s.runtimeBindMu.Unlock()
	if s.accountMetrics == nil {
		s.accountMetrics = reader
		s.accountMetricsOwner = owner
		s.accountMetricsAccount = account
	} else if s.accountMetricsOwner != owner || s.accountMetricsAccount != account {
		return errs.NewMsg(core.ErrBadConfig, "strategy account metrics belongs to another runtime account registry")
	}
	return nil
}

// AccountMetricsBindingsMatch reports whether the state's account metrics
// reader is bound to the supplied owner and default account.
func (s *State) AccountMetricsBindingsMatch(owner any, account string) bool {
	if s == nil {
		return false
	}
	s.runtimeBindMu.Lock()
	defer s.runtimeBindMu.Unlock()
	return s.accountMetrics != nil && s.accountMetricsOwner == owner && s.accountMetricsAccount == account
}

// ObserveAccountEquity updates the High-Water Mark for account when equity is
// finite and positive, returning the resulting High-Water Mark.
func (s *State) ObserveAccountEquity(account string, equity float64) float64 {
	if s == nil {
		return 0
	}
	key := normalizeAccountPeakKey(account)
	s.accountMetricsMu.Lock()
	defer s.accountMetricsMu.Unlock()
	if s.accountPeakEquities == nil {
		s.accountPeakEquities = make(map[string]float64)
	}
	peak := s.accountPeakEquities[key]
	if isFinitePositive(equity) && equity > peak {
		peak = equity
		s.accountPeakEquities[key] = peak
	}
	return peak
}

// AccountPeakEquity returns the High-Water Mark recorded for account.
func (s *State) AccountPeakEquity(account string) float64 {
	if s == nil {
		return 0
	}
	key := normalizeAccountPeakKey(account)
	s.accountMetricsMu.Lock()
	peak := s.accountPeakEquities[key]
	s.accountMetricsMu.Unlock()
	return peak
}

func normalizeAccountPeakKey(account string) string {
	if account == "" {
		return "default"
	}
	return account
}

func isFinitePositive(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0) && v > 0
}

func (s *StratJob) isExplicitRuntimeJob() bool {
	if s == nil {
		return false
	}
	if s.strategyState != nil && s.strategyState != legacyState {
		return true
	}
	return s.runtimeCore != nil || s.runtimeClock != nil || s.runtimePrices != nil ||
		(s.Strat != nil && s.Strat.runtimeExplicit)
}

func (s *StratJob) effectiveAccountKey() string {
	if s == nil {
		return "default"
	}
	if s.Account != "" {
		return s.Account
	}
	if s.strategyState != nil && s.strategyState != legacyState {
		s.strategyState.runtimeBindMu.Lock()
		acc := s.strategyState.accountMetricsAccount
		if acc == "" {
			acc = s.strategyState.orderProcessorAccount
		}
		s.strategyState.runtimeBindMu.Unlock()
		if acc != "" {
			return acc
		}
		if def := runtimeDefaultAccountFor(s.strategyState); def != "" {
			return def
		}
	}
	if !s.isExplicitRuntimeJob() && config.DefAcc != "" {
		return config.DefAcc
	}
	return "default"
}

func (s *StratJob) readAccountMetrics() AccountMetrics {
	if s == nil {
		return AccountMetrics{}
	}
	acc := s.effectiveAccountKey()
	if s.strategyState != nil && s.strategyState != legacyState {
		state := s.strategyState
		state.runtimeBindMu.Lock()
		reader := state.accountMetrics
		state.runtimeBindMu.Unlock()
		if reader == nil {
			return AccountMetrics{}
		}
		return reader(acc)
	}
	if s.isExplicitRuntimeJob() {
		return AccountMetrics{}
	}
	reader := getLegacyAccountMetrics()
	if reader == nil {
		return AccountMetrics{}
	}
	return reader(acc)
}

func (s *StratJob) observePeakEquity(candidates ...float64) float64 {
	if s == nil {
		return 0
	}
	best := 0.0
	for _, v := range candidates {
		if isFinitePositive(v) && v > best {
			best = v
		}
	}
	acc := s.effectiveAccountKey()
	if s.strategyState != nil {
		return s.strategyState.ObserveAccountEquity(acc, best)
	}
	if !s.isExplicitRuntimeJob() {
		return legacyState.ObserveAccountEquity(acc, best)
	}
	execState := s.executionState()
	execState.mu.Lock()
	if best > execState.peakEquity {
		execState.peakEquity = best
	}
	peak := execState.peakEquity
	execState.mu.Unlock()
	return peak
}

func (s *StratJob) currentPeakEquity(hint float64) float64 {
	peak := 0.0
	if isFinitePositive(hint) {
		peak = hint
	}
	if s == nil {
		return peak
	}
	acc := s.effectiveAccountKey()
	var stored float64
	if s.strategyState != nil {
		stored = s.strategyState.AccountPeakEquity(acc)
	} else if !s.isExplicitRuntimeJob() {
		stored = legacyState.AccountPeakEquity(acc)
	} else {
		execState := s.executionState()
		execState.mu.Lock()
		stored = execState.peakEquity
		execState.mu.Unlock()
	}
	if stored > peak {
		peak = stored
	}
	return peak
}

func (s *StratJob) runtimeConfigAndAccounts() (*config.Config, map[string]*config.AccountConfig, *sync.RWMutex) {
	if s == nil {
		return nil, nil, nil
	}
	if s.Strat != nil {
		cfg := s.Strat.runtimeConfigFor(s)
		accounts, mu := s.Strat.runtimeAccountsFor(s)
		return cfg, accounts, mu
	}
	if s.strategyState != nil && s.strategyState != legacyState {
		return s.strategyState.Config, s.strategyState.runtimeAccounts, s.strategyState.runtimeAccountsMu
	}
	return nil, nil, nil
}

func (s *StratJob) coinFallbackPrice(coin string) float64 {
	if core.IsFiat(coin) && !strings.Contains(coin, "/") {
		return 1
	}
	if s != nil && s.runtimePrices != nil {
		if p := s.runtimePrices.GetPriceSafeExpAt(s.runtimeTimeMS(), coin, "", com.Day10MSecs); p > 0 {
			return p
		}
		if p := s.runtimePrices.GetLastBarPriceAt(coin); p > 0 {
			return p
		}
	}
	if s != nil && s.Symbol != nil && s.Env != nil {
		base, quote, _, _ := core.SplitSymbol(s.Symbol.Symbol)
		if base == coin && core.IsFiat(quote) {
			if c := s.Env.Close.Get(0); isFinitePositive(c) {
				return c
			}
		}
	}
	if s != nil && !s.isExplicitRuntimeJob() {
		if p := com.GetPriceSafeExp(coin, "", com.Day10MSecs); p > 0 {
			return p
		}
		if p := com.GetLastBarPrice(coin); p > 0 {
			return p
		}
	}
	return -1
}

func (s *StratJob) fallbackAccountEquity() float64 {
	if s == nil {
		return core.MinStakeAmount
	}
	cfg, accounts, accountsMu := s.runtimeConfigAndAccounts()
	var walletAmounts map[string]float64
	if cfg != nil {
		walletAmounts = cfg.WalletAmounts
	} else if !s.isExplicitRuntimeJob() {
		walletAmounts = config.WalletAmounts
	}
	var configTotal float64
	for coin, amt := range walletAmounts {
		if !isFinitePositive(amt) {
			continue
		}
		if px := s.coinFallbackPrice(coin); isFinitePositive(px) {
			configTotal += amt * px
		}
	}
	if isFinitePositive(configTotal) {
		return configTotal
	}
	var stake float64
	if s.Strat != nil {
		stake = s.Strat.GetStakeAmount(s)
	} else if cfg != nil {
		stake = runtimeStakeAmount(cfg, accounts, accountsMu, s.effectiveAccountKey())
	} else if !s.isExplicitRuntimeJob() {
		stake = config.GetStakeAmount(s.effectiveAccountKey())
	}
	if isFinitePositive(stake) {
		return stake
	}
	return core.MinStakeAmount
}

// AccountEquity returns the real-time total equity (in pricing fiat currency,
// cash balance + unrealized PnL) of the account owning s. During warmup or
// before a wallet/ledger is initialized, it falls back to configured initial
// capital or GetStakeAmount(s) (and at minimum core.MinStakeAmount), never
// panicking or returning 0/NaN/Inf.
func (s *StratJob) AccountEquity() float64 {
	if s == nil {
		return core.MinStakeAmount
	}
	if s.IsWarmUpState() {
		return s.fallbackAccountEquity()
	}
	metrics := s.readAccountMetrics()
	if !metrics.Valid || math.IsNaN(metrics.Equity) || math.IsInf(metrics.Equity, 0) {
		return s.fallbackAccountEquity()
	}
	if metrics.Equity > 0 {
		s.observePeakEquity(metrics.Peak, metrics.Equity)
		return metrics.Equity
	}
	if s.currentPeakEquity(metrics.Peak) > 0 {
		// Initialized account whose equity depleted to <= 0: return a tiny positive
		// dust value so callers dividing by AccountEquity() never hit 0/NaN while
		// risk sizing remains below MinStakeAmount.
		return core.AmtDust
	}
	return s.fallbackAccountEquity()
}

// AvailableBalance returns the current available balance / available margin of
// the account owning s in the pricing fiat currency.
func (s *StratJob) AvailableBalance() float64 {
	if s == nil {
		return 0
	}
	if s.IsWarmUpState() {
		return s.fallbackAccountEquity()
	}
	metrics := s.readAccountMetrics()
	if !metrics.Valid {
		if s.currentPeakEquity(0) == 0 {
			return s.fallbackAccountEquity()
		}
		return 0
	}
	if metrics.Equity <= 0 && metrics.Available <= 0 && s.currentPeakEquity(metrics.Peak) == 0 {
		return s.fallbackAccountEquity()
	}
	if math.IsNaN(metrics.Available) || math.IsInf(metrics.Available, 0) || metrics.Available <= 0 {
		return 0
	}
	return metrics.Available
}

// AccountDrawdown returns the current drawdown ratio in [0, 1] relative to the
// account's High-Water Mark since run start (0 when at peak or during warmup).
func (s *StratJob) AccountDrawdown() float64 {
	if s == nil || s.IsWarmUpState() {
		return 0
	}
	metrics := s.readAccountMetrics()
	if !metrics.Valid {
		return 0
	}
	peak := s.observePeakEquity(metrics.Peak, metrics.Equity)
	if !isFinitePositive(peak) {
		return 0
	}
	cur := metrics.Equity
	if math.IsNaN(cur) || math.IsInf(cur, -1) || cur < 0 {
		cur = 0
	} else if math.IsInf(cur, 1) {
		return 0
	}
	dd := (peak - cur) / peak
	if dd <= 0 || math.IsNaN(dd) {
		return 0
	}
	if dd > 1 {
		return 1
	}
	return dd
}

func (s *StratJob) effectiveMaxLeverage(req *EnterReq) float64 {
	if s == nil || s.runtimeMarket() == banexg.MarketSpot {
		return 1
	}
	if req != nil && isFinitePositive(req.Leverage) {
		return req.Leverage
	}
	cfg, accounts, accountsMu := s.runtimeConfigAndAccounts()
	accName := s.effectiveAccountKey()
	if cfg != nil {
		if accountsMu != nil {
			accountsMu.RLock()
			defer accountsMu.RUnlock()
		}
		if accounts == nil {
			accounts = cfg.Accounts
		}
		if acc := accounts[accName]; acc != nil && isFinitePositive(acc.Leverage) {
			return acc.Leverage
		}
		if isFinitePositive(cfg.Leverage) {
			return cfg.Leverage
		}
		return 1
	}
	if !s.isExplicitRuntimeJob() {
		if lvg := config.GetAccLeverage(accName); isFinitePositive(lvg) {
			return lvg
		}
	}
	return 1
}

func (s *StratJob) maxStakeAmount() float64 {
	if s == nil {
		return 0
	}
	cfg, accounts, accountsMu := s.runtimeConfigAndAccounts()
	accName := s.effectiveAccountKey()
	if cfg != nil {
		if accountsMu != nil {
			accountsMu.RLock()
			defer accountsMu.RUnlock()
		}
		if accounts == nil {
			accounts = cfg.Accounts
		}
		if acc, ok := accounts[accName]; ok && acc != nil && acc.MaxStakeAmt > 0 {
			return acc.MaxStakeAmt
		}
		if cfg.MaxStakeAmt > 0 {
			return cfg.MaxStakeAmt
		}
		return 0
	}
	if !s.isExplicitRuntimeJob() {
		if acc, ok := config.Accounts[accName]; ok && acc != nil && acc.MaxStakeAmt > 0 {
			return acc.MaxStakeAmt
		}
		if config.MaxStakeAmt > 0 {
			return config.MaxStakeAmt
		}
	}
	return 0
}
