package biz

import (
	"cmp"
	"context"
	"maps"
	"math"
	"slices"
	"strings"

	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
	"github.com/shopspring/decimal"
)

func init() {
	strat.RegisterLegacyAccountMetrics(readLegacyAccountMetrics)
}

func resetLegacyWalletPeaks() {
	strat.ResetLegacyAccountPeaks()
}

func bindRuntimeAccountMetrics(deps RuntimeDeps) *errs.Error {
	if deps.Trading == nil || deps.DefaultAccount == "" {
		return nil
	}
	return deps.Strategies.BindAccountMetrics(deps.Trading, deps.DefaultAccount, func(account string) strat.AccountMetrics {
		return readRuntimeAccountMetrics(deps, account)
	})
}

func readRuntimeAccountMetrics(deps RuntimeDeps, account string) strat.AccountMetrics {
	if deps.SharedExecution != nil {
		return readSharedAccountMetrics(deps)
	}
	if deps.HubOrderBridge != nil {
		return readHubAccountMetrics(deps)
	}
	if reader := lookupHubAccountReader(deps); reader != nil {
		return reader.read(deps)
	}
	if deps.Trading == nil {
		return strat.AccountMetrics{}
	}
	acc := account
	if (deps.Core == nil || !deps.Core.EnvReal) && deps.DefaultAccount != "" {
		acc = deps.DefaultAccount
	} else if acc == "" {
		acc = deps.DefaultAccount
	}
	wallet := deps.Trading.LookupWallet(acc)
	if wallet == nil || len(wallet.Items) == 0 {
		return strat.AccountMetrics{}
	}
	var open []*ormo.InOutOrder
	ordersSynced := deps.Core == nil || !deps.Core.EnvReal
	if deps.Orders != nil {
		if deps.Orders.GetSyncStamp(acc) > 0 {
			ordersSynced = true
		}
		openOds, lock := deps.Orders.GetOpenODs(acc)
		lock.Lock()
		open = snapshotOpenOrdersSlice(openOds)
		lock.Unlock()
	}
	return wallet.AccountMetrics(open, ordersSynced)
}

func readLegacyAccountMetrics(account string) strat.AccountMetrics {
	acc := account
	if !core.EnvReal {
		acc = config.DefAcc
	}
	if acc == "" {
		acc = "default"
	}
	wallet, ok := accWallets[acc]
	if !ok || wallet == nil || len(wallet.Items) == 0 {
		return strat.AccountMetrics{}
	}
	openOds, lock := ormo.GetOpenODs(acc)
	lock.Lock()
	open := snapshotOpenOrdersSlice(openOds)
	lock.Unlock()
	ordersSynced := !core.EnvReal || len(open) > 0
	return wallet.AccountMetrics(open, ordersSynced)
}

func snapshotOpenOrdersSlice(openOds map[int64]*ormo.InOutOrder) []*ormo.InOutOrder {
	if len(openOds) == 0 {
		return nil
	}
	res := make([]*ormo.InOutOrder, 0, len(openOds))
	for _, od := range openOds {
		if od != nil && od.Status < ormo.InOutStatusFullExit {
			res = append(res, od)
		}
	}
	return res
}

func sumOpenContractProfits(orders []*ormo.InOutOrder) float64 {
	if len(orders) == 0 {
		return 0
	}
	live := make([]*ormo.InOutOrder, 0, len(orders))
	for _, od := range orders {
		if od != nil && od.Status < ormo.InOutStatusFullExit {
			live = append(live, od)
		}
	}
	slices.SortFunc(live, func(a, b *ormo.InOutOrder) int { return cmp.Compare(a.ID, b.ID) })
	var sum float64
	for _, od := range live {
		if !math.IsNaN(od.Profit) && !math.IsInf(od.Profit, 0) {
			sum += od.Profit
		}
	}
	return sum
}

func (w *BanWallets) realtimeCoinPrice(coin string) float64 {
	if core.IsFiat(coin) && !strings.Contains(coin, "/") {
		return 1
	}
	if p := w.markPrice(coin); p > 0 {
		return p
	}
	if w != nil && w.runtimeBound {
		if w.runtimePrices != nil {
			if p := lastBarPriceOfCoin(w.runtimePrices.GetLastBarPriceAt, coin); p > 0 {
				return p
			}
			if w.runtimeClock != nil {
				if p := w.runtimePrices.GetPriceSafeExpAt(w.runtimeClock.TimeMS(), coin, "", com.Day10MSecs); p > 0 {
					return p
				}
			}
		}
		return -1
	}
	if p := lastBarPriceOfCoin(com.GetLastBarPrice, coin); p > 0 {
		return p
	}
	if p := com.GetPriceSafeExp(coin, "", com.Day10MSecs); p > 0 {
		return p
	}
	return -1
}

func (w *BanWallets) observeWalletMetrics(open []*ormo.InOutOrder, ordersSynced bool) strat.AccountMetrics {
	return w.AccountMetrics(open, ordersSynced)
}

// AccountMetrics computes the wallet's real-time equity, available balance, and
// High-Water Mark without relying on stale cached UnrealizedPOL in simulated modes.
func (w *BanWallets) AccountMetrics(open []*ormo.InOutOrder, ordersSynced bool) strat.AccountMetrics {
	if w == nil || len(w.Items) == 0 {
		return strat.AccountMetrics{}
	}
	isContract := w.contractMarket()
	isReal := w.envReal()

	var openSpotHeld map[string]float64
	if !isContract && !isReal && len(open) > 0 {
		openSpotHeld = make(map[string]float64)
		for _, od := range open {
			if od == nil || od.Short || od.Status < ormo.InOutStatusPartEnter || od.Status >= ormo.InOutStatusFullExit {
				continue
			}
			// Once ExitOd has locked the base coin into Pendings, od.Exit is non-nil
			// and baseWallet.Available has already been debited.
			if od.Exit != nil && od.Exit.Amount > 0 {
				continue
			}
			parts, err := w.settlementSymbolParts(od.Symbol)
			if err != nil || parts[0] == "" {
				continue
			}
			if held := od.HoldAmount(); held > 0 {
				openSpotHeld[parts[0]] += held
			}
		}
	}

	keys := slices.Sorted(maps.Keys(w.Items))
	var availableLegal, settledTotal, cachedUPol float64
	var pricedCoins int
	var hasNonZero bool

	for _, key := range keys {
		item := w.Items[key]
		if item == nil {
			continue
		}
		used := item.Used()
		item.lock.Lock()
		ava := item.Available
		withdraw := item.Withdraw
		upol := item.UnrealizedPOL
		item.lock.Unlock()
		if ava != 0 || withdraw != 0 || used != 0 || upol != 0 {
			hasNonZero = true
		}
		price := w.realtimeCoinPrice(key)
		if price <= 0 {
			continue
		}
		pricedCoins++
		freeCoin := ava
		if held := openSpotHeld[key]; held > 0 && freeCoin > 0 {
			freeCoin -= min(freeCoin, held)
		}
		availableLegal += freeCoin * price
		settledTotal += (ava + withdraw + used) * price
		cachedUPol += upol * price
	}

	if pricedCoins == 0 {
		return strat.AccountMetrics{}
	}

	equity := settledTotal
	if isReal {
		if cachedUPol != 0 {
			equity += cachedUPol
		} else if isContract && ordersSynced {
			equity += sumOpenContractProfits(open)
		}
	} else if isContract {
		equity += sumOpenContractProfits(open)
	}

	w.peakMu.Lock()
	if !math.IsNaN(equity) && !math.IsInf(equity, 0) && equity > w.peakEquity {
		w.peakEquity = equity
	}
	peak := w.peakEquity
	w.peakMu.Unlock()

	if !hasNonZero && peak == 0 && len(open) == 0 {
		return strat.AccountMetrics{}
	}
	if availableLegal < 0 || math.IsNaN(availableLegal) || math.IsInf(availableLegal, 0) {
		availableLegal = 0
	}
	return strat.AccountMetrics{
		Equity:    equity,
		Available: availableLegal,
		Peak:      peak,
		Valid:     true,
	}
}

func readSharedAccountMetrics(deps RuntimeDeps) strat.AccountMetrics {
	if deps.SharedExecution == nil {
		return strat.AccountMetrics{}
	}
	var snapshot execution.AccountSnapshot
	marks := make(map[string]decimal.Decimal)
	var nowMS int64
	if deps.Clock != nil {
		nowMS = deps.Clock.TimeMS()
	}
	err := deps.SharedExecution.WithState(func(sa *SharedAccount) error {
		var snapErr error
		snapshot, snapErr = sa.Store().Snapshot(context.Background())
		if snapErr != nil {
			return snapErr
		}
		for _, pos := range snapshot.ActualPositions {
			if pos.SignedSteps == 0 {
				continue
			}
			if _, exists := marks[pos.Instrument.ID]; exists {
				continue
			}
			if q, qErr := sa.ReadQuote(deps.SharedExecution.Context(), pos.Instrument.ID, nowMS); qErr == nil &&
				q.Bid.IsPositive() && q.Ask.GreaterThanOrEqual(q.Bid) {
				marks[pos.Instrument.ID] = q.Bid.Add(q.Ask).Div(decimal.NewFromInt(2))
			}
		}
		return nil
	})
	if err != nil {
		return strat.AccountMetrics{}
	}
	var bridge *SharedOrderBridgeConfig
	if deps.SharedOrderBridge != nil {
		bridge = deps.SharedOrderBridge
	} else if deps.HubOrderBridge != nil && deps.HubOrderBridge.LocalAccount {
		// HubOrderMgr on an embedded account: same ledger, read once here
		// (never through the port as well), with the bridge's instruments/marks.
		bridge = &deps.HubOrderBridge.Bridge
	}
	return snapshotToAccountMetrics(deps, snapshot, marks, bridge)
}

func readHubAccountMetrics(deps RuntimeDeps) strat.AccountMetrics {
	reader := hubTSAccountReader(deps)
	if reader == nil {
		return strat.AccountMetrics{}
	}
	return reader.read(deps)
}

func snapshotToAccountMetrics(deps RuntimeDeps, snapshot execution.AccountSnapshot, marks map[string]decimal.Decimal, bridge *SharedOrderBridgeConfig) strat.AccountMetrics {
	hasActivity := !snapshot.AccountSettledCash.IsZero() ||
		len(snapshot.ActualPositions) > 0 ||
		len(snapshot.Lots) > 0 ||
		snapshot.Checkpoint > 0
	if !hasActivity {
		return strat.AccountMetrics{}
	}
	instrumentToSymbol := make(map[string]string)
	if bridge != nil {
		for sym, inst := range bridge.Instruments {
			instrumentToSymbol[inst.ID] = sym
		}
	}
	fillInstrumentMark := func(inst execution.Instrument, costBasis decimal.Decimal, steps int64) decimal.Decimal {
		if m, ok := marks[inst.ID]; ok && inst.ValidMark(m) {
			return m
		}
		if bridge != nil && bridge.Risk.Marks != nil {
			if m, ok := bridge.Risk.Marks[inst.ID]; ok && inst.ValidMark(m) {
				marks[inst.ID] = m
				return m
			}
		}
		if deps.Market != nil && deps.Market.Prices != nil {
			nowMS := int64(0)
			if deps.Clock != nil {
				nowMS = deps.Clock.TimeMS()
			}
			for _, candidate := range []string{instrumentToSymbol[inst.ID], inst.ID} {
				if candidate == "" {
					continue
				}
				px := deps.Market.Prices.GetPriceSafeExpAt(nowMS, candidate, "", com.Day10MSecs)
				if px <= 0 {
					px = deps.Market.Prices.GetLastBarPriceAt(candidate)
				}
				if px > 0 {
					m := decimal.NewFromFloat(px)
					if inst.ValidMark(m) {
						marks[inst.ID] = m
						return m
					}
				}
			}
		}
		absN := steps
		if absN < 0 {
			absN = -absN
		}
		if absN > 0 && !inst.IsOption() && costBasis.IsPositive() {
			denom := decimal.NewFromInt(absN).Mul(inst.QuantityStep).Mul(inst.ContractSize)
			if denom.IsPositive() {
				m := costBasis.Div(denom)
				if inst.ValidMark(m) {
					marks[inst.ID] = m
					return m
				}
			}
		}
		return decimal.Zero
	}

	for _, pos := range snapshot.ActualPositions {
		if pos.SignedSteps == 0 {
			continue
		}
		fillInstrumentMark(pos.Instrument, pos.CostBasis, pos.SignedSteps)
	}
	equityDec, err := snapshot.Equity(marks)
	if err != nil {
		return strat.AccountMetrics{}
	}

	marginRate := decimal.NewFromInt(1)
	if bridge != nil && bridge.Risk.MarginRate.IsPositive() {
		marginRate = bridge.Risk.MarginRate
	} else {
		defaults := makeRuntimeOrderConfig(deps, deps.DefaultAccount)
		gate := OrderMgr{runtimeDeps: true, runtimeCfg: defaults}
		if lvg := gate.accountLeverage(); lvg > 0 {
			marginRate = decimal.NewFromInt(1).Div(decimal.NewFromFloat(lvg))
		}
	}

	netSteps := make(map[string]int64)
	instruments := make(map[string]execution.Instrument)
	for _, pos := range snapshot.ActualPositions {
		if pos.SignedSteps != 0 {
			netSteps[pos.Instrument.ID] = pos.SignedSteps
			instruments[pos.Instrument.ID] = pos.Instrument
		}
	}
	for _, order := range snapshot.Orders {
		if order.State != execution.OrderAcknowledged && order.State != execution.OrderPartial {
			continue
		}
		if order.Intent.Conditional != nil {
			continue
		}
		rem := order.Intent.Steps - order.FilledSteps
		if rem <= 0 {
			continue
		}
		if order.Intent.Side == execution.Sell {
			rem = -rem
		}
		id := order.Intent.Instrument.ID
		netSteps[id] += rem
		if _, ok := instruments[id]; !ok {
			instruments[id] = order.Intent.Instrument
		}
	}

	usedCollateral := decimal.Zero
	for id, steps := range netSteps {
		if steps == 0 {
			continue
		}
		inst := instruments[id]
		mark := fillInstrumentMark(inst, decimal.Zero, steps)
		if mark.IsPositive() {
			usedCollateral = usedCollateral.Add(inst.Collateral(steps, mark, marginRate))
		}
	}
	availDec := equityDec.Sub(usedCollateral)
	if availDec.IsNegative() {
		availDec = decimal.Zero
	}
	return strat.AccountMetrics{
		Equity:    equityDec.InexactFloat64(),
		Available: availDec.InexactFloat64(),
		Valid:     true,
	}
}

func (t *Trader) observeAccountsEquity(odMatch bool, accOrders map[string][]*ormo.InOutOrder) {
	accounts := executionAccountConfigs(t.runtime)
	for account := range executionAccountNames(t.runtime) {
		cfg := accounts[account]
		if cfg == nil || cfg.NoTrade {
			continue
		}
		if t.runtime != nil {
			if t.runtime.SharedExecution != nil || t.runtime.HubOrderBridge != nil || lookupHubAccountReader(*t.runtime) != nil {
				if state := t.strategyState(); state != nil {
					accName := t.accountName(account)
					if m := readRuntimeAccountMetrics(*t.runtime, accName); m.Valid && m.Equity > 0 {
						state.ObserveAccountEquity(accName, m.Equity)
					}
				}
				continue
			}
			if t.runtime.Trading == nil {
				continue
			}
			accName := t.accountName(account)
			wallet := t.runtime.Trading.LookupWallet(accName)
			if wallet == nil || len(wallet.Items) == 0 {
				continue
			}
			var open []*ormo.InOutOrder
			if odMatch {
				open = accOrders[account]
			} else {
				openOds, lock := t.openOrders(account)
				lock.Lock()
				open = snapshotOpenOrdersSlice(openOds)
				lock.Unlock()
			}
			ordersSynced := t.runtime.Core == nil || !t.runtime.Core.EnvReal ||
				(t.runtime.Orders != nil && t.runtime.Orders.GetSyncStamp(accName) > 0)
			m := wallet.AccountMetrics(open, ordersSynced)
			if m.Valid && m.Equity > 0 {
				if state := t.strategyState(); state != nil {
					state.ObserveAccountEquity(accName, m.Equity)
				}
			}
			continue
		}

		accName := account
		if !core.EnvReal {
			accName = config.DefAcc
		}
		wallet, ok := accWallets[accName]
		if !ok || wallet == nil || len(wallet.Items) == 0 {
			continue
		}
		var open []*ormo.InOutOrder
		if odMatch {
			open = accOrders[account]
		} else {
			openOds, lock := t.openOrders(account)
			lock.Lock()
			open = snapshotOpenOrdersSlice(openOds)
			lock.Unlock()
		}
		ordersSynced := !core.EnvReal || len(open) > 0
		m := wallet.AccountMetrics(open, ordersSynced)
		if m.Valid && m.Equity > 0 {
			strat.ObserveLegacyAccountEquity(accName, m.Equity)
		}
	}
}

// ExecutionAccountBalance reports the balance of a runtime whose orders are
// owned by an execution account. backed is false for the wallet-based paths.
// remote is true when the account lives behind a Hub: such a robot holds no
// account credentials, so private exchange queries are not available to it.
// The balance is the ledger's settled cash plus marked positions (Equity) and
// what remains after margin (Available), in the account settlement currency.
func ExecutionAccountBalance(deps RuntimeDeps, account string) (currency string, metrics strat.AccountMetrics, backed, remote bool) {
	switch {
	case deps.SharedExecution != nil:
		currency = deps.SharedExecution.AccountKey().SettlementDomain
	case deps.HubOrderBridge != nil:
		currency, remote = deps.HubOrderBridge.Account.SettlementDomain, true
	case lookupHubAccountReader(deps) != nil:
		// Factor-only Hub robot: read-only ledger binding, cached short-term.
		currency, remote = lookupHubAccountReader(deps).Currency(), true
	default:
		return "", strat.AccountMetrics{}, false, false
	}
	return currency, readRuntimeAccountMetrics(deps, account), true, remote
}
