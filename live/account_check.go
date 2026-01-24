package live

import (
	"fmt"
	"sort"
	"strings"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/rpc"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

type accCheckResult struct {
	tradeAllowed    bool
	tradeKnown      bool
	withdrawAllowed bool
	withdrawKnown   bool
	ipAny           bool
	ipKnown         bool
	posMode         string
	acctMode        string
	acctLv          string
	marginMode      string
	balanceSummary  string
	errors          []string
	warns           []string
}

const marginModeMixed = "mixed"

func CheckLiveAccounts() {
	if !core.EnvReal {
		return
	}
	for account, cfg := range config.Accounts {
		if cfg.NoTrade {
			continue
		}
		res := &accCheckResult{balanceSummary: balanceUnknownSummary()}
		balances := fetchAccountBalance(account, res)
		if balances != nil {
			balances.Init()
			res.balanceSummary, res.errors = buildBalanceSummary(balances, config.WalletAmounts)
		}
		access, err := exg.Default.FetchAccountAccess(map[string]interface{}{
			banexg.ParamAccount: account,
			banexg.ParamBalance: balances,
		})
		if err != nil {
			res.errors = append(res.errors, fmt.Sprintf(accMsg("account_access_fetch_failed", "Account access check failed: %v"), err))
		}
		applyAccountAccess(res, access)
		if res.marginMode == "" && (core.IsContract || core.Market == banexg.MarketMargin) {
			res.marginMode = fetchMarginModeFromPositions(account)
		}
		if res.tradeKnown && !res.tradeAllowed {
			res.errors = append(res.errors, accMsg("no_trade_permission", "No trading permission"))
		}
		if res.withdrawKnown && res.withdrawAllowed {
			res.warns = append(res.warns, accMsg("withdraw_permission_enabled", "Withdrawals enabled"))
		}
		if res.ipKnown && res.ipAny {
			res.warns = append(res.warns, accMsg("ip_any_warning", "IP restriction: any"))
		}
		summary := buildAccSummary(account, res)
		log.Info("live account check", zap.String("acc", account), zap.String("summary", summary))
		if len(res.warns) > 0 {
			warnMsg := "WARN: " + strings.Join(res.warns, "; ")
			log.Warn("live account check", zap.String("acc", account), zap.String("warn", warnMsg))
		}
		if len(res.errors) > 0 {
			errMsg := "ERROR: " + strings.Join(res.errors, "; ")
			log.Error("account disabled", zap.String("acc", account), zap.String("error", errMsg))
			rpc.SendMsg(map[string]interface{}{
				"type":    rpc.MsgTypeException,
				"account": account,
				"status":  errMsg,
			})
			cfg.NoTrade = true
		}
	}
}

func fetchAccountBalance(account string, res *accCheckResult) *banexg.Balances {
	balances, err := exg.Default.FetchBalance(map[string]interface{}{
		banexg.ParamAccount: account,
	})
	if err != nil {
		res.errors = append(res.errors, fmt.Sprintf(accMsg("balance_fetch_failed", "Balance fetch failed: %v"), err))
		return nil
	}
	return balances
}

func applyAccountAccess(res *accCheckResult, access *banexg.AccountAccess) {
	if res == nil || access == nil {
		return
	}
	if access.TradeKnown {
		res.tradeKnown = true
		res.tradeAllowed = access.TradeAllowed
	}
	if access.WithdrawKnown {
		res.withdrawKnown = true
		res.withdrawAllowed = access.WithdrawAllowed
	}
	if access.IPKnown {
		res.ipKnown = true
		res.ipAny = access.IPAny
	}
	if access.PosMode != "" {
		res.posMode = access.PosMode
	}
	if access.AcctMode != "" {
		res.acctMode = access.AcctMode
	}
	if access.AcctLv != "" {
		res.acctLv = access.AcctLv
	}
	if access.MarginMode != "" {
		res.marginMode = access.MarginMode
	}
}

func buildBalanceSummary(bal *banexg.Balances, expected map[string]float64) (string, []string) {
	balanceLabel := accMsg("balance_label", "Balance")
	unknown := accMsg("unknown", "unknown")
	if bal == nil {
		return fmt.Sprintf("%s: %s", balanceLabel, unknown), nil
	}
	errs := make([]string, 0)
	if len(expected) == 0 {
		return fmt.Sprintf("%s: %s", balanceLabel, topBalanceSummary(bal, 3)), errs
	}
	coins := make([]string, 0, len(expected))
	for coin := range expected {
		coins = append(coins, coin)
	}
	sort.Strings(coins)
	parts := make([]string, 0, len(coins))
	for _, coin := range coins {
		exp := expected[coin]
		total := balanceTotal(bal, coin)
		if exp > 0 && total < exp/3 {
			errs = append(errs, fmt.Sprintf(
				accMsg("balance_below_wallet_amounts_third", "Balance below 1/3 of wallet_amounts: %s %.8g < %.8g"),
				coin, total, exp/3,
			))
		}
		if exp > 0 {
			parts = append(parts, fmt.Sprintf("%s %.8g/%.8g", coin, total, exp))
		} else {
			parts = append(parts, fmt.Sprintf("%s %.8g", coin, total))
		}
	}
	return fmt.Sprintf("%s: %s", balanceLabel, strings.Join(parts, ", ")), errs
}

func topBalanceSummary(bal *banexg.Balances, max int) string {
	if bal == nil || len(bal.Assets) == 0 {
		return accMsg("unknown", "unknown")
	}
	type pair struct {
		coin  string
		total float64
	}
	arr := make([]pair, 0, len(bal.Assets))
	for coin, asset := range bal.Assets {
		total := asset.Total
		if total == 0 {
			total = asset.Free + asset.Used
		}
		arr = append(arr, pair{coin: coin, total: total})
	}
	sort.Slice(arr, func(i, j int) bool { return arr[i].total > arr[j].total })
	if len(arr) > max {
		arr = arr[:max]
	}
	parts := make([]string, 0, len(arr))
	for _, item := range arr {
		parts = append(parts, fmt.Sprintf("%s %.8g", item.coin, item.total))
	}
	return strings.Join(parts, ", ")
}

func balanceTotal(bal *banexg.Balances, coin string) float64 {
	if bal == nil {
		return 0
	}
	if bal.Total != nil {
		if val, ok := bal.Total[coin]; ok {
			return val
		}
	}
	if bal.Assets != nil {
		if asset, ok := bal.Assets[coin]; ok {
			if asset.Total != 0 {
				return asset.Total
			}
			return asset.Free + asset.Used
		}
	}
	return 0
}

func buildAccSummary(account string, res *accCheckResult) string {
	tradeYes := accMsg("yes", "Yes")
	tradeNo := accMsg("no", "No")
	ipRestricted := accMsg("ip_restricted", "Restricted")
	ipAny := accMsg("ip_any", "Any")
	tradeStr := boolSummary(res.tradeAllowed, res.tradeKnown, tradeYes, tradeNo)
	withdrawStr := boolSummary(res.withdrawAllowed, res.withdrawKnown, tradeYes, tradeNo)
	ipStr := boolSummary(!res.ipAny, res.ipKnown, ipRestricted, ipAny)
	posMode := displayPosMode(res.posMode)
	acctStr := displayAccountMode(res.acctMode)
	if res.acctLv != "" {
		acctStr = fmt.Sprintf("%s(%s)", acctStr, res.acctLv)
	}
	marginMode := displayMarginMode(res.marginMode)
	return fmt.Sprintf("%s %s:%s %s:%s %s:%s %s:%s %s:%s/%s:%s %s",
		account,
		accMsg("trade_label", "Trade"), tradeStr,
		accMsg("withdraw_label", "Withdraw"), withdrawStr,
		accMsg("ip_label", "IP"), ipStr,
		accMsg("position_mode_label", "Position"), posMode,
		accMsg("account_mode_label", "Account"), acctStr,
		accMsg("margin_mode_label", "Margin"), marginMode,
		res.balanceSummary,
	)
}

func boolSummary(val bool, known bool, yes string, no string) string {
	if !known {
		return accMsg("unknown", "unknown")
	}
	if val {
		return yes
	}
	return no
}

func fetchMarginModeFromPositions(account string) string {
	posList, err := exg.Default.FetchAccountPositions(nil, map[string]interface{}{
		banexg.ParamAccount:     account,
		banexg.ParamSettleCoins: config.StakeCurrency,
	})
	if err != nil {
		return ""
	}
	hasCross := false
	hasIsolated := false
	for _, pos := range posList {
		if pos == nil {
			continue
		}
		if pos.MarginMode == banexg.MarginIsolated || pos.Isolated {
			hasIsolated = true
		} else if pos.MarginMode == banexg.MarginCross || !pos.Isolated {
			hasCross = true
		}
	}
	if hasCross && hasIsolated {
		return marginModeMixed
	}
	if hasIsolated {
		return banexg.MarginIsolated
	}
	if hasCross {
		return banexg.MarginCross
	}
	return ""
}

func accMsg(code, defVal string) string {
	return config.GetLangMsg(code, defVal)
}

func balanceUnknownSummary() string {
	return fmt.Sprintf("%s: %s", accMsg("balance_label", "Balance"), accMsg("unknown", "unknown"))
}

func displayPosMode(mode string) string {
	switch mode {
	case banexg.PosModeHedge:
		return accMsg("pos_mode_hedge", "Hedge")
	case banexg.PosModeOneWay:
		return accMsg("pos_mode_oneway", "One-way")
	case "":
		return accMsg("unknown", "unknown")
	default:
		return mode
	}
}

func displayAccountMode(mode string) string {
	switch mode {
	case banexg.AcctModeSpot:
		return accMsg("acct_mode_spot", "Spot")
	case banexg.AcctModeSingleCurrencyMargin:
		return accMsg("acct_mode_single_currency_margin", "Single-currency margin")
	case banexg.AcctModeMultiCurrencyMargin:
		return accMsg("acct_mode_multi_currency_margin", "Multi-currency margin")
	case banexg.AcctModePortfolioMargin:
		return accMsg("acct_mode_portfolio_margin", "Portfolio margin")
	case "":
		return accMsg("unknown", "unknown")
	default:
		return mode
	}
}

func displayMarginMode(mode string) string {
	switch mode {
	case banexg.MarginCross:
		return accMsg("margin_mode_cross", "Cross")
	case banexg.MarginIsolated:
		return accMsg("margin_mode_isolated", "Isolated")
	case marginModeMixed:
		return accMsg("margin_mode_mixed", "Mixed")
	case "":
		return accMsg("unknown", "unknown")
	default:
		return mode
	}
}
