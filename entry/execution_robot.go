package entry

import (
	"errors"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/live"
	"github.com/banbox/banbot/orm"
	runtimepkg "github.com/banbox/banbot/runtime"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/log"
	"github.com/banbox/banexg/utils"
	"go.uber.org/zap"
)

// hourOnlySymbolsFromSubs returns the symbols whose finest kline subscription
// is 1h or coarser. The provider only watches sub-hour klines, so these
// symbols get their live bars from the hourly repair job. The set is computed
// on every call from the (immutable after startup) subscription list.
func hourOnlySymbolsFromSubs(subs []*strat.DataSub) func() map[int32]*orm.ExSymbol {
	return func() map[int32]*orm.ExSymbol {
		finest := make(map[int32]int)
		symbols := make(map[int32]*orm.ExSymbol)
		for _, sub := range subs {
			if sub == nil || sub.ExSymbol == nil || sub.TimeFrame == "" {
				continue
			}
			if sub.Source != "" && sub.Source != orm.SeriesSourceKline {
				continue
			}
			secs := utils.TFToSecs(sub.TimeFrame)
			if secs <= 0 {
				continue
			}
			id := sub.ExSymbol.ID
			if cur, ok := finest[id]; !ok || secs < cur {
				finest[id] = secs
			}
			symbols[id] = sub.ExSymbol
		}
		res := make(map[int32]*orm.ExSymbol)
		for id, secs := range finest {
			if secs >= 3600 {
				res[id] = symbols[id]
			}
		}
		return res
	}
}

// startExecutionRobotCrons starts the maintenance jobs of one execution-backed
// runtime. They are auxiliary: a failure is logged and never stops trading.
// subs are the runtime's legacy kline subscriptions (may be empty).
func startExecutionRobotCrons(rt *runtimepkg.Runtime, trader *live.CryptoTrader, provider *data.LiveProvider, subs []*strat.DataSub) {
	// The delay monitor must see the rows this runtime really feeds: the
	// provider's own market state is isolated from the trader's. One observer
	// serves both the dashboard chart and the monitor's progress state.
	received := com.NewPairCopiedState()
	received.Touch(rt.Clock.TimeMS())
	rt.SetSeriesObserver(func(msg *data.SeriesMsg) {
		trader.PublishSeries(msg)
		if msg == nil || len(msg.Rows) == 0 || msg.TFSecs <= 0 {
			return
		}
		if last := msg.Rows[len(msg.Rows)-1]; last != nil {
			received.SetPairMsAt(rt.Clock.TimeMS(), msg.Pair, last.EndMS, int64(msg.TFSecs)*1000)
		}
	})
	// StartExecutionRobotCrons returns *errs.Error: keep it in its own variable.
	cronErr := live.StartExecutionRobotCrons(trader, provider, live.ExecutionRobotCronOptions{HourOnlySymbols: mergedHourOnlySymbols(rt, subs), Received: received})
	if cronErr != nil {
		log.Warn("execution robot maintenance jobs not started", zap.Error(cronErr))
	}
}

// mergedHourOnlySymbols unions the subscription-derived set with whatever the
// runtime symbol state lists, so neither source can hide a symbol.
func mergedHourOnlySymbols(rt *runtimepkg.Runtime, subs []*strat.DataSub) func() map[int32]*orm.ExSymbol {
	fromSubs := hourOnlySymbolsFromSubs(subs)
	return func() map[int32]*orm.ExSymbol {
		res := fromSubs()
		if rt != nil && rt.Symbols != nil {
			for id, exs := range rt.Symbols.GetHourOnlySymbols() {
				res[id] = exs
			}
		}
		return res
	}
}

// ignoredExecutionConfigKeys lists configuration keys that are set but have no
// effect on the execution-backed path: they belong to the legacy wallet and
// pair machinery. Zero values mean "default" and are never reported; pairlists
// is only reported when it would have selected pairs on the legacy path, i.e.
// when no static pairs are configured or pairmgr.force_filters is set.
func ignoredExecutionConfigKeys(cfg *config.Config) []string {
	if cfg == nil {
		return nil
	}
	var keys []string
	if len(cfg.PairFilters) > 0 {
		static := len(cfg.Pairs) > 0
		for _, policy := range cfg.RunPolicy {
			if policy != nil && len(policy.Pairs) > 0 {
				static = true
			}
		}
		force := cfg.PairMgr != nil && cfg.PairMgr.ForceFilters
		if !static || force {
			keys = append(keys, "pairlists")
		}
	}
	if cfg.PairMgr != nil && cfg.PairMgr.Cron != "" {
		keys = append(keys, "pairmgr.cron")
	}
	// The loader fills these with their defaults, so only a value the user
	// changed is worth a warning.
	if cfg.MinOpenRate != 0 && cfg.MinOpenRate != 0.5 {
		keys = append(keys, "min_open_rate")
	}
	if cfg.ChargeOnBomb {
		keys = append(keys, "charge_on_bomb")
	}
	if cfg.MarginAddRate != 0 && cfg.MarginAddRate != 0.66 {
		keys = append(keys, "margin_add_rate")
	}
	if cfg.PutLimitSecs != 0 && cfg.PutLimitSecs != 180 {
		keys = append(keys, "put_limit_secs")
	}
	if cfg.OdBookTtl != 0 && cfg.OdBookTtl != 500 {
		keys = append(keys, "odbook_ttl")
	}
	if cfg.AccountPullSecs != 0 && cfg.AccountPullSecs != 60 {
		keys = append(keys, "account_pull_secs")
	}
	if cfg.BTInLive != nil && cfg.BTInLive.Cron != "" {
		keys = append(keys, "bt_in_live")
	}
	return keys
}

// warnIgnoredExecutionConfig logs one warning per ignored key. It never fails
// startup: existing configs commonly carry these keys.
func warnIgnoredExecutionConfig(cfg *config.Config) {
	for _, key := range ignoredExecutionConfigKeys(cfg) {
		log.Warn("config key is ignored by the execution-backed live path (legacy wallet/pair machinery only)", zap.String("key", key))
	}
}

// legacySubscriptions returns the legacy kline subscriptions of a binding.
func legacySubscriptions(binding *FactorLiveBinding) []*strat.DataSub {
	if binding == nil || binding.Legacy == nil {
		return nil
	}
	return binding.Legacy.Subscriptions
}

// rejectExecutionDump refuses --out on the execution-backed live path. The
// kline/API dump (rt.Dump, exchange websocket dump) is consumed only by the
// legacy CryptoTrader series handler and LiveOrderMgr, which this path does
// not run; accepting the flag would silently write an empty file.
func rejectExecutionDump(args *config.CmdArgs) error {
	if args != nil && args.OutPath != "" {
		return errors.New("--out (kline/API dump) is not supported by the execution-backed live path; remove it")
	}
	return nil
}

// verifyEmbeddedAccounts runs the startup account checks of an embedded
// (credentialed) robot. It is a variable only so that entry tests, whose
// exchange fakes have no account-access endpoint, can substitute it.
var verifyEmbeddedAccounts func(biz.RuntimeDeps) error = live.VerifyLiveAccountsWithRuntimeDeps
