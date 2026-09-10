package live

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/rpc"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

const runtimeMinPairCronGapMS int64 = 30 * 60 * 1000

func runtimeConfig(deps *biz.RuntimeDeps) *config.Config {
	if deps == nil || deps.Config == nil {
		return nil
	}
	return deps.Config.View()
}

func runtimeDataDeps(deps *biz.RuntimeDeps) *data.RuntimeDeps {
	if deps == nil {
		return nil
	}
	return &data.RuntimeDeps{
		Core: deps.Core, Clock: deps.Clock, Config: deps.Config,
		Market: deps.Market, Symbols: deps.Symbols, Strategies: deps.Strategies,
		Storage: deps.Storage, Exchange: deps.Exchange, Callbacks: nil,
	}
}

func cronRefreshPairsWithRuntime(scheduler com.Scheduler, trader *CryptoTrader, dp data.IProvider,
	deps *biz.RuntimeDeps, afterRefresh func() error) {
	cfg := runtimeConfig(deps)
	if scheduler == nil || trader == nil || dp == nil || cfg == nil || cfg.PairMgr == nil || cfg.PairMgr.Cron == "" {
		return
	}
	lastRefreshMS := trader.currentTimeMS()
	_, err := scheduler.AddFunc(cfg.PairMgr.Cron, func() {
		if !trader.runtimeActive() {
			return
		}
		curMS := trader.currentTimeMS()
		if curMS-lastRefreshMS < runtimeMinPairCronGapMS {
			return
		}
		lastRefreshMS = curMS
		if err := trader.refreshPairJobs(false); err != nil {
			log.Error("RefreshPairJobs fail", zap.Error(err))
			return
		}
		if afterRefresh != nil {
			if err := afterRefresh(); err != nil {
				log.Error("RefreshPairJobs post-refresh fail", zap.Error(err))
			}
		}
	})
	if err != nil {
		log.Error("add runtime RefreshPairList fail", zap.Error(err))
	}
}

func fetchHourKlinesWithRuntime(scheduler com.Scheduler, dp *data.LiveProvider, deps *biz.RuntimeDeps) {
	if scheduler == nil || dp == nil || deps == nil || deps.Symbols == nil {
		return
	}
	endMap := make(map[int32]int64)
	_, err := scheduler.AddFunc("0 0 * * * *", func() {
		exsList := deps.Symbols.GetHourOnlySymbols()
		if len(exsList) == 0 {
			return
		}
		for sid := range exsList {
			if _, ok := endMap[sid]; !ok {
				endMap[sid] = 0
			}
		}
		for sid := range endMap {
			if _, ok := exsList[sid]; !ok {
				delete(endMap, sid)
			}
		}
		data.DownEmitHourKlinesWithRuntimeDeps(runtimeDataDeps(deps), dp, endMap)
	})
	if err != nil {
		log.Error("add runtime FetchHourKlines fail", zap.Error(err))
	}
}

func cronLoadMarketsWithRuntime(scheduler com.Scheduler, exchange banexg.BanExchange, symbols *orm.SymbolState,
	snapshot *config.Snapshot, runtimeCore *core.State,
) {
	if scheduler == nil || exchange == nil {
		return
	}
	_, err := scheduler.AddFunc("30 3 */2 * * *", func() {
		if _, loadErr := orm.LoadMarketsWithRuntime(symbols, exchange, true, snapshot, runtimeCore); loadErr != nil {
			log.Error("runtime LoadMarkets fail", zap.Error(loadErr))
		}
	})
	if err != nil {
		log.Error("add runtime CronLoadMarkets fail", zap.Error(err))
	}
}

func runtimeFatalStops(cfg *config.Config) (map[int]float64, int, int) {
	result := make(map[int]float64)
	if cfg == nil {
		return result, 0, 0
	}
	for text, rate := range cfg.FatalStop {
		minutes, err := strconv.Atoi(strings.TrimSpace(text))
		if err != nil || minutes <= 0 {
			continue
		}
		result[minutes] = rate
	}
	hours := cfg.FatalStopHours
	if hours <= 0 {
		hours = 8
	}
	maxInterval := 0
	for interval := range result {
		if interval > maxInterval {
			maxInterval = interval
		}
	}
	return result, hours, maxInterval
}

func cronFatalLossCheckWithRuntime(scheduler com.Scheduler, deps biz.RuntimeDeps, nowMS func() int64) {
	fatal, hours, maxInterval := runtimeFatalStops(runtimeConfig(&deps))
	if scheduler == nil || len(fatal) == 0 || maxInterval <= 0 {
		return
	}
	interval := maxInterval
	if interval > 5 {
		interval = 5
	}
	_, err := scheduler.AddFunc(fmt.Sprintf("35 */%d * * * *", interval), biz.MakeCheckFatalStopWithRuntime(deps, fatal, hours, nowMS))
	if err != nil {
		log.Error("add runtime CronFatalLossCheck fail", zap.Error(err))
	}
}

func cronKlineDelaysWithRuntime(scheduler com.Scheduler, dp *data.LiveProvider, copied *com.PairCopiedState,
	clock func() int64, deps biz.RuntimeDeps) {
	if scheduler == nil || dp == nil || copied == nil {
		return
	}
	if clock == nil {
		clock = func() int64 { return time.Now().UnixMilli() }
	}
	cfg := runtimeConfig(&deps)
	closeOnStuck := 20
	if cfg != nil && cfg.CloseOnStuck > 0 {
		closeOnStuck = cfg.CloseOnStuck
	}
	lastNotifyDelay := int64(0)
	stuckCount := 0
	logDelay := func(message string) {
		now := clock()
		log.Warn(message)
		if now-lastNotifyDelay > 600000 {
			lastNotifyDelay = now
			sendRuntimeMessage(&deps, map[string]interface{}{"type": rpc.MsgTypeException, "status": message})
		}
	}
	_, err := scheduler.AddFunc("30 * * * * *", func() {
		if deps.Core != nil {
			select {
			case <-deps.Core.Done():
				return
			default:
			}
		}
		if len(dp.GetJobs("ohlcv")) == 0 {
			return
		}
		now := clock()
		if delaySecs := int((now - copied.LastCopiedMs()) / 1000); delaySecs > 120 {
			logDelay("Listen to the spider kline timeout!")
			stuckCount++
			if stuckCount > closeOnStuck {
				for account, accountCfg := range deps.AccountConfigs() {
					if accountCfg == nil || accountCfg.NoTrade || deps.Orders == nil || deps.Trading == nil {
						continue
					}
					orders, lock := deps.Orders.GetOpenODs(account)
					lock.Lock()
					list := utils.ValsOfMap(orders)
					lock.Unlock()
					if len(list) == 0 {
						continue
					}
					closed, failed, closeErr := biz.CloseAccOrdersWithState(deps.Trading, account, list, &strat.ExitReq{Tag: core.ExitTagDataStuck, Force: true})
					if closeErr != nil {
						log.Error("close runtime orders on stuck fail", zap.String("account", account), zap.Int("success", closed), zap.Int("fail", failed), zap.Error(closeErr))
					}
				}
				stuckCount = 0
			}
			return
		}
		stuckCount = 0
		fails := make(map[string][]string)
		for pair, wait := range copied.GetPairCopieds() {
			if wait[0]+wait[1]*2 > now {
				continue
			}
			timeoutMin := strconv.Itoa(int((now-wait[0])/60000)) + "mins"
			fails[timeoutMin] = append(fails[timeoutMin], pair)
		}
		if len(fails) > 0 {
			logDelay("Listen to the spider kline timeout:" + core.GroupByPairQuotes(fails, false))
		}
	})
	if err != nil {
		log.Error("add runtime Monitor Klines fail", zap.Error(err))
	}
}

func cronKlineSummaryWithRuntime(scheduler com.Scheduler, state *core.State) {
	if scheduler == nil || state == nil {
		return
	}
	_, err := scheduler.AddFunc("30 1-59/10 * * * *", func() {
		state.TfPairHitsLock.Lock()
		groups := make(map[string][]string)
		for tf, pairs := range state.TfPairHits {
			byHits := make(map[int][]string)
			for pair, hits := range pairs {
				byHits[hits] = append(byHits[hits], pair)
			}
			for hits, items := range byHits {
				groups[fmt.Sprintf("%s_%d: %d", tf, hits, len(items))] = items
			}
		}
		state.TfPairHits = make(map[string]map[string]int)
		state.TfPairHitsLock.Unlock()
		if len(groups) > 0 {
			log.Info(fmt.Sprintf("receive bars in 10 mins:\n%s", core.GroupByPairQuotes(groups, true)))
		}
	})
	if err != nil {
		log.Error("add runtime Receive Klines Summary fail", zap.Error(err))
	}
}

func cronDumpStratOutputsWithRuntime(scheduler com.Scheduler, state *strat.State, cfg *config.Config, dataDir string) {
	if scheduler == nil || state == nil || cfg == nil || dataDir == "" {
		return
	}
	logDir := filepath.Join(dataDir, "logs")
	_, err := scheduler.AddFunc("31 * * * * *", func() {
		groups := make(map[string][]string)
		for _, items := range state.PairStrats {
			for _, strategy := range items {
				if strategy == nil || len(strategy.Outputs) == 0 {
					continue
				}
				groups[strategy.Name] = append(groups[strategy.Name], strategy.Outputs...)
				strategy.Outputs = nil
			}
		}
		for name, lines := range groups {
			name = strings.ReplaceAll(name, ":", "_")
			if err := utils.EnsureDir(logDir, 0755); err != nil {
				log.Error("create runtime strategy output directory fail", zap.Error(err))
				continue
			}
			path := filepath.Join(logDir, fmt.Sprintf("%s_%s.log", cfg.Name, name))
			file, openErr := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if openErr != nil {
				log.Error("create runtime strategy output file fail", zap.String("name", name), zap.Error(openErr))
				continue
			}
			_, writeErr := file.WriteString(strings.Join(lines, "\n") + "\n")
			if writeErr != nil {
				log.Error("write runtime strategy output fail", zap.String("name", name), zap.Error(writeErr))
			}
			if closeErr := file.Close(); closeErr != nil {
				log.Error("close runtime strategy output file fail", zap.String("name", name), zap.Error(closeErr))
			}
		}
	})
	if err != nil {
		log.Error("add runtime CronDumpStratOutputs fail", zap.Error(err))
	}
}

func cronCheckTriggerOdsWithRuntime(scheduler com.Scheduler, deps biz.RuntimeDeps) {
	if scheduler == nil {
		return
	}
	_, err := scheduler.AddFunc("15,45 * * * * *", func() { biz.VerifyTriggerOdsWithRuntimeDeps(deps) })
	if err != nil {
		log.Error("add runtime VerifyTriggerOds fail", zap.Error(err))
	}
}
