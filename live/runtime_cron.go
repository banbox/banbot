package live

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

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

// runtimeRunGate skips a scheduled invocation while the previous one is still
// running. Cron jobs must never queue work that mutates the same runtime.
type runtimeRunGate struct{ sync.Mutex }

func (g *runtimeRunGate) run(fn func()) bool {
	if !g.TryLock() {
		return false
	}
	defer g.Unlock()
	fn()
	return true
}

func runtimeConfig(deps *biz.RuntimeDeps) *config.Config {
	if deps == nil || deps.Config == nil {
		return nil
	}
	return deps.Config.View()
}

func runtimeDataDeps(deps *biz.RuntimeDeps, catalog *data.DataSourceCatalog) *data.RuntimeDeps {
	if deps == nil {
		return nil
	}
	return &data.RuntimeDeps{
		Core: deps.Core, Clock: deps.Clock, Config: deps.Config,
		Market: deps.Market, Symbols: deps.Symbols, Strategies: deps.Strategies,
		Storage: deps.Storage, Catalog: catalog, Exchange: deps.Exchange, Callbacks: nil,
	}
}

// fetchHourKlinesFromWithRuntime is fetchHourKlinesWithRuntime with an explicit
// source of the hour-only symbol set. The legacy path reads the runtime symbol
// state; the execution-backed path isolates the provider's symbol state, so its
// entries pass the set derived from their own subscriptions.
func fetchHourKlinesFromWithRuntime(scheduler com.Scheduler, dp *data.LiveProvider, deps *biz.RuntimeDeps,
	catalog *data.DataSourceCatalog, hourOnly func() map[int32]*orm.ExSymbol) {
	if scheduler == nil || dp == nil || deps == nil || deps.Symbols == nil || hourOnly == nil {
		return
	}
	endMap := make(map[int32]int64)
	var gate runtimeRunGate
	_, err := scheduler.AddFunc("0 0 * * * *", func() {
		gate.run(func() {
			exsList := hourOnly()
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
			data.DownEmitHourKlinesWithRuntimeDeps(runtimeDataDeps(deps, catalog), dp, endMap)
		})
	})
	if err != nil {
		deps.Logger().Error("add runtime FetchHourKlines fail", zap.Error(err))
	}
}

func cronLoadMarketsWithRuntime(scheduler com.Scheduler, exchange banexg.BanExchange, symbols *orm.SymbolState,
	snapshot *config.Snapshot, runtimeCore *core.State,
) {
	if scheduler == nil || exchange == nil {
		return
	}
	var gate runtimeRunGate
	_, err := scheduler.AddFunc("30 3 */2 * * *", func() {
		gate.run(func() {
			if _, loadErr := orm.LoadMarketsWithRuntime(symbols, exchange, true, snapshot, runtimeCore); loadErr != nil {
				runtimeCore.Log().Error("runtime LoadMarkets fail", zap.Error(loadErr))
			}
		})
	})
	if err != nil {
		runtimeCore.Log().Error("add runtime CronLoadMarkets fail", zap.Error(err))
	}
}

func cronKlineDelaysWithRuntime(scheduler com.Scheduler, dp *data.LiveProvider, copied *com.PairCopiedState,
	clock func() int64, deps biz.RuntimeDeps) {
	if scheduler == nil || dp == nil || copied == nil {
		return
	}
	if clock == nil {
		if deps.Clock == nil {
			return
		}
		clock = deps.Clock.TimeMS
	}
	cfg := runtimeConfig(&deps)
	closeOnStuck := 20
	if cfg != nil && cfg.CloseOnStuck > 0 {
		closeOnStuck = cfg.CloseOnStuck
	}
	lastNotifyDelay := int64(0)
	stuckCount := 0
	var gate runtimeRunGate
	logDelay := func(message string) {
		now := clock()
		deps.Logger().Warn(message)
		if now-lastNotifyDelay > 600000 {
			lastNotifyDelay = now
			sendRuntimeMessage(&deps, map[string]interface{}{"type": rpc.MsgTypeException, "status": message})
		}
	}
	_, err := scheduler.AddFunc("30 * * * * *", func() {
		gate.run(func() {
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
							deps.Logger().Error("close runtime orders on stuck fail", zap.String("account", account), zap.Int("success", closed), zap.Int("fail", failed), zap.Error(closeErr))
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
	})
	if err != nil {
		deps.Logger().Error("add runtime Monitor Klines fail", zap.Error(err))
	}
}

func cronKlineSummaryWithRuntime(scheduler com.Scheduler, state *core.State) {
	if scheduler == nil || state == nil {
		return
	}
	var gate runtimeRunGate
	_, err := scheduler.AddFunc("30 1-59/10 * * * *", func() {
		gate.run(func() {
			groups := make(map[string][]string)
			for tf, pairs := range state.DrainTfPairHits() {
				byHits := make(map[int][]string)
				for pair, hits := range pairs {
					byHits[hits] = append(byHits[hits], pair)
				}
				for hits, items := range byHits {
					groups[fmt.Sprintf("%s_%d: %d", tf, hits, len(items))] = items
				}
			}
			if len(groups) > 0 {
				state.Log().Info(fmt.Sprintf("receive bars in 10 mins:\n%s", core.GroupByPairQuotes(groups, true)))
			}
		})
	})
	if err != nil {
		state.Log().Error("add runtime Receive Klines Summary fail", zap.Error(err))
	}
}

func cronDumpStratOutputsWithRuntime(scheduler com.Scheduler, state *strat.State, cfg *config.Config, dataDir string) {
	if scheduler == nil || state == nil || cfg == nil || dataDir == "" {
		return
	}
	logDir := filepath.Join(dataDir, "logs")
	var gate runtimeRunGate
	_, err := scheduler.AddFunc("31 * * * * *", func() {
		gate.run(func() {
			groups := make(map[string][]string)
			for _, items := range state.PairStrategiesView() {
				for _, strategy := range items {
					if strategy == nil {
						continue
					}
					if rows := strategy.DrainOutputs(); len(rows) > 0 {
						groups[strategy.Name] = append(groups[strategy.Name], rows...)
					}
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
					log.Error("write runtime strategy output fail", zap.Error(writeErr))
				}
				if closeErr := file.Close(); closeErr != nil {
					log.Error("close runtime strategy output file fail", zap.Error(closeErr))
				}
			}
		})
	})
	if err != nil {
		log.Error("add runtime CronDumpStratOutputs fail", zap.Error(err))
	}
}

func sendRuntimeMessage(deps *biz.RuntimeDeps, message map[string]interface{}) {
	if deps == nil {
		rpc.SendMsg(message)
	} else if deps.Notifications != nil {
		deps.Notifications.SendMsg(message)
	}
}
