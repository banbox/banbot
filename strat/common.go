package strat

import (
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

type FuncMakeStrat = func(pol *config.RunPolicyConfig) *TradeStrat

var (
	StratMake   = make(map[string]FuncMakeStrat) // 已加载的策略缓存
	cacheStrats = make(map[string]*TradeStrat)
	cacheMu     sync.Mutex
	stratMakeMu sync.RWMutex
)

func New(pol *config.RunPolicyConfig) *TradeStrat {
	polID := pol.ID()
	cacheKey := utils.MD5([]byte(polID + "\n" + pol.ToYaml()))
	cacheMu.Lock()
	defer cacheMu.Unlock()
	if obj, ok := cacheStrats[cacheKey]; ok {
		obj.Policy = pol
		return obj
	}
	stratMakeMu.RLock()
	makeFn, ok := StratMake[pol.Name]
	stratMakeMu.RUnlock()
	var stgy *TradeStrat
	if ok {
		stgy = makeFn(pol)
	} else {
		panic("strategy not found: " + pol.Name)
		// stgy = loadNative(pol.Name)
	}
	stgy.Name = polID
	validateDataCallbacks(stgy)
	cacheStrats[cacheKey] = stgy
	if stgy.MinTfScore == 0 {
		stgy.MinTfScore = 0.75
	}
	stgy.Policy = pol
	if pol.StakeRate > 0 {
		stgy.StakeRate = pol.StakeRate
	}
	if pol.OrderBarMax > 0 {
		stgy.OdBarMax = pol.OrderBarMax
	}
	if len(pol.RunTimeframes) > 0 {
		stgy.RunTimeFrames = pol.RunTimeframes
	} else if len(stgy.RunTimeFrames) == 0 {
		if len(stgy.TimeFrames) > 0 {
			stgy.RunTimeFrames = config.SplitTimeFrames(stgy.TimeFrames)
		}
	}
	if pol.StopLoss != nil {
		slRate, isFlt := pol.StopLoss.(float64)
		if isFlt {
			if slRate > 0 {
				stgy.StopLoss = slRate
			} else if slRate < 0 {
				log.Error("stop_loss should > 0", zap.String("policy", pol.Name))
			}
		} else if slStr, ok := pol.StopLoss.(string); ok {
			if strings.TrimSpace(slStr) != "" {
				var err error
				slStr = strings.TrimSpace(slStr)
				if strings.HasSuffix(slStr, "%") {
					slRate, err = strconv.ParseFloat(slStr[:len(slStr)-1], 64)
					slRate /= 100
				} else {
					slRate, err = strconv.ParseFloat(slStr, 64)
				}
				if err != nil {
					log.Error("invalid stop_loss", zap.String("policy", pol.Name), zap.Error(err))
				} else if slRate > 0 {
					stgy.StopLoss = slRate
				}
			}
		} else if slInt, ok := pol.StopLoss.(int); ok {
			if slInt != 0 {
				log.Error("stop_loss format error, expect to be 5% or 0.05", zap.String("policy", pol.Name))
			}
		} else {
			log.Error("invalid stop_loss type, expect e.g.: 5% or 0.05", zap.String("policy", pol.Name),
				zap.String("type", fmt.Sprintf("%T", pol.StopLoss)))
		}
	}
	return stgy
}

func Get(pair, stratID string) *TradeStrat {
	data, _ := PairStrats[pair]
	if len(data) == 0 {
		return nil
	}
	res, _ := data[stratID]
	return res
}

func GetStratPerf(pair, strat string) *config.StratPerfConfig {
	stg := Get(pair, strat)
	if stg == nil {
		return nil
	}
	if stg.Policy.StratPerf != nil {
		return stg.Policy.StratPerf
	}
	return config.Data.StratPerf
}

//func loadNative(stratName string) *TradeStrat {
//	filePath := path.Join(config.GetStratDir(), stratName+".o")
//	_, err := os.Stat(filePath)
//	nameVar := zap.String("name", stratName)
//	if err != nil {
//		log.Error("strategy not found", zap.String("path", filePath), zap.Error(err))
//		return nil
//	}
//	linker, err := goloader.ReadObj(filePath, stratName)
//	if err != nil {
//		log.Error("strategy load fail, package is `main`?", nameVar, zap.Error(err))
//		return nil
//	}
//	symPtr := make(map[string]uintptr)
//	err = goloader.RegSymbol(symPtr)
//	if err != nil {
//		log.Error("strategy read symbol fail", nameVar, zap.Error(err))
//		return nil
//	}
//	regLoaderTypes(symPtr)
//	module, err := goloader.Load(linker, symPtr)
//	if err != nil {
//		log.Error("strategy load module fail", nameVar, zap.Error(err))
//		return nil
//	}
//	keys := zap.String("keys", strings.Join(utils.KeysOfMap(module.Syms), ","))
//	prefix := stratName + "."
//	// 加载Main
//	mainPath := prefix + "Main"
//	mainPtr := GetModuleItem(module, mainPath)
//	if mainPtr == nil {
//		log.Error("module item not found", zap.String("p", mainPath), keys)
//		return nil
//	}
//	runFunc := *(*func() *TradeStrat)(mainPtr)
//	stagy := runFunc()
//	stagy.Name = stratName
//	// 这里不能卸载，卸载后结构体的嵌入函数无法调用
//	// module.Unload()
//	return stagy
//}
//
//func GetModuleItem(module *goloader.CodeModule, itemPath string) unsafe.Pointer {
//	main, ok := module.Syms[itemPath]
//	if !ok || main == 0 {
//		return nil
//	}
//	mainPtr := (uintptr)(unsafe.Pointer(&main))
//	return unsafe.Pointer(&mainPtr)
//}
//
//func regLoaderTypes(symPtr map[string]uintptr) {
//	goloader.RegTypes(symPtr, &ta.BarEnv{}, &ta.Series{}, &ta.CrossLog{}, &ta.XState{}, ta.Cross, ta.Sum, ta.SMA,
//		ta.EMA, ta.EMABy, ta.RMA, ta.RMABy, ta.TR, ta.ATR, ta.MACD, ta.MACDBy, ta.RSI, ta.Highest, ta.Lowest, ta.KDJ,
//		ta.KDJBy, ta.StdDev, ta.StdDevBy, ta.BBANDS, ta.TD, &ta.AdxState{}, ta.ADX, ta.ROC, ta.HeikinAshi)
//	stgy := &TradeStrat{}
//	goloader.RegTypes(symPtr, stgy, stgy.OnPairInfos, stgy.OnStartUp, stgy.OnBar, stgy.OnInfoBar, stgy.OnWsTrades,
//		stgy.OnCheckExit, stgy.GetDrawDownExitRate, stgy.PickTimeFrame, stgy.OnShutDown)
//	job := &StratJob{}
//	goloader.RegTypes(symPtr, job, job.OpenOrder)
//	goloader.RegTypes(symPtr, &PairSub{}, &EnterReq{}, &ExitReq{})
//}

func (q *EnterReq) Clone() *EnterReq {
	res := &EnterReq{
		Tag:             q.Tag,
		StratName:       q.StratName,
		Short:           q.Short,
		OrderType:       q.OrderType,
		Limit:           q.Limit,
		Stop:            q.Stop,
		CostRate:        q.CostRate,
		LegalCost:       q.LegalCost,
		Leverage:        q.Leverage,
		Amount:          q.Amount,
		StopLossVal:     q.StopLossVal,
		StopLoss:        q.StopLoss,
		StopLossLimit:   q.StopLossLimit,
		StopLossRate:    q.StopLossRate,
		StopLossTag:     q.StopLossTag,
		TakeProfitVal:   q.TakeProfitVal,
		TakeProfit:      q.TakeProfit,
		TakeProfitLimit: q.TakeProfitLimit,
		TakeProfitRate:  q.TakeProfitRate,
		TakeProfitTag:   q.TakeProfitTag,
		StopBars:        q.StopBars,
		ClientID:        q.ClientID,
		Log:             q.Log,
	}

	// 深度复制map字段Infos
	if q.Infos != nil {
		res.Infos = make(map[string]string, len(q.Infos))
		for k, v := range q.Infos {
			res.Infos[k] = v
		}
	}
	return res
}

func (q *EnterReq) GetZapFields(s *StratJob, fields ...zap.Field) []zap.Field {
	dirt := "long"
	if q.Short {
		dirt = "short"
	}
	stgName := q.StratName
	if stgName == "" && s != nil {
		stgName = s.Strat.Name
	}
	fields = append(fields, zap.String("strat", stgName), zap.String("tag", q.Tag),
		zap.String("dirt", dirt))
	if s != nil {
		fields = append(fields, zap.String("acc", s.Account),
			zap.String("pair", s.Symbol.Symbol), zap.String("tf", s.TimeFrame))
	}
	if q.OrderType != core.OrderTypeEmpty {
		odType, _ := core.OdTypeMap[q.OrderType]
		if odType != "" {
			fields = append(fields, zap.String("odType", odType))
		}
	}
	if q.Limit != 0 {
		fields = append(fields, zap.Float64("limit", q.Limit))
	}
	if q.Stop != 0 {
		fields = append(fields, zap.Float64("trigger", q.Stop))
	}
	if q.CostRate > 0 && q.CostRate < 1 {
		fields = append(fields, zap.Float64("costRate", q.CostRate))
	}
	if q.LegalCost > 0 {
		fields = append(fields, zap.Float64("cost", q.LegalCost))
	}
	if q.Leverage > 0 {
		fields = append(fields, zap.Float64("leverage", q.Leverage))
	}
	if q.Amount > 0 {
		fields = append(fields, zap.Float64("amt", q.Amount))
	}
	if q.StopLossVal > 0 {
		fields = append(fields, zap.Float64("slVal", q.StopLossVal))
	} else if q.StopLoss > 0 {
		fields = append(fields, zap.Float64("sl", q.StopLoss))
	}
	if q.TakeProfitVal > 0 {
		fields = append(fields, zap.Float64("tpVal", q.TakeProfitVal))
	} else if q.TakeProfit > 0 {
		fields = append(fields, zap.Float64("tp", q.TakeProfit))
	}
	if q.StopBars > 0 {
		fields = append(fields, zap.Int("stopBars", q.StopBars))
	}
	if q.ClientID != "" {
		fields = append(fields, zap.String("clientId", q.ClientID))
	}
	if len(q.Infos) > 0 {
		fields = append(fields, zap.Any("info", q.Infos))
	}
	return fields
}

func (q *ExitReq) Clone() *ExitReq {
	res := &ExitReq{
		Tag:        q.Tag,
		StratName:  q.StratName,
		EnterTag:   q.EnterTag,
		Dirt:       q.Dirt,
		OrderType:  q.OrderType,
		Limit:      q.Limit,
		ExitRate:   q.ExitRate,
		Amount:     q.Amount,
		OrderID:    q.OrderID,
		UnFillOnly: q.UnFillOnly,
		FilledOnly: q.FilledOnly,
		Force:      q.Force,
	}
	return res
}

func (q *ExitReq) GetZapFields(s *StratJob, fields ...zap.Field) []zap.Field {
	stgName := q.StratName
	if stgName == "" && s != nil {
		stgName = s.Strat.Name
	}
	fields = append(fields, zap.String("strat", stgName), zap.String("tag", q.Tag))
	if s != nil {
		fields = append(fields, zap.String("acc", s.Account),
			zap.String("pair", s.Symbol.Symbol), zap.String("tf", s.TimeFrame))
	}

	if q.Dirt != 0 {
		fields = append(fields, zap.Int("dirt", q.Dirt))
	}
	if q.EnterTag != "" {
		fields = append(fields, zap.String("entTag", q.EnterTag))
	}
	if q.OrderType != core.OrderTypeEmpty {
		odType, _ := core.OdTypeMap[q.OrderType]
		if odType != "" {
			fields = append(fields, zap.String("odType", odType))
		}
	}
	if q.Limit != 0 {
		fields = append(fields, zap.Float64("limit", q.Limit))
	}
	if q.ExitRate > 0 && q.ExitRate < 1 {
		fields = append(fields, zap.Float64("exitRate", q.ExitRate))
	}
	if q.Amount > 0 {
		fields = append(fields, zap.Float64("amt", q.Amount))
	}
	if q.OrderID != 0 {
		fields = append(fields, zap.Int64("orderId", q.OrderID))
	}
	if q.UnFillOnly {
		fields = append(fields, zap.Int("unFillOnly", 1))
	}
	if q.FilledOnly {
		fields = append(fields, zap.Int("fillOnly", 1))
	}
	if q.Force {
		fields = append(fields, zap.Int("force", 1))
	}
	return fields
}

func (s *StratJob) UpdateOrders(curOrders []*ormo.InOutOrder) {
	if s.recordInspectEffect("UpdateOrders") {
		return
	}
	state := s.executionState()
	state.mu.Lock()
	defer state.mu.Unlock()
	s.LongOrders = nil
	s.ShortOrders = nil
	enteredNum := 0
	for _, od := range curOrders {
		if od.Symbol == s.Symbol.Symbol && od.Timeframe == s.TimeFrame && od.Strategy == s.Strat.Name {
			if od.Status >= ormo.InOutStatusPartEnter && od.Status <= ormo.InOutStatusPartExit {
				enteredNum += 1
			}
			if od.Short {
				s.ShortOrders = append(s.ShortOrders, od)
			} else {
				s.LongOrders = append(s.LongOrders, od)
			}
		}
	}
	s.OrderNum = len(s.LongOrders) + len(s.ShortOrders)
	s.EnteredNum = enteredNum
}

func (s *StratJob) InitBar(curOrders []*ormo.InOutOrder) {
	if s.recordInspectEffect("InitBar") {
		return
	}
	checkMS := s.runtimeTimeMS()
	lastBarMS := s.Env.TimeStop
	state := s.executionState()
	state.mu.Lock()
	s.CheckMS = checkMS
	s.LastBarMS = lastBarMS
	isWarmUp := s.IsWarmUp
	orderNum := s.OrderNum
	state.mu.Unlock()
	if isWarmUp {
		state.mu.Lock()
		s.LongOrders = nil
		s.ShortOrders = nil
		state.mu.Unlock()
	} else if orderNum > 0 || s.runtimeLive() {
		// 针对实盘，重启后OrderNum状态重置，本地未平仓订单无法更新到StratJob中，这里每次都检查
		s.UpdateOrders(curOrders)
	}
	if s.runtimeLive() && !isWarmUp {
		// print warning before clear
		enters, exits := s.DrainOrderRequests()
		for i, q := range enters {
			fields := q.GetZapFields(s)
			fields = append(fields, zap.Int("i", i))
			log.Warn("ignore unhandle Entry", fields...)
		}
		for i, q := range exits {
			fields := q.GetZapFields(s)
			fields = append(fields, zap.Int("i", i))
			log.Warn("ignore unhandle Exit", fields...)
		}
	} else {
		_, _ = s.DrainOrderRequests()
	}
}

func CheckCustomExits(job *StratJob) *errs.Error {
	orders := job.GetOrders(0)
	for _, od := range orders {
		if od.Status >= ormo.InOutStatusFullEnter && od.CanClose() {
			_, err := job.customExit(od)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func LockJobsRead() {
	lockJobs.RLock()
}

func UnlockJobsRead() {
	lockJobs.RUnlock()
}

func LockJobsWrite() {
	lockJobs.Lock()
}

func UnlockJobsWrite() {
	lockJobs.Unlock()
}

/*
GetJobs 返回：pair_tf: [stratID]StratJob
*/
func GetJobs(account string) map[string]map[string]*StratJob {
	if !core.EnvReal {
		account = config.DefAcc
	}
	return LegacyState().Jobs(account)
}

func GetInfoJobs(account string) map[string]map[string]*StratJob {
	if !core.EnvReal {
		account = config.DefAcc
	}
	return LegacyState().InfoJobs(account)
}

func GetHistOrders(args ormo.GetHistOrdersArgs) ([]*ormo.InOutOrder, *errs.Error) {
	var orders []*ormo.InOutOrder
	if core.LiveMode {
		// Retrieve recent orders from the database
		// 从数据库查询最近订单
		sess, conn, err := ormo.Conn(orm.DbTrades, false)
		if err != nil {
			return nil, err
		}
		defer conn.Close()
		odBy := "id desc"
		if args.IDAsc {
			odBy = "id asc"
		}
		orders, err = sess.GetOrders(ormo.GetOrdersArgs{
			Strategy:    args.Strategy,
			Pairs:       args.Pairs,
			TimeFrame:   args.TimeFrame,
			TaskID:      args.TaskID,
			Dirt:        args.Dirt,
			EnterTag:    args.EnterTag,
			ExitTag:     args.ExitTag,
			CloseAfter:  args.CloseAfter,
			CloseBefore: args.CloseBefore,
			Limit:       args.Limit,
			AfterID:     args.AfterID,
			Status:      2,
			OrderBy:     odBy,
		})
		if err != nil {
			return nil, err
		}
	} else {
		// 从HistODs查询
		pairMap := make(map[string]bool)
		for _, pair := range args.Pairs {
			pairMap[pair] = true
		}
		for _, od := range ormo.HistODs {
			// Filter by TaskID: 0 means all, >0 means specific task
			if args.TaskID > 0 && od.TaskID != args.TaskID {
				continue
			}

			// Filter by Strategy
			if args.Strategy != "" && od.Strategy != args.Strategy {
				continue
			}

			// Filter by Pairs (Symbol)
			if len(args.Pairs) > 0 {
				if _, found := pairMap[od.Symbol]; !found {
					continue
				}
			}

			// Filter by TimeFrame
			if args.TimeFrame != "" && od.Timeframe != args.TimeFrame {
				continue
			}

			// Filter by Direction (Dirt): OdDirtLong/OdDirtShort/OdDirtBoth
			if args.Dirt == core.OdDirtLong && od.Short {
				continue
			}
			if args.Dirt == core.OdDirtShort && !od.Short {
				continue
			}

			// Filter by EnterTag
			if args.EnterTag != "" && od.EnterTag != args.EnterTag {
				continue
			}

			// Filter by ExitTag
			if args.ExitTag != "" && od.ExitTag != args.ExitTag {
				continue
			}

			// Filter by CloseAfter (exit_at >= CloseAfter)
			if args.CloseAfter > 0 && od.ExitAt < args.CloseAfter {
				continue
			}

			// Filter by CloseBefore (exit_at < CloseBefore)
			if args.CloseBefore > 0 && od.ExitAt >= args.CloseBefore {
				continue
			}

			orders = append(orders, od)
		}

		// Sort by ID (default order)
		slices.SortFunc(orders, func(a, b *ormo.InOutOrder) int {
			return int(a.ID - b.ID)
		})

		// Handle AfterID pagination
		if args.AfterID != 0 {
			var filtered []*ormo.InOutOrder
			if args.AfterID > 0 {
				// Get orders after this ID (ascending order)
				for _, od := range orders {
					if od.ID > int64(args.AfterID) {
						filtered = append(filtered, od)
					}
				}
				orders = filtered
			} else {
				// Get orders before this ID (descending order)
				targetID := int64(-args.AfterID)
				for i := len(orders) - 1; i >= 0; i-- {
					if orders[i].ID < targetID {
						filtered = append(filtered, orders[i])
					}
				}
				orders = filtered
			}
		}

		if !args.IDAsc {
			slices.SortFunc(orders, func(a, b *ormo.InOutOrder) int {
				return int(b.ID - a.ID)
			})
		}

		// Apply Limit
		if args.Limit > 0 && len(orders) > args.Limit {
			orders = orders[:args.Limit]
		}
	}
	return orders, nil
}

// GetHistOrdersWithState filters the in-memory history owned by an explicit
// runtime. It deliberately never consults the package-level history facade;
// callers that pass nil retain the legacy behavior above.
func GetHistOrdersWithState(state *ormo.OrderState, args ormo.GetHistOrdersArgs) ([]*ormo.InOutOrder, *errs.Error) {
	if state == nil {
		return GetHistOrders(args)
	}
	pairMap := make(map[string]bool, len(args.Pairs))
	for _, pair := range args.Pairs {
		pairMap[pair] = true
	}
	orders := make([]*ormo.InOutOrder, 0)
	for _, od := range state.HistoricalOrders() {
		if od == nil {
			continue
		}
		if args.TaskID > 0 && od.TaskID != args.TaskID {
			continue
		}
		if args.Strategy != "" && od.Strategy != args.Strategy {
			continue
		}
		if len(args.Pairs) > 0 && !pairMap[od.Symbol] {
			continue
		}
		if args.TimeFrame != "" && od.Timeframe != args.TimeFrame {
			continue
		}
		if args.Dirt == core.OdDirtLong && od.Short || args.Dirt == core.OdDirtShort && !od.Short {
			continue
		}
		if args.EnterTag != "" && od.EnterTag != args.EnterTag || args.ExitTag != "" && od.ExitTag != args.ExitTag {
			continue
		}
		if args.CloseAfter > 0 && od.ExitAt < args.CloseAfter || args.CloseBefore > 0 && od.ExitAt >= args.CloseBefore {
			continue
		}
		orders = append(orders, od)
	}
	slices.SortFunc(orders, func(a, b *ormo.InOutOrder) int {
		return int(a.ID - b.ID)
	})
	if args.AfterID > 0 {
		orders = slices.DeleteFunc(orders, func(od *ormo.InOutOrder) bool { return od.ID <= int64(args.AfterID) })
	} else if args.AfterID < 0 {
		target := int64(-args.AfterID)
		filtered := make([]*ormo.InOutOrder, 0, len(orders))
		for i := len(orders) - 1; i >= 0; i-- {
			if orders[i].ID < target {
				filtered = append(filtered, orders[i])
			}
		}
		orders = filtered
	}
	if !args.IDAsc {
		slices.Reverse(orders)
	}
	if args.Limit > 0 && len(orders) > args.Limit {
		orders = orders[:args.Limit]
	}
	return orders, nil
}

// CalcJobScoresWithState updates strategy and performance counters owned by
// one runtime. It is the explicit counterpart to CalcJobScores and keeps all
// hot-path state in the supplied concrete receivers.
func CalcJobScoresWithState(strategyState *State, coreState *core.State, orderState *ormo.OrderState,
	account, pair, tf, stgy string,
) *errs.Error {
	if strategyState == nil || coreState == nil || orderState == nil {
		return CalcJobScores(pair, tf, stgy)
	}
	cfg := strategyState.GetStratPerf(pair, stgy)
	if cfg == nil {
		return nil
	}
	orders, err := GetHistOrdersWithState(orderState, ormo.GetHistOrdersArgs{
		TaskID: orderState.GetTaskID(account), Strategy: stgy, TimeFrame: tf,
		Pairs: []string{pair}, Limit: cfg.MaxOdNum,
	})
	if err != nil {
		return err
	}
	totalPft := 0.0
	for _, od := range orders {
		totalPft += od.ProfitRate
	}
	prefKey := core.KeyStratPairTf(stgy, pair, tf)
	coreState.WithPerformance(func(jobPerfs map[string]*core.JobPerf, stratPerfSta map[string]*core.PerfSta) {
		sta := stratPerfSta[stgy]
		if sta == nil {
			sta = &core.PerfSta{}
			stratPerfSta[stgy] = sta
		}
		sta.OdNum++
		if len(orders) < cfg.MinOdNum {
			return
		}
		perf := jobPerfs[prefKey]
		if perf == nil {
			perf = &core.JobPerf{Num: len(orders), TotProfit: totalPft, Score: 1}
			jobPerfs[prefKey] = perf
		} else {
			perf.Num = len(orders)
		}
		prefs := make([]*core.JobPerf, 0)
		for key, item := range jobPerfs {
			if strings.HasPrefix(key, stgy) {
				prefs = append(prefs, item)
			}
		}
		perf.Score = defaultCalcJobScore(cfg, sta, perf, prefs)
	})
	return nil
}

func CalcJobScores(pair, tf, stgy string) *errs.Error {
	cfg := GetStratPerf(pair, stgy)
	orders, err := GetHistOrders(ormo.GetHistOrdersArgs{
		TaskID:    ormo.GetTaskID(config.DefAcc),
		Strategy:  stgy,
		TimeFrame: tf,
		Pairs:     []string{pair},
		Limit:     cfg.MaxOdNum,
	})
	if err != nil {
		return err
	}
	var updated bool
	totalPft := 0.0
	if len(orders) >= cfg.MinOdNum {
		for _, od := range orders {
			totalPft += od.ProfitRate
		}
	}
	var prefKey = core.KeyStratPairTf(stgy, pair, tf)
	core.WithLegacyPerformance(func(jobPerfs map[string]*core.JobPerf, stratPerfSta map[string]*core.PerfSta) {
		sta := stratPerfSta[stgy]
		if sta == nil {
			sta = &core.PerfSta{}
			stratPerfSta[stgy] = sta
		}
		sta.OdNum++
		if len(orders) < cfg.MinOdNum {
			return
		}
		perf := jobPerfs[prefKey]
		if perf == nil {
			perf = &core.JobPerf{
				Num:       len(orders),
				TotProfit: totalPft,
				Score:     1,
			}
			jobPerfs[prefKey] = perf
		} else {
			perf.Num = len(orders)
		}
		// Collect all trade jobs for this strategy while the compatibility
		// performance lock is held; score calculation mutates these values.
		var prefs []*core.JobPerf
		for key, p := range jobPerfs {
			if strings.HasPrefix(key, stgy) {
				prefs = append(prefs, p)
			}
		}
		perf.Score = defaultCalcJobScore(cfg, sta, perf, prefs)
		updated = true
	})
	if updated && core.LiveMode {
		// Real disk mode, immediately save to data directory
		// 实盘模式，立刻保存到数据目录
		core.DumpPerfs(config.GetDataDir())
	}
	return nil
}

func defaultCalcJobScore(cfg *config.StratPerfConfig, sta *core.PerfSta, p *core.JobPerf, perfs []*core.JobPerf) float64 {
	if len(perfs) < cfg.MinJobNum {
		return 1
	}
	// 按Job总利润分组5档
	if sta.Splits == nil || sta.OdNum-sta.LastGpAt >= len(perfs) {
		// 按总收益率KMeans分组
		perfs = append(perfs, p)
		CalcJobPerfs(cfg, sta, perfs)
		return p.Score
	}
	// 聚类结果依然有效，查找最接近的pref，使用相同的Score
	var near *core.JobPerf
	var minDist = 0.0
	for _, pf := range perfs {
		dist := math.Abs(p.TotProfit - pf.TotProfit)
		if near == nil || dist < minDist {
			near = pf
			minDist = dist
		}
	}
	p.Score = near.Score
	return p.Score
}

func CalcJobPerfs(cfg *config.StratPerfConfig, p *core.PerfSta, perfs []*core.JobPerf) {
	sumProfit := 0.0
	var profits = make([]float64, 0, len(perfs))
	for _, pf := range perfs {
		profits = append(profits, pf.TotProfit)
		sumProfit += math.Abs(pf.TotProfit)
	}
	//Logarithmic processing of returns to make clustering more accurate
	//Map the average positive rate of return to x=9 in log2, ensuring that the logarithmic processing results in an appropriate distribution divergence
	// 对收益率对数处理，使聚类更准确
	// 将平均正收益率固定映射为log2的x=9，确保对数处理后能在合适的分布散度
	avgProfit := sumProfit / float64(len(profits))
	p.Delta = 9 / avgProfit
	for i, val := range profits {
		profits[i] = p.Log2(val)
	}
	res := utils.KMeansVals(profits, 5)
	groups := res.Clusters
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].Center < groups[j].Center
	})
	var maxList = make([]float64, 0, len(groups))
	for _, gp := range groups {
		if len(gp.Items) > 0 {
			maxList = append(maxList, slices.Max(gp.Items))
		}
	}
	// K-means may yield fewer than 5 non-empty clusters when there are few jobs;
	// skip updating splits rather than indexing out of range.
	// 当job较少时K-means可能产生少于5个非空簇，此时跳过更新而不是越界访问
	if len(maxList) < 4 {
		return
	}
	// Calculate the upper and lower bounds of the rate of return for each group
	// 计算每组的收益率上下界
	p.Splits = &[4]float64{maxList[0], maxList[1], maxList[2], maxList[3]}
	p.LastGpAt = p.OdNum
	// Recalculate the weight of each job
	// 重新计算每个job的权重
	idxList := make([]int, 0, len(perfs))
	var totalAdd, goodNum, bestNum = 0.0, 0, 0
	for i, profit := range profits {
		pf := perfs[i]
		gid := p.FindGID(profit)
		if gid == 0 {
			pf.Score = core.PrefMinRate
			totalAdd += 1
		} else if gid == 1 {
			pf.Score = cfg.BadWeight
			totalAdd += 1 - cfg.BadWeight
		} else if gid == 2 {
			pf.Score = cfg.MidWeight
			totalAdd += 1 - cfg.MidWeight
		} else if gid == 3 {
			pf.Score = 1
			goodNum += 1
		} else if gid == 4 {
			pf.Score = 1
			bestNum += 1
		}
		idxList = append(idxList, gid)
	}
	if totalAdd > 0 {
		// Overlay the weight of losses onto profitable jobs
		// 将亏损的权重，叠加到盈利的job上
		totalWeight := float64(goodNum) + float64(bestNum)*2
		unitAdd := totalAdd / totalWeight
		for i, gid := range idxList {
			if gid < 3 {
				continue
			}
			pf := perfs[i]
			pf.Score += unitAdd * (float64(gid) - 2)
		}
	}
}

func AddOdSub(acc string, cb FnOdChange) {
	lockOdSub.Lock()
	subs, _ := accOdSubs[acc]
	accOdSubs[acc] = append(subs, cb)
	lockOdSub.Unlock()
}

func FireOdChange(acc string, od *ormo.InOutOrder, evt int) {
	lockOdSub.Lock()
	subs, _ := accOdSubs[acc]
	subs2, _ := accOdSubs["*"]
	lockOdSub.Unlock()
	fireOdChange(subs, subs2, acc, od, evt, nil)
}

// FireOdChangeWithState dispatches an order event through one runtime's
// strategy callbacks. A nil state keeps the legacy global behavior.
func FireOdChangeWithState(state *State, acc string, od *ormo.InOutOrder, evt int) {
	if state == nil {
		FireOdChange(acc, od, evt)
		return
	}
	state.orderSubLock.Lock()
	subs := append([]FnOdChange(nil), state.AccOdSubs[acc]...)
	subs2 := append([]FnOdChange(nil), state.AccOdSubs["*"]...)
	state.orderSubLock.Unlock()
	fireOdChange(subs, subs2, acc, od, evt, state.Clock)
}

func fireOdChange(subs, wildcard []FnOdChange, acc string, od *ormo.InOutOrder, evt int, clock *btime.ClockState) {
	subs = append(subs, wildcard...)
	// 将模拟时间置为事件触发时间，并备份当前时间
	evtTime := int64(0)
	if od == nil {
		return
	}
	if evt == OdChgEnter && od.Enter != nil {
		evtTime = od.Enter.CreateAt
	} else if evt == OdChgEnterFill && od.Enter != nil {
		evtTime = od.Enter.UpdateAt
	} else if evt == OdChgExit && od.Exit != nil {
		evtTime = od.Exit.CreateAt
	} else if evt == OdChgExitFill && od.Exit != nil {
		evtTime = od.Exit.UpdateAt
	}
	backMS := int64(0)
	if evtTime > 0 {
		if clock != nil {
			backMS = clock.TimeMS()
			clock.SetTimeMS(evtTime)
		} else {
			backMS = btime.TimeMS()
			btime.CurTimeMS = evtTime
		}
	}
	for _, cb := range subs {
		cb(acc, od, evt)
	}
	// 恢复原始时间
	if evtTime > 0 {
		if clock != nil {
			clock.SetTimeMS(backMS)
		} else {
			btime.CurTimeMS = backMS
		}
	}
}

func AddStratGroup(group string, items map[string]FuncMakeStrat) {
	stratMakeMu.Lock()
	defer stratMakeMu.Unlock()
	for k, v := range items {
		StratMake[group+":"+k] = v
	}
}

func snapshotStratFactories() map[string]FuncMakeStrat {
	stratMakeMu.RLock()
	defer stratMakeMu.RUnlock()
	factories := make(map[string]FuncMakeStrat, len(StratMake))
	for name, makeFn := range StratMake {
		factories[name] = makeFn
	}
	return factories
}

func (w Warms) Update(pair, tf string, num int) {
	if warms, ok := w[pair]; ok {
		if oldNum, ok := warms[tf]; ok {
			warms[tf] = max(oldNum, num)
		} else {
			warms[tf] = num
		}
	} else {
		w[pair] = map[string]int{tf: num}
	}
}

/*
JobForbidType 0 allow; 1 forbid; 2 forbid & occupy a slot
*/
func JobForbidType(pair, tf, stratID string) int {
	return jobForbidType(LegacyState(), pair, tf, stratID)
}

func jobForbidType(state *State, pair, tf, stratID string) int {
	if state == nil {
		state = LegacyState()
	}
	if jobs, ok := state.ForbidJobs[fmt.Sprintf("%s_%s", pair, tf)]; ok {
		hold, ok2 := jobs[stratID]
		if ok2 {
			if hold {
				return 2
			}
			return 1
		}
	}
	return 0
}

/*
GetJobKeys 获取已订阅的所有策略任务记录
return map[pair_tf][stratID]bool
*/
func GetJobKeys() map[string]map[string]bool {
	jobs := make(map[string]map[string]bool)
	for acc := range utils.MapKeys(AccJobs, config.StrictBacktest()) {
		jobsMap := AccJobs[acc]
		for pairTF, stgMap := range jobsMap {
			idMap := make(map[string]bool)
			for polID := range stgMap {
				idMap[polID] = true
			}
			jobs[pairTF] = idMap
		}
		break
	}
	return jobs
}

func AddAccFailOpen(acc, tag string) {
	AddAccFailOpens(acc, tag, 1)
}

func AddAccFailOpens(acc, tag string, num int) {
	addAccFailOpens(nil, acc, tag, num)
}

// AddAccFailOpensWithState records a failed-entry reason in one runtime's
// strategy state. The legacy helper above remains the package facade.
func AddAccFailOpensWithState(state *State, acc, tag string, num int) {
	if state == nil {
		AddAccFailOpens(acc, tag, num)
		return
	}
	state.AddAccFailOpens(acc, tag, num)
}

func addAccFailOpens(state *State, acc, tag string, num int) {
	if state != nil {
		state.AddAccFailOpens(acc, tag, num)
		return
	}
	lockAccFailOpen.Lock()
	tagMap, ok1 := accFailOpens[acc]
	if !ok1 {
		tagMap = make(map[string]int)
		accFailOpens[acc] = tagMap
	}
	count, _ := tagMap[tag]
	tagMap[tag] = count + num
	lockAccFailOpen.Unlock()
}

func DumpAccFailOpens() string {
	return dumpAccFailOpens(nil)
}

// DumpAccFailOpensWithState returns failed-entry counters owned by a runtime.
func DumpAccFailOpensWithState(state *State) string {
	return dumpAccFailOpens(state)
}

func dumpAccFailOpens(state *State) string {
	if state != nil {
		state.failOpenLock.Lock()
		defer state.failOpenLock.Unlock()
		return formatFailOpenCounts(state.AccFailOpens)
	}
	lockAccFailOpen.Lock()
	defer lockAccFailOpen.Unlock()
	return formatFailOpenCounts(accFailOpens)
}

func formatFailOpenCounts(counts map[string]map[string]int) string {
	var b strings.Builder
	isFirst := true
	for k, v := range counts {
		if isFirst {
			isFirst = false
		} else {
			b.WriteString("\n")
		}
		b.WriteString(k)
		b.WriteString(": ")
		b.WriteString(utils.MapToStr(v, true, 0))
	}
	return b.String()
}

func newAccStratLimits() (accStratLimits, int) {
	return newAccStratLimitsForState(nil)
}

func newAccStratLimitsForState(state *State) (accStratLimits, int) {
	res := make(accStratLimits)
	maxJobNum := 1
	accounts := runtimeAccountsFor(state)
	for acc, cfg := range accounts {
		if cfg == nil || cfg.NoTrade {
			continue
		}
		res[acc] = &stgLimits{
			limit:  cfg.MaxPair,
			strats: make(map[string]int),
		}
		if cfg.MaxPair == 0 {
			maxJobNum = 0
		} else if maxJobNum > 0 && cfg.MaxPair > maxJobNum {
			maxJobNum = cfg.MaxPair
		}
	}
	if maxJobNum == 0 {
		maxJobNum = 999
	}
	return res, maxJobNum
}

/*
尝试对某个策略增加计数
*/
func (l *stgLimits) tryAdd(name string) bool {
	num, _ := l.strats[name]
	if l.limit > 0 && num >= l.limit {
		return false
	}
	l.strats[name] = num + 1
	return true
}

/*
尝试对某个账户的某个策略增加计数
*/
func (a accStratLimits) tryAdd(acc, name string) bool {
	li, ok := a[acc]
	if ok {
		return li.tryAdd(name)
	}
	return true
}

/*
PrintStratGroups
print strategy+timeframe from `core.StgPairTfs`
从core.StgPairTfs输出策略+时间周期的币种信息到控制台
*/
func PrintStratGroups() {
	text := core.GroupByPairQuotes(map[string][]string{"Pairs": core.LegacyAdmissionPairs()}, false)
	log.Info("global pairs", zap.String("res", "\n"+text))
	for acc, jobMap := range AccJobs {
		allows := make(map[string][]string)
		disables := make(map[string][]string)
		for pairTF, stratMap := range jobMap {
			arrP := strings.Split(pairTF, "_")
			pair, tf := arrP[0], arrP[1]
			for stratID := range stratMap {
				key := fmt.Sprintf("%s_%s", stratID, tf)
				if core.LegacyPairEnabled(pair) {
					arr, _ := allows[key]
					allows[key] = append(arr, pair)
				} else {
					arr, _ := disables[key]
					disables[key] = append(arr, pair)
				}
			}
		}
		if len(allows) > 0 {
			text := core.GroupByPairQuotes(allows, true)
			log.Info("group jobs by strat_tf", zap.String("acc", acc), zap.String("res", "\n"+text))
		}
		if len(disables) > 0 {
			text := core.GroupByPairQuotes(disables, true)
			log.Info("group disable jobs by strat_tf", zap.String("acc", acc), zap.String("res", "\n"+text))
		}
	}
}

// PrintStratGroupsWithState is the typed logging view used by Runtime
// runners. It intentionally performs the same low-frequency formatting as
// PrintStratGroups while reading only the supplied strategy/core registries.
func PrintStratGroupsWithState(strategyState *State, coreState *core.State) {
	if strategyState == nil {
		PrintStratGroups()
		return
	}
	strategyState.ensureMaps()
	if coreState == nil && strategyState != legacyState {
		coreState = strategyState.Core
	}
	pairs := []string(nil)
	if coreState != nil {
		pairs = coreState.AdmissionPairs()
	} else {
		pairs = core.LegacyAdmissionPairs()
	}
	log.Info("global pairs", zap.String("res", "\n"+core.GroupByPairQuotes(map[string][]string{"Pairs": pairs}, false)))
	for _, acc := range strategyState.Accounts() {
		jobMap := strategyState.JobMapsView(acc)
		allows := make(map[string][]string)
		disables := make(map[string][]string)
		for pairTF, stratMap := range jobMap {
			arrP := strings.Split(pairTF, "_")
			if len(arrP) != 2 {
				continue
			}
			pair, tf := arrP[0], arrP[1]
			for stratID := range stratMap {
				key := fmt.Sprintf("%s_%s", stratID, tf)
				enabled := false
				if coreState != nil {
					enabled = coreState.PairEnabled(pair)
				} else {
					enabled = core.LegacyPairEnabled(pair)
				}
				if enabled {
					allows[key] = append(allows[key], pair)
				} else {
					disables[key] = append(disables[key], pair)
				}
			}
		}
		if len(allows) > 0 {
			log.Info("group jobs by strat_tf", zap.String("acc", acc), zap.String("res", "\n"+core.GroupByPairQuotes(allows, true)))
		}
		if len(disables) > 0 {
			log.Info("group disable jobs by strat_tf", zap.String("acc", acc), zap.String("res", "\n"+core.GroupByPairQuotes(disables, true)))
		}
	}
}

func GetJobInOutNum(job *StratJob) (int, int) {
	snapshot := job.ExecutionSnapshot()
	return len(snapshot.Entrys), len(snapshot.Exits)
}

func CheckJobInOutNum(job *StratJob, tag string, inNum, outNum int) {
	msg := "not support, please call `biz.GetOdMgr(s.Account).ProcessOrders(nil, s)` manually"
	snapshot := job.ExecutionSnapshot()
	if len(snapshot.Entrys) > inNum {
		log.Warn("OpenOrder "+msg, zap.String("method", tag))
	}
	if len(snapshot.Exits) > outNum {
		log.Warn("CloseOrder "+msg, zap.String("method", tag))
	}
}
