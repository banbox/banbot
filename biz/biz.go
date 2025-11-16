package biz

import (
	"context"
	"embed"
	_ "embed"
	"fmt"
	"github.com/banbox/banbot/com"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/sasha-s/go-deadlock"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/goods"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/rpc"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	utils2 "github.com/banbox/banexg/utils"
	ta "github.com/banbox/banta"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

//go:embed config.yml
var configData []byte

//go:embed config.local.yml
var configLocalData []byte

//go:embed zh-CN/*
var zhCNData embed.FS

//go:embed en-US/*
var enUSData embed.FS

func SetupComs(args *config.CmdArgs) *errs.Error {
	args.Init()
	if core.LiveMode {
		// 实时模式下启用死锁检测
		deadlock.Opts.Disable = false
	}
	errs.PrintErr = utils.PrintErr
	ctx, cancel := context.WithCancel(context.Background())
	core.Ctx = ctx
	core.StopAll = cancel
	err := InitDataDir()
	if err != nil {
		return err
	}
	err = config.LoadConfig(args)
	if err != nil {
		return err
	}
	var logCores []zapcore.Core
	if core.LiveMode {
		logCores = append(logCores, rpc.NewExcNotify())
		if args.Logfile == "" {
			args.Logfile = filepath.Join(config.GetLogsDir(), config.Name+".log")
		}
	}
	args.SetLog(true, logCores...)
	err = core.Setup()
	if err != nil {
		return err
	}
	if config.Mail != nil && config.Mail.Enable {
		c := config.Mail
		utils.SetMailSender(c.Host, c.Port, c.Username, c.Password)
	}
	config.LoadLangMessages()
	err = utils.Setup()
	if err != nil {
		return err
	}
	err = exg.Setup()
	if err != nil {
		return err
	}
	err = orm.Setup()
	if err != nil {
		return err
	}
	err = goods.Setup()
	if err != nil {
		return err
	}
	return nil
}

func SetupComsExg(args *config.CmdArgs) *errs.Error {
	err := SetupComs(args)
	if err != nil {
		return err
	}
	return orm.InitExg(exg.Default)
}

func RefreshPairs(showLog bool, timeMS int64, pBar *utils.StagedPrg) ([]string, map[string]map[string]float64, *errs.Error) {
	goods.ShowLog = showLog
	pairs, err := goods.RefreshPairList(timeMS)
	if err != nil {
		return nil, nil, err
	}
	if pBar != nil {
		pBar.SetProgress("loadPairs", 1)
	}
	allPairs := make([]string, 0, len(pairs))
	allPairs = append(allPairs, pairs...)
	for _, r := range config.RunPolicy {
		if len(r.Pairs) > 0 {
			allPairs = append(allPairs, r.Pairs...)
		}
	}
	allPairs, _ = utils.UniqueItems(allPairs)
	pairTfScores, err := strat.CalcPairTfScores(exg.Default, allPairs)
	if err != nil {
		return nil, nil, err
	}
	if pBar != nil {
		pBar.SetProgress("tfScores", 1)
	}
	return pairs, pairTfScores, nil
}

func RefreshJobs(pairs []string, pairTfScores map[string]map[string]float64, showLog bool, pBar *utils.StagedPrg) (map[string]map[string]int, *errs.Error) {
	warms, accOds, err := strat.LoadStratJobs(pairs, pairTfScores)
	if err != nil {
		return nil, err
	}
	if len(accOds) > 0 {
		for acc, odList := range accOds {
			odMgr := GetOdMgr(acc)
			err = odMgr.ExitAndFill(odList, &strat.ExitReq{Tag: core.ExitTagPairDel})
			if err != nil {
				return nil, err
			}
			log.Info("exit old orders as pair rotation", zap.Int("num", len(odList)))
		}
	}
	if showLog {
		strat.PrintStratGroups()
	}
	if pBar != nil {
		pBar.SetProgress("loadJobs", 1)
	}
	return warms, nil
}

/*
InitOdSubs 为所有策略OnOrderChange注册订单事件监听。

只需在LoadStratJobs后调用一次，交易的Accounts不变就始终生效
*/
func InitOdSubs() {
	// 这里只调用成员函数，不读取变量，所以每个策略只存储一个实例即可
	var subStgys = map[string]*strat.TradeStrat{}
	for _, items := range strat.PairStrats {
		for stgName, stagy := range items {
			if stagy.OnOrderChange != nil || stagy.HedgeOff {
				subStgys[stgName] = stagy
			}
		}
	}
	if len(subStgys) == 0 {
		return
	}
	for acc := range strat.AccJobs {
		strat.AddOdSub(acc, func(acc string, od *ormo.InOutOrder, evt int) {
			stgy, ok := subStgys[od.Strategy]
			if !ok {
				// The current strategy does not monitor order status
				// 当前策略未监听订单状态
				return
			}
			items, _ := strat.AccJobs[acc]
			if len(items) == 0 {
				return
			}
			pairTF := strings.Join([]string{od.Symbol, od.Timeframe}, "_")
			its, _ := items[pairTF]
			if len(its) == 0 {
				return
			}
			job, _ := its[od.Strategy]
			if job != nil {
				if stgy.HedgeOff && evt == strat.OdChgEnterFill {
					// 策略为单向持仓，成交时尝试关闭另一侧订单
					closeSideOrders(job, !od.Short)
				}
				if stgy.OnOrderChange != nil {
					if evt == strat.OdChgExitFill {
						openOds, lock := ormo.GetOpenODs(acc)
						lock.Lock()
						job.UpdateOrders(utils.ValsOfMap(openOds))
						lock.Unlock()
					}
					stgy.OnOrderChange(job, od, evt)
				}
				if len(job.Entrys) > 0 || len(job.Exits) > 0 {
					_, _, err := GetOdMgr(acc).ProcessOrders(job)
					if err != nil {
						log.Error("process orders fail", zap.Error(err))
					}
				}
			}
		})
	}
}

func closeSideOrders(s *strat.StratJob, isShort bool) {
	var closeList = s.LongOrders
	if isShort {
		closeList = s.ShortOrders
	}
	if len(closeList) > 0 {
		for _, o := range closeList {
			if o.Status >= ormo.InOutStatusPartEnter && o.Status <= ormo.InOutStatusPartExit {
				_ = s.CloseOrders(&strat.ExitReq{
					Tag:     core.ExitTagHedgeOff,
					OrderID: o.ID,
					Force:   true,
					Log:     true,
				})
			}
		}
	}
}

// Preventing Concurrent Modification of BatchTasks
var lockBatch = deadlock.Mutex{} // 防止并发修改BatchTasks

/*
AddBatchJob
Add batch entry tasks.
Even if the job has no entry tasks, this method should be called to postpone the entry time TFEnterMS
添加批量入场任务。
即使job没有入场任务，也应该调用此方法，用于推迟入场时间TFEnterMS
*/
func AddBatchJob(account, tf string, job *strat.StratJob, infoEnv *ta.BarEnv) {
	lockBatch.Lock()
	defer lockBatch.Unlock()
	key := fmt.Sprintf("%s_%s_%s", tf, account, job.Strat.Name)
	tasks, ok := strat.BatchTasks[key]
	if !ok {
		tasks = &strat.BatchMap{
			Map:     make(map[string]*strat.JobEnv),
			TFMSecs: int64(utils2.TFToSecs(tf) * 1000),
		}
		strat.BatchTasks[key] = tasks
	}
	// Delay 3s to wait for execution
	// 推迟3s等待执行
	tasks.ExecMS = btime.TimeMS() + core.DelayBatchMS
	var pair = job.Symbol.Symbol
	var pairKey = pair + "_main"
	if infoEnv != nil {
		pair = infoEnv.Symbol
		pairKey = pair + "_info"
	}
	tasks.Map[pairKey] = &strat.JobEnv{Job: job, Env: infoEnv, Symbol: pair}
}

func TryFireBatches(currMS int64, isWarmUp bool) int {
	lockBatch.Lock()
	defer lockBatch.Unlock()
	var err *errs.Error
	var waitNum = 0
	for key, tasks := range strat.BatchTasks {
		if currMS < tasks.ExecMS {
			if tasks.ExecMS-currMS < tasks.TFMSecs/2 {
				// Batch processing time has not yet arrived
				// 尚未到达批量处理时间
				waitNum += 1
			}
			continue
		}
		var mainJobs []*strat.StratJob
		var infoJobs = make(map[string]*strat.JobEnv)
		var stgy *strat.TradeStrat
		for _, task := range tasks.Map {
			stgy = task.Job.Strat
			if task.Env == nil {
				mainJobs = append(mainJobs, task.Job)
			} else {
				infoJobs[task.Symbol] = task
			}
		}
		if stgy == nil {
			continue
		}
		arr := strings.Split(key, "_")
		timeframe, account := arr[0], arr[1]
		openOds, lock := ormo.GetOpenODs(account)
		lock.Lock()
		allOrders := utils.ValsOfMap(openOds)
		lock.Unlock()
		delete(strat.BatchTasks, key)
		for _, job := range mainJobs {
			job.InitBar(allOrders)
		}
		if len(infoJobs) > 0 {
			num1, num2 := 0, 0
			for _, j := range infoJobs {
				num1 += len(j.Job.Entrys)
				num2 += len(j.Job.Exits)
			}
			stgy.OnBatchInfos(timeframe, infoJobs)
			num3, num4 := 0, 0
			for _, j := range infoJobs {
				num3 += len(j.Job.Entrys)
				num4 += len(j.Job.Exits)
			}
			if num3 > num1 || num4 > num2 {
				log.Warn("Open/Close order in OnBatchInfos not support, please call `biz.GetOdMgr(s.Account).ProcessOrders(nil, s)` manually")
			}
		}
		if len(mainJobs) > 0 {
			// Check all batch tasks at this time and decide which ones to enter or exit
			// 检查此时间所有批量任务，决定哪些入场或那些出场
			stgy.OnBatchJobs(mainJobs)
			// Perform entry/exit tasks
			// 执行入场/出场任务
			if !isWarmUp {
				odMgr := GetOdMgr(account)
				for _, job := range mainJobs {
					_, _, err = odMgr.ProcessOrders(job)
					if err != nil {
						log.Error("process orders fail", zap.Error(err))
					}
				}
			}
		}
	}
	return waitNum
}

func ResetVars() {
	core.NoEnterUntil = make(map[string]int64)
	com.DelPairCopieds()
	core.TfPairHits = make(map[string]map[string]int)
	core.JobPerfs = make(map[string]*core.JobPerf)
	core.StratPerfSta = make(map[string]*core.PerfSta)
	accLiveOdMgrs = make(map[string]*LiveOrderMgr)
	accOdMgrs = make(map[string]IOrderMgr)
	accWallets = make(map[string]*BanWallets)
	core.LastBarMs = 0
	core.OdBooks = make(map[string]*banexg.OrderBook)
	ormo.HistODs = make([]*ormo.InOutOrder, 0)
	ormo.ResetVars()
	strat.Envs = make(map[string]*ta.BarEnv)
	strat.TmpEnvs = make(map[string]*ta.BarEnv)
	strat.AccJobs = make(map[string]map[string]map[string]*strat.StratJob)
	strat.AccInfoJobs = make(map[string]map[string]map[string]*strat.StratJob)
	strat.PairStrats = make(map[string]map[string]*strat.TradeStrat)
	strat.WsSubJobs = make(map[string]map[string]map[*strat.StratJob]bool)
	strat.BatchTasks = make(map[string]*strat.BatchMap)
	strat.ForbidJobs = make(map[string]map[string]bool)
	strat.LastBatchMS = 0
}

type VarsBackup struct {
	Pairs         []string
	PairMap       map[string]bool
	NoEnterUntil  map[string]int64
	PairCopiedMs  map[string][2]int64
	TfPairHits    map[string]map[string]int
	JobPerfs      map[string]*core.JobPerf
	StratPerfSta  map[string]*core.PerfSta
	AccLiveOdMgrs map[string]*LiveOrderMgr
	AccOdMgrs     map[string]IOrderMgr
	AccWallets    map[string]*BanWallets
	LastBarMs     int64
	OdBooks       map[string]*banexg.OrderBook
	HistODs       []*ormo.InOutOrder
	Envs          map[string]*ta.BarEnv
	TmpEnvs       map[string]*ta.BarEnv
	AccJobs       map[string]map[string]map[string]*strat.StratJob
	AccInfoJobs   map[string]map[string]map[string]*strat.StratJob
	PairStrats    map[string]map[string]*strat.TradeStrat
	WsSubJobs     map[string]map[string]map[*strat.StratJob]bool
	BatchTasks    map[string]*strat.BatchMap
	ForbidJobs    map[string]map[string]bool
	LastBatchMS   int64
	OrmoBackup    *ormo.VarsBackup
}

// BackupVars 备份所有全局变量
func BackupVars() *VarsBackup {
	return &VarsBackup{
		Pairs:         slices.Clone(core.Pairs),
		PairMap:       maps.Clone(core.PairsMap),
		NoEnterUntil:  core.NoEnterUntil,
		PairCopiedMs:  com.GetPairCopieds(),
		TfPairHits:    core.TfPairHits,
		JobPerfs:      core.JobPerfs,
		StratPerfSta:  core.StratPerfSta,
		AccLiveOdMgrs: accLiveOdMgrs,
		AccOdMgrs:     accOdMgrs,
		AccWallets:    accWallets,
		LastBarMs:     core.LastBarMs,
		OdBooks:       core.OdBooks,
		HistODs:       ormo.HistODs,
		Envs:          strat.Envs,
		TmpEnvs:       strat.TmpEnvs,
		AccJobs:       strat.AccJobs,
		AccInfoJobs:   strat.AccInfoJobs,
		PairStrats:    strat.PairStrats,
		WsSubJobs:     strat.WsSubJobs,
		BatchTasks:    strat.BatchTasks,
		ForbidJobs:    strat.ForbidJobs,
		LastBatchMS:   strat.LastBatchMS,
		OrmoBackup:    ormo.BackupVars(),
	}
}

// RestoreVars 从备份中恢复所有全局变量
func RestoreVars(backup *VarsBackup) {
	if backup == nil {
		return
	}
	core.Pairs = backup.Pairs
	core.PairsMap = backup.PairMap
	core.NoEnterUntil = backup.NoEnterUntil
	com.DelPairCopieds()
	com.SetPairCopieds(backup.PairCopiedMs)
	core.TfPairHits = backup.TfPairHits
	core.JobPerfs = backup.JobPerfs
	core.StratPerfSta = backup.StratPerfSta
	accLiveOdMgrs = backup.AccLiveOdMgrs
	accOdMgrs = backup.AccOdMgrs
	accWallets = backup.AccWallets
	core.LastBarMs = backup.LastBarMs
	core.OdBooks = backup.OdBooks
	ormo.HistODs = backup.HistODs
	strat.Envs = backup.Envs
	strat.TmpEnvs = backup.TmpEnvs
	strat.AccJobs = backup.AccJobs
	strat.AccInfoJobs = backup.AccInfoJobs
	strat.PairStrats = backup.PairStrats
	strat.WsSubJobs = backup.WsSubJobs
	strat.BatchTasks = backup.BatchTasks
	strat.ForbidJobs = backup.ForbidJobs
	strat.LastBatchMS = backup.LastBatchMS
	ormo.RestoreVars(backup.OrmoBackup)
}

func replaceDockerHosts(data []byte) []byte {
	if !utils.IsDocker() {
		return data
	}
	content := string(data)
	content = strings.ReplaceAll(content, "127.0.0.1", "host.docker.internal")
	content = strings.ReplaceAll(content, "localhost", "host.docker.internal")
	return []byte(content)
}

func InitDataDir() *errs.Error {
	dataDir := config.GetDataDir()
	err_ := utils.EnsureDir(dataDir, 0755)
	if dataDir == "" {
		return errs.NewMsg(errs.CodeParamRequired, "-datadir or env `BanDataDir` is required")
	}
	if err_ != nil {
		return errs.New(errs.CodeIOWriteFail, err_)
	}
	configPath := filepath.Join(dataDir, "config.yml")
	configLocalPath := filepath.Join(dataDir, "config.local.yml")
	if !utils.Exists(configPath) && !utils.Exists(configLocalPath) {
		// dont init config in dataDir if any of config.yml/config.local.yml exist
		err := utils.WriteFile(configPath, replaceDockerHosts(configData))
		if err != nil {
			return err
		}
		log.Info("init done", zap.String("p", configPath))
		err = utils.WriteFile(configLocalPath, replaceDockerHosts(configLocalData))
		log.Info("init done", zap.String("p", configLocalPath))
		if err != nil {
			return err
		}
	}

	// 初始化语言文件
	for _, lang := range []string{"zh-CN", "en-US"} {
		err := initLangFile(dataDir, lang)
		if err != nil {
			return err
		}
	}

	return nil
}

func initLangFile(dataDir, lang string) *errs.Error {
	langDir := filepath.Join(dataDir, lang)
	err_ := utils.EnsureDir(langDir, 0755)
	if err_ != nil {
		return errs.New(errs.CodeIOWriteFail, err_)
	}

	// 获取对应的嵌入文件系统
	var embedFS embed.FS
	if lang == "zh-CN" {
		embedFS = zhCNData
	} else if lang == "en-US" {
		embedFS = enUSData
	} else {
		return nil // 不支持的语言，跳过
	}

	// 读取嵌入文件系统中的所有文件
	entries, err_ := embedFS.ReadDir(lang)
	if err_ != nil {
		return errs.New(errs.CodeRunTime, err_)
	}

	// 遍历所有文件
	for _, entry := range entries {
		if entry.IsDir() {
			continue // 跳过目录
		}

		fileName := entry.Name()
		sourcePath := path.Join(lang, fileName)
		targetPath := filepath.Join(langDir, fileName)

		// 从嵌入文件系统读取文件内容
		sourceData, err_ := embedFS.ReadFile(sourcePath)
		if err_ != nil {
			log.Warn("failed to read embedded file", zap.String("file", sourcePath), zap.Error(err_))
			continue
		}

		// 对于 messages.json 文件，进行合并更新
		if fileName == "messages.json" {
			err := mergeMessagesFile(targetPath, sourceData)
			if err == nil {
				continue
			}
		}
		err := utils.WriteFile(targetPath, sourceData)
		if err != nil {
			return err
		}
	}

	return nil
}

// mergeMessagesFile 合并更新 messages.json 文件
func mergeMessagesFile(targetPath string, sourceData []byte) *errs.Error {
	// 读取目标文件
	targetData, err_ := os.ReadFile(targetPath)
	if err_ != nil {
		return errs.New(errs.CodeIOReadFail, err_)
	}

	var sourceMap, targetMap map[string]string
	if err := utils2.UnmarshalString(string(sourceData), &sourceMap, utils2.JsonNumDefault); err != nil {
		return errs.New(errs.CodeUnmarshalFail, err)
	}
	if err := utils2.UnmarshalString(string(targetData), &targetMap, utils2.JsonNumDefault); err != nil {
		return errs.New(errs.CodeUnmarshalFail, err)
	}

	updated := false
	for key, value := range sourceMap {
		if _, exists := targetMap[key]; !exists {
			targetMap[key] = value
			updated = true
		}
	}

	if updated {
		newData, err_ := utils2.MarshalString(targetMap)
		if err_ != nil {
			return errs.New(errs.CodeMarshalFail, err_)
		}
		err := utils.WriteFile(targetPath, []byte(newData))
		if err != nil {
			return err
		}
		log.Info("updated messages file", zap.String("file", targetPath))
	}

	return nil
}
