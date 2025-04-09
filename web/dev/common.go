package dev

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/banbox/banbot/orm/ormo"

	utils2 "github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"

	"github.com/banbox/banbot/core"

	"github.com/banbox/banexg/log"
	"go.uber.org/zap"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/orm/ormu"
	"github.com/banbox/banexg/utils"
)

type CmdArgs struct {
	Port     int
	Host     string
	Configs  config.ArrString
	DataDir  string
	LogLevel string
	LogFile  string
	TimeZone string
	DBFile   string
}

var (
	btInfoKeyList = []string{"maxOpenOrders", "showDrawDownPct", "barNum", "maxDrawDownVal", "showDrawDownVal", "totalInvest",
		"totProfit", "totCost", "totFee", "totProfitPct", "sortinoRatio"}
	btInfoKeys      = make(map[string]bool)
	maxBtTasks      = 3 // 最大并发回测任务数
	runBtTasks      = make(map[int64]*exec.Cmd)
	runBtTasksMutex sync.Mutex

	// 缓存回测任务订单到内存，加速分页查看。
	cacheOrders []*ormo.InOutOrder
	cachePath   string
	ordersLock  sync.Mutex
)

func init() {
	for _, k := range btInfoKeyList {
		btInfoKeys[k] = true
	}
}

func getGobOrders(path string) ([]*ormo.InOutOrder, *sync.Mutex, *errs.Error) {
	ordersLock.Lock()
	defer ordersLock.Unlock()
	if cachePath == path {
		return cacheOrders, &ordersLock, nil
	}
	orders, err := ormo.LoadOrdersGob(path)
	if err != nil {
		return nil, nil, err
	}
	cacheOrders = orders
	cachePath = path
	return cacheOrders, &ordersLock, nil
}

// 执行单个回测任务
func executeBtTask(task *ormu.Task) {
	defer func() {
		runBtTasksMutex.Lock()
		delete(runBtTasks, task.ID)
		runBtTasksMutex.Unlock()
	}()

	// 获取当前执行文件路径
	exePath, err := os.Executable()
	if err != nil {
		log.Error("get executable path failed", zap.Error(err))
		return
	}

	// 构建命令
	cmdArgsStr := "backtest " + task.Args
	cmd := exec.Command(exePath, strings.Split(cmdArgsStr, " ")...)
	cmd.Env = append(cmd.Env, "BanDataDir="+config.GetDataDir())
	cmd.Env = append(cmd.Env, "BanStratDir="+config.GetStratDir())

	// 添加到运行列表
	runBtTasksMutex.Lock()
	runBtTasks[task.ID] = cmd
	runBtTasksMutex.Unlock()

	qu, conn, err2 := ormu.Conn()
	if err2 != nil {
		log.Error("connect to db failed", zap.Error(err2))
		return
	}
	err = updateTaskStatus(qu, task.ID, int64(ormu.BtStatusRunning), 0)
	_ = conn.Close()
	if err != nil {
		return
	}

	err = runBtCommand(cmd, task)

	// 收集并更新任务结果
	updateBtTaskResult(task, err)
}

// 更新任务状态
func updateTaskStatus(qu *ormu.Queries, taskID int64, status int64, progress float64) error {
	err := qu.UpdateTask(context.Background(), ormu.UpdateTaskParams{
		Status:   status,
		Progress: progress,
		ID:       taskID,
	})
	if err != nil {
		log.Error("update task status failed", zap.Error(err))
	}
	return err
}

// 执行回测命令并处理输出
func runBtCommand(cmd *exec.Cmd, task *ormu.Task) error {
	stdOut, err := cmd.StdoutPipe()
	if err != nil {
		log.Error("get stdout failed", zap.Error(err))
		return err
	}
	defer stdOut.Close()

	stdErr, err := cmd.StderrPipe()
	if err != nil {
		log.Error("get stderr failed", zap.Error(err))
		return err
	}
	defer stdErr.Close()

	log.Info("start backtest", zap.Int64("id", task.ID), zap.String("args", task.Args))
	if err := cmd.Start(); err != nil {
		log.Error("start backtest fail", zap.Error(err))
		return err
	}

	// 处理输出
	var b strings.Builder
	var wg sync.WaitGroup
	wg.Add(2)

	// 处理标准输出
	go func() {
		defer wg.Done()
		scanner := readScanner(stdOut)
		prefix := "uiPrg: "
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, prefix) {
				if err := handleProgress(line[len(prefix):], task.ID); err != nil {
					log.Error("handle progress failed", zap.Error(err))
				}
			} else {
				b.WriteString(line)
				b.WriteString("\n")
			}
		}
		if err := scanner.Err(); err != nil {
			log.Error("stdout scanner error", zap.Error(err))
		}
	}()

	// 处理错误输出
	go func() {
		defer wg.Done()
		scanner := readScanner(stdErr)
		for scanner.Scan() {
			b.WriteString(scanner.Text())
			b.WriteString("\n")
		}
		if err := scanner.Err(); err != nil {
			log.Error("stderr scanner error", zap.Error(err))
		}
	}()

	// 等待所有输出处理完成
	wg.Wait()

	// 等待命令执行完成
	err = cmd.Wait()
	BroadcastWS("", map[string]interface{}{
		"type":     "btPrg",
		"taskId":   task.ID,
		"progress": 1,
	})
	if err != nil {
		log.Error("run backtest failed", zap.Int64("task", task.ID), zap.String("args", task.Args),
			zap.String("path", task.Path), zap.String("output", b.String()), zap.Error(err))
	} else {
		log.Info("done backtest", zap.Int64("id", task.ID), zap.String("args", task.Args))
	}

	return err
}

func readScanner(out io.ReadCloser) *bufio.Scanner {
	scanner := bufio.NewScanner(out)
	buf := make([]byte, 1024*1024)
	scanner.Buffer(buf, 1024*1024)
	return scanner
}

// 处理进度更新
func handleProgress(progressStr string, taskID int64) error {
	qu, conn, err2 := ormu.Conn()
	if err2 != nil {
		return err2
	}
	defer conn.Close()
	prgVal, err := strconv.ParseFloat(progressStr, 64)
	if err != nil {
		log.Warn("invalid progress", zap.String("progress", progressStr))
		return err
	}
	BroadcastWS("", map[string]interface{}{
		"type":     "btPrg",
		"taskId":   taskID,
		"progress": prgVal,
	})
	return updateTaskStatus(qu, taskID, int64(ormu.BtStatusRunning), prgVal)
}

// 更新回测任务结果
func updateBtTaskResult(task *ormu.Task, errTask error) {
	qu, conn, err2 := ormu.Conn()
	if err2 != nil {
		log.Error("get dev conn fail", zap.Error(err2))
		return
	}
	defer conn.Close()
	btRoot := fmt.Sprintf("%s/backtest", config.GetDataDir())
	taskRes, err := collectBtTask(btRoot, task.Path)
	if err != nil {
		var errMsg string
		if errTask != nil {
			errMsg = errTask.Error()
		} else {
			errMsg = err.Error()
		}
		log.Error("collect backtest task failed", zap.Error(err))
		err = qu.UpdateTask(context.Background(), ormu.UpdateTaskParams{
			Status:   int64(ormu.BtStatusFail),
			Progress: 1,
			Info:     errMsg,
			ID:       task.ID,
		})
		if err != nil {
			log.Error("update task status fail", zap.Error(err))
		}
		return
	}
	if taskRes == nil {
		taskRes = &ormu.Task{
			Status: ormu.BtStatusFail,
		}
	}
	if taskRes.Status < ormu.BtStatusDone {
		taskRes.Status = ormu.BtStatusDone
	}
	err = qu.UpdateTask(context.Background(), ormu.UpdateTaskParams{
		Status:      taskRes.Status,
		Progress:    1,
		OrderNum:    taskRes.OrderNum,
		ProfitRate:  taskRes.ProfitRate,
		WinRate:     taskRes.WinRate,
		MaxDrawdown: taskRes.MaxDrawdown,
		Sharpe:      taskRes.Sharpe,
		Info:        taskRes.Info,
		ID:          task.ID,
	})
	if err != nil {
		log.Error("update task status failed", zap.Error(err))
	}
}

// 启动后台任务处理
func startBtTaskScheduler() {
	go func() {
		for {
			time.Sleep(300 * time.Millisecond)

			runBtTasksMutex.Lock()
			runningCount := len(runBtTasks)
			runBtTasksMutex.Unlock()

			if runningCount >= maxBtTasks {
				continue
			}

			// 获取待执行的任务
			qu, conn, err := ormu.Conn()
			if err != nil {
				log.Error("connect to db failed", zap.Error(err))
				continue
			}

			tasks, err := qu.FindTasks(context.Background(), ormu.FindTasksParams{
				Mode:   "backtest",
				Status: int64(ormu.BtStatusInit),
				Limit:  1,
			})
			_ = conn.Close()
			if err != nil {
				log.Warn("find tasks failed", zap.Error(err))
				continue
			}

			if len(tasks) == 0 {
				continue
			}

			task := tasks[0]
			runBtTasksMutex.Lock()
			_, exist := runBtTasks[task.ID]
			if !exist {
				runBtTasks[task.ID] = nil
			}
			runBtTasksMutex.Unlock()

			if exist {
				continue
			}

			// 启动新的回测任务
			go executeBtTask(task)
		}
	}()
}

func collectBtResults() error {
	qu, conn, err2 := ormu.Conn()
	if err2 != nil {
		return err2
	}
	defer conn.Close()
	tasks, err2 := qu.FindTasks(context.Background(), ormu.FindTasksParams{
		Mode:  "backtest",
		Limit: 1000,
	})
	if err2 != nil {
		return err2
	}
	taskMap := make(map[string]*ormu.Task)
	for _, t := range tasks {
		taskMap[t.Path] = t
	}

	addNum, delNum := 0, 0
	btRoot := fmt.Sprintf("%s/backtest", config.GetDataDir())
	err := utils2.EnsureDir(btRoot, 0755)
	if err != nil {
		return err
	}
	err = filepath.Walk(btRoot, func(fullPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() || fullPath == btRoot {
			return nil
		}

		relPath, err := filepath.Rel(btRoot, fullPath)
		if err != nil {
			return err
		}
		if _, ok := taskMap[relPath]; ok {
			delete(taskMap, relPath)
			return nil
		}

		task, err := collectBtTask(btRoot, relPath)
		if err != nil || task == nil {
			return err
		}

		_, err = qu.AddTask(context.Background(), ormu.AddTaskParams{
			Mode:        task.Mode,
			Path:        task.Path,
			Strats:      task.Strats,
			Periods:     task.Periods,
			Pairs:       task.Pairs,
			CreateAt:    task.CreateAt,
			StartAt:     task.StartAt,
			StopAt:      task.StopAt,
			Status:      task.Status,
			Progress:    task.Progress,
			OrderNum:    task.OrderNum,
			ProfitRate:  task.ProfitRate,
			WinRate:     task.WinRate,
			MaxDrawdown: task.MaxDrawdown,
			Sharpe:      task.Sharpe,
			Info:        task.Info,
		})
		addNum += 1
		return err
	})
	if err != nil {
		return err
	}
	if len(taskMap) > 0 {
		delIds := make([]int64, 0, len(taskMap))
		for _, t := range taskMap {
			delIds = append(delIds, t.ID)
		}
		delNum = len(delIds)
		err = qu.DelTasks(context.Background(), delIds)
	}

	log.Info("collect backtest tasks", zap.Int("add", addNum), zap.Int("del", delNum))

	return err
}

func collectBtTask(rootDir, relPath string) (*ormu.Task, error) {
	btDir := filepath.Join(rootDir, relPath)
	fileInfo, err := os.Stat(filepath.Join(btDir, "assets.html"))
	if os.IsNotExist(err) {
		return nil, nil
	}

	// 读取并解析 detail.json
	detailPath := filepath.Join(btDir, "detail.json")
	detailBytes, err := os.ReadFile(detailPath)
	if err != nil {
		return nil, nil // 如果detail.json不存在则跳过
	}

	var data = make(map[string]interface{})
	if err = utils.Unmarshal(detailBytes, &data, utils.JsonNumDefault); err != nil {
		return nil, nil
	}

	cfg, err2 := config.GetConfig(&config.CmdArgs{
		Configs:   []string{filepath.Join(btDir, "config.yml")},
		NoDefault: true,
	}, false)
	if err2 != nil {
		return nil, err2
	}

	d := make(map[string]interface{})
	for k := range btInfoKeys {
		d[k] = data[k]
	}
	d["leverage"] = cfg.Leverage
	walletTot := float64(0)
	for code, amt := range cfg.WalletAmounts {
		walletTot += core.GetPriceSafe(code) * amt
	}
	d["walletAmount"] = walletTot
	d["stakeAmount"] = cfg.StakeAmount
	infoText, err := utils.MarshalString(d)
	if err != nil {
		return nil, err
	}
	dayMSecs := int64(utils.TFToSecs("1d") * 1000)
	createMS := int64(utils.GetMapVal(data, "createMS", float64(0)))
	if createMS == 0 {
		createMS = fileInfo.ModTime().UnixMilli()
	}
	return &ormu.Task{
		Mode:        "backtest",
		Path:        relPath,
		Strats:      strings.Join(cfg.Strats(), ","),
		Periods:     strings.Join(cfg.TimeFrames(), ","),
		Pairs:       cfg.ShowPairs(),
		CreateAt:    createMS,
		StartAt:     utils.AlignTfMSecs(cfg.TimeRange.StartMS, dayMSecs),
		StopAt:      utils.AlignTfMSecs(cfg.TimeRange.EndMS, dayMSecs),
		Status:      ormu.BtStatusDone,
		Progress:    1,
		OrderNum:    int64(utils.GetMapVal(data, "orderNum", float64(0))),
		ProfitRate:  utils.GetMapVal(data, "totProfitPct", float64(0)),
		WinRate:     utils.GetMapVal(data, "winRatePct", float64(0)),
		MaxDrawdown: utils.GetMapVal(data, "maxDrawDownPct", float64(0)),
		Sharpe:      utils.GetMapVal(data, "sharpeRatio", float64(0)),
		Info:        infoText,
	}, nil
}

func MergeConfig(inText string, skips ...string) (string, error) {
	dataDir := config.GetDataDir()
	if dataDir == "" {
		return "", errs.NewMsg(errs.CodeParamRequired, "-datadir is empty")
	}
	tryNames := []string{"config.yml", "config.local.yml"}
	var paths []string
	for _, name := range tryNames {
		path := filepath.Join(dataDir, name)
		if _, err := os.Stat(path); err == nil {
			paths = append(paths, path)
		}
	}
	if config.Args != nil && len(config.Args.Configs) > 0 {
		paths = append(paths, config.Args.Configs...)
	}
	if inText != "" {
		tmp, err := os.CreateTemp(os.TempDir(), "tmp_cfg")
		if err != nil {
			return "", err
		}
		defer os.Remove(tmp.Name())
		tmp.WriteString(inText)
		paths = append(paths, tmp.Name())
	}
	return MergeConfigPaths(paths, skips...)
}

func MergeConfigPaths(paths []string, skips ...string) (string, error) {
	var content string
	var err error
	if len(paths) > 1 {
		content, err = utils2.MergeYamlStr(paths, skips...)
		if err != nil {
			return "", err
		}
	} else if len(paths) == 1 {
		var data []byte
		data, err = os.ReadFile(paths[0])
		if err != nil {
			return "", err
		}
		content = string(data)
	}
	return content, nil
}
