package dev

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/banbox/banbot/com"
	"go.uber.org/zap"

	"github.com/banbox/banbot/orm/ormo"

	utils2 "github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"

	"github.com/banbox/banexg/log"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/orm/ormu"
	"github.com/banbox/banexg/utils"
)

type CmdArgs struct {
	Port       int
	Host       string
	Configs    config.ArrString
	ConfigData string
	DataDir    string
	LogLevel   string
	LogFile    string
	TimeZone   string
	Password   string
}

func validateWebAuth(host, password string) error {
	ip := net.ParseIP(strings.Trim(host, "[]"))
	if ip != nil && ip.IsUnspecified() && password == "" {
		return fmt.Errorf("web password is required when binding to %s", host)
	}
	return nil
}

var (
	btInfoKeyList = []string{"maxOpenOrders", "showDrawDownPct", "barNum", "maxDrawDownVal", "showDrawDownVal", "totalInvest",
		"totProfit", "totCost", "totFee", "totProfitPct", "sortinoRatio"}
	btInfoKeys = make(map[string]bool)
	maxBtTasks = 3 // 最大并发回测任务数

)

type taskStatusInfo struct {
	status       int64
	progress     float64
	lastUpdateAt time.Time
}

func init() {
	for _, k := range btInfoKeyList {
		btInfoKeys[k] = true
	}
}

func (s *DevServer) getGobOrders(path string) ([]*ormo.InOutOrder, *errs.Error) {
	s.ordersMu.Lock()
	defer s.ordersMu.Unlock()
	if s.cachePath == path {
		return append([]*ormo.InOutOrder(nil), s.cacheOrders...), nil
	}
	orders, err := ormo.LoadOrdersGob(path)
	if err != nil {
		return nil, err
	}
	s.cacheOrders = orders
	s.cachePath = path
	return append([]*ormo.InOutOrder(nil), s.cacheOrders...), nil
}

// 执行单个回测任务
func (s *DevServer) executeBtTask(task *ormu.Task) {
	defer func() {
		s.taskMu.Lock()
		delete(s.runningBtTasks, task.ID)
		s.taskMu.Unlock()
		s.wg.Done()
	}()

	// 获取当前执行文件路径
	exePath, err := os.Executable()
	if err != nil {
		log.Error("get executable path failed", zap.Error(err))
		return
	}

	// 构建命令
	cmdArgsStr := "backtest " + task.Args
	cmd := exec.CommandContext(s.ctx, exePath, strings.Split(cmdArgsStr, " ")...)
	cmd.Env = append(os.Environ(), "BanDataDir="+s.DataDir())
	cmd.Env = append(cmd.Env, "BanStratDir="+s.StrategyDir())

	// 添加到运行列表
	s.taskMu.Lock()
	s.runningBtTasks[task.ID] = cmd
	s.taskMu.Unlock()

	if err := s.updateTaskStatus(task.ID, int64(ormu.BtStatusRunning), 0); err != nil {
		return
	}

	err = s.runBtCommand(cmd, task)

	// 收集并更新任务结果
	s.updateBtTaskResult(task, err)
}

// 更新任务状态，基于taskID进行缓存，每个task间隔5s才更新一次数据库
func (s *DevServer) updateTaskStatus(taskID int64, status int64, progress float64) error {
	s.taskMu.Lock()
	cached, exists := s.taskStatusCache[taskID]
	now := time.Now()

	// 检查是否需要更新数据库
	needUpdate := false
	if !exists {
		needUpdate = true
		s.taskStatusCache[taskID] = &taskStatusInfo{
			status:       status,
			progress:     progress,
			lastUpdateAt: now,
		}
	} else {
		// 检查是否需要更新数据库：状态或进度有变化，且距离上次更新超过5秒
		if (cached.status != status || cached.progress != progress) && now.Sub(cached.lastUpdateAt) >= 5*time.Second {
			needUpdate = true
			cached.lastUpdateAt = now
		}
		// 始终更新缓存为最新值，即使不写数据库
		cached.status = status
		cached.progress = progress
	}
	s.taskMu.Unlock()

	if !needUpdate {
		return nil
	}

	// 获取数据库连接并更新
	qu, conn, err := s.Conn()
	if err != nil {
		log.Error("connect to db failed", zap.Error(err))
		return err
	}
	defer conn.Close()

	err2 := qu.UpdateTask(context.Background(), ormu.UpdateTaskParams{
		Status:   status,
		Progress: progress,
		ID:       taskID,
	})
	if err2 != nil {
		log.Error("update task status failed", zap.Error(err2))
	}
	return err2
}

// 执行回测命令并处理输出
func (s *DevServer) runBtCommand(cmd *exec.Cmd, task *ormu.Task) error {
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
	// Stdout and stderr are consumed concurrently. strings.Builder is not
	// safe for concurrent writes, so keep the critical section limited to the
	// two append operations and leave scanning/progress handling parallel.
	var outputMu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(2)

	// 处理标准输出
	go func() {
		defer wg.Done()
		scanner := utils2.ReadScanner(stdOut)
		prefix := "uiPrg: "
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, prefix) {
				if err := s.handleProgress(line[len(prefix):], task.ID); err != nil {
					log.Error("handle progress failed", zap.Error(err))
				}
			} else {
				outputMu.Lock()
				b.WriteString(line)
				b.WriteString("\n")
				outputMu.Unlock()
			}
		}
		if err := scanner.Err(); err != nil {
			log.Error("stdout scanner error", zap.Error(err))
		}
	}()

	// 处理错误输出
	go func() {
		defer wg.Done()
		scanner := utils2.ReadScanner(stdErr)
		for scanner.Scan() {
			outputMu.Lock()
			b.WriteString(scanner.Text())
			b.WriteString("\n")
			outputMu.Unlock()
		}
		if err := scanner.Err(); err != nil {
			log.Error("stderr scanner error", zap.Error(err))
		}
	}()

	// 等待所有输出处理完成
	wg.Wait()

	// 等待命令执行完成
	err = cmd.Wait()
	s.BroadcastWS("", map[string]interface{}{
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

// 处理进度更新
func (s *DevServer) handleProgress(progressStr string, taskID int64) error {
	prgVal, err := strconv.ParseFloat(progressStr, 64)
	if err != nil {
		log.Warn("invalid progress", zap.String("progress", progressStr))
		return err
	}
	s.BroadcastWS("", map[string]interface{}{
		"type":     "btPrg",
		"taskId":   taskID,
		"progress": prgVal,
	})
	return s.updateTaskStatus(taskID, int64(ormu.BtStatusRunning), prgVal)
}

// 更新回测任务结果
func (s *DevServer) updateBtTaskResult(task *ormu.Task, errTask error) {
	qu, conn, err2 := s.Conn()
	if err2 != nil {
		log.Error("get dev conn fail", zap.Error(err2))
		return
	}
	defer conn.Close()
	btRoot := s.BacktestDir()
	taskRes, err := collectBtTaskResult(btRoot, task.Path)
	if errTask != nil {
		if updateErr := qu.UpdateTask(context.Background(), ormu.UpdateTaskParams{
			Status:   int64(ormu.BtStatusFail),
			Progress: 1,
			Info:     errTask.Error(),
			ID:       task.ID,
		}); updateErr != nil {
			log.Error("update failed backtest task status fail", zap.Error(updateErr))
		}
		return
	}
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
func (s *DevServer) startBtTaskScheduler() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			var task *ormu.Task
			select {
			case <-s.ctx.Done():
				return
			case task = <-s.notify:
			}
			for {
				s.taskMu.Lock()
				runningCount := len(s.runningBtTasks)
				s.taskMu.Unlock()
				if runningCount < maxBtTasks {
					break
				}
				// Keep the retry inside the owned scheduler so Stop can cancel it.
				timer := time.NewTimer(500 * time.Millisecond)
				select {
				case <-s.ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}

			// 检查任务状态是否为待执行
			if task.Status != int64(ormu.BtStatusInit) {
				continue
			}

			s.taskMu.Lock()
			_, exist := s.runningBtTasks[task.ID]
			if !exist {
				s.runningBtTasks[task.ID] = nil
			}
			s.taskMu.Unlock()

			if exist {
				continue
			}

			// 启动新的回测任务
			s.wg.Add(1)
			go s.executeBtTask(task)
		}
	}()
}

func (s *DevServer) collectBtResults() error {
	qu, conn, err2 := s.Conn()
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
	btRoot := s.BacktestDir()
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
		relPath = filepath.ToSlash(relPath)
		if _, ok := taskMap[relPath]; ok {
			// A registered task owns its complete output subtree. Separate
			// policy reports live below this directory and must never be
			// re-discovered as independent tasks.
			delete(taskMap, relPath)
			return filepath.SkipDir
		}

		separateDir, readErr := hasPolicyReportDirs(fullPath)
		if readErr != nil {
			return readErr
		}
		task, err := collectBtTaskResult(btRoot, relPath)
		if err != nil || task == nil {
			if err == nil && separateDir {
				// Keep partial --separate output below the parent task. A
				// policy report is not a standalone Web task.
				return filepath.SkipDir
			}
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
		// A discovered report owns its complete subtree. This is required for
		// --separate runs, where policy_1/policy_2 are reports of the parent
		// task rather than independent tasks.
		if err == nil {
			return filepath.SkipDir
		}
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
	btDir, pathErr := resolveReportRoot(rootDir, relPath)
	if pathErr != nil {
		return nil, pathErr
	}
	fileInfo, err := os.Stat(filepath.Join(btDir, "assets.html"))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
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
		walletTot += com.GetPriceSafe(code, "") * amt
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
		Periods:     strings.Join(cfg.RunTimeFrames(), ","),
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

// resolveReportRoot resolves a persisted report path below rootDir. Report
// discovery normally supplies paths produced by filepath.Walk, but callers
// can also pass database values; reject traversal before touching the
// filesystem so a malformed task cannot turn collection into an arbitrary
// file read.
func resolveReportRoot(rootDir, relPath string) (string, error) {
	root, err := filepath.Abs(rootDir)
	if err != nil {
		return "", err
	}
	path := filepath.FromSlash(strings.TrimSpace(relPath))
	if path == "" || filepath.IsAbs(path) {
		return "", fmt.Errorf("report path must be relative: %q", relPath)
	}
	resolved, err := filepath.Abs(filepath.Join(root, path))
	if err != nil {
		return "", err
	}
	if !pathWithin(resolved, root) || resolved == root {
		return "", fmt.Errorf("report path escapes output root: %q", relPath)
	}
	return resolved, nil
}

// collectBtTaskResult reads either a regular report or a --separate report.
// Separate backtests keep the task root as a metadata directory and put one
// complete report in policy_1, policy_2, ... . We only publish the parent
// task after every configured policy has produced a report; a partial run is
// left for the scheduler to collect after the child process exits.
func collectBtTaskResult(rootDir, relPath string) (*ormu.Task, error) {
	task, err := collectBtTask(rootDir, relPath)
	if err != nil || task != nil {
		return task, err
	}
	return collectSeparateBtTask(rootDir, relPath)
}

func collectSeparateBtTask(rootDir, relPath string) (*ormu.Task, error) {
	btDir, pathErr := resolveReportRoot(rootDir, relPath)
	if pathErr != nil {
		return nil, pathErr
	}
	configPath := filepath.Join(btDir, "config.yml")
	hasPolicyDir, err := hasPolicyReportDirs(btDir)
	if err != nil {
		return nil, err
	}
	if !hasPolicyDir {
		return nil, nil
	}
	cfg, cfgErr := config.GetConfig(&config.CmdArgs{
		Configs:   []string{configPath},
		NoDefault: true,
	}, false)
	if cfgErr != nil || cfg == nil || len(cfg.RunPolicy) <= 1 {
		return nil, cfgErr
	}

	// Require the expected contiguous policy directories. This avoids
	// publishing a task while one policy is still running or when an old,
	// unrelated directory happens to be present below the task root.
	children := make([]*ormu.Task, 0, len(cfg.RunPolicy))
	reportPaths := make([]string, 0, len(cfg.RunPolicy))
	for i := range cfg.RunPolicy {
		policyRel := filepath.Join(relPath, fmt.Sprintf("policy_%d", i+1))
		child, childErr := collectBtTask(rootDir, policyRel)
		if childErr != nil {
			return nil, childErr
		}
		if child == nil {
			return nil, nil
		}
		children = append(children, child)
		reportPaths = append(reportPaths, filepath.ToSlash(policyRel))
	}

	createMS := int64(0)
	if info, statErr := os.Stat(configPath); statErr == nil {
		createMS = info.ModTime().UnixMilli()
	}
	if createMS == 0 {
		createMS = children[0].CreateAt
	}

	// Metrics from independent policy runs do not have a mathematically
	// correct parent aggregation (each report starts with its own wallet).
	// Keep the order count and explicit child paths, while leaving the
	// per-policy metrics in their own reports for consumers to inspect.
	infoData := map[string]interface{}{
		"separate":    true,
		"reportPaths": reportPaths,
		"policyCount": len(children),
	}
	infoText, marshalErr := utils.MarshalString(infoData)
	if marshalErr != nil {
		return nil, marshalErr
	}
	orderNum := int64(0)
	for _, child := range children {
		orderNum += child.OrderNum
	}
	return &ormu.Task{
		Mode:     "backtest",
		Path:     relPath,
		Strats:   strings.Join(cfg.Strats(), ","),
		Periods:  strings.Join(cfg.RunTimeFrames(), ","),
		Pairs:    cfg.ShowPairs(),
		CreateAt: createMS,
		StartAt:  utils.AlignTfMSecs(cfg.TimeRange.StartMS, int64(utils.TFToSecs("1d")*1000)),
		StopAt:   utils.AlignTfMSecs(cfg.TimeRange.EndMS, int64(utils.TFToSecs("1d")*1000)),
		Status:   ormu.BtStatusDone,
		Progress: 1,
		OrderNum: orderNum,
		Info:     infoText,
	}, nil
}

func hasPolicyReportDirs(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "policy_") {
			return true, nil
		}
	}
	return false, nil
}

// taskBaseDir resolves a task path below the configured backtest root. Task
// paths are persisted by the server, but validating the boundary here keeps a
// malformed row from turning the report APIs into arbitrary file readers.
func (s *DevServer) taskBaseDir(task *ormu.Task) (string, error) {
	if task == nil {
		return "", fmt.Errorf("backtest task is required")
	}
	rawPath := strings.TrimSpace(task.Path)
	if rawPath == "" {
		return "", fmt.Errorf("backtest task path is empty")
	}
	path := filepath.FromSlash(rawPath)
	if filepath.IsAbs(path) {
		return "", fmt.Errorf("backtest task path must be relative: %q", task.Path)
	}
	root, err := filepath.Abs(s.BacktestDir())
	if err != nil {
		return "", err
	}
	base, err := filepath.Abs(filepath.Join(root, path))
	if err != nil {
		return "", err
	}
	if !pathWithin(base, root) || base == root {
		return "", fmt.Errorf("backtest task path escapes output root: %q", task.Path)
	}
	return base, nil
}

func pathWithin(path, parent string) bool {
	rel, err := filepath.Rel(parent, path)
	if err != nil || rel == "." || rel == ".." {
		return err == nil && rel == "."
	}
	return !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}

// taskReportDirs returns the report directories owned by a task. A regular
// task has one directory. A completed --separate task records child reports in
// Info.reportPaths; only paths below the task directory are accepted. The
// order is stable so handlers consistently pick policy_1 as the summary view.
func (s *DevServer) taskReportDirs(task *ormu.Task) ([]string, error) {
	base, err := s.taskBaseDir(task)
	if err != nil {
		return nil, err
	}
	dirs := []string{base}
	if strings.TrimSpace(task.Info) == "" {
		return dirs, nil
	}
	var info struct {
		Separate    bool     `json:"separate"`
		ReportPaths []string `json:"reportPaths"`
	}
	if err := utils.Unmarshal([]byte(task.Info), &info, utils.JsonNumDefault); err != nil {
		// Info predates separate reports for older tasks. Keep the regular path
		// behavior when an unrelated legacy payload is malformed.
		return dirs, nil
	}
	if !info.Separate || len(info.ReportPaths) == 0 {
		return dirs, nil
	}
	resolved := make([]string, 0, len(info.ReportPaths))
	for _, rawPath := range info.ReportPaths {
		relPath := filepath.FromSlash(strings.TrimSpace(rawPath))
		if relPath == "" || filepath.IsAbs(relPath) {
			return nil, fmt.Errorf("invalid separate report path: %q", rawPath)
		}
		root := s.BacktestDir()
		rooted, err := filepath.Abs(filepath.Join(root, relPath))
		if err != nil {
			return nil, err
		}
		if !pathWithin(rooted, base) {
			// Older callers may store paths relative to the task directory
			// ("policy_1") instead of relative to the backtest root
			// ("task/policy_1"). Accept that form only after the same boundary
			// check; traversal remains rejected.
			if hasParentPathComponent(relPath) {
				return nil, fmt.Errorf("separate report path escapes task directory: %q", rawPath)
			}
			rooted, err = filepath.Abs(filepath.Join(base, relPath))
			if err != nil || !pathWithin(rooted, base) {
				return nil, fmt.Errorf("separate report path escapes task directory: %q", rawPath)
			}
		}
		resolved = append(resolved, rooted)
	}
	if len(resolved) > 0 {
		return resolved, nil
	}
	return dirs, nil
}

func firstReportDir(dirs []string, marker string) string {
	if len(dirs) == 0 {
		return ""
	}
	if marker == "" {
		return dirs[0]
	}
	for _, dir := range dirs {
		if _, err := os.Stat(filepath.Join(dir, marker)); err == nil {
			return dir
		}
	}
	return dirs[0]
}

func reportFilePath(reportDir, rawPath string) (string, error) {
	relPath := filepath.FromSlash(strings.TrimSpace(rawPath))
	if relPath == "" || filepath.IsAbs(relPath) {
		return "", fmt.Errorf("report file path must be relative: %q", rawPath)
	}
	path, err := filepath.Abs(filepath.Join(reportDir, relPath))
	if err != nil {
		return "", err
	}
	if !pathWithin(path, reportDir) {
		return "", fmt.Errorf("report file path escapes report directory: %q", rawPath)
	}
	return path, nil
}

func hasParentPathComponent(path string) bool {
	for _, part := range strings.FieldsFunc(path, func(r rune) bool {
		return r == '/' || r == '\\'
	}) {
		if part == ".." {
			return true
		}
	}
	return false
}

func (s *DevServer) MergeConfig(inText string, skips ...string) (string, error) {
	dataDir := s.DataDir()
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
	paths = append(paths, s.configPaths...)
	if inText != "" {
		tmp, err := os.CreateTemp(os.TempDir(), "tmp_cfg")
		if err != nil {
			return "", err
		}
		defer os.Remove(tmp.Name())
		tmp.WriteString(inText)
		paths = append(paths, tmp.Name())
	}
	return config.MergeConfigPaths(paths, skips...)
}
