package dev

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sasha-s/go-deadlock"

	"github.com/banbox/banexg"
	utils2 "github.com/banbox/banexg/utils"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/opt"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/orm/ormu"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banbot/web/base"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"

	"github.com/banbox/banbot/core"
	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
)

// FileNode 表示文件树的节点
type FileNode struct {
	Path  string `json:"path"`            // 相对路径
	Size  int64  `json:"size,omitempty"`  // 文件大小（文件夹时忽略）
	Stamp int64  `json:"stamp,omitempty"` // 最后修改时间戳（文件夹时忽略）
}

// 添加一个互斥锁来控制编译状态
var buildMutex deadlock.Mutex

func regApiDev(api fiber.Router) {
	api.Get("/ws", websocket.New(onWsDev))
	api.Get("/strat_tree", getStratTree)
	api.Get("/bt_tasks", getBtTasks)
	api.Get("/bt_options", getBtOptions)
	api.Get("/symbol_info", getSymbolInfo)
	api.Get("/symbol_gaps", getSymbolGaps)
	api.Get("/symbol_data", getSymbolData)
	api.Get("/series_ranges", getSeriesRanges)
	api.Post("/file_op", handleFileOp)
	api.Post("/new_strat", handleNewStrat)
	api.Get("/text", getText)
	api.Get("/texts", getTexts)
	api.Post("/save_text", saveText)
	api.Get("/build_envs", getBuildEnvs)
	api.Post("/build", handleBuild)
	api.Get("/logs", getLogs)
	api.Get("/available_strats", getAvailableStrats)
	api.Post("/run_backtest", handleRunBacktest)
	api.Get("/bt_detail", getBtDetail)
	api.Get("/bt_orders", getBtOrders)
	api.Get("/bt_config", getBtConfig)
	api.Get("/bt_logs", getBtLogs)
	api.Get("/bt_html", getBtHtml)
	api.Get("/bt_strat_tree", getBtStratTree)
	api.Get("/bt_strat_text", getBtStratText)
	api.Get("/symbols", GetSymbolsHandler)
	api.Post("/data_tools", handleDataTools)
	api.Get("/download", handleDownload)
	api.Get("/compare_assets", getCompareAssets)
	api.Post("/update_note", handleUpdateNote)
	api.Post("/del_bt_reports", delBacktestReports)
}

func onWsDev(c *websocket.Conn) {
	NewWsClient(c).HandleForever()
}

func getStratTree(c *fiber.Ctx) error {
	baseDir, err := getRootDir()
	if err != nil {
		return err
	}

	var files []FileNode
	// 遍历目录
	err = filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if strings.HasPrefix(info.Name(), ".") {
			return nil
		}

		// 计算相对路径
		relPath, err := filepath.Rel(baseDir, path)
		if err != nil {
			return err
		}

		if relPath == "." {
			return nil
		}
		relPath = strings.ReplaceAll(relPath, "\\", "/")

		if info.IsDir() {
			// 对于目录，添加末尾斜杠
			files = append(files, FileNode{
				Path: relPath + "/",
			})
		} else {
			files = append(files, FileNode{
				Path:  relPath,
				Size:  info.Size(),
				Stamp: info.ModTime().UnixMilli(),
			})
		}

		return nil
	})

	if err != nil {
		return err
	}

	return c.JSON(fiber.Map{
		"data": files,
	})
}

// handleFileOp 处理文件操作请求
func handleFileOp(c *fiber.Ctx) error {
	type FileOp struct {
		Op     string `json:"op"`
		Path   string `json:"path"`
		Target string `json:"target,omitempty"`
	}
	var op = new(FileOp)
	if err := base.VerifyArg(c, op, base.ArgBody); err != nil {
		return err
	}

	baseDir, err := getRootDir()
	if err != nil {
		return err
	}
	srcPath := filepath.Join(baseDir, op.Path)

	switch op.Op {
	case "newFile":
		newPath := filepath.Join(srcPath, op.Target)
		file, err := os.Create(newPath)
		if err != nil {
			return err
		}
		file.Close()

	case "newFolder":
		newPath := filepath.Join(srcPath, op.Target)
		if err = os.MkdirAll(newPath, 0755); err != nil {
			return err
		}

	case "rename":
		newPath := filepath.Join(filepath.Dir(srcPath), op.Target)
		if err = os.Rename(srcPath, newPath); err != nil {
			return err
		}

	case "cut":
		targetPath := filepath.Join(baseDir, op.Target, filepath.Base(srcPath))
		if err = utils.MovePath(srcPath, targetPath); err != nil {
			return err
		}

	case "copy":
		targetPath := filepath.Join(baseDir, op.Target, filepath.Base(srcPath))
		if err = utils.CopyDir(srcPath, targetPath); err != nil {
			return err
		}

	case "delete":
		if err = os.RemoveAll(srcPath); err != nil {
			return err
		}

	default:
		return fmt.Errorf("unsupport operation: %s", op.Op)
	}

	return c.JSON(fiber.Map{
		"code": 200,
	})
}

func handleNewStrat(c *fiber.Ctx) error {
	type NewStratArgs struct {
		Folder string `json:"folder" validate:"required"`
		Name   string `json:"name" validate:"required"`
	}

	var args = new(NewStratArgs)
	if err := base.VerifyArg(c, args, base.ArgBody); err != nil {
		return err
	}
	err := makeNewStrat(args.Folder, args.Name)
	if err != nil {
		return err
	}

	return c.JSON(fiber.Map{
		"code": 200,
	})
}

func parsePath(curPath string) (string, error) {
	if curPath == "" {
		return "", nil
	}
	if strings.HasPrefix(curPath, "$") || strings.HasPrefix(curPath, "@") {
		curPath = config.ParsePath(curPath)
	} else {
		baseDir, err := getRootDir()
		if err != nil {
			return "", err
		}
		curPath = filepath.Join(baseDir, curPath)
	}
	return curPath, nil
}

// prepareBacktestConfigFiles snapshots all configuration inputs for one Web
// request into a private temporary directory. The editor commonly sends
// @config.yml, which normally resolves to the shared data directory; writing
// there before parsing lets concurrent requests overwrite one another. The
// returned paths are absolute and can be passed directly to config.GetConfig.
func prepareBacktestConfigFiles(configs map[string]string, paths []string) (string, []string, error) {
	tempDir, err := os.MkdirTemp("", "banbot-web-config-")
	if err != nil {
		return "", nil, err
	}
	cleanup := func() {
		_ = os.RemoveAll(tempDir)
	}

	pathTargets := make(map[string]string, len(configs)+len(paths))
	targetOwners := make(map[string]string, len(configs)+len(paths))
	targetOwnerKeys := make(map[string]string, len(configs)+len(paths))
	for rawPath, text := range configs {
		if strings.TrimSpace(text) == "" {
			continue
		}
		relPath, pathErr := backtestConfigTempPath(rawPath)
		if pathErr != nil {
			cleanup()
			return "", nil, pathErr
		}
		key := backtestConfigKey(rawPath)
		if owner := targetOwners[relPath]; owner != "" &&
			(owner != rawPath || targetOwnerKeys[relPath] != key) {
			cleanup()
			return "", nil, fmt.Errorf("configuration paths %q and %q resolve to the same temporary file", owner, rawPath)
		}
		targetOwners[relPath] = rawPath
		targetOwnerKeys[relPath] = key
		target := filepath.Join(tempDir, relPath)
		if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			cleanup()
			return "", nil, err
		}
		if err := os.WriteFile(target, []byte(text), 0644); err != nil {
			cleanup()
			return "", nil, err
		}
		pathTargets[key] = target
	}

	resolved := make([]string, 0, len(paths))
	for _, rawPath := range paths {
		if target := pathTargets[backtestConfigKey(rawPath)]; target != "" {
			resolved = append(resolved, target)
			continue
		}
		source, pathErr := parsePath(rawPath)
		if pathErr != nil {
			cleanup()
			return "", nil, pathErr
		}
		data, readErr := os.ReadFile(source)
		if readErr != nil {
			cleanup()
			return "", nil, readErr
		}
		relPath, relErr := backtestConfigTempPath(rawPath)
		if relErr != nil {
			cleanup()
			return "", nil, relErr
		}
		key := backtestConfigKey(rawPath)
		if owner := targetOwners[relPath]; owner != "" &&
			(owner != rawPath || targetOwnerKeys[relPath] != key) {
			cleanup()
			return "", nil, fmt.Errorf("configuration paths %q and %q resolve to the same temporary file", owner, rawPath)
		}
		targetOwners[relPath] = rawPath
		targetOwnerKeys[relPath] = key
		target := filepath.Join(tempDir, relPath)
		if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			cleanup()
			return "", nil, err
		}
		if _, statErr := os.Stat(target); os.IsNotExist(statErr) {
			if err := os.WriteFile(target, data, 0644); err != nil {
				cleanup()
				return "", nil, err
			}
		} else if statErr != nil {
			cleanup()
			return "", nil, statErr
		}
		pathTargets[key] = target
		resolved = append(resolved, target)
	}
	return tempDir, resolved, nil
}

func backtestConfigTempPath(rawPath string) (string, error) {
	path := strings.TrimSpace(rawPath)
	if path == "" {
		return "", fmt.Errorf("configuration path is empty")
	}
	if path[0] == '$' || path[0] == '@' {
		path = strings.TrimLeft(path[1:], "\\/")
	}
	path = filepath.Clean(filepath.FromSlash(path))
	if path == "." || path == ".." || strings.HasPrefix(path, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("configuration path escapes temporary directory: %q", rawPath)
	}
	if filepath.IsAbs(path) {
		path = filepath.Base(path)
	}
	if path == "" || path == "." {
		return "", fmt.Errorf("configuration path is invalid: %q", rawPath)
	}
	return path, nil
}

func backtestConfigKey(rawPath string) string {
	path := strings.TrimSpace(rawPath)
	if path != "" && (path[0] == '$' || path[0] == '@') {
		path = strings.TrimLeft(path[1:], "\\/")
	}
	return filepath.Clean(filepath.FromSlash(path))
}

func getText(c *fiber.Ctx) error {
	type TextArgs struct {
		Path string `query:"path" validate:"required"`
	}

	var args = new(TextArgs)
	err := base.VerifyArg(c, args, base.ArgQuery)
	if err != nil {
		return err
	}

	args.Path, err = parsePath(args.Path)
	if err != nil {
		return err
	}

	content, err2 := utils.ReadTextFile(args.Path)
	if err2 != nil {
		return err2
	}

	return c.JSON(fiber.Map{
		"data": content,
	})
}

func getTexts(c *fiber.Ctx) error {
	type TextArgs struct {
		Paths []string `query:"paths" validate:"required"`
	}

	var args = new(TextArgs)
	err := base.VerifyArg(c, args, base.ArgQuery)
	if err != nil {
		return err
	}

	contents := make(map[string]string)
	var realPath string
	for _, path := range args.Paths {
		realPath, err = parsePath(path)
		if err != nil {
			return err
		}
		if !utils.Exists(realPath) {
			continue
		}

		content, err2 := utils.ReadTextFile(realPath)
		if err2 != nil {
			return err2
		}
		contents[path] = content
	}

	return c.JSON(contents)
}

func saveText(c *fiber.Ctx) error {
	type SaveTextArgs struct {
		Path    string `json:"path" validate:"required"`
		Content string `json:"content" validate:"required"`
	}

	var args = new(SaveTextArgs)
	err := base.VerifyArg(c, args, base.ArgBody)
	if err != nil {
		return err
	}

	// 检查内容是否为空
	if len(strings.TrimSpace(args.Content)) == 0 {
		return c.Status(400).JSON(fiber.Map{
			"msg": "Content cannot be empty",
		})
	}

	args.Path, err = parsePath(args.Path)
	if err != nil {
		return err
	}

	// 检查文件是否存在
	_, err = os.Stat(args.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return c.Status(400).JSON(fiber.Map{
				"msg": "File not found",
			})
		}
		return err
	}

	// 写入文件内容
	err = os.WriteFile(args.Path, []byte(args.Content), 0644)
	if err != nil {
		return err
	}
	if strings.HasSuffix(args.Path, ".go") {
		status.DirtyBin = true
		BroadcastStatus()
	}

	return c.JSON(fiber.Map{
		"code": 200,
	})
}

// handleBuild 处理编译请求
func handleBuild(c *fiber.Ctx) error {
	type BuildArgs struct {
		OS   string `json:"os"`
		Arch string `json:"arch"`
		Path string `json:"path"`
	}
	var args = new(BuildArgs)
	if err := base.VerifyArg(c, args, base.ArgBody); err != nil {
		return err
	}

	// 检查是否正在编译
	buildMutex.Lock()
	if status.Building {
		buildMutex.Unlock()
		return c.Status(400).JSON(fiber.Map{
			"msg": "Another build is in progress",
		})
	}
	status.DirtyBin = false
	status.Building = true
	BroadcastStatus()
	buildMutex.Unlock()

	// 在函数结束时确保重置编译状态
	defer func() {
		buildMutex.Lock()
		status.Building = false
		BroadcastStatus()
		buildMutex.Unlock()
	}()

	// 设置目标操作系统和架构
	targetOS := args.OS
	if targetOS == "" {
		targetOS = runtime.GOOS
	}
	targetArch := args.Arch
	if targetArch == "" {
		targetArch = runtime.GOARCH
	}

	// 设置输出路径
	outputPath, err := parsePath(args.Path)
	if err != nil {
		return err
	}
	if outputPath == "" {
		exePath, err := os.Executable()
		if err != nil {
			return err
		}
		outputPath = exePath
	} else if targetOS == "windows" && !strings.HasSuffix(outputPath, ".exe") {
		outputPath = outputPath + ".exe"
	}

	// 检测是否为国内网络环境，如果是则设置国内Go代理加速
	conn, err := net.DialTimeout("tcp", "google.com:443", 3*time.Second)
	isChinaNet := err != nil
	if conn != nil {
		conn.Close()
	}

	// 准备编译命令
	cmd := exec.Command("go", "build", "-o", outputPath)
	env := append(os.Environ(), fmt.Sprintf("GOARCH=%s", targetArch))
	env = append(env, fmt.Sprintf("GOOS=%s", targetOS))
	if isChinaNet && os.Getenv("GOPROXY") == "" {
		env = append(env, "GO111MODULE=on", "GOPROXY=https://goproxy.cn,direct")
	}
	cmd.Env = env

	// 捕获命令输出
	output, err := cmd.CombinedOutput()

	if err != nil {
		log.Warn("Build failed", zap.Error(err), zap.String("output", string(output)))
		return c.Status(500).JSON(fiber.Map{
			"msg": fmt.Sprintf("Build failed: %v", err),
		})
	}

	if len(output) > 0 {
		log.Info("Build success", zap.String("output", string(output)))
	} else {
		log.Info("Build completed successfully")
	}

	return c.JSON(fiber.Map{
		"code": 200,
	})
}

func getLogs(c *fiber.Ctx) error {
	type LogArgs struct {
		End   int64 `query:"end"`
		Limit int64 `query:"limit"`
	}

	var args = new(LogArgs)
	if err := base.VerifyArg(c, args, base.ArgQuery); err != nil {
		return err
	}

	logFile := log.LogFilePath()
	if logFile == "" {
		return c.JSON(fiber.Map{"code": 400, "msg": "no log file"})
	}
	data, pos, err := utils.ReadFileTail(logFile, args.Limit, args.End)
	if err != nil {
		return err
	}

	return c.JSON(fiber.Map{
		"data":  string(data),
		"start": pos,
	})
}

// getBtTasks 获取回测任务列表
func getBtTasks(c *fiber.Ctx) error {
	type TaskArgs struct {
		Mode     string `query:"mode"`
		Path     string `query:"path"`
		Strat    string `query:"strat"`
		Period   string `query:"period"`
		RangeStr string `query:"range"`
		MinStart int64  `query:"minStart"`
		MaxStart int64  `query:"maxStart"`
		MaxID    int64  `query:"maxId"`
		Limit    int64  `query:"limit"`
		Assets   bool   `query:"assets"`
	}

	var args = new(TaskArgs)
	if err := base.VerifyArg(c, args, base.ArgQuery); err != nil {
		return err
	}

	if args.Limit <= 0 {
		args.Limit = 20
	}

	qu, conn, err := ormu.Conn()
	if err != nil {
		return err
	}
	defer conn.Close()

	var startMS, endMS int64
	if args.RangeStr != "" {
		startMS, endMS, _ = config.ParseTimeRange(args.RangeStr)
	}

	tasks, err := qu.FindTasks(context.Background(), ormu.FindTasksParams{
		Mode:     args.Mode,
		Path:     args.Path,
		Strat:    args.Strat,
		Period:   args.Period,
		StartAt:  startMS,
		StopAt:   endMS,
		MinStart: args.MinStart,
		MaxStart: args.MaxStart,
		MaxID:    args.MaxID,
		Limit:    args.Limit,
	})
	if err != nil {
		return err
	}

	// 处理返回数据
	result := make([]map[string]interface{}, 0, len(tasks))
	for _, task := range tasks {
		taskMap := task.ToMap()
		if args.Assets {
			appendTaskAssets(task, taskMap)
		}
		result = append(result, taskMap)
	}

	return c.JSON(fiber.Map{
		"data": result,
	})
}

const btCardAssetPoints = 160

type assetChartDataset struct {
	Label string    `json:"label"`
	Data  []float64 `json:"data"`
}

type assetChartData struct {
	Labels   []string            `json:"labels"`
	Datasets []assetChartDataset `json:"datasets"`
}

func appendTaskAssets(task *ormu.Task, taskMap map[string]interface{}) {
	if task == nil || task.Path == "" || task.Status < int64(ormu.BtStatusDone) {
		return
	}
	reportDirs, err := taskReportDirs(task)
	if err != nil {
		return
	}
	reportDir := firstReportDir(reportDirs, "assets.html")
	assetPath := filepath.Join(reportDir, "assets.html")
	if !utils.Exists(assetPath) {
		return
	}
	reals, used, err := readAssetLines(assetPath, btCardAssetPoints)
	if err != nil || len(reals) == 0 {
		return
	}
	taskMap["reals"] = reals
	if len(used) > 0 {
		taskMap["used"] = used
	}
}

func readAssetLines(file string, maxPoints int) ([]float64, []float64, error) {
	content, err := os.ReadFile(file)
	if err != nil {
		return nil, nil, err
	}
	text := string(content)
	start := strings.Index(text, "chartData = ")
	keyLen := len("chartData = ")
	if start < 0 {
		start = strings.Index(text, "chartData=")
		keyLen = len("chartData=")
	}
	if start < 0 {
		return nil, nil, fmt.Errorf("chart data not found")
	}
	start += keyLen
	end := strings.Index(text[start:], "\n")
	if end < 0 {
		end = strings.Index(text[start:], ";")
	}
	if end < 0 {
		end = strings.Index(text[start:], "</script>")
	}
	if end < 0 {
		return nil, nil, fmt.Errorf("chart data not terminated")
	}
	jsonStr := strings.TrimSpace(text[start : start+end])

	var data assetChartData
	if err := utils2.UnmarshalString(jsonStr, &data, utils2.JsonNumDefault); err != nil {
		return nil, nil, err
	}

	var reals []float64
	var used []float64
	for _, ds := range data.Datasets {
		switch ds.Label {
		case "Real":
			reals = ds.Data
		case "Available":
			used = ds.Data
		}
	}
	if len(reals) == 0 && len(data.Datasets) > 0 {
		reals = data.Datasets[0].Data
	}

	reals = downsampleSeries(reals, maxPoints)
	used = downsampleSeries(used, maxPoints)
	return reals, used, nil
}

func downsampleSeries(values []float64, maxPoints int) []float64 {
	if maxPoints <= 0 || len(values) <= maxPoints {
		return values
	}
	if maxPoints == 1 {
		return []float64{values[len(values)-1]}
	}
	step := float64(len(values)-1) / float64(maxPoints-1)
	result := make([]float64, 0, maxPoints)
	for i := 0; i < maxPoints; i++ {
		idx := int(step*float64(i) + 0.5)
		if idx >= len(values) {
			idx = len(values) - 1
		}
		result = append(result, values[idx])
	}
	return result
}

// getBtOptions 获取回测选项列表
func getBtOptions(c *fiber.Ctx) error {
	qu, conn, err2 := ormu.Conn()
	if err2 != nil {
		return err2
	}
	defer conn.Close()

	options, err := qu.GetTaskOptions(context.Background())
	if err != nil {
		return err
	}

	// 处理策略列表
	stratMap := make(map[string]int)
	for _, o := range options {
		strats := strings.Split(o.Strats, ",")
		for _, s := range strats {
			if s = strings.TrimSpace(s); s != "" {
				oldNum, _ := stratMap[s]
				stratMap[s] = oldNum + 1
			}
		}
	}
	strats := make([]core.StrVal[int], 0, len(stratMap))
	for s, v := range stratMap {
		strats = append(strats, core.StrVal[int]{
			Str: s, Val: v,
		})
	}
	sort.Slice(strats, func(i, j int) bool {
		return strats[i].Val > strats[j].Val
	})

	// 处理周期列表
	periodMap := make(map[string]int)
	for _, o := range options {
		periods := strings.Split(o.Periods, ",")
		for _, p := range periods {
			if p = strings.TrimSpace(p); p != "" {
				periodMap[p] = utils2.TFToSecs(p)
			}
		}
	}
	periods := make([]string, 0, len(periodMap))
	for p := range periodMap {
		periods = append(periods, p)
	}
	sort.SliceStable(periods, func(i, j int) bool {
		return periodMap[periods[i]] < periodMap[periods[j]]
	})

	// 处理日期范围
	dateMap := make(map[string]bool)
	for _, o := range options {
		if o.StartAt > 0 && o.StopAt > 0 {
			startStr := btime.ToDateStr(o.StartAt, "20060102")
			stopStr := btime.ToDateStr(o.StopAt, "20060102")
			dateRange := fmt.Sprintf("%s-%s", startStr, stopStr)
			dateMap[dateRange] = true
		}
	}
	dates := make([]string, 0, len(dateMap))
	for d := range dateMap {
		dates = append(dates, d)
	}
	sort.Strings(dates)

	return c.JSON(fiber.Map{
		"strats":  strats,
		"periods": periods,
		"ranges":  dates,
	})
}

func getAvailableStrats(c *fiber.Ctx) error {
	exePath, err := os.Executable()
	if err != nil {
		return err
	}
	cmd := exec.Command(exePath, "tool", "list_strats")
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("execute command failed: %v", err)
	}
	arr := strings.Split(strings.TrimSpace(string(output)), "\n")
	return c.JSON(fiber.Map{
		"data": arr,
	})
}

// handleRunBacktest 处理回测请求
func handleRunBacktest(c *fiber.Ctx) error {
	type RunBtArgs struct {
		Separate bool              `json:"separate"`
		Configs  map[string]string `json:"configs" validate:"required"`
		Paths    []string          `json:"paths"`
		DupMode  string            `json:"dupMode"`
	}

	var args = new(RunBtArgs)
	if err := base.VerifyArg(c, args, base.ArgBody); err != nil {
		return err
	}

	// Snapshot request-owned editor contents before parsing. In particular,
	// @config.yml must never be written to the shared data directory because a
	// concurrent request could otherwise change the file between hash and load.
	configTempDir, paths, err := prepareBacktestConfigFiles(args.Configs, args.Paths)
	if err != nil {
		return err
	}
	defer os.RemoveAll(configTempDir)

	// 加载并验证配置
	cfg, err2 := config.GetConfig(&config.CmdArgs{Configs: paths, NoDefault: true}, false)
	if err2 != nil {
		return err2
	}

	// 检查必要的配置项
	if len(cfg.RunPolicy) == 0 {
		return errs.NewMsg(errs.CodeParamRequired, "run_policy is required")
	}
	if cfg.TimeRange.StartMS == 0 || cfg.TimeRange.EndMS == 0 {
		return errs.NewMsg(errs.CodeParamRequired, "time_range is required")
	}
	if len(cfg.WalletAmounts) == 0 {
		return errs.NewMsg(errs.CodeParamRequired, "wallet_amounts is required")
	}
	if cfg.StakeAmount == 0 && cfg.StakePct == 0 {
		return errs.NewMsg(errs.CodeParamRequired, "stake_amount or stake_pct is required")
	}
	if len(cfg.StakeCurrency) == 0 {
		return errs.NewMsg(errs.CodeParamRequired, "stake_currency is required")
	}
	if cfg.Exchange.Name == "" {
		return errs.NewMsg(errs.CodeParamRequired, "exchange.name is required")
	}
	if cfg.Database.Url == "" {
		return errs.NewMsg(errs.CodeParamRequired, "database.url is required")
	}

	// 获取配置内容并计算哈希
	cfgData, err2 := cfg.DumpYaml()
	if err2 != nil {
		return err2
	}
	hashVal := utils.MD5(cfgData)[:10]
	backtestRoot := config.ParsePath("$backtest")
	basePath := filepath.Join(backtestRoot, hashVal)
	// Reserve the output before writing the config. Mkdir is the allocation
	// operation, so concurrent submissions of the same configuration receive
	// hash, hash_1, hash_2, ... without sharing report files.
	absPath, err := config.AllocateOutputDir(basePath)
	if err != nil {
		return err
	}
	keepOutput := false
	backupAbs := ""
	backupOwned := false
	defer func() {
		if !keepOutput {
			_ = os.RemoveAll(absPath)
		}
		if backupAbs != "" && !backupOwned {
			_ = os.RemoveAll(backupAbs)
		}
	}()
	relPath, err := filepath.Rel(backtestRoot, absPath)
	if err != nil {
		_ = os.RemoveAll(absPath)
		return err
	}
	taskPath := filepath.ToSlash(relPath)
	btPath := fmt.Sprintf("$backtest/%s", taskPath)

	// 添加回测任务
	qu, conn, err2 := ormu.Conn()
	if err2 != nil {
		return err2
	}
	defer conn.Close()

	// Keep the old UI "backup" action useful: archive the original hash
	// directory under a unique path before adding the newly allocated task.
	// The new run always writes to its reserved directory, so backup/overwrite
	// can never truncate another task's reports.
	cfgPath := filepath.Join(absPath, "config.yml")
	baseConfigPath := filepath.Join(basePath, "config.yml")
	if utils.Exists(baseConfigPath) && args.DupMode == "backup" {
		oldTasks, err2 := qu.FindTasks(context.Background(), ormu.FindTasksParams{
			Mode: "backtest",
			Path: hashVal,
		})
		if err2 != nil {
			return err2
		}
		var old *ormu.Task
		if len(oldTasks) > 0 {
			old = oldTasks[0]
		}
		backupBase := hashVal + "_bak"
		if old != nil {
			backupBase = hashVal + "_" + strconv.FormatInt(old.ID, 10)
		}
		var allocErr error
		backupAbs, allocErr = config.AllocateOutputDir(filepath.Join(backtestRoot, backupBase))
		if allocErr != nil {
			_ = os.RemoveAll(absPath)
			return allocErr
		}
		backupPathRel, relErr := filepath.Rel(backtestRoot, backupAbs)
		if relErr != nil {
			_ = os.RemoveAll(absPath)
			_ = os.RemoveAll(backupAbs)
			return relErr
		}
		backupPath := filepath.ToSlash(backupPathRel)
		if err = utils.CopyDir(basePath, backupAbs); err != nil {
			_ = os.RemoveAll(absPath)
			_ = os.RemoveAll(backupAbs)
			return err
		}
		if old != nil {
			err = qu.SetTaskPath(context.Background(), ormu.SetTaskPathParams{
				ID:   old.ID,
				Path: backupPath,
			})
			if err != nil {
				return err
			}
			// Once the old row points at the copied directory, that row owns the
			// backup even if registering the new task fails below. Keep it in that
			// case; otherwise the old task would reference a deleted report.
			backupOwned = true
		}
	}
	if err = os.WriteFile(cfgPath, cfgData, 0644); err != nil {
		return err
	}

	// 构建回测参数
	btArgs := fmt.Sprintf("-out %s -prg uiPrg -no-default -config %s", btPath, btPath+"/config.yml")
	if args.Separate {
		btArgs = "-separate " + btArgs
	}

	task, err := qu.AddTask(context.Background(), ormu.AddTaskParams{
		Mode:     "backtest",
		Path:     taskPath,
		Args:     btArgs,
		Config:   string(cfgData),
		Strats:   strings.Join(cfg.Strats(), ","),
		Periods:  strings.Join(cfg.RunTimeFrames(), ","),
		Pairs:    cfg.ShowPairs(),
		CreateAt: btime.UTCStamp(),
		StartAt:  cfg.TimeRange.StartMS,
		StopAt:   cfg.TimeRange.EndMS,
		Status:   ormu.BtStatusInit,
		Progress: 0,
	})
	if err != nil {
		return err
	}
	log.Info("add backtest", zap.Int64("id", task.ID), zap.String("path", taskPath))

	// 通知任务调度器有新任务
	// If there was no old database row, the backup has no owner until this new
	// task is accepted. Preserve it only after AddTask succeeds; failures before
	// that point are cleaned by the defer above.
	if backupAbs != "" && !backupOwned {
		backupOwned = true
	}
	keepOutput = true
	taskNotifyChan <- task

	return c.JSON(fiber.Map{
		"code": 200,
		"data": taskPath,
	})
}

// getBtPath 获取回测输出目录
func getBtPath(taskID int64) (string, error) {
	qu, conn, err2 := ormu.Conn()
	if err2 != nil {
		return "", err2
	}
	defer conn.Close()
	task, err := qu.GetTask(context.Background(), taskID)
	if err != nil {
		return "", fmt.Errorf("query task failed: %v", err)
	}
	return taskBaseDir(task)
}

func getBtReportPath(taskID int64, marker string) (string, error) {
	qu, conn, connErr := ormu.Conn()
	if connErr != nil {
		return "", connErr
	}
	defer conn.Close()
	task, queryErr := qu.GetTask(context.Background(), taskID)
	if queryErr != nil {
		return "", fmt.Errorf("query task failed: %v", queryErr)
	}
	dirs, pathErr := taskReportDirs(task)
	if pathErr != nil {
		return "", pathErr
	}
	return firstReportDir(dirs, marker), nil
}

// parseBtResult 解析回测结果
func parseBtResult(path string) (*opt.BTResult, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read file failed: %v", err)
	}
	var res = new(opt.BTResult)
	err = utils2.Unmarshal(data, res, utils2.JsonNumAuto)
	if err != nil {
		return nil, fmt.Errorf("unmarshal json failed: %v", err)
	}
	return res, nil
}

// getBtDetail 获取回测详情
func getBtDetail(c *fiber.Ctx) error {
	type DetailArgs struct {
		TaskID int64 `query:"task_id" validate:"required"`
	}
	var args = new(DetailArgs)
	if err := base.VerifyArg(c, args, base.ArgQuery); err != nil {
		return err
	}

	qu, conn, err2 := ormu.Conn()
	if err2 != nil {
		return err2
	}
	defer conn.Close()
	task, err := qu.GetTask(context.Background(), args.TaskID)
	if err != nil {
		return fmt.Errorf("query task failed: %v", err)
	}
	basePath, pathErr := taskBaseDir(task)
	if pathErr != nil {
		return pathErr
	}
	reportDirs, pathErr := taskReportDirs(task)
	if pathErr != nil {
		return pathErr
	}
	btPath := firstReportDir(reportDirs, "detail.json")

	configPath := filepath.Join(basePath, "config.yml")
	if !utils.Exists(configPath) {
		configPath = filepath.Join(btPath, "config.yml")
	}
	var cfg *config.Config
	if utils.Exists(configPath) {
		cfg, err2 = config.ParseConfig(configPath)
		if err2 != nil {
			return err2
		}
	}

	// 读取detail.json
	detailPath := filepath.Join(btPath, "detail.json")
	var detail *opt.BTResult
	if utils.Exists(detailPath) {
		detail, err = parseBtResult(detailPath)
		if err != nil {
			return fmt.Errorf("parse backtest result failed: %v", err)
		}
	} else if cfg != nil && cfg.TimeRange != nil {
		detail = &opt.BTResult{
			StartMS: cfg.TimeRange.StartMS,
			EndMS:   cfg.TimeRange.EndMS,
			OutDir:  btPath,
		}
	}
	var exsMap map[string]*orm.ExSymbol
	if cfg != nil {
		exsMap = orm.GetExSymbolMap(cfg.Exchange.Name, cfg.MarketType)
	}

	return c.JSON(fiber.Map{
		"path":   btPath,
		"detail": detail,
		"task":   task.ToMap(),
		"exsMap": exsMap,
	})
}

// getBtOrders 获取回测订单
func getBtOrders(c *fiber.Ctx) error {
	type OrderArgs struct {
		TaskID    int64  `query:"task_id" validate:"required"`
		Page      int    `query:"page"`
		PageSize  int    `query:"page_size"`
		Symbol    string `query:"symbol"`
		Strategy  string `query:"strategy"`
		EnterTag  string `query:"enter_tag"`
		ExitTag   string `query:"exit_tag"`
		StartMS   int64  `query:"start_ms"`
		EndMS     int64  `query:"end_ms"`
		SortField string `query:"sort_field"`
		SortOrder string `query:"sort_order"`
	}
	var args = new(OrderArgs)
	if err := base.VerifyArg(c, args, base.ArgQuery); err != nil {
		return err
	}

	if args.Page <= 0 {
		args.Page = 1
	}
	if args.PageSize <= 0 {
		args.PageSize = 20
	}

	qu, conn, err2 := ormu.Conn()
	if err2 != nil {
		return err2
	}
	defer conn.Close()

	task, err := qu.GetTask(context.Background(), args.TaskID)
	if err != nil {
		return fmt.Errorf("query task failed: %v", err)
	}

	reportDirs, pathErr := taskReportDirs(task)
	if pathErr != nil {
		return pathErr
	}
	allOrders := make([]*ormo.InOutOrder, 0)
	for _, reportDir := range reportDirs {
		dbPath := filepath.Join(reportDir, "orders.gob")
		if _, statErr := os.Stat(dbPath); statErr != nil {
			if os.IsNotExist(statErr) && len(reportDirs) > 1 {
				continue
			}
			return statErr
		}
		cached, lock, loadErr := getGobOrders(dbPath)
		if loadErr != nil {
			return loadErr
		}
		lock.Lock()
		allOrders = append(allOrders, cached...)
		lock.Unlock()
	}

	var orders = make([]*ormo.InOutOrder, 0, len(allOrders)/10)
	for _, od := range allOrders {
		if args.Symbol != "" && od.Symbol != args.Symbol {
			continue
		}
		if args.Strategy != "" && od.Strategy != args.Strategy {
			continue
		}
		if args.EnterTag != "" && od.EnterTag != args.EnterTag {
			continue
		}
		if args.ExitTag != "" && od.ExitTag != args.ExitTag {
			continue
		}
		// 时间筛选逻辑：EnterAt < endTime && ExitAt > startTime
		if args.StartMS > 0 && od.ExitAt < args.StartMS {
			continue
		}
		if args.EndMS > 0 && od.EnterAt > args.EndMS {
			continue
		}
		orders = append(orders, od)
	}

	// 排序处理
	if args.SortField != "" {
		sort.Slice(orders, func(i, j int) bool {
			var less bool
			switch args.SortField {
			case "symbol":
				less = orders[i].Symbol < orders[j].Symbol
			case "direction":
				less = !orders[i].Short && orders[j].Short // long < short
			case "leverage":
				less = orders[i].Leverage < orders[j].Leverage
			case "enter_at":
				less = orders[i].EnterAt < orders[j].EnterAt
			case "enter_tag":
				less = orders[i].EnterTag < orders[j].EnterTag
			case "enter_price":
				enterPriceI := float64(0)
				enterPriceJ := float64(0)
				if orders[i].Enter != nil {
					if orders[i].Enter.Average > 0 {
						enterPriceI = orders[i].Enter.Average
					} else {
						enterPriceI = orders[i].Enter.Price
					}
				}
				if orders[j].Enter != nil {
					if orders[j].Enter.Average > 0 {
						enterPriceJ = orders[j].Enter.Average
					} else {
						enterPriceJ = orders[j].Enter.Price
					}
				}
				less = enterPriceI < enterPriceJ
			case "enter_amount":
				enterAmountI := float64(0)
				enterAmountJ := float64(0)
				if orders[i].Enter != nil {
					if orders[i].Enter.Filled > 0 {
						enterAmountI = orders[i].Enter.Filled
					} else {
						enterAmountI = orders[i].Enter.Amount
					}
				}
				if orders[j].Enter != nil {
					if orders[j].Enter.Filled > 0 {
						enterAmountJ = orders[j].Enter.Filled
					} else {
						enterAmountJ = orders[j].Enter.Amount
					}
				}
				less = enterAmountI < enterAmountJ
			case "exit_at":
				less = orders[i].ExitAt < orders[j].ExitAt
			case "exit_tag":
				less = orders[i].ExitTag < orders[j].ExitTag
			case "exit_price":
				exitPriceI := float64(0)
				exitPriceJ := float64(0)
				if orders[i].Exit != nil {
					if orders[i].Exit.Average > 0 {
						exitPriceI = orders[i].Exit.Average
					} else {
						exitPriceI = orders[i].Exit.Price
					}
				}
				if orders[j].Exit != nil {
					if orders[j].Exit.Average > 0 {
						exitPriceJ = orders[j].Exit.Average
					} else {
						exitPriceJ = orders[j].Exit.Price
					}
				}
				less = exitPriceI < exitPriceJ
			case "exit_amount":
				exitAmountI := float64(0)
				exitAmountJ := float64(0)
				if orders[i].Exit != nil {
					if orders[i].Exit.Filled > 0 {
						exitAmountI = orders[i].Exit.Filled
					} else {
						exitAmountI = orders[i].Exit.Amount
					}
				}
				if orders[j].Exit != nil {
					if orders[j].Exit.Filled > 0 {
						exitAmountJ = orders[j].Exit.Filled
					} else {
						exitAmountJ = orders[j].Exit.Amount
					}
				}
				less = exitAmountI < exitAmountJ
			case "profit":
				less = orders[i].Profit < orders[j].Profit
			default:
				// 默认按时间排序
				less = orders[i].EnterAt < orders[j].EnterAt
			}

			// 如果是降序，反转结果
			if args.SortOrder == "desc" {
				return !less
			}
			return less
		})
	}

	total := len(orders)
	start := (args.Page - 1) * args.PageSize
	end := start + args.PageSize
	if end > total {
		end = total
	}
	if start >= total {
		orders = []*ormo.InOutOrder{}
	} else {
		orders = orders[start:end]
	}

	return c.JSON(fiber.Map{
		"total":  total,
		"orders": orders,
	})
}

// getBtConfig 获取回测配置
func getBtConfig(c *fiber.Ctx) error {
	type ConfigArgs struct {
		TaskID int64 `query:"task_id" validate:"required"`
	}
	var args = new(ConfigArgs)
	if err := base.VerifyArg(c, args, base.ArgQuery); err != nil {
		return err
	}

	btPath, err := getBtPath(args.TaskID)
	if err != nil {
		return fmt.Errorf("get backtest path failed: %v", err)
	}

	// 读取config.yml
	configPath := filepath.Join(btPath, "config.yml")
	if !utils.Exists(configPath) {
		btPath, err = getBtReportPath(args.TaskID, "config.yml")
		if err != nil {
			return fmt.Errorf("get backtest path failed: %v", err)
		}
		configPath = filepath.Join(btPath, "config.yml")
	}
	content, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("read config file failed: %v", err)
	}

	return c.JSON(fiber.Map{
		"data": string(content),
	})
}

// getBtLogs 获取回测日志
func getBtLogs(c *fiber.Ctx) error {
	type LogArgs struct {
		TaskID int64 `query:"task_id" validate:"required"`
		End    int64 `query:"end"`
		Limit  int64 `query:"limit"`
	}
	var args = new(LogArgs)
	if err := base.VerifyArg(c, args, base.ArgQuery); err != nil {
		return err
	}

	btPath, err := getBtReportPath(args.TaskID, "out.log")
	if err != nil {
		return fmt.Errorf("get backtest path failed: %v", err)
	}

	// 读取out.log
	logPath := filepath.Join(btPath, "out.log")
	if !utils.Exists(logPath) {
		return c.JSON(fiber.Map{
			"data":  "no logs",
			"start": 0,
		})
	}

	data, pos, err := utils.ReadFileTail(logPath, args.Limit, args.End)
	if err != nil {
		return fmt.Errorf("read log file failed: %v", err)
	}

	return c.JSON(fiber.Map{
		"data":  string(data),
		"start": pos,
	})
}

// getBtHtml 获取回测HTML报告
func getBtHtml(c *fiber.Ctx) error {
	type HtmlArgs struct {
		TaskID int64  `query:"task_id" validate:"required"`
		Type   string `query:"type" validate:"required"` // assets 或 enters
	}
	var args = new(HtmlArgs)
	if err := base.VerifyArg(c, args, base.ArgQuery); err != nil {
		return err
	}

	marker := "assets.html"
	if args.Type == "enters" {
		marker = "enters.html"
	}
	btPath, err := getBtReportPath(args.TaskID, marker)
	if err != nil {
		return fmt.Errorf("get backtest path failed: %v", err)
	}

	var htmlPath string
	if args.Type == "assets" {
		htmlPath = filepath.Join(btPath, "assets.html")
	} else if args.Type == "enters" {
		htmlPath = filepath.Join(btPath, "enters.html")
	} else {
		return fmt.Errorf("invalid type: %s", args.Type)
	}

	content, err := os.ReadFile(htmlPath)
	if err != nil {
		return fmt.Errorf("read html file failed: %v", err)
	}

	c.Set("Content-Type", "text/html")
	return c.Send(content)
}

// getBtStratTree 获取回测策略代码文件树
func getBtStratTree(c *fiber.Ctx) error {
	type TreeArgs struct {
		TaskID int64 `query:"task_id" validate:"required"`
	}
	var args = new(TreeArgs)
	if err := base.VerifyArg(c, args, base.ArgQuery); err != nil {
		return err
	}

	btPath, err := getBtReportPath(args.TaskID, "detail.json")
	if err != nil {
		return fmt.Errorf("get backtest path failed: %v", err)
	}

	var files []FileNode
	err = filepath.Walk(btPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// 只处理strat_开头的目录及其内容
		relPath, err := filepath.Rel(btPath, path)
		if err != nil {
			return err
		}
		relPath = strings.ReplaceAll(relPath, "\\", "/")

		parts := strings.Split(relPath, "/")
		if len(parts) > 0 && !strings.HasPrefix(parts[0], "strat_") && parts[0] != "." {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		if relPath == "." {
			return nil
		}

		if info.IsDir() {
			files = append(files, FileNode{
				Path: relPath + "/",
			})
		} else {
			files = append(files, FileNode{
				Path:  relPath,
				Size:  info.Size(),
				Stamp: info.ModTime().UnixMilli(),
			})
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("walk directory failed: %v", err)
	}

	return c.JSON(fiber.Map{
		"code": 200,
		"data": files,
	})
}

func getBtStratText(c *fiber.Ctx) error {
	type TextArgs struct {
		TaskID int64  `query:"task_id" validate:"required"`
		Path   string `query:"path" validate:"required"`
	}

	var args = new(TextArgs)
	if err := base.VerifyArg(c, args, base.ArgQuery); err != nil {
		return err
	}

	btPath, err := getBtReportPath(args.TaskID, "detail.json")
	if err != nil {
		return err
	}

	filePath, pathErr := reportFilePath(btPath, args.Path)
	if pathErr != nil {
		return pathErr
	}
	content, err2 := utils.ReadTextFile(filePath)
	if err2 != nil {
		return err2
	}

	return c.JSON(fiber.Map{
		"data": content,
	})
}

// GetSymbolsHandler 获取交易品种列表
func GetSymbolsHandler(c *fiber.Ctx) error {
	type SymbolArgs struct {
		Exchange string `query:"exchange"`
		Market   string `query:"market"`
		Symbol   string `query:"symbol"`
		Settle   string `query:"settle"`
		Limit    int    `query:"limit"`
		AfterID  int32  `query:"after_id"`
		Short    bool   `query:"short"`
	}

	var args = new(SymbolArgs)
	if err := base.VerifyArg(c, args, base.ArgQuery); err != nil {
		return err
	}
	if strings.TrimSpace(args.Exchange) == "" {
		args.Exchange = core.ExgName
	}
	if strings.TrimSpace(args.Market) == "" {
		args.Market = core.Market
	}

	if _, ok := exg.AllowExgIds[args.Exchange]; !ok && args.Exchange != "" {
		return fmt.Errorf("invalid exchange: %s", args.Exchange)
	}
	if _, ok := banexg.AllMarketTypes[args.Market]; !ok && args.Market != "" {
		return fmt.Errorf("invalid market: %s", args.Market)
	}

	if args.Limit <= 0 && !args.Short {
		args.Limit = 20
	}

	// 获取所有品种
	allSymbols := orm.GetExSymbols(args.Exchange, args.Market)

	if len(allSymbols) == 0 && args.Exchange != "" {
		exchange, err := exg.GetWith(args.Exchange, args.Market, "")
		if err != nil {
			return err
		}
		err = orm.InitExg(exchange)
		if err != nil {
			return err
		}
		allSymbols = orm.GetExSymbols(args.Exchange, args.Market)
	}

	// 过滤
	var filtered []*orm.ExSymbol
	if args.Symbol != "" || args.Settle != "" {
		lowSymbol := strings.ToLower(args.Symbol)
		for _, s := range allSymbols {
			if lowSymbol != "" && !strings.Contains(strings.ToLower(s.Symbol), lowSymbol) {
				continue
			}
			if args.Settle != "" && !strings.HasSuffix(s.Symbol, args.Settle) {
				continue
			}
			filtered = append(filtered, s)
		}
	} else {
		filtered = make([]*orm.ExSymbol, 0, len(allSymbols))
		for _, exs := range allSymbols {
			filtered = append(filtered, exs)
		}
	}

	// 按ID排序
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].ID < filtered[j].ID
	})

	// 获取总数
	total := len(filtered)

	// 根据afterId过滤
	if args.AfterID > 0 {
		for i, s := range filtered {
			if s.ID > args.AfterID {
				filtered = filtered[i:]
				break
			}
		}
	}

	// 截取limit个
	if args.Limit > 0 && len(filtered) > args.Limit {
		filtered = filtered[:args.Limit]
	}

	if args.Short {
		dataMap := make(map[string]int32)
		for _, exs := range filtered {
			dataMap[exs.Symbol] = exs.ID
		}
		return c.JSON(fiber.Map{
			"total":    total,
			"data":     dataMap,
			"exchange": args.Exchange,
			"market":   args.Market,
		})
	}

	return c.JSON(fiber.Map{
		"total":    total,
		"data":     filtered,
		"exchange": args.Exchange,
		"market":   args.Market,
	})
}

// getSymbolInfo 获取品种详情
func getSymbolInfo(c *fiber.Ctx) error {
	type SymbolArgs struct {
		ID int32 `query:"id" validate:"required"`
	}
	var args = new(SymbolArgs)
	if err_ := base.VerifyArg(c, args, base.ArgQuery); err_ != nil {
		return err_
	}

	// 获取品种信息
	symbol := orm.GetSymbolByID(args.ID)
	if symbol == nil {
		return fmt.Errorf("symbol not found: %d", args.ID)
	}

	// 获取K线信息
	sess, conn, err := orm.Conn(nil)
	if err != nil {
		return err
	}
	defer conn.Release()
	sranges, err_ := sess.ListSRangesBySid(context.Background(), args.ID)
	if err_ != nil {
		return err_
	}

	// 获取复权因子
	var adjFactors []*orm.AdjInfo
	if symbol.Combined {
		var err *errs.Error
		adjFactors, err = orm.GetAdjs(args.ID)
		if err != nil {
			return err
		}
	}

	return c.JSON(fiber.Map{
		"symbol":     symbol,
		"sranges":    sranges,
		"adjFactors": adjFactors,
	})
}

// getSymbolGaps 获取品种空洞数据
func getSymbolGaps(c *fiber.Ctx) error {
	type GapsArgs struct {
		ID        int32  `query:"id" validate:"required"`
		TimeFrame string `query:"tf"`
		StartMS   int64  `query:"start"`
		EndMS     int64  `query:"end"`
		Offset    int    `query:"offset"`
		Limit     int    `query:"limit"`
	}
	var args = new(GapsArgs)
	if err := base.VerifyArg(c, args, base.ArgQuery); err != nil {
		return err
	}

	if args.Limit <= 0 {
		args.Limit = 20
	}

	// 查询范围数据（has_data=false表示空洞/无数据区间）
	sess, conn, err := orm.Conn(nil)
	if err != nil {
		return err
	}
	defer conn.Release()
	hasData := false
	ranges, total, err2 := sess.FindSRanges(orm.FindSRangesArgs{
		Sid:       args.ID,
		Table:     "kline_" + args.TimeFrame,
		TimeFrame: args.TimeFrame,
		Start:     args.StartMS,
		Stop:      args.EndMS,
		Offset:    args.Offset,
		Limit:     args.Limit,
		HasData:   &hasData,
	})
	if err2 != nil {
		return err2
	}

	return c.JSON(fiber.Map{
		"data":  ranges,
		"total": total,
	})
}

// getSymbolData 获取品种K线数据
func getSymbolData(c *fiber.Ctx) error {
	type DataArgs struct {
		ID        int32  `query:"id" validate:"required"`
		TimeFrame string `query:"tf" validate:"required"`
		StartMS   int64  `query:"start"`
		EndMS     int64  `query:"end"`
		Limit     int    `query:"limit"`
	}
	var args = new(DataArgs)
	if err := base.VerifyArg(c, args, base.ArgQuery); err != nil {
		return err
	}

	if args.Limit <= 0 {
		args.Limit = 100
	}
	if args.StartMS <= 0 && args.EndMS <= 0 {
		args.StartMS = core.MSMinStamp
	}

	sess, conn, err := orm.Conn(nil)
	if err != nil {
		return err
	}
	defer conn.Release()

	// 查询K线数据
	exs := orm.GetSymbolByID(args.ID)
	data, err := sess.QuerySeries(exs, args.TimeFrame, args.StartMS, args.EndMS, args.Limit, false)
	if err != nil {
		return err
	}

	// 转换为float64数组
	result := make([][]float64, len(data))
	for i, row := range data {
		if row == nil {
			return errs.NewMsg(core.ErrInvalidBars, "series row is nil")
		}
		open, err_ := row.OpenValue()
		if err_ != nil {
			return errs.New(core.ErrInvalidBars, err_)
		}
		high, err_ := row.HighValue()
		if err_ != nil {
			return errs.New(core.ErrInvalidBars, err_)
		}
		low, err_ := row.LowValue()
		if err_ != nil {
			return errs.New(core.ErrInvalidBars, err_)
		}
		closeVal, err_ := row.CloseValue()
		if err_ != nil {
			return errs.New(core.ErrInvalidBars, err_)
		}
		volume, err_ := row.VolumeValue()
		if err_ != nil {
			return errs.New(core.ErrInvalidBars, err_)
		}
		result[i] = []float64{
			float64(row.TimeMS), open, high, low, closeVal, volume,
		}
	}

	return c.JSON(fiber.Map{
		"data": result,
	})
}

func getSeriesRanges(c *fiber.Ctx) error {
	type Args struct {
		Source    string `query:"source"`
		Table     string `query:"table"`
		TimeFrame string `query:"tf"`
		Sid       int32  `query:"sid"`
		HasData   string `query:"has_data"`
		Offset    int    `query:"offset"`
		Limit     int    `query:"limit"`
	}
	var args Args
	if err := base.VerifyArg(c, &args, base.ArgQuery); err != nil {
		return err
	}
	var hasData *bool
	if args.HasData != "" && args.HasData != "all" {
		val := args.HasData == "true" || args.HasData == "1"
		hasData = &val
	}
	sess, conn, err := orm.Conn(nil)
	if err != nil {
		return err
	}
	defer conn.Release()
	ranges, total, err2 := sess.ListSeriesRangeSummaries(orm.ListSeriesRangeSummariesArgs{
		Source:    args.Source,
		Table:     args.Table,
		TimeFrame: args.TimeFrame,
		Sid:       args.Sid,
		HasData:   hasData,
		Offset:    args.Offset,
		Limit:     args.Limit,
	})
	if err2 != nil {
		return err2
	}
	return c.JSON(fiber.Map{"data": ranges, "total": total})
}

// getBuildEnvs 获取Go支持的所有构建环境
func getBuildEnvs(c *fiber.Ctx) error {
	// 执行 go tool dist list 命令
	cmd := exec.Command("go", "tool", "dist", "list")
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("execute command failed: %v", err)
	}

	// 将输出按行分割
	envs := strings.Split(strings.TrimSpace(string(output)), "\n")

	return c.JSON(fiber.Map{
		"data": envs,
	})
}

// handleDownload 处理文件下载请求
func handleDownload(c *fiber.Ctx) error {
	type DownloadArgs struct {
		Path string `query:"path" validate:"required"`
	}
	var args = new(DownloadArgs)
	if err := base.VerifyArg(c, args, base.ArgQuery); err != nil {
		return err
	}

	// 解析路径
	absPath, err := parsePath(args.Path)
	if err != nil {
		return err
	}

	// 检查文件是否存在
	if _, err := os.Stat(absPath); err != nil {
		if os.IsNotExist(err) {
			return c.Status(404).JSON(fiber.Map{
				"msg": "File not found",
			})
		}
		return err
	}

	// 获取文件名
	fileName := filepath.Base(absPath)

	// 设置下载头
	c.Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, fileName))
	c.Set("Content-Type", "application/octet-stream")

	// 发送文件
	return c.SendFile(absPath)
}

func getCompareAssets(c *fiber.Ctx) error {
	ids := c.Query("ids")
	if ids == "" {
		return fiber.NewError(fiber.StatusBadRequest, "ids is required")
	}
	idList := strings.Split(ids, ",")
	if len(idList) < 2 {
		return fiber.NewError(fiber.StatusBadRequest, "at least 2 ids are required")
	}

	qu, conn, err2 := ormu.Conn()
	if err2 != nil {
		return err2
	}
	defer conn.Close()

	// 构建files参数
	files := make(map[string]string)
	for _, id := range idList {
		idVal, err := strconv.Atoi(id)
		if err != nil {
			return fmt.Errorf("task id must be int, current: %v", idVal)
		}
		task, err := qu.GetTask(context.Background(), int64(idVal))
		if err != nil {
			return fmt.Errorf("query task %v failed: %v", idVal, err)
		}
		if task.Path == "" {
			continue
		}
		reportDirs, pathErr := taskReportDirs(task)
		if pathErr != nil {
			return pathErr
		}
		reportDir := firstReportDir(reportDirs, "assets.html")
		path := filepath.Join(reportDir, "assets.html")
		if !utils.Exists(path) {
			return fiber.NewError(fiber.StatusBadRequest, fmt.Sprintf("assets.html not found for id %s, path: %s", id, path))
		}
		files[path] = id
	}

	// 创建临时文件
	file, err := os.CreateTemp("", "ban_merge_assets")
	if err != nil {
		return err
	}
	tmpFile := file.Name()
	defer os.Remove(tmpFile)

	err2 = opt.MergeAssetsHtml(tmpFile, files, nil, true)
	if err2 != nil {
		return err2
	}

	// 读取临时文件内容
	content, err_ := os.ReadFile(tmpFile)
	if err_ != nil {
		return fiber.NewError(fiber.StatusInternalServerError, "read temp file failed")
	}

	// 设置响应头
	c.Set("Content-Type", "text/html")
	return c.Send(content)
}

func delBacktestReports(c *fiber.Ctx) error {
	type DelArgs struct {
		IDs    []int64  `json:"ids"`
		Hashes []string `json:"hashes"`
	}
	var args = new(DelArgs)
	if err := base.VerifyArg(c, args, base.ArgBody); err != nil {
		return err
	}

	qu, conn, err2 := ormu.Conn()
	if err2 != nil {
		return err2
	}
	defer conn.Close()

	// 构建files参数
	files := make(map[string]bool)
	tasks := make([]int64, 0, len(args.IDs))
	taskIDs := make(map[int64]struct{}, len(args.IDs))
	failNum := 0
	for _, id := range args.IDs {
		task, err := qu.GetTask(context.Background(), id)
		if err != nil {
			failNum += 1
			log.Error("query task fail", zap.Int64("id", id), zap.Error(err))
			continue
		}
		if _, seen := taskIDs[id]; !seen {
			tasks = append(tasks, id)
			taskIDs[id] = struct{}{}
		}
		if task.Path != "" {
			path, pathErr := taskBaseDir(task)
			if pathErr != nil {
				failNum++
				log.Error("invalid backtest task path", zap.Int64("id", id), zap.String("path", task.Path), zap.Error(pathErr))
				continue
			}
			files[path] = utils.Exists(path)
		}
	}
	for _, hash := range args.Hashes {
		path, pathErr := taskBaseDir(&ormu.Task{Path: hash})
		if pathErr != nil {
			return fiber.NewError(fiber.StatusBadRequest, fmt.Sprintf("invalid backtest path: %v", pathErr))
		}
		files[path] = utils.Exists(path)
		rows, err := qu.FindTasks(context.Background(), ormu.FindTasksParams{
			Path: hash,
		})
		if err != nil {
			log.Error("FindTasks by hash fail", zap.String("hash", hash), zap.Error(err))
			continue
		}
		for _, r := range rows {
			if _, seen := taskIDs[r.ID]; !seen {
				tasks = append(tasks, r.ID)
				taskIDs[r.ID] = struct{}{}
			}
		}
	}
	for path, exist := range files {
		if !exist {
			continue
		}
		err := utils.RemovePath(path, true)
		if err != nil {
			log.Error("delete fail", zap.Error(err))
			failNum += 1
		}
	}
	if len(tasks) > 0 {
		err := qu.DelTasks(context.Background(), tasks)
		if err != nil {
			log.Error("delete records fail", zap.Error(err))
		}
	}
	return c.JSON(fiber.Map{
		"success": len(files) - failNum,
		"fail":    failNum,
	})
}

// handleUpdateNote 处理更新回测任务备注的请求
func handleUpdateNote(c *fiber.Ctx) error {
	type UpdateNoteArgs struct {
		TaskID int64  `json:"taskId" validate:"required"`
		Note   string `json:"note"`
	}
	var args = new(UpdateNoteArgs)
	if err := base.VerifyArg(c, args, base.ArgBody); err != nil {
		return err
	}

	qu, conn, err := ormu.Conn()
	if err != nil {
		return err
	}
	defer conn.Close()

	err_ := qu.SetTaskNote(context.Background(), ormu.SetTaskNoteParams{
		ID:   args.TaskID,
		Note: args.Note,
	})
	if err_ != nil {
		return fmt.Errorf("update task note failed: %v", err_)
	}

	return c.JSON(fiber.Map{
		"code": 200,
	})
}
