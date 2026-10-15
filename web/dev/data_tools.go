package dev

import (
	"fmt"

	"github.com/sasha-s/go-deadlock"

	"github.com/banbox/banbot/utils"
	utils2 "github.com/banbox/banexg/utils"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/web/base"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
)

// DataToolsManager 数据工具任务管理器
type DataToolsManager struct {
	running    bool
	runningMux deadlock.Mutex
}

type DataToolsArgs struct {
	Action      string `json:"action" validate:"required"`
	Folder      string `json:"folder"`
	Exchange    string `json:"exchange"`
	ExgReal     string `json:"exgReal"`
	Market      string `json:"market"`
	Exg         banexg.BanExchange
	Pairs       []string `json:"pairs"`
	Periods     []string `json:"periods"`
	StartMs     int64    `json:"startMs"`
	EndMs       int64    `json:"endMs"`
	Force       bool     `json:"force"`
	Concurrency int      `json:"concurrency"`
	Config      string   `json:"config"`
}

var validActions = map[string]bool{
	"download": true,
	"export":   true,
	"import":   true,
	"purge":    true,
	"correct":  true,
}

// StartTask 开始一个任务
func (m *DataToolsManager) StartTask() error {
	m.runningMux.Lock()
	defer m.runningMux.Unlock()

	if m.running {
		return fmt.Errorf("another task is running, please wait")
	}
	m.running = true
	return nil
}

// EndTask 结束任务
func (m *DataToolsManager) EndTask() {
	m.runningMux.Lock()
	m.running = false
	m.runningMux.Unlock()
}

func (s *DevServer) runDataTools(deps *data.RuntimeDeps, args *DataToolsArgs) *errs.Error {
	if s.maintenance == nil {
		return errs.NewMsg(errs.CodeParamRequired, "dev server maintenance runner is required")
	}
	tasks, weights := dataToolProgress(args.Action)
	pBar := utils.NewStagedPrg(tasks, weights)
	pBar.AddTrigger("", func(task string, progress float64) {
		s.BroadcastWS("", map[string]interface{}{"type": "heavyPrg", "name": task, "progress": progress})
	})
	return s.maintenance(s.ctx, deps, args, pBar)
}

func dataToolProgress(action string) ([]string, []float64) {
	switch action {
	case "download":
		return []string{"downKline"}, []float64{1}
	case "export":
		return []string{"holes", "kline"}, []float64{1, 5}
	case "import":
		return []string{"kline", "range"}, []float64{3, 1}
	case "purge":
		return []string{"purge"}, []float64{1}
	default:
		return []string{"syncTFs"}, []float64{1}
	}
}

// handleDataTools 处理数据工具请求
func (s *DevServer) handleDataTools(c *fiber.Ctx) error {
	if s.stopped.Load() {
		return errs.NewMsg(errs.CodeRunTime, "dev server is stopping")
	}
	var args = new(DataToolsArgs)
	if err := base.VerifyArg(c, args, base.ArgBody); err != nil {
		return err
	}

	if !validActions[args.Action] {
		return c.Status(400).JSON(fiber.Map{
			"msg": "invalid action",
		})
	}

	// 验证必填参数
	if args.StartMs > 0 && args.EndMs == 0 {
		if s.Data != nil && s.Data.Clock != nil {
			args.EndMs = s.Data.Clock.TimeMS()
		} else {
			return errs.NewMsg(errs.CodeParamRequired, "dev server clock is required")
		}
	}
	var errMsg string
	mustMarket := false
	if args.Action == "export" || args.Action == "import" {
		if args.Folder == "" {
			errMsg = "folder is required"
		}
	} else {
		mustMarket = true
		if args.Exchange == "" || args.Market == "" {
			errMsg = "exchange & market is required"
		}
		if args.Action != "correct" {
			if args.StartMs == 0 {
				errMsg = "startTime is required"
			} else if len(args.Periods) == 0 {
				errMsg = "periods is required"
			}
		}
	}
	if errMsg != "" {
		return c.Status(400).JSON(fiber.Map{
			"msg": errMsg,
		})
	}

	if err := s.dataTools.StartTask(); err != nil {
		return c.Status(400).JSON(fiber.Map{
			"msg": err.Error(),
		})
	}
	// Keep the admission claim until the asynchronous task is handed off.
	// Every synchronous failure path below must release it.
	claimed := true
	defer func() {
		if claimed {
			s.dataTools.EndTask()
		}
	}()
	deps, cleanup, err := s.dataFor(s.ctx, args.Exchange, args.Market)
	if err != nil {
		return err
	}
	defer func() {
		if claimed {
			cleanup()
		}
	}()

	if mustMarket {
		args.Exg = deps.Exchange

		if len(args.Pairs) == 0 {
			symbols := deps.Symbols
			if symbols == nil {
				return errs.NewMsg(errs.CodeParamRequired, "dev server symbol state is required")
			}
			exsMap := symbols.GetExSymbols(args.Exchange, args.Market)
			for _, exs := range exsMap {
				args.Pairs = append(args.Pairs, exs.Symbol)
			}
		}
	}

	if !args.Force {
		msgTpl := "\nExchange: %s\nExgReal: %s\nMarket: %s\nPairs: %v\nPeriods: %v\nStartMs: %d\nEndMs: %d\n"
		msg := fmt.Sprintf(msgTpl, args.Exchange, args.ExgReal, args.Market, len(args.Pairs),
			args.Periods, args.StartMs, args.EndMs)
		if args.Action == "download" {
			barNum := 0
			for _, tf := range args.Periods {
				tfMSec := int64(utils2.TFToSecs(tf) * 1000)
				singleNum := int((args.EndMs - args.StartMs) / tfMSec)
				barNum += singleNum * len(args.Pairs)
			}
			concurrency := 1
			if s.Data != nil && s.Data.Config != nil && s.Data.Config.View() != nil && s.Data.Config.View().ConcurNum > 0 {
				concurrency = s.Data.Config.View().ConcurNum
			}
			totalMins := barNum/concurrency/core.DownKNumMin + 1
			msg += fmt.Sprintf("Cost Time: %d Hours %d Minutes", totalMins/60, totalMins%60)
		} else if !mustMarket {
			msg = fmt.Sprintf("\nFolder: %s", args.Folder)
		}
		return c.JSON(fiber.Map{
			"code": 401,
			"msg":  msg,
		})
	}

	// 尝试启动任务
	if args.Action == "export" {
		args.Folder = s.ParsePath(args.Folder)
	}

	// The server owns the callback lifetime so Stop/Join cannot leave a
	// maintenance action running against a closed entry session.
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.dataTools.EndTask()
		defer cleanup()
		err := s.runDataTools(deps, args)
		if err != nil {
			log.Error("data tools task failed",
				zap.String("action", args.Action),
				zap.Error(err))
		}
	}()
	claimed = false

	return c.JSON(fiber.Map{
		"code": 200,
		"msg":  "task started",
	})
}
