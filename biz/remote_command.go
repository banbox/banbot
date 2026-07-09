package biz

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

const (
	RemoteSourceWeb      = "web"
	RemoteSourceTelegram = "telegram"
	RemoteSourceCLI      = "cli"

	RemoteActionCloseOrder    = "close_order"
	RemoteActionTradingSwitch = "trading_switch"
)

type RemoteCommand struct {
	Source       string
	Actor        string
	Account      string
	Action       string
	Idempotency  string
	Confirmed    bool
	OrderID      int64
	All          bool
	Enable       bool
	DisableHours int
	UntilMS      int64
	ExitTag      string
	ExecClose    func() (int, int, *errs.Error)
}

type RemoteCommandResult struct {
	Idempotent bool
	CloseNum   int
	FailNum    int
	UntilMS    int64
}

type RemoteCommandService struct {
	mu      sync.Mutex
	seen    map[string]*RemoteCommandResult
	lastHit map[string]time.Time
	now     func() time.Time
}

func NewRemoteCommandService() *RemoteCommandService {
	return &RemoteCommandService{
		seen:    make(map[string]*RemoteCommandResult),
		lastHit: make(map[string]time.Time),
		now:     time.Now,
	}
}

var defaultRemoteCommandService = NewRemoteCommandService()

func RunRemoteCommand(cmd RemoteCommand) (*RemoteCommandResult, *errs.Error) {
	return defaultRemoteCommandService.Run(cmd)
}

func (s *RemoteCommandService) Run(cmd RemoteCommand) (*RemoteCommandResult, *errs.Error) {
	if cmd.Source == "" || cmd.Action == "" || cmd.Account == "" {
		return nil, errs.NewMsg(errs.CodeParamRequired, "source/action/account required")
	}
	if cfg, ok := config.Accounts[cmd.Account]; !ok {
		return nil, errs.NewMsg(errs.CodeParamInvalid, "account invalid: %s", cmd.Account)
	} else if cfg.NoTrade {
		return nil, errs.NewMsg(errs.CodeParamInvalid, "account no trade permission: %s", cmd.Account)
	}
	if cmd.Actor == "" {
		cmd.Actor = cmd.Source
	}
	hasIdempotency := cmd.Idempotency != "" || (cmd.Action == RemoteActionCloseOrder && cmd.OrderID > 0)
	if hasIdempotency && cmd.Idempotency == "" {
		cmd.Idempotency = cmd.defaultKey()
	}

	s.mu.Lock()
	if hasIdempotency {
		if res, ok := s.seen[cmd.Idempotency]; ok {
			s.auditLocked(cmd, "idempotent", res, nil)
			s.mu.Unlock()
			cp := *res
			cp.Idempotent = true
			return &cp, nil
		}
	}
	now := s.now()
	rateKey := cmd.Source + ":" + cmd.Actor + ":" + cmd.Action
	if last, ok := s.lastHit[rateKey]; ok && now.Sub(last) < time.Second {
		s.auditLocked(cmd, "rate_limited", nil, nil)
		s.mu.Unlock()
		return nil, errs.NewMsg(core.ErrOutOfResource, "remote command rate limited")
	}
	s.lastHit[rateKey] = now
	s.mu.Unlock()

	var res *RemoteCommandResult
	var err *errs.Error
	switch cmd.Action {
	case RemoteActionCloseOrder:
		res, err = s.runClose(cmd)
	case RemoteActionTradingSwitch:
		res, err = s.runTradingSwitch(cmd)
	default:
		err = errs.NewMsg(errs.CodeParamInvalid, "invalid remote action: %s", cmd.Action)
	}

	s.mu.Lock()
	if err == nil {
		if hasIdempotency {
			s.seen[cmd.Idempotency] = res
		}
		s.auditLocked(cmd, "ok", res, nil)
	} else {
		s.auditLocked(cmd, "error", res, err)
	}
	s.mu.Unlock()
	return res, err
}

func (s *RemoteCommandService) runClose(cmd RemoteCommand) (*RemoteCommandResult, *errs.Error) {
	if cmd.All && !cmd.Confirmed {
		return nil, errs.NewMsg(errs.CodeParamInvalid, "close all requires confirmation")
	}
	if cmd.ExecClose == nil {
		return nil, errs.NewMsg(errs.CodeParamRequired, "close executor required")
	}
	closeNum, failNum, err := cmd.ExecClose()
	return &RemoteCommandResult{CloseNum: closeNum, FailNum: failNum}, err
}

func (s *RemoteCommandService) runTradingSwitch(cmd RemoteCommand) (*RemoteCommandResult, *errs.Error) {
	if cmd.Enable == (cmd.DisableHours > 0 || cmd.UntilMS > 0) {
		return nil, errs.NewMsg(errs.CodeParamInvalid, "set exactly one trading switch action")
	}
	var untilMS int64
	if cmd.Enable {
		delete(core.NoEnterUntil, cmd.Account)
	} else {
		untilMS = cmd.UntilMS
		if untilMS == 0 {
			untilMS = btime.TimeMS() + int64(cmd.DisableHours)*3600*1000
		}
		core.NoEnterUntil[cmd.Account] = untilMS
	}
	return &RemoteCommandResult{UntilMS: untilMS}, nil
}

func (s *RemoteCommandService) auditLocked(cmd RemoteCommand, status string, res *RemoteCommandResult, err *errs.Error) {
	fields := []zap.Field{
		zap.String("source", cmd.Source),
		zap.String("actor", cmd.Actor),
		zap.String("account", cmd.Account),
		zap.String("action", cmd.Action),
		zap.String("idempotency", cmd.Idempotency),
		zap.String("status", status),
	}
	if cmd.OrderID > 0 {
		fields = append(fields, zap.Int64("order_id", cmd.OrderID))
	}
	if cmd.All {
		fields = append(fields, zap.Bool("all", true))
	}
	if res != nil {
		fields = append(fields, zap.Int("close_num", res.CloseNum), zap.Int("fail_num", res.FailNum), zap.Int64("until_ms", res.UntilMS))
	}
	if err != nil {
		fields = append(fields, zap.Error(err))
	}
	log.Info("remote command audit", fields...)
}

func (cmd RemoteCommand) defaultKey() string {
	parts := []string{cmd.Source, cmd.Actor, cmd.Account, cmd.Action}
	if cmd.OrderID > 0 {
		parts = append(parts, fmt.Sprintf("order:%d", cmd.OrderID))
	}
	if cmd.All {
		parts = append(parts, "all")
	}
	if cmd.Enable {
		parts = append(parts, "enable")
	}
	if cmd.DisableHours > 0 {
		parts = append(parts, fmt.Sprintf("disable:%d", cmd.DisableHours))
	}
	if cmd.UntilMS > 0 {
		parts = append(parts, fmt.Sprintf("until:%d", cmd.UntilMS))
	}
	return strings.Join(parts, ":")
}

func CloseBotOrdersRemote(cmd RemoteCommand) (*RemoteCommandResult, *errs.Error) {
	if cmd.ExitTag == "" {
		cmd.ExitTag = core.ExitTagUserExit
	}
	cmd.Action = RemoteActionCloseOrder
	cmd.ExecClose = func() (int, int, *errs.Error) {
		orders, err := FindOpenOrders(cmd.Account, cmd.OrderID)
		if err != nil {
			return 0, 0, err
		}
		return CloseAccOrders(cmd.Account, orders, &strat.ExitReq{Tag: cmd.ExitTag, Force: true})
	}
	return RunRemoteCommand(cmd)
}

func FindOpenOrders(account string, orderID int64) ([]*ormo.InOutOrder, *errs.Error) {
	sess, conn, err := ormo.Conn(orm.DbTrades, false)
	if err != nil {
		return nil, errs.New(errs.CodeRunTime, err)
	}
	defer conn.Close()
	orders, getErr := sess.GetOrders(ormo.GetOrdersArgs{
		TaskID: ormo.GetTaskID(account),
		Status: 1,
	})
	if getErr != nil {
		return nil, errs.New(errs.CodeRunTime, getErr)
	}
	if orderID <= 0 {
		return orders, nil
	}
	for _, od := range orders {
		if od.ID == orderID {
			return []*ormo.InOutOrder{od}, nil
		}
	}
	return nil, errs.NewMsg(errs.CodeParamInvalid, "order not found: %d", orderID)
}
