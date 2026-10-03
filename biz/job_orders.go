package biz

import (
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
)

// ProcessJobOrders handles requests from asynchronous strategy callbacks using
// the job's own runtime, or the legacy manager for a legacy job.
func ProcessJobOrders(job *strat.StratJob) ([]*ormo.InOutOrder, []*ormo.InOutOrder, *errs.Error) {
	if job == nil {
		return nil, nil, errs.NewMsg(core.ErrBadConfig, "strategy job is required")
	}
	if entered, exited, err, bound := job.ProcessRuntimeOrders(); bound {
		return entered, exited, err
	}
	mgr := GetOdMgr(job.Account)
	if mgr == nil {
		return nil, nil, errs.NewMsg(core.ErrBadConfig, "order manager is not available for account %s", job.Account)
	}
	return mgr.ProcessOrders(job)
}

func bindRuntimeOrderProcessor(deps RuntimeDeps) *errs.Error {
	if deps.Trading == nil || deps.DefaultAccount == "" {
		return nil
	}
	return deps.Strategies.BindOrderProcessor(deps.Trading, deps.DefaultAccount, func(job *strat.StratJob) ([]*ormo.InOutOrder, []*ormo.InOutOrder, *errs.Error) {
		account := job.Account
		if account == "" {
			account = deps.DefaultAccount
		}
		mgr := GetOdMgrWithState(deps.Trading, account)
		if mgr == nil {
			return nil, nil, errs.NewMsg(core.ErrBadConfig, "runtime order manager is not available for account %s", account)
		}
		return mgr.ProcessOrders(job)
	})
}
