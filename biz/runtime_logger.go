package biz

import (
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

func (d *RuntimeDeps) Logger() *zap.Logger {
	if d == nil {
		return log.L()
	}
	return d.Core.Log()
}

func (t *Trader) Logger() *zap.Logger { return t.RuntimeDependencies().Logger() }

func (o *OrderMgr) Logger() *zap.Logger {
	if o.runtimeDeps {
		return o.walletDeps.Logger()
	}
	return log.L()
}
