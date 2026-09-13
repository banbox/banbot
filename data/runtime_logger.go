package data

import (
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

func (d *RuntimeDeps) logger() *zap.Logger {
	if d == nil {
		return log.L()
	}
	return d.Core.Log()
}
