package data

import (
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

func (d *RuntimeDeps) logger() *zap.Logger {
	if d == nil || d.Core == nil {
		return log.L()
	}
	if logger := d.Core.Log(); logger != nil {
		return logger
	}
	return log.L()
}
