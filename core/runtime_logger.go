package core

import (
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

var silentLogger = zap.NewNop()

// Log returns the logger bound before execution. A nil State denotes a legacy
// caller; an explicit zero-value State never discovers a process logger.
func (s *State) Log() *zap.Logger {
	if s == nil {
		return log.L()
	}
	if s.Logger == nil {
		return silentLogger
	}
	return s.Logger
}
