package entry

import (
	"time"

	runtimepkg "github.com/banbox/banbot/runtime"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// shutdownNotifyWait bounds how long process exit may wait for the "bot stop"
// message to be handed to the notification channels.
const shutdownNotifyWait = 3 * time.Second

// attachRuntimeNotifications wires the notification session of an
// execution-backed robot runtime: it starts the rpc channels in the background
// (so Telegram commands work from startup) and adds the exception-alert log
// core to the runtime logger, as configureRuntimeLogger does on the legacy
// paths. The returned function sends the shutdown status message (bounded wait)
// and must be deferred after the runtime's Close/Join defer so it runs first.
// Nothing here can fail the robot.
func attachRuntimeNotifications(rt *runtimepkg.Runtime) func() {
	if rt == nil || rt.Notifications == nil || rt.Core == nil {
		return func() {}
	}
	if core := rt.ExcNotifyCore(); core != nil {
		rt.Core.Logger = rt.Core.Log().WithOptions(zap.WrapCore(func(base zapcore.Core) zapcore.Core {
			return zapcore.NewTee(base, core)
		}))
	}
	return func() { rt.NotifyShutdown(shutdownNotifyWait) }
}
