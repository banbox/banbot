package entry

import (
	"os"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// openEntryLogger owns only this command's outputs. banexg.InitLogger also
// replaces its process-level loggers, so use the explicit writer constructor.
func openEntryLogger(args config.CmdArgs) (*zap.Logger, func(), *errs.Error) {
	level := args.LogLevel
	if level == "" {
		level = "info"
	}
	writers := []zapcore.WriteSyncer{zapcore.Lock(zapcore.AddSync(os.Stdout))}
	var file *lumberjack.Logger
	if args.Logfile != "" {
		if info, err := os.Stat(args.Logfile); err == nil && info.IsDir() {
			return nil, nil, errs.NewMsg(core.ErrBadConfig, "log file is a directory: %s", args.Logfile)
		}
		file = &lumberjack.Logger{Filename: args.Logfile, MaxSize: 300, MaxBackups: 10, MaxAge: 30}
		writers = append(writers, zapcore.AddSync(file))
	}
	logger, _, err := log.InitLoggerWithWriteSyncer(&log.Config{
		Level: level, Format: "text", DisableStacktrace: true,
	}, zapcore.NewMultiWriteSyncer(writers...), nil)
	if err != nil {
		if file != nil {
			_ = file.Close()
		}
		return nil, nil, errs.New(core.ErrBadConfig, err)
	}
	return logger, func() {
		_ = logger.Sync()
		if file != nil {
			_ = file.Close()
		}
	}, nil
}
