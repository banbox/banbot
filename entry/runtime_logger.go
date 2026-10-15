package entry

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

func runtimeLogArgs(args config.CmdArgs, snapshot *config.Snapshot, commands ...string) config.CmdArgs {
	if args.Logfile != "" {
		args.Logfile = snapshot.ParsePath(args.Logfile)
	} else if len(commands) > 0 {
		name := filepath.Base(snapshot.View().Name)
		if name == "." || name == string(filepath.Separator) {
			name = "banbot"
		}
		args.Logfile = filepath.Join(snapshot.DataDir, "logs", fmt.Sprintf("%s-%s-%d.log", name, commands[0], os.Getpid()))
	}
	return args
}

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
