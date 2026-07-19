package config

import (
	"time"

	"github.com/sasha-s/go-deadlock"
	"go.uber.org/zap"

	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap/zapcore"
)

func (a *CmdArgs) Init() {
	if a.Inited {
		return
	}
	a.TimeFrames = utils.SplitSolid(a.RawTimeFrames, ",", true)
	a.Pairs = utils.SplitSolid(a.RawPairs, ",", true)
	a.Tables = utils.SplitSolid(a.RawTables, ",", true)
	if a.DataDir != "" {
		DataDir = a.DataDir
	}
	if a.InPath != "" {
		a.InPath = ParsePath(a.InPath)
	}
	if a.OutPath != "" {
		a.OutPath = ParsePath(a.OutPath)
	}
	if a.DeadLock {
		deadlock.Opts.Disable = false
	}
	a.Inited = true
}

func (a *CmdArgs) parseTimeZone() (*time.Location, *errs.Error) {
	if a.TimeZone != "" {
		loc, err_ := time.LoadLocation(a.TimeZone)
		if err_ != nil {
			err := errs.NewMsg(errs.CodeRunTime, "unsupport timezone: %s, %v", a.TimeZone, err_)
			return nil, err
		}
		return loc, nil
	}
	return nil, nil
}

func (a *CmdArgs) SetLog(showLog bool, handlers ...zapcore.Core) {
	logCfg := &log.Config{
		Stdout:            true,
		Format:            "text",
		Level:             a.LogLevel,
		Handlers:          handlers,
		DisableStacktrace: true,
	}
	if a.LogLevel == "" {
		logCfg.Level = "info"
	}
	logFile := a.Logfile
	if logFile != "" {
		logCfg.File = &log.FileLogConfig{
			LogPath:    logFile,
			MaxSize:    300,
			MaxBackups: 10,
			MaxDays:    30,
		}
	}
	log.SetupLogger(logCfg)
	if showLog && logFile != "" {
		log.Info("Log To", zap.String("path", logFile))
	}
}
