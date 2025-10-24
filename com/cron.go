package com

import (
	"github.com/banbox/banbot/btime"
	"github.com/banbox/bntp"
	"github.com/banbox/cron/v3"
	"log/slog"
	"sync"
)

var (
	cronObj  *cron.Cron // Use cron to run tasks regularly 使用cron定时运行任务
	cronOnce sync.Once
)

func Cron() *cron.Cron {
	cronOnce.Do(func() {
		// for cron logging
		slog.SetLogLoggerLevel(slog.LevelWarn)
		clock := cron.NewNtpClock(btime.LocShow, bntp.LangCode)
		cronObj = cron.New(cron.WithSeconds(), cron.WithClock(clock))
	})
	return cronObj
}
