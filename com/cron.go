package com

import (
	"context"
	"log/slog"
	"sync"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/bntp"
	"github.com/banbox/cron/v3"
)

// Scheduler is the small lifecycle surface used by runtime-owned jobs. The
// concrete cron implementation remains hidden so callers can own one per
// Runtime without depending on the package singleton.
type Scheduler interface {
	AddFunc(spec string, cmd func()) (cron.EntryID, error)
	Start()
	Stop() context.Context
}

var (
	cronObj  *cron.Cron // Use cron to run tasks regularly 使用cron定时运行任务
	cronOnce sync.Once
)

func newScheduler() *cron.Cron {
	// for cron logging
	slog.SetLogLoggerLevel(slog.LevelWarn)
	clock := cron.NewNtpClock(btime.LocShow, bntp.LangCode)
	return cron.New(cron.WithSeconds(), cron.WithClock(clock))
}

// NewScheduler creates an isolated scheduler for one Runtime.
func NewScheduler() Scheduler {
	return newScheduler()
}

func Cron() *cron.Cron {
	cronOnce.Do(func() {
		cronObj = newScheduler()
	})
	return cronObj
}
