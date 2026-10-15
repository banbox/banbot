package com

import (
	"context"
	"sync"
	"time"

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

func newScheduler(location *time.Location, lang string) *cron.Cron {
	if location == nil {
		location = time.UTC
	}
	clock := cron.NewNtpClock(location, lang)
	return cron.New(cron.WithSeconds(), cron.WithClock(clock))
}

// NewScheduler creates an isolated scheduler for one Runtime. The optional
// location keeps the old no-argument API usable by legacy callers; explicit
// runtimes should use NewSchedulerWithConfig.
func NewScheduler(locations ...*time.Location) Scheduler {
	if len(locations) == 0 {
		return newScheduler(btime.LocShow, bntp.LangCode)
	}
	return NewSchedulerWithConfig(locations[0], "")
}

// NewSchedulerWithConfig creates an isolated scheduler with both its clock
// location and optional NTP language fixed at construction time. It never
// reads or mutates the process-wide btime/bntp settings.
func NewSchedulerWithConfig(location *time.Location, lang string) Scheduler {
	return newScheduler(location, lang)
}

func Cron() *cron.Cron {
	cronOnce.Do(func() {
		cronObj = newScheduler(btime.LocShow, bntp.LangCode)
	})
	return cronObj
}
