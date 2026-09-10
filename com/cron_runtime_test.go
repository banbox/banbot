package com

import (
	"testing"
	"time"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/bntp"
	"github.com/banbox/cron/v3"
)

func TestSchedulerUsesExplicitTimezone(t *testing.T) {
	previous := btime.LocShow
	previousLang := bntp.LangCode
	btime.LocShow = time.FixedZone("legacy", -5*3600)
	t.Cleanup(func() { btime.LocShow = previous })
	start := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	for _, offset := range []int{0, 8 * 3600} {
		location := time.FixedZone("runtime", offset)
		scheduler := NewScheduler(location).(*cron.Cron)
		entryID, err := scheduler.AddFunc("0 0 9 * * *", func() {})
		if err != nil {
			t.Fatal(err)
		}
		next := scheduler.Entry(entryID).Schedule.Next(start)
		want := time.Date(2026, 9, 8, 9, 0, 0, 0, location)
		if !next.Equal(want) {
			t.Fatalf("offset %d: next = %v, want %v", offset, next, want)
		}
		if bntp.LangCode != previousLang {
			t.Fatal("runtime scheduler changed process NTP configuration")
		}
	}
}
