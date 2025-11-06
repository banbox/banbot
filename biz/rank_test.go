package biz

import (
	"github.com/banbox/banbot/btime"
	"sort"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/goods"
	"github.com/banbox/banbot/utils"
)

// TestPairRankings calculates rankings for source pairs across multiple cron-triggered refreshes
func TestPairRankings(t *testing.T) {
	core.BackTestMode = true
	args := &config.CmdArgs{NoDefault: false}

	if err := SetupComs(args); err != nil {
		t.Fatalf("setup filters failed: %v", err)
	}

	sourcePairs := config.Pairs
	if len(sourcePairs) == 0 {
		t.Skip("config.Pairs is empty")
	}
	config.Pairs = nil

	if config.PairMgr.Cron == "" {
		t.Skip("config.PairMgr.Cron not configured")
	}

	schedule, err_ := utils.NewCronScheduler(config.PairMgr.Cron)
	if err_ != nil {
		t.Fatalf("parse cron failed: %v", err_)
	}

	goods.ShowLog = false

	t.Logf("Source pairs: %v\n", sourcePairs)

	// Generate test times from TimeRange
	startTime := time.UnixMilli(config.TimeRange.StartMS)
	endTime := time.UnixMilli(config.TimeRange.EndMS)

	currentTime := startTime
	for currentTime.Before(endTime) {
		cronTime := schedule.Next(currentTime)
		if cronTime.After(endTime) {
			break
		}
		cronTimeMS := cronTime.UnixMilli()

		btime.CurTimeMS = cronTimeMS
		dynamicPairs, err := goods.RefreshPairList(cronTimeMS)
		if err != nil {
			t.Errorf("refresh pair list failed (%s): %v", cronTime.Format("2006-01-02"), err)
			currentTime = cronTime.Add(time.Hour)
			continue
		}

		rankings := calculateRankings(sourcePairs, dynamicPairs)

		t.Logf("\n=== %s ===", cronTime.Format("2006-01-02 15:04:05"))
		t.Logf("Total count: %d, Rankings: %v", len(dynamicPairs), rankings)

		currentTime = cronTime.Add(time.Hour)
	}
}

func calculateRankings(sourcePairs, dynamicPairs []string) []int {
	positionMap := make(map[string]int)
	for i, pair := range dynamicPairs {
		positionMap[pair] = i + 1
	}

	rankings := make([]int, len(sourcePairs))
	for i, pair := range sourcePairs {
		if rank, exists := positionMap[pair]; exists {
			rankings[i] = rank
		}
	}
	sort.Ints(rankings)
	return rankings
}
