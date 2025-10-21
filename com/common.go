package com

import (
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/core"
	"maps"
	"sync"
)

/*
SetPairMs
update LastBarMs/wait interval from spider
更新bot端从爬虫收到的标的最新时间和等待间隔
*/
func SetPairMs(pair string, barMS, waitMS int64) {
	SetPairCopieds(map[string][2]int64{
		pair: {barMS, waitMS},
	})
	core.LastBarMs = max(core.LastBarMs, barMS)
	core.LastCopiedMs = btime.TimeMS()
}

var (
	pairCopiedMs     = map[string][2]int64{} // The latest time that all targets received K lines from the crawler, as well as the waiting interval, are used to determine whether there are any that have not been received for a long time. 所有标的从爬虫收到K线的最新时间，以及等待间隔，用于判断是否有长期未收到的。
	lockPairCopiedMs sync.RWMutex            // 确认正确，无需 deadlock
)

func GetPairCopieds() map[string][2]int64 {
	lockPairCopiedMs.Lock()
	data := maps.Clone(pairCopiedMs)
	lockPairCopiedMs.Unlock()
	return data
}

func DelPairCopieds(keys ...string) {
	lockPairCopiedMs.Lock()
	if len(keys) == 0 {
		pairCopiedMs = make(map[string][2]int64)
	} else {
		for _, k := range keys {
			delete(pairCopiedMs, k)
		}
	}
	lockPairCopiedMs.Unlock()
}

func SetPairCopieds(items map[string][2]int64) {
	lockPairCopiedMs.Lock()
	for k, v := range items {
		pairCopiedMs[k] = v
	}
	lockPairCopiedMs.Unlock()
}
