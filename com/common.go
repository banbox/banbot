package com

import (
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/core"
)

var legacyPairCopied = NewPairCopiedState()

// LegacyPairCopiedState exposes the state used by the compatibility facade.
// Runtime-owned callers must receive PairCopiedState from their MarketState.
func LegacyPairCopiedState() *PairCopiedState {
	return legacyPairCopied
}

func SetPairMs(pair string, barMS, waitMS int64) {
	legacyPairCopied.SetPairMsAt(btime.TimeMS(), pair, barMS, waitMS)
	core.LastBarMs = max(core.LastBarMs, barMS)
	core.LastCopiedMs = btime.TimeMS()
}

func GetPairCopieds() map[string][2]int64 {
	return legacyPairCopied.GetPairCopieds()
}

func DelPairCopieds(keys ...string) {
	legacyPairCopied.DelPairCopieds(keys...)
}

func SetPairCopieds(items map[string][2]int64) {
	legacyPairCopied.SetPairCopieds(items)
}
