package com

import (
	"maps"
	"sync"
	"sync/atomic"

	"github.com/banbox/banbot/exg"
	"github.com/banbox/banexg"
)

// PairCopiedState owns crawler progress for one Runtime.
type PairCopiedState struct {
	values       map[string][2]int64
	lastBarMs    atomic.Int64
	lastCopiedMs atomic.Int64
	lock         sync.RWMutex
}

// NewPairCopiedState creates isolated crawler progress.
func NewPairCopiedState() *PairCopiedState {
	return &PairCopiedState{values: make(map[string][2]int64)}
}

func (s *PairCopiedState) SetPairMsAt(nowMS int64, pair string, barMS, waitMS int64) {
	if s == nil {
		return
	}
	s.lock.Lock()
	s.values[pair] = [2]int64{barMS, waitMS}
	atomicMax(&s.lastBarMs, barMS)
	atomicMax(&s.lastCopiedMs, nowMS)
	s.lock.Unlock()
}

func atomicMax(dst *atomic.Int64, value int64) {
	for {
		current := dst.Load()
		if value <= current || dst.CompareAndSwap(current, value) {
			return
		}
	}
}

func (s *PairCopiedState) LastBarMs() int64 {
	if s == nil {
		return 0
	}
	return s.lastBarMs.Load()
}

func (s *PairCopiedState) LastCopiedMs() int64 {
	if s == nil {
		return 0
	}
	return s.lastCopiedMs.Load()
}

func (s *PairCopiedState) GetPairCopieds() map[string][2]int64 {
	if s == nil {
		return nil
	}
	s.lock.RLock()
	data := maps.Clone(s.values)
	s.lock.RUnlock()
	return data
}

func (s *PairCopiedState) DelPairCopieds(keys ...string) {
	if s == nil {
		return
	}
	s.lock.Lock()
	if len(keys) == 0 {
		s.values = make(map[string][2]int64)
		s.lastBarMs.Store(0)
		s.lastCopiedMs.Store(0)
	} else {
		lastBarMs := s.lastBarMs.Load()
		needsRecalc := false
		for _, key := range keys {
			if value, ok := s.values[key]; ok && value[0] == lastBarMs {
				needsRecalc = true
			}
			delete(s.values, key)
		}
		if needsRecalc {
			var maxBarMs int64
			for _, value := range s.values {
				if value[0] > maxBarMs {
					maxBarMs = value[0]
				}
			}
			s.lastBarMs.Store(maxBarMs)
		}
	}
	s.lock.Unlock()
}

func (s *PairCopiedState) SetPairCopieds(items map[string][2]int64) {
	if s == nil {
		return
	}
	s.lock.Lock()
	for key, value := range items {
		s.values[key] = value
		atomicMax(&s.lastBarMs, value[0])
	}
	s.lock.Unlock()
}

// MarketState groups the mutable market caches owned by one Runtime.
type MarketState struct {
	Prices     *PriceState
	PairCopied *PairCopiedState
}

func NewMarketState(exgName string) *MarketState {
	return NewMarketStateWithExchange(exgName, nil)
}

// NewMarketStateWithExchange binds exchange-owned symbol semantics at the
// composition boundary while keeping PriceState's hot path typed and local.
func NewMarketStateWithExchange(exgName string, exchange banexg.BanExchange) *MarketState {
	return &MarketState{
		Prices:     NewPriceStateWithStrategy(exgName, exg.NewPriceSymbolParser(exgName, exchange)),
		PairCopied: NewPairCopiedState(),
	}
}

func (m *MarketState) Reset() {
	if m == nil {
		return
	}
	if m.Prices != nil {
		m.Prices.Reset()
	}
	if m.PairCopied != nil {
		m.PairCopied.DelPairCopieds()
	}
}
