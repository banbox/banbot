package data

import (
	"context"
	"errors"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg/log"
	"time"
)

// ClosedSeriesFact contains immutable clock metadata, not mutable user values.
// Observers never narrow or rewrite DataSeries.Values.
type ClosedSeriesFact struct {
	SID           int32
	TimeFrame     string
	TimeMS, EndMS int64
}

type ClosedSeriesObserver func(context.Context, ClosedSeriesFact) error

// SetClosedSeriesObserver is a startup-only composition hook. It runs after
// successful series persistence, independently of socket subscribers.
func (s *LiveSpider) SetClosedSeriesObserver(observer ClosedSeriesObserver) {
	s.feedMu.Lock()
	s.closedObserver = observer
	s.feedMu.Unlock()
}

func (s *LiveSpider) observeClosedSeries(rows []*orm.DataSeries) error {
	s.feedMu.RLock()
	observer := s.closedObserver
	s.feedMu.RUnlock()
	if observer == nil {
		return nil
	}
	var failures []error
	for _, row := range rows {
		if row == nil || !row.Closed || row.IsWarmUp {
			continue
		}
		fact := ClosedSeriesFact{SID: row.Sid, TimeFrame: row.TimeFrame, TimeMS: row.TimeMS, EndMS: row.EndMS}
		if err := observer(s.ctx, fact); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// PinClosedFeed retains a public OHLCV subscription until this host stops.
// Robots may unwatch their own stream but cannot remove an execution clock.
func (s *LiveSpider) PinClosedFeed(exchange, market, symbol string, expectedSID int32) error {
	if exchange == "" || market == "" || symbol == "" || expectedSID <= 0 {
		return errors.New("spider: explicit pinned feed identity required")
	}
	miner := s.getMiner(exchange, market)
	if miner == nil {
		return errors.New("spider: pinned feed miner unavailable")
	}
	if miner.deps == nil || miner.deps.Symbols == nil {
		return errors.New("spider: pinned feed symbol authority unavailable")
	}
	identity := miner.deps.Symbols.GetExSymbol2(exchange, market, symbol)
	if identity == nil {
		if err := miner.deps.Symbols.EnsureSymbols([]*orm.ExSymbol{{Exchange: exchange, Market: market, Symbol: symbol}}, exchange); err != nil {
			return err
		}
		identity = miner.deps.Symbols.GetExSymbol2(exchange, market, symbol)
	}
	if identity == nil || identity.ID != expectedSID {
		return errors.New("spider: pinned feed SID does not match public exchange/market/symbol authority")
	}
	key := exchange + "\x00" + market + "\x00" + symbol
	s.feedMu.Lock()
	if s.pinnedFeeds == nil {
		s.pinnedFeeds = map[string]bool{}
	}
	already := s.pinnedFeeds[key]
	s.pinnedFeeds[key] = true
	s.feedMu.Unlock()
	if already {
		return nil
	}
	if err := miner.SubPairs("ohlcv", symbol); err != nil {
		s.feedMu.Lock()
		delete(s.pinnedFeeds, key)
		s.feedMu.Unlock()
		return err
	}
	return nil
}

func waitSeriesPredecessor(ctx context.Context, previous <-chan struct{}) bool {
	if previous == nil {
		return ctx.Err() == nil
	}
	select {
	case <-ctx.Done():
		return false
	case <-previous:
		return ctx.Err() == nil
	}
}

// Waiting for the same symbol's previous write must not consume a global
// storage slot; unrelated symbols remain able to persist and publish facts.
func acquireSeriesWriteSlot(ctx context.Context, previous <-chan struct{}, guard chan struct{}) bool {
	if !waitSeriesPredecessor(ctx, previous) {
		return false
	}
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()
	logged := false
	for {
		select {
		case <-ctx.Done():
			return false
		case guard <- struct{}{}:
			return true
		case <-ticker.C:
			if !logged {
				log.Error("wait save in spider timeout")
				logged = true
			}
		}
	}
}
