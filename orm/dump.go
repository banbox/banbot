package orm

import (
	"encoding/gob"
	"os"
	"sync"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

type DumpRow struct {
	Time int64
	Type string
	Key  string
	Val  interface{}
}

const (
	DumpKline     = "kline"
	DumpStartUp   = "startup"
	DumpApiOrder  = "api_order"
	DumpWsMyTrade = "ws_my_trade"
)

// DumpSink owns one application's gob dump stream. It is a concrete owner,
// rather than a package registry, so independent runtimes can write and close
// their own streams without sharing buffers or file handles.
type DumpSink struct {
	mu      sync.Mutex
	rows    []*DumpRow
	encoder *gob.Encoder
	file    *os.File
	now     func() int64
	closed  bool
}

func registerDumpTypes() {
	gob.Register(banexg.Kline{})
	gob.Register(banexg.MyTrade{})
	gob.Register(exg.PutOrderRes{})
}

// NewDumpSink creates an isolated dump writer and records the startup marker.
// The caller owns the returned sink and must call Close when the runtime ends.
func NewDumpSink(file *os.File, now func() int64) *DumpSink {
	if file == nil {
		return nil
	}
	registerDumpTypes()
	if now == nil {
		now = btime.UTCStamp
	}
	sink := &DumpSink{file: file, encoder: gob.NewEncoder(file), now: now}
	sink.Add(DumpStartUp, "", nil)
	return sink
}

func (s *DumpSink) Add(src, key string, val interface{}) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.encoder == nil {
		return
	}
	s.rows = append(s.rows, &DumpRow{Time: s.now(), Type: src, Key: key, Val: val})
}

func (s *DumpSink) Flush() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.encoder == nil || len(s.rows) == 0 {
		return nil
	}
	if err := s.encoder.Encode(s.rows); err != nil {
		return err
	}
	if err := s.file.Sync(); err != nil {
		return err
	}
	s.rows = nil
	return nil
}

func (s *DumpSink) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	var firstErr error
	if s.encoder != nil && len(s.rows) > 0 {
		if err := s.encoder.Encode(s.rows); err != nil {
			firstErr = err
		} else if err := s.file.Sync(); err != nil {
			firstErr = err
		} else {
			s.rows = nil
		}
	}
	if s.file != nil {
		if err := s.file.Close(); firstErr == nil && err != nil {
			firstErr = err
		}
	}
	s.closed = true
	s.encoder = nil
	s.file = nil
	return firstErr
}

var (
	legacyDumpMu sync.Mutex
	legacyDump   *DumpSink
)

func SetDump(file *os.File) {
	legacyDumpMu.Lock()
	old := legacyDump
	legacyDump = NewDumpSink(file, btime.UTCStamp)
	legacyDumpMu.Unlock()
	if old != nil {
		_ = old.Close()
	}
}

func AddDumpRow(src, key string, val interface{}) {
	legacyDumpMu.Lock()
	sink := legacyDump
	legacyDumpMu.Unlock()
	sink.Add(src, key, val)
}

func FlushDumps() {
	legacyDumpMu.Lock()
	sink := legacyDump
	legacyDumpMu.Unlock()
	if err := sink.Flush(); err != nil {
		log.Error("flush dump rows fail", zap.Error(err))
	}
}

func CloseDump() {
	legacyDumpMu.Lock()
	sink := legacyDump
	legacyDump = nil
	legacyDumpMu.Unlock()
	if err := sink.Close(); err != nil {
		log.Error("close dump file fail", zap.Error(err))
	}
}
