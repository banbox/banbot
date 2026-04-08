package strat

import (
	"fmt"
	"sync"

	"github.com/banbox/banbot/orm"
)

type DataHub interface {
	Set(evt *orm.DataSeries)
	Latest(source string, sid int32, tf string) *orm.DataSeries
	Window(source string, sid int32, tf string, n int) []*orm.DataSeries
}

type memDataHub struct {
	mu      sync.RWMutex
	latest  map[string]*orm.DataSeries
	windows map[string][]*orm.DataSeries
	limit   int
}

func NewDataHub(limit ...int) DataHub {
	curLimit := 512
	if len(limit) > 0 {
		curLimit = limit[0]
	}
	if curLimit <= 0 {
		curLimit = 512
	}
	return &memDataHub{
		latest:  make(map[string]*orm.DataSeries),
		windows: make(map[string][]*orm.DataSeries),
		limit:   curLimit,
	}
}

func dataHubKey(source string, sid int32, tf string) string {
	return fmt.Sprintf("%s:%d:%s", orm.NormalizeSeriesSource(source), sid, tf)
}

func (h *memDataHub) Set(evt *orm.DataSeries) {
	if h == nil || evt == nil {
		return
	}
	key := dataHubKey(evt.Source, evt.Sid, evt.TimeFrame)
	h.mu.Lock()
	h.latest[key] = evt
	items := append(h.windows[key], evt)
	if len(items) > h.limit {
		items = items[len(items)-h.limit:]
	}
	h.windows[key] = items
	h.mu.Unlock()
}

func (h *memDataHub) Latest(source string, sid int32, tf string) *orm.DataSeries {
	if h == nil {
		return nil
	}
	key := dataHubKey(source, sid, tf)
	h.mu.RLock()
	item := h.latest[key]
	h.mu.RUnlock()
	return item
}

func (h *memDataHub) Window(source string, sid int32, tf string, n int) []*orm.DataSeries {
	if h == nil {
		return nil
	}
	key := dataHubKey(source, sid, tf)
	h.mu.RLock()
	items := append([]*orm.DataSeries(nil), h.windows[key]...)
	h.mu.RUnlock()
	if n <= 0 || n >= len(items) {
		return items
	}
	return items[len(items)-n:]
}
