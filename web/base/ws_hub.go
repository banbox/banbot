package base

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/utils"
	"github.com/gofiber/contrib/websocket"
)

// WsHub owns subscriptions and connected clients for one API server.
// Closing admission precedes closing sockets, so Join cannot race an Add.
type WsHub struct {
	deps          *data.RuntimeDeps
	mu            sync.Mutex
	clients       map[*WsClient]bool
	subscriptions map[string]map[*WsClient]bool
	closed        bool
	active        sync.WaitGroup
	done          chan struct{}
	outgoing      chan wsBroadcast
}

type wsBroadcast struct {
	clients []*WsClient
	raw     []byte
}

func NewWsHub(deps *data.RuntimeDeps) *WsHub {
	h := &WsHub{deps: deps, clients: make(map[*WsClient]bool), subscriptions: make(map[string]map[*WsClient]bool), done: make(chan struct{}), outgoing: make(chan wsBroadcast, 64)}
	h.active.Add(1)
	go func() {
		defer h.active.Done()
		for {
			select {
			case <-h.done:
				return
			case msg := <-h.outgoing:
				for _, c := range msg.clients {
					if err := c.write(msg.raw); err != nil {
						c.Close(true)
					}
				}
			}
		}
	}()
	return h
}

func (h *WsHub) serve(conn *websocket.Conn) {
	c := &WsClient{Conn: conn, Subs: make(map[string]bool), remote: conn.RemoteAddr().String(), hub: h}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		_ = conn.Close()
		return
	}
	h.clients[c] = true
	h.active.Add(1)
	h.mu.Unlock()
	defer h.active.Done()
	c.HandleForever()
}

func (h *WsHub) setSubscription(c *WsClient, subscribe bool, keys ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if subscribe && (h.closed || c.closed.Load()) {
		return
	}
	for _, key := range keys {
		if subscribe {
			if h.subscriptions[key] == nil {
				h.subscriptions[key] = make(map[*WsClient]bool)
			}
			h.subscriptions[key][c] = true
			c.Subs[key] = true
		} else {
			delete(h.subscriptions[key], c)
			delete(c.Subs, key)
			if len(h.subscriptions[key]) == 0 {
				delete(h.subscriptions, key)
			}
		}
	}
}

func (h *WsHub) remove(c *WsClient) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for key := range c.Subs {
		delete(h.subscriptions[key], c)
		if len(h.subscriptions[key]) == 0 {
			delete(h.subscriptions, key)
		}
	}
	c.Subs = nil
	delete(h.clients, c)
}

func (h *WsHub) Close() {
	h.mu.Lock()
	if !h.closed {
		h.closed = true
		close(h.done)
	}
	clients := make([]*WsClient, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.Unlock()
	for _, c := range clients {
		c.Close(true)
	}
}

func (h *WsHub) Join() { h.active.Wait() }

func (h *WsHub) parseSymbol(exchange, symbol string) (*orm.ExSymbol, *errs.Error) {
	if h.deps == nil {
		return orm.ParseShort(exchange, symbol)
	}
	if h.deps.Symbols == nil {
		return nil, errs.NewMsg(errs.CodeParamRequired, "runtime symbol state is required")
	}
	return h.deps.Symbols.ParseShort(exchange, symbol)
}

// Publish retains the complete DataSeries payload, including custom fields.
func (h *WsHub) Publish(msg *data.SeriesMsg) {
	if msg == nil || len(msg.Rows) == 0 {
		return
	}
	key := fmt.Sprintf("%s_%s_%s", msg.ExgName, msg.Market, msg.Pair)
	h.mu.Lock()
	clients := make([]*WsClient, 0, len(h.subscriptions[key]))
	for c := range h.subscriptions[key] {
		clients = append(clients, c)
	}
	h.mu.Unlock()
	if len(clients) == 0 {
		return
	}
	raw, err := utils.Marshal(map[string]interface{}{"a": "subscribe", "series": msg.Rows, "secs": msg.TFSecs, "upd": msg.Interval})
	if err != nil {
		return
	}
	// Monitoring must never hold up the trading event loop. A slow client
	// can miss an update and reload history through the HTTP series endpoint.
	select {
	case <-h.done:
	case h.outgoing <- wsBroadcast{clients: clients, raw: raw}:
	default:
	}

}

func (c *WsClient) write(raw []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.Conn == nil || c.closed.Load() {
		return net.ErrClosed
	}
	_ = c.Conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return c.Conn.WriteMessage(websocket.TextMessage, raw)
}
