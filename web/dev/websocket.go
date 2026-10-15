package dev

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/banbox/banexg/log"
	"github.com/banbox/banexg/utils"
	"github.com/gofiber/contrib/websocket"
	"go.uber.org/zap"
)

type ServerStatus struct {
	DirtyBin bool `json:"dirtyBin"`
	Building bool `json:"building"`
}

type WsClient struct {
	Conn      *websocket.Conn
	netConn   net.Conn
	server    *DevServer
	remote    string
	Tags      map[string]interface{}
	writeLock sync.Mutex
	closed    atomic.Bool
}

func (s *DevServer) NewWsClient(c *websocket.Conn) *WsClient {
	client := &WsClient{
		Conn:    c,
		netConn: c.NetConn(),
		server:  s,
		remote:  c.RemoteAddr().String(),
		Tags:    make(map[string]interface{}),
	}

	s.wsMu.Lock()
	if s.stopped.Load() {
		s.wsMu.Unlock()
		client.closed.Store(true)
		_ = client.netConn.SetDeadline(time.Now())
		return client
	}
	s.clients[client] = struct{}{}
	s.wsMu.Unlock()

	return client
}

func (c *WsClient) HandleForever() {
	log.Debug("dev ws client joined", zap.String("ip", c.remote))
	for {
		mt, data, err := c.Conn.ReadMessage()
		if err != nil {
			log.Warn("ws read fail", zap.Error(err))
			c.Close()
			break
		}
		if mt == websocket.CloseMessage {
			c.Close()
			break
		}
		if mt != websocket.TextMessage {
			continue
		}

		var msg = map[string]interface{}{}
		err = utils.Unmarshal(data, &msg, utils.JsonNumAuto)
		if err != nil {
			log.Info("unexpedted ws msg", zap.String("str", string(data)))
			continue
		}

		action, ok := msg["action"]
		if !ok {
			log.Info("no action ws msg", zap.String("str", string(data)))
			continue
		}
		id := utils.GetMapVal(msg, "id", "")

		switch action {
		case "status":
			c.WriteMsg(map[string]interface{}{
				"id":   id,
				"type": "status",
				"data": c.server.Status(),
			})
		default:
			c.WriteMsg(map[string]interface{}{"error": "unsupported action"})
		}
	}
}

func (c *WsClient) WriteMsg(msg map[string]interface{}) {
	data, err := utils.Marshal(msg)
	if err != nil {
		log.Warn("marshal ws msg fail", zap.Error(err))
		return
	}
	c.writeLock.Lock()
	defer c.writeLock.Unlock()
	if c.closed.Load() {
		return
	}
	err = c.Conn.WriteMessage(websocket.TextMessage, data)
	if err != nil {
		log.Warn("write ws msg fail", zap.Error(err))
	}
}

func (c *WsClient) Close() {
	if !c.closed.CompareAndSwap(false, true) {
		return
	}
	if c.server != nil {
		c.server.wsMu.Lock()
		delete(c.server.clients, c)
		c.server.wsMu.Unlock()
	}

	if c.netConn != nil {
		_ = c.netConn.SetDeadline(time.Now())
	}
	log.Debug("dev ws client removed", zap.String("addr", c.remote))
}

func (c *WsClient) interrupt() {
	if c == nil || c.netConn == nil {
		return
	}
	_ = c.netConn.SetDeadline(time.Now())
}

func (s *DevServer) BroadcastWS(tag string, msg map[string]interface{}) {
	if s == nil {
		return
	}
	s.wsMu.RLock()
	targets := make([]*WsClient, 0, len(s.clients))
	for client := range s.clients {
		if tag == "" {
			targets = append(targets, client)
		} else if _, ok := client.Tags[tag]; ok {
			targets = append(targets, client)
		}
	}
	s.wsMu.RUnlock()

	for _, client := range targets {
		client.WriteMsg(msg)
	}
}

func (s *DevServer) Status() ServerStatus {
	if s == nil {
		return ServerStatus{}
	}
	s.wsMu.RLock()
	defer s.wsMu.RUnlock()
	return s.status
}

func (s *DevServer) SetStatus(status ServerStatus) {
	if s == nil {
		return
	}
	s.wsMu.Lock()
	s.status = status
	s.wsMu.Unlock()
}

func (s *DevServer) setDirtyBin() {
	if s == nil {
		return
	}
	s.wsMu.Lock()
	s.status.DirtyBin = true
	s.wsMu.Unlock()
}

func (s *DevServer) beginBuild() bool {
	if s == nil {
		return false
	}
	s.buildMu.Lock()
	defer s.buildMu.Unlock()
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	if s.status.Building {
		return false
	}
	s.status.DirtyBin = false
	s.status.Building = true
	return true
}

func (s *DevServer) finishBuild() {
	if s == nil {
		return
	}
	s.buildMu.Lock()
	s.wsMu.Lock()
	s.status.Building = false
	s.wsMu.Unlock()
	s.buildMu.Unlock()
}

func (s *DevServer) BroadcastStatus() {
	s.BroadcastWS("", map[string]interface{}{
		"type": "status",
		"data": s.Status(),
	})
}
