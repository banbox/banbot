package base

import (
	"fmt"
	"github.com/banbox/banbot/orm"
	utils2 "github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/log"
	"github.com/banbox/banexg/utils"
	"github.com/gofiber/contrib/websocket"
	"go.uber.org/zap"
)

type WsClient struct {
	Conn   *websocket.Conn
	Subs   map[string]bool
	remote string
}

func NewWsClient(c *websocket.Conn) *WsClient {
	return &WsClient{Conn: c, Subs: make(map[string]bool), remote: c.RemoteAddr().String()}
}

func (c *WsClient) HandleForever() {
	log.Debug("ws client joined", zap.String("ip", c.remote))
	for {
		if c.Conn == nil {
			break
		}
		mt, data, err := c.Conn.ReadMessage()
		if err != nil {
			log.Warn("ws read fail", zap.Error(err))
			c.Close(true)
			break
		}
		if mt == websocket.CloseMessage {
			c.Close(true)
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
		switch action {
		case "subscribe":
			c.Subscribe(msg)
		case "unsubscribe":
			c.UnSubscribe(msg)
		default:
			c.WriteMsg(map[string]interface{}{"error": "unsupported action"})
		}
	}
}

func (c *WsClient) WriteMsg(msg map[string]interface{}) {
	if c.Conn == nil {
		return
	}
	data, err := utils.Marshal(msg)
	if err != nil {
		log.Warn("marshal ws msg fail", zap.Error(err))
		return
	}
	err = c.Conn.WriteMessage(websocket.TextMessage, data)
	if err != nil {
		log.Warn("write ws msg fail", zap.Error(err))
	}
}

func (c *WsClient) Subscribe(msg map[string]interface{}) {
	exs := parseExSymbol(msg)
	if exs == nil {
		return
	}
	key := fmt.Sprintf("%s_%s_%s", exs.Exchange, exs.Market, exs.Symbol)
	c.Subs[key] = true
	SetSeriesSub(c, true, true, key)
}

func parseExSymbol(msg map[string]interface{}) *orm.ExSymbol {
	exchange := utils.GetMapVal(msg, "exchange", "")
	symbol := utils.GetMapVal(msg, "symbol", "")
	exs, err2 := orm.ParseShort(exchange, symbol)
	if err2 != nil {
		log.Info("invalid ws subscribe", zap.String("exg", exchange), zap.String("pair", symbol))
		return nil
	}
	return exs
}

func (c *WsClient) UnSubscribe(msg map[string]interface{}) {
	exs := parseExSymbol(msg)
	if exs == nil {
		return
	}
	key := fmt.Sprintf("%s_%s_%s", exs.Exchange, exs.Market, exs.Symbol)
	if _, ok := c.Subs[key]; ok {
		SetSeriesSub(c, false, true, key)
		delete(c.Subs, key)
	}
}

func (c *WsClient) Close(lock bool) {
	keys := utils2.KeysOfMap(c.Subs)
	SetSeriesSub(c, false, lock, keys...)
	c.Subs = nil
	_ = c.Conn.Close()
	c.Conn = nil
	log.Debug("ws client removed", zap.String("addr", c.remote))
}
