package rpc

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/banbox/banbot/core"
	utils2 "github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/go-viper/mapstructure/v2"
	"go.uber.org/zap"
)

type WebHook struct {
	webHookItem
	name       string
	wg         sync.WaitGroup
	doSendMsgs func([]map[string]string) []map[string]string
	Config     map[string]interface{}
	MsgTypes   map[string]bool
	Accounts   map[string]bool
	Queue      chan map[string]string
}

type webHookItem struct {
	MsgTypesRaw []string `mapstructure:"msg_types"`
	AccountsRaw []string `mapstructure:"accounts"`
	Keywords    []string `mapstructure:"keywords"`
	RetryNum    int      `mapstructure:"retry_num"`   // Retry times 重试次数
	RetryDelay  int      `mapstructure:"retry_delay"` // Retry interval 重试间隔
	Disable     bool     `mapstructure:"disable"`     // 是否禁用
	ChlType     string   `mapstructure:"type"`        // Channel Type 渠道类型
}

const (
	MsgTypeStatus    = "status"
	MsgTypeException = "exception"
	MsgTypeStartUp   = "startup"

	MsgTypeEntry  = "entry"
	MsgTypeExit   = "exit"
	MsgTypeMarket = "market"
)

var (
	client *http.Client
)

type IWebHook interface {
	GetName() string
	IsDisable() bool
	SetDisable(val bool)
	CleanUp()
	/*
		Send a message, payload is the data to be sent after msg rendering
			发送消息，payload是msg渲染后的待发送数据
	*/
	SendMsg(msgType string, account string, payload map[string]string) bool
	ConsumeForever()
}

func NewWebHook(name string, item map[string]interface{}) *WebHook {
	var cfg webHookItem
	err_ := mapstructure.Decode(item, &cfg)
	if err_ != nil {
		panic(fmt.Sprintf("rpc_channels.%v is invalid: %v", name, err_))
	}
	res := &WebHook{
		webHookItem: cfg,
		name:        name,
		Config:      item,
		MsgTypes:    make(map[string]bool),
		Accounts:    make(map[string]bool),
		Queue:       make(chan map[string]string, 100),
	}
	if len(cfg.MsgTypesRaw) > 0 {
		for _, val := range cfg.MsgTypesRaw {
			res.MsgTypes[val] = true
		}
	}
	if len(cfg.AccountsRaw) > 0 {
		for _, val := range cfg.AccountsRaw {
			res.Accounts[val] = true
		}
	}
	return res
}

func (h *WebHook) GetName() string {
	return fmt.Sprintf("%s:%s", h.ChlType, h.name)
}

func (h *WebHook) IsDisable() bool {
	return h.Disable
}

func (h *WebHook) SetDisable(val bool) {
	h.Disable = val
}

func (h *WebHook) SendMsg(msgType string, account string, payload map[string]string) bool {
	if h.Disable {
		return false
	}
	if len(h.MsgTypes) > 0 {
		if _, ok := h.MsgTypes[msgType]; !ok {
			return false
		}
	}
	if account != "" && len(h.Accounts) > 0 {
		if _, ok := h.Accounts[account]; !ok {
			return false
		}
	}
	if content, ok := payload["content"]; ok && len(h.Keywords) > 0 {
		match := false
		for _, word := range h.Keywords {
			if strings.Contains(content, word) {
				match = true
				break
			}
		}
		if !match {
			return false
		}
	}
	h.Queue <- payload
	h.wg.Add(1)
	return true
}

func (h *WebHook) CleanUp() {
	h.Disable = true
	h.wg.Wait()
	if client != nil {
		client = nil
	}
	close(h.Queue)
}

func (h *WebHook) ConsumeForever() {
	if h.Disable {
		return
	}
	name := h.GetName()
	log.Debug("start consume rpc for", zap.String("name", name))
	for {
		first, ok := <-h.Queue
		if !ok {
			break
		}
		var cache = []map[string]string{first}
	readCache:
		for {
			select {
			case item, ok := <-h.Queue:
				if !ok {
					break readCache
				}
				cache = append(cache, item)
			default:
				break readCache
			}
		}
		if len(cache) > 0 {
			h.doSendRetry(cache)
		}
	}
}

func (h *WebHook) doSendRetry(msgList []map[string]string) {
	attempts, totalNum := 0, len(msgList)
	for len(msgList) > 0 && attempts < h.RetryNum+1 {
		if attempts > 0 {
			core.Sleep(time.Duration(h.RetryDelay) * time.Second)
		}
		attempts += 1
		msgList = h.doSendMsgs(msgList)
	}
	h.wg.Add(0 - totalNum)
}

func request(method, url, body string) *banexg.HttpRes {
	if client == nil {
		client = &http.Client{}
	}
	var reqBody io.Reader
	if body != "" {
		reqBody = bytes.NewBufferString(body)
	}
	req, err_ := http.NewRequest(method, url, reqBody)
	if err_ != nil {
		return &banexg.HttpRes{Error: errs.New(core.ErrRunTime, err_)}
	}
	return utils2.DoHttp(client, req)
}
