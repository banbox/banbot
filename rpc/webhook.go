package rpc

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/banbox/banbot/btime"
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
	stateMu    sync.RWMutex
	owner      *rpcOwner
	msgArr     []map[string]string // 待发送列表
	retryNum   int                 // msgArr中已经尝试发送、无需再次计数的消息数
	retryCnt   int                 // 重试次数，发送成功时重置
	lastSentAt int64               // 上次发送时间戳
	doSendMsgs func([]map[string]string) []map[string]string
	Config     map[string]interface{}
	MsgTypes   map[string]bool
	Accounts   map[string]bool
	Queue      chan map[string]string
}

// rpcOwner closes admission before closing the queue and tracks every
// goroutine owned by one RPC channel instance.
type rpcOwner struct {
	mu          sync.Mutex
	closed      bool
	stop        chan struct{}
	closeOnce   sync.Once
	enqueueWait sync.WaitGroup
	taskDone    chan struct{}
	taskCount   int
}

func newRPCOwner() *rpcOwner {
	done := make(chan struct{})
	close(done)
	return &rpcOwner{
		stop:     make(chan struct{}),
		taskDone: done,
	}
}

func admitRPC[T any](o *rpcOwner, queue chan T, payload T, beforeSend func()) bool {
	if o == nil || queue == nil {
		return false
	}
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return false
	}
	if beforeSend != nil {
		beforeSend()
	}
	o.enqueueWait.Add(1)
	o.mu.Unlock()
	defer o.enqueueWait.Done()

	select {
	case queue <- payload:
		return true
	case <-o.stop:
		return false
	}
}

func (o *rpcOwner) startTask() bool {
	if o == nil {
		return true
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return false
	}
	if o.taskCount == 0 {
		o.taskDone = make(chan struct{})
	}
	o.taskCount++
	return true
}

func (o *rpcOwner) doneTask() {
	if o == nil {
		return
	}
	o.mu.Lock()
	if o.taskCount > 0 {
		o.taskCount--
		if o.taskCount == 0 {
			close(o.taskDone)
		}
	}
	o.mu.Unlock()
}

func (o *rpcOwner) isClosed() bool {
	if o == nil {
		return false
	}
	o.mu.Lock()
	closed := o.closed
	o.mu.Unlock()
	return closed
}

func (o *rpcOwner) stopQueue(queue chan map[string]string) {
	if o == nil {
		return
	}
	o.closeOnce.Do(func() {
		o.mu.Lock()
		o.closed = true
		close(o.stop)
		o.mu.Unlock()
		o.enqueueWait.Wait()
		if queue != nil {
			close(queue)
		}
	})
}

func (o *rpcOwner) join() {
	if o == nil {
		return
	}
	o.mu.Lock()
	done := o.taskDone
	o.mu.Unlock()
	<-done
}

// 这是rpc_channels中的通用参数
type webHookItem struct {
	MsgTypesRaw []string `mapstructure:"msg_types"`
	AccountsRaw []string `mapstructure:"accounts"`
	Keywords    []string `mapstructure:"keywords"`
	RetryDelay  int      `mapstructure:"retry_delay"`   // Retry interval 重试间隔
	MinIntvSecs int      `mapstructure:"min_intv_secs"` // 最小发送间隔(秒)
	Disable     bool     `mapstructure:"disable"`       // 是否禁用
	ChlType     string   `mapstructure:"type"`          // Channel Type 渠道类型
	Proxy       string   `mapstructure:"proxy"`         // 代理地址
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
	clientMap   = make(map[string]*http.Client)
	clientMutex sync.RWMutex
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
		owner:       newRPCOwner(),
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
	h.stateMu.RLock()
	disabled := h.Disable
	h.stateMu.RUnlock()
	return disabled
}

func (h *WebHook) SetDisable(val bool) {
	h.stateMu.Lock()
	h.Disable = val
	h.stateMu.Unlock()
}

func (h *WebHook) SendMsg(msgType string, account string, payload map[string]string) bool {
	if h.IsDisable() {
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
	added := false
	ok := admitRPC(h.lifecycleOwner(), h.Queue, payload, func() {
		h.wg.Add(1)
		added = true
	})
	if !ok && added {
		h.wg.Done()
	}
	return ok
}

func (h *WebHook) CleanUp() {
	h.Stop()
	h.Join()
}

func (h *WebHook) Close() {
	h.Stop()
	h.Join()
}

// Stop closes message admission and signals all owned workers without waiting
// for callbacks that are already running.
func (h *WebHook) Stop() {
	if h == nil {
		return
	}
	h.SetDisable(true)
	owner := h.lifecycleOwner()
	owner.stopQueue(h.Queue)
}

// Join waits for all workers admitted before Stop and discards any messages
// that were queued but could not be sent after shutdown.
func (h *WebHook) Join() {
	if h == nil {
		return
	}
	h.Stop()
	owner := h.lifecycleOwner()
	owner.join()
	h.discardPending()
}

func (h *WebHook) lifecycleOwner() *rpcOwner {
	if h == nil {
		return nil
	}
	h.stateMu.Lock()
	if h.owner == nil {
		h.owner = newRPCOwner()
	}
	owner := h.owner
	h.stateMu.Unlock()
	return owner
}

func (h *WebHook) ConsumeForever() {
	if h.IsDisable() {
		return
	}
	owner := h.lifecycleOwner()
	if owner != nil {
		if !owner.startTask() {
			return
		}
		defer owner.doneTask()
	}
	defer h.discardPending()
	name := h.GetName()
	log.Debug("start consume rpc for", zap.String("name", name))
	for {
		first, ok := <-h.Queue
		if !ok {
			break
		}
		h.msgArr = append(h.msgArr, first)
		h.doSend()
	}
}

func (h *WebHook) readCache() {
	for {
		select {
		case item, ok := <-h.Queue:
			if !ok {
				return
			}
			h.msgArr = append(h.msgArr, item)
		default:
			return
		}
	}
}

func (h *WebHook) doSend() {
	minGapSecs := h.MinIntvSecs
	if h.retryCnt > 0 {
		minGapSecs = max(minGapSecs, h.RetryDelay)
	}
	sleepMSecs := int64(minGapSecs)*1000 - (btime.UTCStamp() - h.lastSentAt)
	if sleepMSecs > 0 {
		timer := time.NewTimer(time.Duration(sleepMSecs) * time.Millisecond)
		select {
		case <-timer.C:
		case <-h.stopChan():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			h.discardPending()
			return
		}
	}
	if owner := h.lifecycleOwner(); owner != nil && owner.isClosed() {
		h.discardPending()
		return
	}
	h.readCache()
	beforeNum := len(h.msgArr)
	trackedNum := beforeNum - h.retryNum
	if trackedNum < 0 {
		trackedNum = 0
	}
	defer h.doneMessages(trackedNum)
	if h.doSendMsgs == nil {
		h.msgArr = nil
		h.retryNum = 0
		return
	}
	h.msgArr = h.doSendMsgs(h.msgArr)
	h.retryNum = len(h.msgArr)
	okNum := beforeNum - len(h.msgArr)
	if okNum > 0 {
		h.retryCnt = 0
		h.lastSentAt = btime.UTCStamp()
	} else {
		h.retryCnt += 1
	}
}

func (h *WebHook) stopChan() <-chan struct{} {
	owner := h.lifecycleOwner()
	if owner == nil {
		return nil
	}
	return owner.stop
}

func (h *WebHook) doneMessages(num int) {
	for i := 0; i < num; i++ {
		h.wg.Done()
	}
}

func (h *WebHook) discardPending() {
	trackedNum := len(h.msgArr) - h.retryNum
	if trackedNum < 0 {
		trackedNum = 0
	}
	h.msgArr = nil
	h.retryNum = 0
	h.doneMessages(trackedNum)
	for h.Queue != nil {
		select {
		case _, ok := <-h.Queue:
			if !ok {
				return
			}
			h.doneMessages(1)
		default:
			return
		}
	}
}

// createWebHookClient 为webhook创建支持代理的HTTP客户端
func createWebHookClient(proxyURL string) *http.Client {
	transport := &http.Transport{
		MaxIdleConns:        100,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}

	if proxyURL != "" {
		if proxy, err := url.Parse(proxyURL); err == nil {
			transport.Proxy = http.ProxyURL(proxy)
			log.Info("Using proxy for webhook", zap.String("proxy", proxyURL))
		} else {
			log.Warn("Invalid proxy URL for webhook", zap.String("proxy", proxyURL), zap.Error(err))
		}
	}

	return &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}
}

func request(method, reqURL, body string) *banexg.HttpRes {
	client, req, err := prepareRequest(method, reqURL, body, "")
	if err != nil {
		return &banexg.HttpRes{Error: errs.New(core.ErrRunTime, err)}
	}
	return utils2.DoHttp(client, req)
}

func prepareRequest(method, reqURL, body, proxy string) (*http.Client, *http.Request, error) {
	// 根据proxy获取对应的client
	clientMutex.RLock()
	client, exists := clientMap[proxy]
	clientMutex.RUnlock()
	if !exists {
		clientMutex.Lock()
		client, exists = clientMap[proxy]
		if !exists {
			client = createWebHookClient(proxy)
			clientMap[proxy] = client
		}
		clientMutex.Unlock()
	}

	var reqBody io.Reader
	if body != "" {
		reqBody = bytes.NewBufferString(body)
	}
	req, err_ := http.NewRequest(method, reqURL, reqBody)
	if err_ != nil {
		return nil, nil, err_
	}
	return client, req, nil
}
