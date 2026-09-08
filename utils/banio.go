package utils

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/banbox/banexg/utils"
	"github.com/google/uuid"
	"github.com/sasha-s/go-deadlock"
	"go.uber.org/zap"
	"io"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	banConnMaxFrameBytes   = 96 << 20
	banConnMaxMessageBytes = 128 << 20
)

type ConnCB = func(msg *IOMsgRaw)

type IBanConn interface {
	WriteMsg(msg *IOMsg) *errs.Error
	Write(msg *IOMsgRaw) *errs.Error
	ReadMsg() (*IOMsgRaw, *errs.Error)

	SetData(val interface{}, tags ...string)
	GetData(key string) (interface{}, bool)
	PopData(key string) (interface{}, bool)
	DeleteData(tags ...string)
	SetAesKey(aesKey string)
	GetAesKey() string

	SetWait(key string, size int) string
	GetWaitChan(key string) (chan []byte, bool)
	CloseWaitChan(key string)
	SendWaitRes(key string, data interface{}) *errs.Error
	WaitResult(ctx context.Context, key string, timeout time.Duration) ([]byte, *errs.Error)

	GetRemote() string
	GetRemoteHost() string
	IsClosed() bool
	RunForever() *errs.Error
	Close() *errs.Error
}

type BanConn struct {
	Conn         net.Conn               // Original socket connection 原始的socket连接
	Data         map[string]interface{} // Message subscription list 消息订阅列表
	Remote       string                 // Remote Name 远端名称
	Listens      map[string]ConnCB      // Message processing function 消息处理函数
	waits        map[string]chan []byte // 用于等待结果的chan
	RefreshMS    int64                  // Connection ready timestamp 连接就绪的时间戳
	Ready        bool
	IsReading    bool
	aesKey       string
	lockConnect  deadlock.Mutex
	connecting   bool
	stopped      bool
	lockWrite    deadlock.Mutex
	lockData     deadlock.Mutex
	lockWait     deadlock.Mutex
	handlerLock  sync.Mutex
	handlerWait  sync.WaitGroup
	loopWait     sync.WaitGroup
	handlerStop  bool
	loopPingStop chan struct{}
	ctx          context.Context
	cancel       context.CancelFunc
	heartBeatMs  int64               // Timestamp of the latest received ping/pong
	DoConnect    func(conn *BanConn) // Reconnect function, no attempt to reconnect provided 重新连接函数，未提供不尝试重新连接
	ReInitConn   func()              // Initialize callback function after successful reconnection 重新连接成功后初始化回调函数
	Fallback     ConnCB
	BadMsgCB     func(err *errs.Error)
}

type IOMsg struct {
	Action    string      `json:"action"`
	Data      interface{} `json:"data"`
	NoEncrypt bool        `json:"no_encrypt"`
}

type IOMsgRaw struct {
	Action    string `json:"action"`
	Data      []byte `json:"data"`
	NoEncrypt bool   `json:"no_encrypt"`
}

var (
	tipRetryTimes           = make(map[string]int64)
	tipRetryTimesLock       deadlock.Mutex
	banConnWriteIdleTimeout = 30 * time.Second
)

const banConnWriteChunkSize = 64 << 10

func (c *BanConn) GetRemote() string {
	return c.Remote
}
func (c *BanConn) GetRemoteHost() string {
	end := strings.Index(c.Remote, ":")
	if end > 0 {
		return c.Remote[:end]
	}
	return c.Remote
}
func (c *BanConn) IsClosed() bool {
	c.lockConnect.Lock()
	defer c.lockConnect.Unlock()
	return c.Conn == nil || !c.Ready
}

func (c *BanConn) GetData(key string) (interface{}, bool) {
	c.lockData.Lock()
	val, ok := c.Data[key]
	c.lockData.Unlock()
	return val, ok
}

func (c *BanConn) PopData(key string) (interface{}, bool) {
	c.lockData.Lock()
	val, ok := c.Data[key]
	delete(c.Data, key)
	c.lockData.Unlock()
	return val, ok
}

func (c *BanConn) WriteMsg(msg *IOMsg) *errs.Error {
	if msg == nil {
		return nil
	}
	data, err := msg.Marshal(c.aesKey)
	if err != nil {
		return err
	}
	return c.Write(data)
}

func (c *BanConn) Write(msg *IOMsgRaw) *errs.Error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err_ := enc.Encode(msg); err_ != nil {
		return errs.New(core.ErrMarshalFail, err_)
	}
	return c.write(buf.Bytes(), 3)
}

func (c *BanConn) write(data []byte, retryNum int) *errs.Error {
	c.lockWrite.Lock()
	locked := true
	defer func() {
		if locked {
			c.lockWrite.Unlock()
		}
	}()
	if len(data) > banConnMaxFrameBytes {
		return errs.NewMsg(errs.CodeParamInvalid, "ban connection frame exceeds %d bytes", banConnMaxFrameBytes)
	}
	dataLen := uint32(len(data))
	lenBt := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBt, dataLen)
	conn := c.getConn()
	if conn != nil {
		defer func() { _ = conn.SetWriteDeadline(time.Time{}) }()
		// 先写长度头
		if err_ := writeFully(conn, lenBt); err_ != nil {
			c.discardConn(conn)
			errCode, errType := getErrType(err_)
			if c.canReconnect() && retryNum > 0 {
				log.Warn("write fail, wait 3s and retry", zap.String("type", errType))
				c.lockWrite.Unlock()
				locked = false
				if !waitBanConnContext(c.context(), time.Second*3) {
					if cancelErr := c.contextCancelled(); cancelErr != nil {
						return cancelErr
					}
					return errs.NewMsg(errs.CodeCancel, "ban connection reconnect canceled")
				}
				c.connect()
				if cancelErr := c.contextCancelled(); cancelErr != nil {
					return cancelErr
				}
				return c.write(data, retryNum-1)
			}
			return errs.New(errCode, err_)
		}
		// 再写数据内容
		if err_ := writeFully(conn, data); err_ != nil {
			c.discardConn(conn)
			errCode, _ := getErrType(err_)
			return errs.New(errCode, err_)
		}
		return nil
	}
	return errs.NewMsg(errs.CodeIOWriteFail, "write fail as disconnected")
}

func (c *BanConn) getConn() net.Conn {
	c.lockConnect.Lock()
	defer c.lockConnect.Unlock()
	return c.Conn
}

func (c *BanConn) canReconnect() bool {
	c.lockConnect.Lock()
	defer c.lockConnect.Unlock()
	return !c.stopped && c.DoConnect != nil
}

// SetContext binds the connection to an owner lifecycle. It must be called
// before starting RunForever, LoopPing, or any reconnecting operation.
func (c *BanConn) SetContext(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	derived, cancel := context.WithCancel(ctx)
	c.lockConnect.Lock()
	oldCancel := c.cancel
	c.ctx = derived
	c.cancel = cancel
	c.lockConnect.Unlock()
	if oldCancel != nil {
		oldCancel()
	}
}

func (c *BanConn) context() context.Context {
	c.lockConnect.Lock()
	if c.ctx == nil {
		c.ctx, c.cancel = context.WithCancel(context.Background())
	}
	ctx := c.ctx
	c.lockConnect.Unlock()
	return ctx
}

func (c *BanConn) cancelContext() {
	c.lockConnect.Lock()
	if c.cancel == nil {
		c.ctx, c.cancel = context.WithCancel(context.Background())
	}
	cancel := c.cancel
	c.lockConnect.Unlock()
	cancel()
}

func (c *BanConn) contextCancelled() *errs.Error {
	if err := c.context().Err(); err != nil {
		return errs.New(errs.CodeCancel, err)
	}
	return nil
}

func waitBanConnContext(ctx context.Context, delay time.Duration) bool {
	if ctx == nil {
		ctx = context.Background()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (c *BanConn) setReconnectConn(conn net.Conn) bool {
	c.lockConnect.Lock()
	if c.stopped || c.DoConnect == nil {
		c.lockConnect.Unlock()
		_ = conn.Close()
		return false
	}
	old := c.Conn
	c.Conn = conn
	c.lockConnect.Unlock()
	if old != nil && old != conn {
		_ = old.Close()
	}
	return true
}

func (c *BanConn) beginReading() bool {
	c.lockConnect.Lock()
	defer c.lockConnect.Unlock()
	if c.stopped {
		return false
	}
	c.IsReading = true
	return true
}

func (c *BanConn) setReading(reading bool) {
	c.lockConnect.Lock()
	c.IsReading = reading
	c.lockConnect.Unlock()
}

func (c *BanConn) isReading() bool {
	c.lockConnect.Lock()
	defer c.lockConnect.Unlock()
	return c.IsReading
}

func (c *BanConn) isReady() bool {
	c.lockConnect.Lock()
	defer c.lockConnect.Unlock()
	return c.Ready
}

func (c *BanConn) beginHandler() bool {
	c.handlerLock.Lock()
	defer c.handlerLock.Unlock()
	if c.handlerStop {
		return false
	}
	c.handlerWait.Add(1)
	return true
}

func (c *BanConn) beginLoop() bool {
	c.handlerLock.Lock()
	defer c.handlerLock.Unlock()
	if c.handlerStop {
		return false
	}
	c.loopWait.Add(1)
	return true
}

func (c *BanConn) leaveLoop() {
	c.loopWait.Done()
}

func (c *BanConn) leaveHandler() {
	c.handlerWait.Done()
}

func (c *BanConn) stopHandlers() {
	c.handlerLock.Lock()
	if !c.handlerStop {
		c.handlerStop = true
		if c.loopPingStop != nil {
			close(c.loopPingStop)
		}
	}
	c.handlerLock.Unlock()
}

func (c *BanConn) beginLoopPing() (<-chan struct{}, bool) {
	c.handlerLock.Lock()
	defer c.handlerLock.Unlock()
	if c.handlerStop {
		return nil, false
	}
	if c.loopPingStop == nil {
		c.loopPingStop = make(chan struct{})
	}
	c.handlerWait.Add(1)
	return c.loopPingStop, true
}

// Join seals handler admission, then waits for every handler admitted before
// the seal. Join belongs to the lifecycle owner; callbacks must request Stop
// and return instead of joining themselves.
func (c *BanConn) Join() {
	if c == nil {
		return
	}
	c.stopHandlers()
	c.handlerWait.Wait()
	c.loopWait.Wait()
}

func (c *BanConn) runHandler(handle ConnCB, msg *IOMsgRaw) {
	defer c.leaveHandler()
	handle(msg)
}

func (c *BanConn) runBadMsg(err *errs.Error) {
	defer c.leaveHandler()
	c.BadMsgCB(err)
}

func (c *BanConn) runReInit(reInitConn func()) {
	defer c.leaveHandler()
	reInitConn()
}

func (c *BanConn) discardConn(conn net.Conn) {
	c.lockConnect.Lock()
	if c.Conn == conn {
		c.Conn = nil
		c.Ready = false
	}
	c.lockConnect.Unlock()
	_ = conn.Close()
}

func (c *BanConn) ReadMsg() (*IOMsgRaw, *errs.Error) {
	data, err := c.Read()
	if err != nil {
		return nil, err
	}
	var msg IOMsgRaw
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err_ := dec.Decode(&msg); err_ != nil {
		return nil, errs.New(errs.CodeUnmarshalFail, err_)
	}
	if len(msg.Data) > 0 {
		aesKey := c.aesKey
		if msg.NoEncrypt {
			aesKey = ""
		}
		msg.Data, err = deConvertData(msg.Data, aesKey)
		if err != nil {
			return nil, err
		}
	}
	return &msg, nil
}

// readFully 确保完整读取指定长度的数据
func (c *BanConn) readFully(conn net.Conn, buf []byte) error {
	totalRead := 0
	for totalRead < len(buf) {
		n, err := conn.Read(buf[totalRead:])
		if err != nil {
			return err
		}
		totalRead += n
	}
	return nil
}

// writeFully 确保完整写入指定长度的数据
func writeFully(conn net.Conn, data []byte) error {
	totalWritten := 0
	for totalWritten < len(data) {
		end := min(totalWritten+banConnWriteChunkSize, len(data))
		if err := conn.SetWriteDeadline(time.Now().Add(banConnWriteIdleTimeout)); err != nil {
			return err
		}
		n, err := conn.Write(data[totalWritten:end])
		if err != nil {
			return err
		}
		totalWritten += n
	}
	return nil
}

func (c *BanConn) Read() ([]byte, *errs.Error) {
	conn := c.getConn()
	if conn == nil {
		return nil, errs.NewMsg(core.ErrRunTime, "BanConn Read nil, connection already closed")
	}
	// 读取长度头
	lenBuf := make([]byte, 4)
	if err_ := c.readFully(conn, lenBuf); err_ != nil {
		errCode, errType := getErrType(err_)
		if c.canReconnect() && errCode == core.ErrNetConnect {
			log.Warn("read fail, wait 3s and retry", zap.String("type", errType))
			if !waitBanConnContext(c.context(), time.Second*3) {
				if cancelErr := c.contextCancelled(); cancelErr != nil {
					return nil, cancelErr
				}
				return nil, errs.NewMsg(errs.CodeCancel, "ban connection reconnect canceled")
			}
			c.connect()
			if cancelErr := c.contextCancelled(); cancelErr != nil {
				return nil, cancelErr
			}
			return c.Read()
		}
		return nil, errs.New(errCode, err_)
	}
	// 获取数据长度
	dataLen := binary.LittleEndian.Uint32(lenBuf)
	if dataLen == 0 {
		return []byte{}, nil
	}
	if dataLen > banConnMaxFrameBytes {
		return nil, errs.NewMsg(core.ErrNetReadFail, "ban connection frame exceeds %d bytes", banConnMaxFrameBytes)
	}
	// 读取完整的数据
	buf := make([]byte, dataLen)
	if err_ := c.readFully(conn, buf); err_ != nil {
		return nil, errs.New(core.ErrNetReadFail, err_)
	}
	return buf, nil
}

func (c *BanConn) SetData(val interface{}, tags ...string) {
	c.lockData.Lock()
	for _, tag := range tags {
		c.Data[tag] = val
	}
	c.lockData.Unlock()
}
func (c *BanConn) DeleteData(tags ...string) {
	c.lockData.Lock()
	for _, tag := range tags {
		delete(c.Data, tag)
	}
	c.lockData.Unlock()
}

func (c *BanConn) SetAesKey(aesKey string) {
	c.aesKey = aesKey
}

func (c *BanConn) GetAesKey() string {
	return c.aesKey
}

// SetWait set chan for key to wait async
func (c *BanConn) SetWait(key string, size int) string {
	if key == "" {
		key = uuid.New().String()
	}
	c.lockWait.Lock()
	c.waits[key] = make(chan []byte, size)
	c.lockWait.Unlock()
	return key
}

// GetWaitChan get chan for key to wait or set
func (c *BanConn) GetWaitChan(key string) (chan []byte, bool) {
	c.lockWait.Lock()
	out, ok := c.waits[key]
	c.lockWait.Unlock()
	if !ok {
		return nil, false
	}
	return out, true
}

func (c *BanConn) CloseWaitChan(key string) {
	c.lockWait.Lock()
	if out, ok := c.waits[key]; ok {
		close(out)
		delete(c.waits, key)
	}
	c.lockWait.Unlock()
}

// SetWaitResult set result for key
func (c *BanConn) SetWaitResult(key string, data []byte) error {
	out, ok := c.GetWaitChan(key)
	if !ok {
		return fmt.Errorf("key not found: %s", key)
	}
	out <- data
	return nil
}

func (c *BanConn) SendWaitRes(key string, data interface{}) *errs.Error {
	return c.WriteMsg(&IOMsg{
		Action: "__res__" + key,
		Data:   data,
	})
}

// WaitResult wait result for key with specified timeout; wait __res__[key] to be trigger
func (c *BanConn) WaitResult(ctx context.Context, key string, timeout time.Duration) ([]byte, *errs.Error) {
	out, ok := c.GetWaitChan(key)
	if !ok {
		return nil, errs.NewMsg(errs.CodeRunTime, "key not found: %s", key)
	}
	var res []byte
	select {
	case res = <-out:
		c.CloseWaitChan(key)
		return res, nil
	case <-ctx.Done():
		c.CloseWaitChan(key)
		return nil, errs.NewMsg(errs.CodeCancel, "user cancel task")
	case <-time.After(timeout):
		c.CloseWaitChan(key)
		return nil, errs.NewMsg(errs.CodeRunTime, "timeout waiting for key: %s", key)
	}
}

/*
RunForever
Monitor the information sent by the connection and process it.
According to the action of the message:
Call the corresponding member function for processing; Directly input msd_data
Or find the corresponding processing function from listeners. If there is an exact match, pass msd_data. Otherwise, pass action and msd_data
Both the server-side and client-side will call this method
监听连接发送的信息并处理。
根据消息的action：

	调用对应成员函数处理；直接传入msg_data
	或从listens中找对应的处理函数，如果精确匹配，传入msg_data，否则传入action, msg_data

服务器端和客户端都会调用此方法
*/
func (c *BanConn) RunForever() *errs.Error {
	return c.runForever("")
}

// runForever uses runMode when supplied. An empty mode retains the legacy
// process-wide gate used by BanConn and callers built before runtime state was
// introduced.
func (c *BanConn) runForever(runMode string) *errs.Error {
	if runMode == "" {
		if !core.LiveMode {
			return errs.NewMsg(errs.CodeRunTime, "BanConn is unavailable in mode %s", core.RunMode)
		}
	} else if runMode != core.RunModeLive {
		return errs.NewMsg(errs.CodeRunTime, "BanConn is unavailable in mode %s", runMode)
	}
	if !c.beginLoop() {
		return nil
	}
	if !c.beginReading() {
		c.leaveLoop()
		return nil
	}
	defer func() {
		c.setReading(false)
		if err := c.Close(); err != nil {
			log.Error("close conn fail", zap.String("remote", c.Remote), zap.Error(err))
		}
		c.leaveLoop()
		c.Join()
		log.Info("close banConn as RunForever exit")
	}()
	for {
		msg, err := c.ReadMsg()
		if err != nil {
			if err.Code == core.ErrDeCompressFail || err.Code == errs.CodeUnmarshalFail || err.Code == core.ErrDecryptFail {
				// 无效消息，忽略
				if c.BadMsgCB != nil {
					if c.beginHandler() {
						c.runBadMsg(err)
					}
				} else {
					log.Error("invalid banIO msg", zap.Error(err))
				}
				continue
			}
			return err
		}
		if strings.HasPrefix(msg.Action, "__res__") {
			requestID := msg.Action[7:]
			if out, ok := c.GetWaitChan(requestID); ok {
				select {
				case out <- msg.Data:
					break
				default:
					log.Error("wait chan full, closing and skip", zap.String("requestID", requestID))
					c.CloseWaitChan(requestID)
				}
			}
			continue
		}
		var matchHandle ConnCB
		for prefix, handle := range c.Listens {
			if strings.HasPrefix(msg.Action, prefix) {
				matchHandle = handle
				break
			}
		}
		if matchHandle == nil {
			if c.Fallback != nil {
				if c.beginHandler() {
					c.runHandler(c.Fallback, msg)
				}
			} else {
				log.Info("unhandle msg", zap.String("action", msg.Action))
			}
		} else if c.beginHandler() {
			go c.runHandler(matchHandle, msg)
		}
	}
}

/*
connect
A function used for reconnecting.
用于重新连接的函数。
*/
func (c *BanConn) connect() {
	c.lockConnect.Lock()
	if c.stopped || c.DoConnect == nil || c.connecting {
		c.lockConnect.Unlock()
		return
	}
	if c.Ready && btime.TimeMS()-c.RefreshMS < 2000 {
		// 连接已经刷新，跳过本次重试
		c.lockConnect.Unlock()
		return
	}
	c.connecting = true
	c.Ready = false
	oldConn := c.Conn
	c.Conn = nil
	doConnect := c.DoConnect
	c.lockConnect.Unlock()
	if oldConn != nil {
		_ = oldConn.Close()
		log.Info("closed old banConn for reconnect")
	}
	doConnect(c)
	c.lockConnect.Lock()
	c.connecting = false
	c.RefreshMS = btime.TimeMS()
	conn := c.Conn
	stopped := c.stopped
	if conn != nil && !stopped {
		c.Ready = true
		log.Info("reconnect ok", zap.String("remote", c.Remote))
	}
	reInitConn := c.ReInitConn
	if stopped {
		c.Conn = nil
	}
	c.lockConnect.Unlock()
	if stopped {
		if conn != nil {
			_ = conn.Close()
		}
	} else if conn != nil && reInitConn != nil {
		if c.beginHandler() {
			c.runReInit(reInitConn)
		}
	}
}

// LoopPing should be called from client
func (c *BanConn) LoopPing(intvSecs int) {
	c.loopPing(core.Ctx, intvSecs)
}

// LoopPingContext runs the client ping loop against an explicit lifecycle.
// The caller owns the goroutine, and Stop/Join wait for it like other handlers.
func (c *BanConn) LoopPingContext(ctx context.Context, intvSecs int) {
	c.loopPing(ctx, intvSecs)
}

func (c *BanConn) loopPing(ctx context.Context, intvSecs int) {
	stop, ok := c.beginLoopPing()
	if !ok {
		return
	}
	defer c.leaveHandler()
	if ctx == nil {
		ctx = context.Background()
	}
	id := 0
	failNum := 0
	addrField := zap.String("addr", c.Remote)
	// 针对LoopPing早于RunForever很久启动的情况，先设置状态
	if !c.beginReading() {
		return
	}
	for {
		if !waitLoopPing(ctx, stop, time.Duration(intvSecs)*time.Second) || !c.isReading() || loopPingStopped(ctx, stop) {
			// 不再处理消息，连接失效退出
			break
		}
		if !c.isReady() {
			if c.lockConnect.TryLock() {
				// 获取锁成功，未正在连接，可继续ping
				c.lockConnect.Unlock()
			} else {
				// 获取锁失败，正在重新连接，跳过ping
				continue
			}
		}
		id += 1
		err := c.WriteMsg(&IOMsg{Action: "ping", Data: id, NoEncrypt: true})
		if err != nil {
			failNum += 1
			if failNum >= 2 {
				failNum = 0
				log.Warn("write ping fail twice", addrField, zap.String("err", err.Short()))
			}
			// 客户端不应该因为ping失败就退出，网络问题会自动重连
		} else {
			failNum = 0
		}
	}
	log.Warn("LoopPing exit as IsReading=false", addrField)
}

func waitLoopPing(ctx context.Context, stop <-chan struct{}, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	case <-stop:
		return false
	}
}

func loopPingStopped(ctx context.Context, stop <-chan struct{}) bool {
	select {
	case <-ctx.Done():
		return true
	case <-stop:
		return true
	default:
		return false
	}
}

// Close stops admission and closes the active socket. It is intentionally
// non-blocking; the lifecycle owner must call Join after callbacks unwind.
func (c *BanConn) Close() *errs.Error {
	c.stopHandlers()
	c.cancelContext()
	c.lockConnect.Lock()
	c.stopped = true
	c.IsReading = false
	c.DoConnect = nil
	conn := c.Conn
	c.Conn = nil
	c.Ready = false
	c.lockConnect.Unlock()
	if conn != nil {
		if err_ := conn.Close(); err_ != nil {
			return errs.New(errs.CodeIOWriteFail, err_)
		}
	}
	return nil
}

// Stop permanently stops reads and reconnects, then closes the active socket.
// It intentionally does not join handlers; callers outside a handler should
// call Join after Stop.
func (c *BanConn) Stop() *errs.Error {
	return c.Close()
}

func (c *BanConn) initListens() {
	c.Listens["subscribe"] = makeArrStrHandle(func(arr []string) {
		c.SetData(true, arr...)
	})
	c.Listens["unsubscribe"] = makeArrStrHandle(func(arr []string) {
		c.DeleteData(arr...)
	})
	c.Listens["ping"] = func(msg *IOMsgRaw) {
		var val int64
		err_ := utils.Unmarshal(msg.Data, &val, utils.JsonNumDefault)
		if err_ != nil {
			log.Warn("got bad ping", zap.ByteString("data", msg.Data))
			return
		}
		err := c.WriteMsg(&IOMsg{Action: "pong", Data: val + 1, NoEncrypt: true})
		if err != nil {
			log.Warn("write pong fail", zap.Int64("v", val), zap.Error(err))
		} else {
			c.heartBeatMs = btime.UTCStamp()
			log.Debug("receive ping", zap.String("from", c.Remote), zap.Int64("v", val))
		}
	}
	c.Listens["pong"] = func(msg *IOMsgRaw) {
		c.heartBeatMs = btime.UTCStamp()
		log.Debug("receive pong", zap.String("from", c.Remote))
	}
}

func makeArrStrHandle(cb func(arr []string)) func(msg *IOMsgRaw) {
	return func(msg *IOMsgRaw) {
		var tags = make([]string, 0, 8)
		err := utils.Unmarshal(msg.Data, &tags, utils.JsonNumDefault)
		if err != nil {
			log.Error("receive invalid data", zap.String("n", msg.Action), zap.String("raw", string(msg.Data)),
				zap.Error(err))
			return
		}
		if len(tags) > 0 {
			cb(tags)
		}
	}
}

func marshalAny(data interface{}) ([]byte, error) {
	if strVal, isStr := data.(string); isStr {
		return []byte(strVal), nil
	}
	var err_ error
	byteRaw, isBytes := data.([]byte)
	if !isBytes {
		byteRaw, err_ = utils.Marshal(data)
		if err_ != nil {
			return nil, err_
		}
	}
	return byteRaw, nil
}

func (msg *IOMsg) Marshal(aesKey string) (*IOMsgRaw, *errs.Error) {
	msgRaw := &IOMsgRaw{
		Action:    msg.Action,
		NoEncrypt: msg.NoEncrypt,
	}
	if msg.Data == nil {
		return msgRaw, nil
	}
	data, err_ := marshalAny(msg.Data)
	if err_ != nil {
		return nil, errs.New(core.ErrMarshalFail, err_)
	}
	var err *errs.Error
	data, err = compress(data)
	if err != nil {
		return nil, err
	}
	if aesKey != "" && !msg.NoEncrypt {
		data, err_ = EncryptData(data, aesKey)
		if err_ != nil {
			return nil, errs.New(core.ErrEncryptFail, err_)
		}
	}
	msgRaw.Data = data
	return msgRaw, nil
}

func deConvertData(compressed []byte, aesKey string) ([]byte, *errs.Error) {
	if aesKey != "" {
		var err_ error
		compressed, err_ = DecryptData(compressed, aesKey)
		if err_ != nil {
			return nil, errs.New(core.ErrDecryptFail, err_)
		}
	}
	data, err := deCompress(compressed)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func compress(data []byte) ([]byte, *errs.Error) {
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	_, err_ := w.Write(data)
	if err_ != nil {
		return nil, errs.New(core.ErrCompressFail, err_)
	}
	err_ = w.Close()
	if err_ != nil {
		return nil, errs.New(core.ErrCompressFail, err_)
	}
	return b.Bytes(), nil
}

func deCompress(compressed []byte) ([]byte, *errs.Error) {
	var result bytes.Buffer
	b := bytes.NewReader(compressed)

	// Create zlib decompressor
	// 创建 zlib 解压缩器
	r, err := zlib.NewReader(b)
	if err != nil {
		return nil, errs.New(core.ErrDeCompressFail, err)
	}
	defer r.Close()

	// Copy the decompressed data to the result
	// 将解压后的数据复制到 result 中
	_, err = io.Copy(&result, io.LimitReader(r, banConnMaxMessageBytes+1))
	if err != nil {
		return nil, errs.New(core.ErrDeCompressFail, err)
	}
	if result.Len() > banConnMaxMessageBytes {
		return nil, errs.NewMsg(core.ErrDeCompressFail, "ban connection message exceeds %d bytes", banConnMaxMessageBytes)
	}

	return result.Bytes(), nil
}

func getErrType(err error) (int, string) {
	if err == nil {
		return 0, ""
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if opErr.Timeout() {
			return core.ErrNetTimeout, "op_timeout"
		} else if opErr.Temporary() {
			return core.ErrNetTemporary, "op_temporary"
		} else if opErr.Op == "dial" || opErr.Op == "read" || opErr.Op == "write" {
			return core.ErrNetConnect, "op_conn_dial"
		} else {
			return core.ErrNetUnknown, "op_err"
		}
	}
	var callErr *syscall.Errno
	if errors.As(err, &callErr) {
		if errors.Is(callErr, syscall.ECONNRESET) {
			return core.ErrNetConnect, "call_reset"
		} else if errors.Is(callErr, syscall.EPIPE) {
			return core.ErrNetConnect, "call_pipe"
		} else {
			return core.ErrNetUnknown, "call_err"
		}
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return core.ErrNetTimeout, "dead_exceeded"
	}
	if errors.Is(err, io.EOF) {
		return core.ErrNetConnect, "io_eof"
	} else if errors.Is(err, io.ErrClosedPipe) {
		return core.ErrNetConnect, "pipe_closed"
	} else {
		return core.ErrNetUnknown, "net_fail"
	}
}

type ServerIO struct {
	Addr        string
	aesKey      string
	connections *serverConnections
	Data        map[string]string // Cache data available for remote access 缓存的数据，可供远程端访问
	DataExp     map[string]int64  // Cache data expiration timestamp, 13 bits 缓存数据的过期时间戳，13位
	InitConn    func(*BanConn)
	OnConnExit  func(*BanConn, *errs.Error)
}

type serverConnections struct {
	sync.RWMutex
	items []IBanConn
}

var (
	banServer *ServerIO
)

func NewBanServer(addr, aesKey string) *ServerIO {
	var server ServerIO
	server.Addr = addr
	server.aesKey = aesKey
	server.connections = &serverConnections{}
	server.Data = map[string]string{}
	banServer = &server
	gob.Register(IOMsgRaw{})
	return &server
}

func (s *ServerIO) AddConnection(conn IBanConn) {
	if s.connections == nil {
		s.connections = &serverConnections{}
	}
	s.connections.Lock()
	s.connections.items = append(s.connections.items, conn)
	s.connections.Unlock()
}

func (s *ServerIO) RemoveConnection(target IBanConn) {
	if s.connections == nil {
		return
	}
	s.connections.Lock()
	for i, conn := range s.connections.items {
		if conn == target {
			s.connections.items = append(s.connections.items[:i], s.connections.items[i+1:]...)
			break
		}
	}
	s.connections.Unlock()
}

func (s *ServerIO) ConnectionsSnapshot() []IBanConn {
	if s.connections == nil {
		return nil
	}
	s.connections.RLock()
	result := append([]IBanConn(nil), s.connections.items...)
	s.connections.RUnlock()
	return result
}

func (s *ServerIO) RunForever(intvSecs, timeoutSecs int) *errs.Error {
	ln, err_ := net.Listen("tcp", s.Addr)
	if err_ != nil {
		return errs.New(core.ErrNetConnect, err_)
	}
	defer ln.Close()
	log.Info("banio started", zap.String("addr", s.Addr))
	if intvSecs > 0 && timeoutSecs > 0 {
		go s.loopCheckTimeout(intvSecs, timeoutSecs)
	}
	for {
		conn_, err_ := ln.Accept()
		if err_ != nil {
			return errs.New(core.ErrNetConnect, err_)
		}
		conn := s.WrapConn(conn_)
		log.Info("receive client", zap.String("remote", conn.GetRemote()))
		s.AddConnection(conn)
		go func() {
			err := conn.RunForever()
			if err != nil {
				log.Warn("read client fail", zap.String("remote", conn.GetRemote()),
					zap.String("err", err.Message()))
			}
			s.RemoveConnection(conn)
			if s.OnConnExit != nil {
				s.OnConnExit(conn, err)
			}
		}()
	}
}

type KeyValExpire struct {
	Key        string
	Val        string
	ExpireSecs int `json:"expireSecs"`
}

type IOKeyVal struct {
	Key string `json:"key"`
	Val string `json:"val"`
}

func (s *ServerIO) SetVal(args *KeyValExpire) {
	if args.Val == "" {
		// 删除值
		delete(s.Data, args.Key)
		return
	}
	s.Data[args.Key] = args.Val
	if args.ExpireSecs > 0 {
		s.DataExp[args.Key] = btime.TimeMS() + int64(args.ExpireSecs*1000)
	}
}

func (s *ServerIO) GetVal(key string) string {
	val, ok := s.Data[key]
	if !ok {
		return ""
	}
	if exp, ok := s.DataExp[key]; ok {
		if btime.TimeMS() >= exp {
			delete(s.Data, key)
			delete(s.DataExp, key)
			return ""
		}
	}
	return val
}

func (s *ServerIO) Broadcast(msg *IOMsg) *errs.Error {
	conns := s.ConnectionsSnapshot()
	curConns := make([]IBanConn, 0)
	for _, conn := range conns {
		if conn.IsClosed() {
			s.RemoveConnection(conn)
			continue
		}
		if _, ok := conn.GetData(msg.Action); ok {
			curConns = append(curConns, conn)
		}
	}
	if len(curConns) == 0 {
		return nil
	}
	raw, err := msg.Marshal(s.aesKey)
	if err != nil {
		return err
	}
	for _, conn := range curConns {
		go func(c IBanConn) {
			var msgRaw = raw
			var writeErr *errs.Error
			aesKey := c.GetAesKey()
			if aesKey != "" {
				msgRaw, writeErr = msg.Marshal(aesKey)
			}
			if writeErr != nil {
				log.Warn("marshal fail", zap.String("remote", c.GetRemote()),
					zap.String("tag", msg.Action), zap.Error(writeErr))
				return
			}
			writeErr = c.Write(msgRaw)
			if writeErr != nil {
				log.Warn("broadcast fail", zap.String("remote", c.GetRemote()),
					zap.String("tag", msg.Action), zap.Error(writeErr))
			}
		}(conn)
	}
	return nil
}

// loopCheckTimeout Server checks ping timeout and closes stale connections
func (s *ServerIO) loopCheckTimeout(intvSecs, timeoutSecs int) {
	for {
		core.Sleep(time.Duration(intvSecs) * time.Second)
		nowMS := btime.UTCStamp()
		timeoutMS := int64(timeoutSecs * 1000)

		for _, conn := range s.ConnectionsSnapshot() {
			if conn.IsClosed() {
				s.RemoveConnection(conn)
				continue
			}
			banConn, ok := conn.(*BanConn)
			if !ok {
				continue
			}
			// Check if heartbeat timeout
			if banConn.heartBeatMs > 0 && nowMS-banConn.heartBeatMs > timeoutMS {
				log.Error("close conn as ping timeout",
					zap.String("remote", banConn.Remote),
					zap.Int64("lastHeartbeat", banConn.heartBeatMs),
					zap.Int64("timeoutMs", timeoutMS))
				_ = banConn.Close()
				s.RemoveConnection(conn)
				continue
			}
		}
	}
}

func (s *ServerIO) WrapConn(conn net.Conn) *BanConn {
	res := &BanConn{
		Conn:        conn,
		Data:        map[string]interface{}{},
		Listens:     map[string]ConnCB{},
		waits:       make(map[string]chan []byte),
		RefreshMS:   btime.TimeMS(),
		Ready:       true,
		Remote:      conn.RemoteAddr().String(),
		aesKey:      s.aesKey,
		heartBeatMs: btime.UTCStamp(), // Initialize heartbeat timestamp
	}
	res.Listens["onGetVal"] = func(msg *IOMsgRaw) {
		var key string
		err_ := utils.Unmarshal(msg.Data, &key, utils.JsonNumDefault)
		if err_ != nil {
			log.Error("unmarshal fail onGetVal", zap.String("raw", string(msg.Data)), zap.Error(err_))
			return
		}
		val := s.GetVal(key)
		err := res.SendWaitRes(key, []byte(val))
		if err != nil {
			log.Error("write val res fail", zap.Error(err))
		}
	}
	res.Listens["onSetVal"] = func(msg *IOMsgRaw) {
		var args KeyValExpire
		err := utils.Unmarshal(msg.Data, &args, utils.JsonNumDefault)
		if err != nil {
			log.Error("unmarshal fail onSetVal", zap.String("raw", string(msg.Data)), zap.Error(err))
			return
		}
		s.SetVal(&args)
	}
	res.initListens()
	if s.InitConn != nil {
		s.InitConn(res)
	}
	return res
}

type ClientIO struct {
	BanConn
	Addr    string
	RunMode string
}

// RunForever uses the mode captured by a typed constructor. A zero mode keeps
// the legacy global fallback for callers using NewClientIO or a literal
// ClientIO.
func (c *ClientIO) RunForever() *errs.Error {
	return c.BanConn.runForever(c.RunMode)
}

// NewClientIO should call LoopPing & RunForever later
func NewClientIO(addr, aesKey string) (*ClientIO, *errs.Error) {
	return NewClientIOWithContext(core.Ctx, addr, aesKey)
}

// NewClientIOWithContext creates a client whose initial dial and reconnects
// observe ctx. The context is captured once at construction and is not looked
// up dynamically on the message hot path.
func NewClientIOWithContext(ctx context.Context, addr, aesKey string) (*ClientIO, *errs.Error) {
	return newClientIOWithContext(ctx, addr, aesKey, true)
}

func newClientIOWithContext(ctx context.Context, addr, aesKey string, installLegacy bool) (*ClientIO, *errs.Error) {
	if ctx == nil {
		ctx = context.Background()
	}
	dialer := net.Dialer{}
	conn, err_ := dialer.DialContext(ctx, "tcp", addr)
	if err_ != nil {
		if ctx.Err() != nil {
			return nil, errs.New(errs.CodeCancel, ctx.Err())
		}
		return nil, errs.New(core.ErrNetConnect, err_)
	}
	gob.Register(IOMsgRaw{})
	res := &ClientIO{
		Addr: addr,
		BanConn: BanConn{
			Conn:      conn,
			Data:      map[string]interface{}{},
			Remote:    conn.RemoteAddr().String(),
			Listens:   map[string]ConnCB{},
			waits:     make(map[string]chan []byte),
			RefreshMS: btime.TimeMS(),
			Ready:     true,
			aesKey:    aesKey,
		},
	}
	res.SetContext(ctx)
	res.initListens()
	// This is only responsible for connection, no initialization required, leave it to connect for initialization
	// 这里只负责连接，无需初始化，交给connect初始化
	res.DoConnect = func(c *BanConn) {
		dialer := net.Dialer{}
		for {
			if !c.canReconnect() {
				return
			}
			dialCtx := c.context()
			if dialCtx.Err() != nil {
				return
			}
			cn, err_ := dialer.DialContext(dialCtx, "tcp", addr)
			if err_ != nil {
				if dialCtx.Err() != nil {
					return
				}
				curMS := btime.TimeMS()
				tipRetryTimesLock.Lock()
				nextMS, _ := tipRetryTimes[addr]
				if curMS > nextMS {
					tipRetryTimes[addr] = curMS + 10000
					log.Error("connect fail, sleep 10s and retry..", zap.String("addr", addr))
				}
				tipRetryTimesLock.Unlock()
				if !waitBanConnContext(c.context(), time.Second*10) {
					return
				}
				continue
			}
			if c.setReconnectConn(cn) {
				return
			}
		}
	}
	if installLegacy {
		banClient = res
	}
	return res, nil
}

// NewClientIOWithState binds the client lifecycle and run mode to one runtime
// state. A missing or empty state mode is deliberately treated as non-live;
// this constructor never falls back to core.LiveMode.
func NewClientIOWithState(state *core.State, addr, aesKey string) (*ClientIO, *errs.Error) {
	ctx := context.Background()
	runMode := core.RunModeOther
	if state != nil {
		if state.Context() != nil {
			ctx = state.Context()
		}
		if state.RunMode != "" {
			runMode = state.RunMode
		}
	}
	client, err := newClientIOWithContext(ctx, addr, aesKey, false)
	if client != nil {
		client.RunMode = runMode
	}
	return client, err
}

const (
	readTimeout = 120
)

func (c *ClientIO) GetVal(key string, timeout int) (string, *errs.Error) {
	c.SetWait(key, 1)
	err := c.WriteMsg(&IOMsg{
		Action: "onGetVal",
		Data:   key,
	})
	if err != nil {
		c.CloseWaitChan(key)
		return "", err
	}
	if timeout == 0 {
		timeout = readTimeout
	}
	res, err := c.WaitResult(context.Background(), key, time.Second*time.Duration(timeout))
	if err != nil {
		return "", err
	}
	return string(res), nil
}

func (c *ClientIO) SetVal(args *KeyValExpire) *errs.Error {
	return c.WriteMsg(&IOMsg{
		Action: "onSetVal",
		Data:   *args,
	})
}

var (
	banClient *ClientIO
)

func HasBanConn() bool {
	return banClient != nil || banServer != nil
}

func GetServerData(key string) (string, *errs.Error) {
	if banServer != nil {
		data := banServer.GetVal(key)
		return data, nil
	}
	if banClient == nil {
		return "", errs.NewMsg(core.ErrRunTime, "banClient not load")
	}
	return banClient.GetVal(key, 0)
}

func SetServerData(args *KeyValExpire) *errs.Error {
	if banServer != nil {
		banServer.SetVal(args)
		return nil
	}
	if banClient == nil {
		return errs.NewMsg(core.ErrRunTime, "banClient not load")
	}
	return banClient.SetVal(args)
}

func GetNetLock(key string, timeout int) (int32, *errs.Error) {
	lockKey := "lock_" + key
	val, err := GetServerData(lockKey)
	if err != nil {
		return 0, err
	}
	lockVal := rand.Int31()
	lockStr := fmt.Sprintf("%v", lockVal)
	if val == "" {
		err = SetServerData(&KeyValExpire{Key: lockKey, Val: lockStr})
		return lockVal, err
	}
	if timeout == 0 {
		timeout = 30
	}
	stopAt := btime.Time() + float64(timeout)
	for btime.Time() < stopAt {
		core.Sleep(time.Microsecond * 10)
		val, err = GetServerData(lockKey)
		if err != nil {
			return 0, err
		}
		if val == "" {
			err = SetServerData(&KeyValExpire{Key: lockKey, Val: lockStr})
			return lockVal, err
		}
	}
	return 0, errs.NewMsg(core.ErrTimeout, "GetNetLock for %s", key)
}

func DelNetLock(key string, lockVal int32) *errs.Error {
	lockKey := "lock_" + key
	val, err := GetServerData(lockKey)
	if err != nil {
		return err
	}
	lockStr := fmt.Sprintf("%v", lockVal)
	if val == lockStr {
		return SetServerData(&KeyValExpire{Key: lockKey, Val: ""})
	}
	log.Info("del lock fail", zap.String("val", val), zap.Int32("exp", lockVal))
	return nil
}
