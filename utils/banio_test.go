package utils

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/internal/testutil"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
	"io"
	"math/big"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestBanConnWriteIdleDeadlineDiscardsConnection(t *testing.T) {
	oldTimeout := banConnWriteIdleTimeout
	banConnWriteIdleTimeout = 20 * time.Millisecond
	t.Cleanup(func() { banConnWriteIdleTimeout = oldTimeout })
	left, right := net.Pipe()
	conn := &BanConn{Conn: left, Ready: true}
	t.Cleanup(func() {
		_ = left.Close()
		_ = right.Close()
	})

	started := time.Now()
	err := conn.WriteMsg(&IOMsg{Action: "blocked"})
	if err == nil {
		t.Fatal("blocked write unexpectedly succeeded")
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("blocked write took %s", elapsed)
	}
	if conn.getConn() != nil || conn.Ready {
		t.Fatal("failed frame connection remained reusable")
	}
}

func TestBanConnReadRejectsOversizedFrameBeforeAllocation(t *testing.T) {
	left, right := net.Pipe()
	conn := &BanConn{Conn: left, Ready: true}
	t.Cleanup(func() {
		_ = left.Close()
		_ = right.Close()
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		var header [4]byte
		binary.LittleEndian.PutUint32(header[:], banConnMaxFrameBytes+1)
		_, _ = right.Write(header[:])
	}()
	if _, err := conn.Read(); err == nil || !strings.Contains(err.Error(), "frame exceeds") {
		t.Fatalf("oversized frame error=%v", err)
	}
	<-done
}

func TestDeCompressRejectsOversizedMessage(t *testing.T) {
	payload := make([]byte, banConnMaxMessageBytes+1)
	compressed, err := compress(payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = deCompress(compressed); err == nil || !strings.Contains(err.Error(), "message exceeds") {
		t.Fatalf("oversized message error=%v", err)
	}
}

func TestBanConnWriteIdleDeadlineAllowsSlowProgress(t *testing.T) {
	oldTimeout := banConnWriteIdleTimeout
	banConnWriteIdleTimeout = 50 * time.Millisecond
	t.Cleanup(func() { banConnWriteIdleTimeout = oldTimeout })
	left, right := net.Pipe()
	conn := &BanConn{Conn: left, Ready: true}
	t.Cleanup(func() {
		_ = left.Close()
		_ = right.Close()
	})
	payload := make([]byte, 256<<10)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	readDone := make(chan error, 1)
	go func() {
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(right, lenBuf); err != nil {
			readDone <- err
			return
		}
		remaining := int(binary.LittleEndian.Uint32(lenBuf))
		buf := make([]byte, 32<<10)
		for remaining > 0 {
			size := min(len(buf), remaining)
			if _, err := io.ReadFull(right, buf[:size]); err != nil {
				readDone <- err
				return
			}
			remaining -= size
			time.Sleep(10 * time.Millisecond)
		}
		readDone <- nil
	}()
	started := time.Now()
	if err := conn.WriteMsg(&IOMsg{Action: "slow", Data: payload}); err != nil {
		t.Fatal(err)
	}
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed <= banConnWriteIdleTimeout {
		t.Fatalf("write completed too quickly to test idle deadline: %s", elapsed)
	}
}

func TestBanConnCoreStopFromHandlerDoesNotDeadlock(t *testing.T) {
	oldMode, oldLive := core.RunMode, core.LiveMode
	core.RunMode = core.RunModeLive
	core.LiveMode = true
	t.Cleanup(func() { core.RunMode, core.LiveMode = oldMode, oldLive })

	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = clientConn.Close()
	})
	state, err := core.NewState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	conn := &BanConn{
		Conn:    clientConn,
		Data:    map[string]interface{}{},
		Listens: map[string]ConnCB{},
		Ready:   true,
	}
	state.OnExit(func() { _ = conn.Stop() })
	callbackDone := make(chan struct{})
	conn.Listens["stop"] = func(*IOMsgRaw) {
		state.Stop()
		close(callbackDone)
	}
	loopDone := make(chan struct{})
	go func() {
		_ = conn.RunForever()
		close(loopDone)
	}()

	server := &BanConn{Conn: serverConn, Ready: true}
	if err := server.Write(&IOMsgRaw{Action: "stop"}); err != nil {
		t.Fatalf("write stop message: %v", err)
	}
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("Core.Stop from BanConn handler deadlocked")
	}
	select {
	case <-loopDone:
	case <-time.After(time.Second):
		t.Fatal("BanConn loop did not exit after Core.Stop")
	}
}

func TestBanConnStopJoinWaitsForAdmittedHandler(t *testing.T) {
	oldMode, oldLive := core.RunMode, core.LiveMode
	core.RunMode = core.RunModeLive
	core.LiveMode = true
	t.Cleanup(func() { core.RunMode, core.LiveMode = oldMode, oldLive })

	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = clientConn.Close()
	})
	entered := make(chan struct{})
	release := make(chan struct{})
	conn := &BanConn{
		Conn: clientConn,
		Data: map[string]interface{}{},
		Listens: map[string]ConnCB{"block": func(*IOMsgRaw) {
			close(entered)
			<-release
		}},
		Ready: true,
	}
	loopDone := make(chan struct{})
	go func() {
		_ = conn.RunForever()
		close(loopDone)
	}()

	server := &BanConn{Conn: serverConn, Ready: true}
	if err := server.Write(&IOMsgRaw{Action: "block"}); err != nil {
		t.Fatalf("write blocking message: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("socket handler did not start")
	}

	joined := make(chan struct{})
	go func() {
		if err := conn.Stop(); err != nil {
			t.Errorf("stop connection: %v", err)
		}
		conn.Join()
		close(joined)
	}()
	select {
	case <-joined:
		t.Fatal("Stop+Join returned before handler completed")
	case <-time.After(50 * time.Millisecond):
	}
	if conn.beginHandler() {
		conn.handlerWait.Done()
		t.Fatal("handler admitted after Stop")
	}
	close(release)
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("Join did not wait for handler")
	}
	select {
	case <-loopDone:
	case <-time.After(time.Second):
		t.Fatal("connection loop did not exit after Stop")
	}
}

func TestBanServer(t *testing.T) {
	requireManualBanIOTest(t)
	core.SetRunMode(core.RunModeLive)
	server := NewBanServer("127.0.0.1:6789", "")
	go func() {
		for {
			time.Sleep(time.Millisecond * 300)
			for _, conn := range server.ConnectionsSnapshot() {
				if conn.IsClosed() {
					continue
				}
				err_ := conn.WriteMsg(&IOMsg{Action: "ping", Data: 1, NoEncrypt: true})
				if err_ != nil {
					log.Warn("broadcast fail", zap.Error(err_))
				}
			}
		}
	}()
	err := server.RunForever(0, 0)
	if err != nil {
		panic(err)
	}
}

func TestServerIOConnectionsConcurrentSnapshot(t *testing.T) {
	server := NewBanServer("", "")
	const count = 100
	conns := make([]IBanConn, count)
	for i := range conns {
		left, right := net.Pipe()
		conns[i] = server.WrapConn(left)
		t.Cleanup(func() {
			_ = left.Close()
			_ = right.Close()
		})
	}

	var wg sync.WaitGroup
	for _, conn := range conns {
		wg.Add(2)
		go func() {
			defer wg.Done()
			server.AddConnection(conn)
			server.RemoveConnection(conn)
		}()
		go func() {
			defer wg.Done()
			for range 10 {
				_ = server.ConnectionsSnapshot()
			}
		}()
	}
	wg.Wait()
	if got := len(server.ConnectionsSnapshot()); got != 0 {
		t.Fatalf("connections after removal = %d", got)
	}
}

func TestNewServerIOKeepsLegacyServerStateUntouched(t *testing.T) {
	oldServer, oldClient := banServer, banClient
	banServer, banClient = nil, nil
	t.Cleanup(func() { banServer, banClient = oldServer, oldClient })

	legacy := NewBanServer("", "")
	owned := NewServerIO("", "")
	if banServer != legacy {
		t.Fatal("NewServerIO replaced the legacy server")
	}
	if !HasBanConn() {
		t.Fatal("legacy server was not visible to legacy connection helpers")
	}
	lock, err := GetNetLock("owned_constructor", 0)
	if err != nil {
		t.Fatal(err)
	}
	if legacy.GetVal("lock_owned_constructor") == "" {
		t.Fatal("legacy net lock was not stored on the legacy server")
	}
	if owned.GetVal("lock_owned_constructor") != "" {
		t.Fatal("legacy net lock was stored on the owned server")
	}
	if err := DelNetLock("owned_constructor", lock); err != nil {
		t.Fatal(err)
	}

	banServer = nil
	if HasBanConn() {
		t.Fatal("owned server affected legacy connection detection")
	}
}

func TestServerIOValuesConcurrentAndExpired(t *testing.T) {
	server := NewServerIO("", "")
	if server.DataExp == nil {
		t.Fatal("owned server did not initialize expiration state")
	}
	const workers = 16
	const iterations = 100
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range iterations {
				key := fmt.Sprintf("key-%d", i%4)
				server.SetVal(&KeyValExpire{Key: key, Val: fmt.Sprintf("%d-%d", worker, i)})
				_ = server.GetVal(key)
			}
		}()
	}
	wg.Wait()

	server.SetVal(&KeyValExpire{Key: "expired", Val: "value"})
	server.dataMu.Lock()
	server.DataExp["expired"] = btime.TimeMS() - 1
	server.dataMu.Unlock()
	if got := server.GetVal("expired"); got != "" {
		t.Fatalf("expired value = %q, want empty", got)
	}
}

func TestBanClient(t *testing.T) {
	requireManualBanIOTest(t)
	core.SetRunMode(core.RunModeLive)
	ctx, cancel := context.WithCancel(context.Background())
	core.Ctx = ctx
	core.StopAll = cancel
	log.Setup("debug", "")
	client, err := NewClientIO("127.0.0.1:6789", "")
	if err != nil {
		panic(err)
	}
	go client.LoopPing(1)
	go client.RunForever()
	err = client.SetVal(&KeyValExpire{
		Key: "vv1",
		Val: "vvvv",
	})
	if err != nil {
		panic(err)
	}
	val, err := client.GetVal("vv1", 5)
	if err != nil {
		panic(err)
	}
	log.Info("get val of vv1", zap.String("val", val))
	lockVal, err := GetNetLock("lk1", 5)
	if err != nil {
		panic(err)
	}
	log.Info("set lock", zap.Int32("val", lockVal))
	val, err = client.GetVal("lock_lk1", 5)
	if err != nil {
		panic(err)
	}
	log.Info("lock real val", zap.String("val", val))
	time.Sleep(time.Second * 3)
	err = DelNetLock("lk1", lockVal)
	if err != nil {
		panic(err)
	}
	val, err = client.GetVal("lock_lk1", 5)
	if err != nil {
		panic(err)
	}
	log.Info("lock val after del", zap.String("val", val))
}

func requireManualBanIOTest(t *testing.T) {
	t.Helper()
	testutil.RequireIntegration(t)
	if os.Getenv("BANBOT_RUN_MANUAL_BANIO_TESTS") != "1" {
		t.Skip("set BANBOT_RUN_MANUAL_BANIO_TESTS=1 for the external server/client smoke test")
	}
}

// 计算数据的MD5哈希值
func calculateMD5(data []byte) string {
	hash := md5.Sum(data)
	return hex.EncodeToString(hash[:])
}

// TestBanConnConcurrentLargeData 测试并发发送大数据时的连接稳定性
func TestBanConnConcurrentLargeData(t *testing.T) {
	core.SetRunMode(core.RunModeLive)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second) // 增加超时时间，因为数据变大了
	defer cancel()
	core.Ctx = ctx
	core.StopAll = cancel
	log.Setup("info", "")

	// 启动服务器（使用基于时间的端口避免冲突）
	port := 6791 + int(time.Now().UnixNano()%1000) // 基于时间的端口围6791-7790
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	server := NewBanServer(addr, "")
	var wg sync.WaitGroup

	// 用于统计接收到的消息（每次测试重新初始化）
	var receivedCount int64 = 0 // 显式重置为0
	var receivedMutex sync.Mutex
	receivedMessages := make(chan string, 100)
	// 存储消息ID和MD5的映射（每次测试重新初始化）
	messageMD5s := make(map[string]string)
	var md5Mutex sync.Mutex

	server.InitConn = func(conn *BanConn) {
		// 处理并发大数据
		conn.Listens["concurrent_large_data"] = func(msg *IOMsgRaw) {
			data := msg.Data
			receivedMutex.Lock()
			receivedCount++ // 每个测试的独立计数器
			currentCount := receivedCount
			receivedMutex.Unlock()

			// 从数据中提取消息ID（前16个字节是消息ID）
			if len(data) >= 16 {
				msgID := string(data[:16])

				// 计算接收数据的MD5哈希
				receivedMD5 := calculateMD5(data)

				log.Info("server received concurrent data",
					zap.String("msgID", msgID),
					zap.Int("size", len(data)),
					zap.Int64("count", currentCount),
					zap.String("receivedMD5", receivedMD5))

				// 存储消息的MD5
				md5Mutex.Lock()
				messageMD5s[msgID] = receivedMD5
				md5Mutex.Unlock()

				receivedMessages <- msgID

				// 发送确认，包含MD5
				err := conn.WriteMsg(&IOMsg{
					Action: "concurrent_ack",
					Data:   []byte(fmt.Sprintf("%s:%s", msgID, receivedMD5)),
				})
				if err != nil {
					log.Error("send concurrent ack fail", zap.String("msgID", msgID), zap.Error(err))
				}
			}
		}
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = server.RunForever(0, 0)
	}()

	// 等待服务器启动
	time.Sleep(100 * time.Millisecond)

	// 创建客户端连接
	client, err := NewClientIO(addr, "")
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	// 统计收到的确认消息
	receivedAcks := make(chan string, 100)
	client.Listens["concurrent_ack"] = func(msg *IOMsgRaw) {
		ackData := string(msg.Data)
		parts := strings.Split(ackData, ":")

		if len(parts) == 2 {
			msgID := parts[0]
			receivedMD5 := parts[1]
			log.Info("client received concurrent ack",
				zap.String("msgID", msgID),
				zap.String("receivedMD5", receivedMD5))
			receivedAcks <- msgID
		} else {
			log.Error("invalid ack format", zap.String("ackData", ackData))
			receivedAcks <- ackData // 还是把数据放到通道中以避免阻塞
		}
	}

	go client.RunForever()

	// 等待连接建立
	time.Sleep(100 * time.Millisecond)

	// 并发发送多个大数据包
	const (
		concurrentCount = 5 // 减少并发数，但增加每个数据大小
	)

	// 生成随机大小的数据，范围在1MB到10MB之间
	maxSize := 10 * 1024 * 1024 // 最大10MB
	minSize := 1 * 1024 * 1024  // 最小1MB

	var sendWg sync.WaitGroup
	sentMessages := make([]string, concurrentCount)

	for i := 0; i < concurrentCount; i++ {
		sendWg.Add(1)
		go func(index int) {
			defer sendWg.Done()

			// 为这个消息创建随机大小
			max := big.NewInt(int64(maxSize - minSize))
			n, _ := rand.Int(rand.Reader, max)
			dataSize := minSize + int(n.Int64())

			// 创建唯一的消息ID
			msgID := fmt.Sprintf("msg_%08d____", index) // 确保16字节
			sentMessages[index] = msgID                 // 存储完整的16字节ID

			// 创建随机测试数据（前16字节是消息ID）
			testData := make([]byte, dataSize)
			// 先放入消息ID
			copy(testData[:16], msgID)
			// 生成剩余的随机数据
			_, err := rand.Read(testData[16:])
			if err != nil {
				t.Errorf("failed to generate random data: %v", err)
				return
			}

			// 计算发送数据的MD5
			sendMD5 := calculateMD5(testData)

			// 存储消息ID和MD5的映射，用于后续验证
			md5Mutex.Lock()
			messageMD5s[msgID] = sendMD5 // 使用完整的16字节ID作为key
			md5Mutex.Unlock()

			log.Info("sending concurrent large data",
				zap.Int("index", index),
				zap.String("msgID", msgID),
				zap.Int("size", dataSize),
				zap.String("sendMD5", sendMD5))

			// 发送数据
			err2 := client.WriteMsg(&IOMsg{
				Action: "concurrent_large_data",
				Data:   testData,
			})

			if err2 != nil {
				t.Errorf("failed to send concurrent large data (index=%d): %v", index, err2)
			}
		}(i)

		// 稍微错开发送时间，模拟真实场景
		time.Sleep(10 * time.Millisecond)
	}

	// 等待所有发送完成
	sendWg.Wait()
	log.Info("all concurrent sends completed")

	// 验证所有消息都被接收
	receivedSet := make(map[string]bool)
	for i := 0; i < concurrentCount; i++ {
		select {
		case msgID := <-receivedMessages:
			receivedSet[msgID] = true

			// 检查MD5是否匹配
			md5Mutex.Lock()
			sendMD5, hasSendMD5 := messageMD5s[msgID]
			receiveMD5, hasReceiveMD5 := messageMD5s[msgID]
			md5Mutex.Unlock()

			if hasSendMD5 && hasReceiveMD5 {
				log.Info("MD5 verification for message",
					zap.String("msgID", msgID),
					zap.String("sendMD5", sendMD5),
					zap.String("receiveMD5", receiveMD5))

				if sendMD5 != receiveMD5 {
					t.Errorf("MD5 mismatch for message %s: send=%s receive=%s",
						msgID, sendMD5, receiveMD5)
				}
			}

			log.Info("marked message as received", zap.String("msgID", msgID))
		case <-time.After(30 * time.Second): // 增加超时时间
			t.Fatalf("timeout waiting for message %d", i+1)
		}
	}

	// 检查是否所有消息都被接收
	for i, sentID := range sentMessages {
		if !receivedSet[sentID] {
			t.Errorf("message %d (%s) was not received", i, sentID)
		}
	}

	// 验证所有确认消息
	ackSet := make(map[string]bool)
	for i := 0; i < concurrentCount; i++ {
		select {
		case ackID := <-receivedAcks:
			ackSet[ackID] = true
			log.Info("marked ack as received", zap.String("ackID", ackID))
		case <-time.After(10 * time.Second):
			t.Fatalf("timeout waiting for ack %d", i+1)
		}
	}

	// 检查是否所有确认都被接收
	for i, sentID := range sentMessages {
		if !ackSet[sentID] {
			t.Errorf("ack for message %d (%s) was not received", i, sentID)
		}
	}

	log.Info("concurrent large data test completed",
		zap.Int64("total_received", receivedCount),
		zap.Int("expected", concurrentCount))

	if receivedCount != int64(concurrentCount) {
		t.Errorf("message count mismatch: expected %d, received %d", concurrentCount, receivedCount)
	}
}
