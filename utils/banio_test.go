package utils

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestBanServer(t *testing.T) {
	core.SetRunMode(core.RunModeLive)
	server := NewBanServer("127.0.0.1:6789", "")
	go func() {
		for {
			time.Sleep(time.Millisecond * 300)
			for _, conn := range server.Conns {
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
	err := server.RunForever()
	if err != nil {
		panic(err)
	}
}

func TestBanClient(t *testing.T) {
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
		_ = server.RunForever()
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
			t.Errorf("timeout waiting for message %d", i+1)
			break
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
			t.Errorf("timeout waiting for ack %d", i+1)
			break
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
