package dev

import (
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	devws "github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	"github.com/gorilla/websocket"
)

func TestBroadcastWSConcurrentWrites(t *testing.T) {
	serverClient, browserClient := newTestWsClient(t)
	if tcpConn, ok := serverClient.Conn.UnderlyingConn().(*net.TCPConn); ok {
		if err := tcpConn.SetWriteBuffer(1024); err != nil {
			t.Fatal(err)
		}
	}

	const writerCount = 4
	payload := strings.Repeat("x", 1<<20)
	start := make(chan struct{})
	panics := make(chan any, writerCount)
	var writers sync.WaitGroup
	writers.Add(writerCount)
	for i := 0; i < writerCount; i++ {
		go func(id int) {
			defer writers.Done()
			defer func() { panics <- recover() }()
			<-start
			BroadcastWS("", map[string]interface{}{
				"type":    "test",
				"writer":  id,
				"payload": payload,
			})
		}(i)
	}
	close(start)

	// Let one server write fill the small socket buffer before draining it.
	time.Sleep(50 * time.Millisecond)
	readErr := make(chan error, 1)
	go func() {
		for i := 0; i < writerCount; i++ {
			if _, _, err := browserClient.ReadMessage(); err != nil {
				readErr <- err
				return
			}
		}
		readErr <- nil
	}()

	writers.Wait()
	close(panics)
	for recovered := range panics {
		if recovered != nil {
			t.Fatalf("concurrent BroadcastWS panicked: %v", recovered)
		}
	}
	if err := <-readErr; err != nil {
		t.Fatalf("read broadcast: %v", err)
	}
}

func TestWsClientCloseDoesNotWaitForBroadcast(t *testing.T) {
	serverClient, browserClient := newTestWsClient(t)
	if tcpConn, ok := serverClient.Conn.UnderlyingConn().(*net.TCPConn); ok {
		if err := tcpConn.SetWriteBuffer(1024); err != nil {
			t.Fatal(err)
		}
	}

	broadcastDone := make(chan any, 1)
	go func() {
		defer func() { broadcastDone <- recover() }()
		BroadcastWS("", map[string]interface{}{
			"type":    "test",
			"payload": strings.Repeat("x", 8<<20),
		})
	}()
	time.Sleep(50 * time.Millisecond)

	closeDone := make(chan struct{})
	go func() {
		serverClient.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Close blocked behind a websocket write")
	}
	if err := browserClient.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case recovered := <-broadcastDone:
		if recovered != nil {
			t.Fatalf("broadcast panicked while closing: %v", recovered)
		}
	case <-time.After(time.Second):
		t.Fatal("broadcast did not stop after the peer closed")
	}

	wsLock.RLock()
	_, exists := clients[serverClient]
	wsLock.RUnlock()
	if exists {
		t.Fatal("closed websocket remains registered")
	}
}

func newTestWsClient(t *testing.T) (*WsClient, *websocket.Conn) {
	t.Helper()
	wsLock.Lock()
	clients = make(map[*WsClient]bool)
	wsLock.Unlock()

	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	connected := make(chan *WsClient, 1)
	handlerDone := make(chan struct{})
	app.Get("/ws", devws.New(func(conn *devws.Conn) {
		connected <- NewWsClient(conn)
		<-handlerDone
	}))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverDone := make(chan error, 1)
	go func() { serverDone <- app.Listener(listener) }()

	browserClient, _, err := websocket.DefaultDialer.Dial("ws://"+listener.Addr().String()+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	serverClient := <-connected
	t.Cleanup(func() {
		serverClient.Close()
		_ = browserClient.Close()
		close(handlerDone)
		_ = app.Shutdown()
		<-serverDone
		wsLock.Lock()
		clients = make(map[*WsClient]bool)
		wsLock.Unlock()
	})
	return serverClient, browserClient
}
