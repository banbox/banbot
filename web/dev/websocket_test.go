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
	server := newDevServer(DevDeps{})
	serverClient, browserClient := newTestWsClient(t, server)
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
			server.BroadcastWS("", map[string]interface{}{
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
	server := newDevServer(DevDeps{})
	serverClient, browserClient := newTestWsClient(t, server)
	if tcpConn, ok := serverClient.Conn.UnderlyingConn().(*net.TCPConn); ok {
		if err := tcpConn.SetWriteBuffer(1024); err != nil {
			t.Fatal(err)
		}
	}

	broadcastDone := make(chan any, 1)
	go func() {
		defer func() { broadcastDone <- recover() }()
		server.BroadcastWS("", map[string]interface{}{
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

	server.wsMu.RLock()
	_, exists := server.clients[serverClient]
	server.wsMu.RUnlock()
	if exists {
		t.Fatal("closed websocket remains registered")
	}
}

func TestDevServersIsolateStatusClientsAndStop(t *testing.T) {
	first := newDevServer(DevDeps{})
	second := newDevServer(DevDeps{})
	firstClient, firstBrowser := newTestWsClient(t, first)
	_, secondBrowser := newTestWsClient(t, second)

	first.SetStatus(ServerStatus{DirtyBin: true})
	first.BroadcastStatus()
	if _, payload, err := firstBrowser.ReadMessage(); err != nil || !strings.Contains(string(payload), `"dirtyBin":true`) {
		t.Fatalf("first server did not receive its status: %v %s", err, payload)
	}
	if got := second.Status(); got != (ServerStatus{}) {
		t.Fatalf("second server inherited first server status: %+v", got)
	}

	first.Stop()
	deadline := time.Now().Add(time.Second)
	for !firstClient.closed.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !firstClient.closed.Load() {
		t.Fatal("stopping first server did not close its client")
	}
	second.BroadcastWS("", map[string]interface{}{"type": "second"})
	if _, payload, err := secondBrowser.ReadMessage(); err != nil || !strings.Contains(string(payload), `"second"`) {
		t.Fatalf("second server stopped with first: %v %s", err, payload)
	}
}

func TestDevServerStopJoinsWebSocketHandler(t *testing.T) {
	server := newDevServer(DevDeps{})
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Get("/ws", devws.New(server.onWsDev))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverDone := make(chan error, 1)
	go func() { serverDone <- app.Listener(listener) }()
	browser, _, err := websocket.DefaultDialer.Dial("ws://"+listener.Addr().String()+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = browser.Close()
		_ = app.Shutdown()
		<-serverDone
	})

	deadline := time.Now().Add(time.Second)
	for {
		server.wsMu.RLock()
		connected := len(server.clients) == 1
		server.wsMu.RUnlock()
		if connected {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("websocket handler was not admitted")
		}
		time.Sleep(time.Millisecond)
	}

	server.Stop()
	joined := make(chan struct{})
	go func() {
		server.Join()
		close(joined)
	}()
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("Join did not wait for the websocket handler to exit")
	}
}

func newTestWsClient(t *testing.T, server *DevServer) (*WsClient, *websocket.Conn) {
	t.Helper()

	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	connected := make(chan *WsClient, 1)
	app.Get("/ws", devws.New(func(conn *devws.Conn) {
		client := server.NewWsClient(conn)
		connected <- client
		client.HandleForever()
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
		_ = app.Shutdown()
		<-serverDone
	})
	return serverClient, browserClient
}
