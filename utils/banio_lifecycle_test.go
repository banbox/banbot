package utils

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/errs"
)

func TestClientIORunForeverUsesRuntimeState(t *testing.T) {
	oldMode, oldLive := core.RunMode, core.LiveMode
	core.RunMode = core.RunModeLive
	core.LiveMode = true
	t.Cleanup(func() { core.RunMode, core.LiveMode = oldMode, oldLive })

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr == nil {
			accepted <- conn
		}
	}()

	state, stateErr := core.NewState(context.Background())
	if stateErr != nil {
		_ = listener.Close()
		t.Fatal(stateErr)
	}
	state.SetRunMode(core.RunModeBackTest)
	client, clientErr := NewClientIOWithState(state, listener.Addr().String(), "")
	if clientErr != nil {
		state.Close()
		_ = listener.Close()
		t.Fatal(clientErr)
	}
	var serverConn net.Conn
	select {
	case serverConn = <-accepted:
	case <-time.After(time.Second):
		_ = client.Stop()
		client.Join()
		state.Close()
		_ = listener.Close()
		t.Fatal("client did not connect")
	}
	t.Cleanup(func() {
		_ = client.Stop()
		client.Join()
		state.Close()
		_ = serverConn.Close()
		_ = listener.Close()
	})

	result := make(chan *errs.Error, 1)
	go func() { result <- client.RunForever() }()
	select {
	case runErr := <-result:
		if runErr == nil {
			t.Fatal("typed client unexpectedly entered the read loop")
		}
	case <-time.After(time.Second):
		_ = client.Stop()
		client.Join()
		t.Fatal("typed client ignored its runtime run mode")
	}
}

func TestBanConnLoopPingContextStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	observed := &loopPingContext{
		Context: ctx,
		started: make(chan struct{}),
	}
	conn := &BanConn{
		Conn:  &testPingConn{},
		Ready: true,
	}
	t.Cleanup(func() {
		cancel()
		_ = conn.Stop()
		conn.Join()
	})

	done := make(chan struct{})
	go func() {
		conn.LoopPingContext(observed, 30)
		close(done)
	}()
	select {
	case <-observed.started:
	case <-time.After(time.Second):
		t.Fatal("LoopPingContext did not start waiting")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("LoopPingContext did not stop after context cancellation")
	}
}

func TestNewClientIOWithContextCancelsReconnectBackoff(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	acceptDone := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr == nil {
			acceptDone <- conn
			return
		}
		close(acceptDone)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	client, clientErr := NewClientIOWithContext(ctx, listener.Addr().String(), "")
	if clientErr != nil {
		cancel()
		_ = listener.Close()
		t.Fatal(clientErr)
	}
	serverConn := <-acceptDone
	_ = listener.Close()
	_ = serverConn.Close()
	t.Cleanup(func() {
		cancel()
		_ = client.Stop()
		client.Join()
	})

	client.lockConnect.Lock()
	client.Ready = false
	client.RefreshMS = 0
	client.lockConnect.Unlock()

	started := time.Now()
	connected := make(chan struct{})
	go func() {
		client.connect()
		close(connected)
	}()
	select {
	case <-time.After(100 * time.Millisecond):
		cancel()
	case <-connected:
		t.Fatal("reconnect unexpectedly completed before cancellation")
	}
	select {
	case <-connected:
	case <-time.After(time.Second):
		t.Fatal("reconnect did not stop after context cancellation")
	}
	if elapsed := time.Since(started); elapsed >= 2*time.Second {
		t.Fatalf("canceled reconnect took too long: %s", elapsed)
	}
}

func TestBanConnLoopPingStopJoinWaitsForPing(t *testing.T) {
	writeStarted := make(chan struct{})
	releaseWrite := make(chan struct{})
	var writeOnce sync.Once
	var releaseOnce sync.Once
	conn := &BanConn{
		Conn: &testPingConn{
			onWrite: func() {
				writeOnce.Do(func() { close(writeStarted) })
			},
			writeBlock: releaseWrite,
		},
		Ready: true,
	}
	t.Cleanup(func() {
		_ = conn.Stop()
		releaseOnce.Do(func() { close(releaseWrite) })
		conn.Join()
	})

	loopDone := make(chan struct{})
	go func() {
		conn.LoopPingContext(context.Background(), 0)
		close(loopDone)
	}()
	select {
	case <-writeStarted:
	case <-time.After(time.Second):
		t.Fatal("LoopPing did not start a ping write")
	}

	stopReturned := make(chan struct{})
	joined := make(chan struct{})
	go func() {
		if err := conn.Stop(); err != nil {
			t.Errorf("stop connection: %v", err)
		}
		close(stopReturned)
		conn.Join()
		close(joined)
	}()
	select {
	case <-stopReturned:
	case <-time.After(time.Second):
		t.Fatal("Stop did not return")
	}
	select {
	case <-joined:
		t.Fatal("Join returned while LoopPing was still writing")
	case <-time.After(50 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(releaseWrite) })
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("Join did not wait for LoopPing")
	}
	select {
	case <-loopDone:
	case <-time.After(time.Second):
		t.Fatal("LoopPing did not exit after Stop")
	}
}

func TestBanConnStopJoinWaitsForFallbackAndBadMessageHandlers(t *testing.T) {
	oldMode, oldLive := core.RunMode, core.LiveMode
	core.RunMode = core.RunModeLive
	core.LiveMode = true
	t.Cleanup(func() { core.RunMode, core.LiveMode = oldMode, oldLive })

	for _, test := range []struct {
		name  string
		write func(t *testing.T, conn net.Conn)
		set   func(*BanConn, chan struct{}, <-chan struct{})
	}{
		{
			name: "fallback",
			write: func(t *testing.T, conn net.Conn) {
				server := &BanConn{Conn: conn, Ready: true}
				if err := server.Write(&IOMsgRaw{Action: "unmatched"}); err != nil {
					t.Fatalf("write fallback message: %v", err)
				}
			},
			set: func(conn *BanConn, entered chan struct{}, release <-chan struct{}) {
				conn.Fallback = func(*IOMsgRaw) {
					close(entered)
					<-release
				}
			},
		},
		{
			name: "bad-message",
			write: func(t *testing.T, conn net.Conn) {
				var header [4]byte
				binary.LittleEndian.PutUint32(header[:], 1)
				if _, err := conn.Write(header[:]); err != nil {
					t.Fatalf("write bad-message frame header: %v", err)
				}
				if _, err := conn.Write([]byte{0}); err != nil {
					t.Fatalf("write bad-message frame: %v", err)
				}
			},
			set: func(conn *BanConn, entered chan struct{}, release <-chan struct{}) {
				conn.BadMsgCB = func(*errs.Error) {
					close(entered)
					<-release
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			serverConn, clientConn := net.Pipe()
			entered := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			conn := &BanConn{
				Conn:    clientConn,
				Data:    map[string]interface{}{},
				Listens: map[string]ConnCB{},
				Ready:   true,
			}
			test.set(conn, entered, release)
			t.Cleanup(func() {
				releaseOnce.Do(func() { close(release) })
				_ = conn.Stop()
				conn.Join()
				_ = serverConn.Close()
				_ = clientConn.Close()
			})

			loopDone := make(chan struct{})
			go func() {
				_ = conn.RunForever()
				close(loopDone)
			}()
			test.write(t, serverConn)
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("handler did not start")
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

			releaseOnce.Do(func() { close(release) })
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
		})
	}
}

func TestBanConnCloseFromHandlerRequiresOwnerJoin(t *testing.T) {
	oldMode, oldLive := core.RunMode, core.LiveMode
	core.RunMode = core.RunModeLive
	core.LiveMode = true
	t.Cleanup(func() { core.RunMode, core.LiveMode = oldMode, oldLive })

	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = clientConn.Close()
	})
	joined := make(chan struct{})
	conn := &BanConn{
		Conn:    clientConn,
		Data:    map[string]interface{}{},
		Listens: map[string]ConnCB{},
		Ready:   true,
	}
	conn.Listens["join"] = func(*IOMsgRaw) {
		if err := conn.Close(); err != nil {
			t.Errorf("close connection: %v", err)
		}
		close(joined)
	}
	loopDone := make(chan struct{})
	go func() {
		_ = conn.RunForever()
		close(loopDone)
	}()

	server := &BanConn{Conn: serverConn, Ready: true}
	if err := server.Write(&IOMsgRaw{Action: "join"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("BanConn.Close from handler did not return")
	}
	// The owner joins after the callback has unwound. A callback must not join
	// its own admission token.
	conn.Join()
	select {
	case <-loopDone:
	case <-time.After(time.Second):
		t.Fatal("BanConn loop did not exit after handler close")
	}
}

func TestBanConnCloseCancelsReconnectBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := &BanConn{DoConnect: func(c *BanConn) {
		waitBanConnContext(c.context(), 10*time.Second)
	}}
	conn.SetContext(ctx)
	conn.lockConnect.Lock()
	conn.Ready = false
	conn.RefreshMS = 0
	conn.lockConnect.Unlock()

	connected := make(chan struct{})
	go func() {
		conn.connect()
		close(connected)
	}()
	select {
	case <-time.After(50 * time.Millisecond):
	case <-connected:
		t.Fatal("reconnect completed before stop")
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-connected:
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel reconnect backoff")
	}
}

type loopPingContext struct {
	context.Context
	started chan struct{}
	once    sync.Once
}

func (c *loopPingContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.started) })
	return c.Context.Done()
}

type testPingConn struct {
	onWrite    func()
	writeBlock <-chan struct{}
}

func (c *testPingConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c *testPingConn) Close() error                     { return nil }
func (c *testPingConn) LocalAddr() net.Addr              { return testPingAddr{} }
func (c *testPingConn) RemoteAddr() net.Addr             { return testPingAddr{} }
func (c *testPingConn) SetDeadline(time.Time) error      { return nil }
func (c *testPingConn) SetReadDeadline(time.Time) error  { return nil }
func (c *testPingConn) SetWriteDeadline(time.Time) error { return nil }

func (c *testPingConn) Write(data []byte) (int, error) {
	if c.onWrite != nil {
		c.onWrite()
	}
	if c.writeBlock != nil {
		<-c.writeBlock
	}
	return len(data), nil
}

type testPingAddr struct{}

func (testPingAddr) Network() string { return "test" }
func (testPingAddr) String() string  { return "test" }
