package base

import (
	"bytes"
	"context"
	"github.com/banbox/banbot/strat"
	"github.com/fasthttp/websocket"
	"github.com/gofiber/fiber/v2"
	"net"
	"testing"
	"time"

	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/orm"
)

func TestRuntimeSeriesCatalogDoesNotReadLegacyCatalog(t *testing.T) {
	first, second := data.NewDataSourceCatalog(), data.NewDataSourceCatalog()
	info := &orm.SeriesInfo{Name: "runtime_web_metric", TimeFrame: "1d", Binding: orm.SeriesBinding{Table: "runtime_web_metric", TimeColumn: "time", EndColumn: "end_ms", Fields: []orm.SeriesField{{Name: "value", Type: "float"}, {Name: "label", Type: "string"}}}}
	if err := first.RegisterFuncDataSource(info, func(context.Context, *strat.DataSub, int64, int64) ([]*orm.DataRecord, error) { return nil, nil }, nil); err != nil {
		t.Fatal(err)
	}
	got, _, err := resolveSeriesQueryInfoWithCatalog(first, info.Name, "1d", []string{"label"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Binding.Fields) != 1 || got.Binding.Fields[0].Name != "label" {
		t.Fatalf("fields = %#v", got.Binding.Fields)
	}
	if _, _, err := resolveSeriesQueryInfoWithCatalog(second, info.Name, "1d", nil); err == nil {
		t.Fatal("second runtime discovered first runtime's data source")
	}
	if len(info.Binding.Fields) != 2 {
		t.Fatal("query mutated catalog metadata")
	}
}

func TestWsHubSubscriptionsAreOwnedAndStopRejectsAdmission(t *testing.T) {
	first, second := NewWsHub(nil), NewWsHub(nil)
	a := &WsClient{hub: first, Subs: make(map[string]bool)}
	b := &WsClient{hub: second, Subs: make(map[string]bool)}
	first.setSubscription(a, true, "shared")
	second.setSubscription(b, true, "shared")
	a.Close(true)
	if len(first.subscriptions) != 0 || !second.subscriptions["shared"][b] {
		t.Fatal("closing one client affected another hub")
	}
	first.Close()
	first.setSubscription(a, true, "late")
	if len(first.subscriptions) != 0 {
		t.Fatal("closed hub admitted a new subscription")
	}
	first.Join()
	second.Close()
	second.Join()
}

func TestRuntimeWsHubPreservesFieldsAndJoinsConcurrentClose(t *testing.T) {
	symbols := orm.NewSymbolState()
	if err := symbols.SetExSymbols([]*orm.ExSymbol{{ID: 7, Exchange: "owned", Market: "spot", Symbol: "BTC/USDT"}}); err != nil {
		t.Fatal(err)
	}
	hub := NewWsHub(&data.RuntimeDeps{Symbols: symbols})
	app := fiber.New()
	RegApiWebsocketWithHub(app.Group("/api/ws"), hub)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverDone := make(chan struct{})
	go func() { defer close(serverDone); _ = app.Listener(listener) }()
	defer func() { hub.Close(); hub.Join(); _ = app.Shutdown(); <-serverDone }()
	conn, _, err := websocket.DefaultDialer.Dial("ws://"+listener.Addr().String()+"/api/ws/ohlcv", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.WriteJSON(map[string]any{"action": "subscribe", "exchange": "owned", "symbol": "BTC/USDT"}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		hub.mu.Lock()
		ready := len(hub.subscriptions["owned_spot_BTC/USDT"]) == 1
		hub.mu.Unlock()
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("subscription not admitted")
		}
		time.Sleep(time.Millisecond)
	}
	msg := &data.SeriesMsg{ExgName: "owned", Market: "spot", Pair: "BTC/USDT", NotifySeries: data.NotifySeries{TFSecs: 60, Rows: []*orm.DataSeries{{Sid: 7, TimeMS: 100, EndMS: 200, Values: map[string]any{"label": "custom", "nullable": nil, "count": int64(3)}}}}}
	hub.Publish(msg)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte(`"label":"custom"`)) || !bytes.Contains(raw, []byte(`"nullable":null`)) {
		t.Fatalf("custom fields lost: %s", raw)
	}
	published := make(chan struct{})
	go func() {
		defer close(published)
		for range 100 {
			hub.Publish(msg)
		}
	}()
	hub.Close()
	joined := make(chan struct{})
	go func() { hub.Join(); close(joined) }()
	select {
	case <-joined:
	case <-time.After(2 * time.Second):
		t.Fatal("hub close failed to join socket and writer")
	}
	<-published
}
