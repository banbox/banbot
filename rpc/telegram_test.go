package rpc

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
)

type telegramOrderManagerStub struct {
	orders []*OrderInfo
}

func (s *telegramOrderManagerStub) GetActiveOrders(string) ([]*OrderInfo, error) {
	return s.orders, nil
}

func (s *telegramOrderManagerStub) CloseOrder(string, int64) error { return nil }
func (s *telegramOrderManagerStub) CloseAllOrders(string) (int, int, error) {
	return 0, 0, nil
}
func (s *telegramOrderManagerStub) GetOrderStats(string) (int, int, error) { return 0, 0, nil }
func (s *telegramOrderManagerStub) DisableTrading(string, int) (int64, error) {
	return 0, nil
}
func (s *telegramOrderManagerStub) EnableTrading(string) error { return nil }

func newTelegramTestInstance(chatID int64) *Telegram {
	return &Telegram{
		chatId:        chatID,
		chanSend:      make(chan *bot.SendMessageParams, 4),
		activeAccount: "default",
	}
}

func telegramCallbackUpdate(chatID int64) *models.Update {
	return &models.Update{CallbackQuery: &models.CallbackQuery{
		ID:   "callback-1",
		From: models.User{ID: chatID},
		Data: "action:orders",
		Message: models.MaybeInaccessibleMessage{
			Type: models.MaybeInaccessibleMessageTypeMessage,
			Message: &models.Message{
				ID:   7,
				Chat: models.Chat{ID: chatID},
			},
		},
	}}
}

func TestTelegramSharedHandlersMatchCommandsAndCallbacks(t *testing.T) {
	tg := newTelegramTestInstance(42)
	tg.setupUpdateHandlers()

	command := &models.Update{Message: &models.Message{
		Text: "/orders",
		From: &models.User{ID: 42},
		Chat: models.Chat{ID: 42},
	}}
	if handler := tg.findUpdateHandler(command); handler == nil {
		t.Fatal("/orders handler was not registered")
	}
	if handler := tg.findUpdateHandler(telegramCallbackUpdate(42)); handler == nil {
		t.Fatal("callback handler was not registered")
	}
}

func TestTelegramCustomBotRoutesCommand(t *testing.T) {
	oldManager := orderManager
	orderManager = &telegramOrderManagerStub{}
	t.Cleanup(func() { orderManager = oldManager })

	client, err := bot.New("1:test", bot.WithSkipGetMe(), bot.WithNotAsyncHandlers())
	if err != nil {
		t.Fatalf("create telegram client: %v", err)
	}
	tg := newTelegramTestInstance(42)
	tg.setupUpdateHandlers()
	tg.bot = client
	tg.ctx, tg.cancel = context.WithCancel(context.Background())
	tg.cancel()
	tg.setupCommandHandlers()

	client.ProcessUpdate(context.Background(), &models.Update{Message: &models.Message{
		Text: "/orders",
		From: &models.User{ID: 42},
		Chat: models.Chat{ID: 42},
	}})

	select {
	case msg := <-tg.chanSend:
		if msg.ChatID != int64(42) {
			t.Fatalf("unexpected command response chat: %v", msg.ChatID)
		}
	default:
		t.Fatal("custom bot command was not routed")
	}
}

func TestTelegramCustomBotRefreshEditsOriginalMessage(t *testing.T) {
	oldManager := orderManager
	orderManager = &telegramOrderManagerStub{orders: []*OrderInfo{{
		ID:          11,
		Symbol:      "BTC/USDT:USDT",
		Price:       100,
		Amount:      2,
		Profit:      19.8,
		ProfitRate:  0.099,
		ProfitValid: true,
		EnterTag:    "test",
	}}}
	t.Cleanup(func() { orderManager = oldManager })

	type apiCall struct {
		method string
		form   map[string]string
	}
	var mu sync.Mutex
	var calls []apiCall
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		form := make(map[string]string)
		for key := range r.MultipartForm.Value {
			form[key] = r.FormValue(key)
		}
		mu.Lock()
		calls = append(calls, apiCall{method: r.URL.Path, form: form})
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/answerCallbackQuery") {
			fmt.Fprint(w, `{"ok":true,"result":true}`)
			return
		}
		fmt.Fprint(w, `{"ok":true,"result":{"message_id":7,"date":1,"chat":{"id":42,"type":"private"}}}`)
	}))
	defer server.Close()

	client, err := bot.New("1:test",
		bot.WithServerURL(server.URL),
		bot.WithSkipGetMe(),
		bot.WithNotAsyncHandlers(),
	)
	if err != nil {
		t.Fatalf("create telegram client: %v", err)
	}
	tg := newTelegramTestInstance(42)
	tg.setupUpdateHandlers()
	tg.bot = client
	tg.ctx, tg.cancel = context.WithCancel(context.Background())
	tg.cancel()
	tg.setupCommandHandlers()

	client.ProcessUpdate(context.Background(), telegramCallbackUpdate(42))

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 2 {
		t.Fatalf("expected answer and edit calls, got %d: %#v", len(calls), calls)
	}
	if !strings.HasSuffix(calls[0].method, "/answerCallbackQuery") {
		t.Fatalf("first call should answer callback, got %s", calls[0].method)
	}
	if !strings.HasSuffix(calls[1].method, "/editMessageText") {
		t.Fatalf("second call should edit message, got %s", calls[1].method)
	}
	if calls[1].form["chat_id"] != "42" || calls[1].form["message_id"] != "7" {
		t.Fatalf("edit target mismatch: %#v", calls[1].form)
	}
	if !strings.Contains(calls[1].form["text"], "+19.80 (+9.90%)") {
		t.Fatalf("edited orders do not contain calculated PnL: %s", calls[1].form["text"])
	}
}

func TestTelegramOfficialBotRefreshSendsUpdatedOrders(t *testing.T) {
	oldManager := orderManager
	orderManager = &telegramOrderManagerStub{orders: []*OrderInfo{{
		ID:          12,
		Symbol:      "ETH/USDT:USDT",
		Profit:      -3.5,
		ProfitRate:  -0.0125,
		ProfitValid: true,
	}}}
	t.Cleanup(func() { orderManager = oldManager })

	tg := newTelegramTestInstance(42)
	tg.setupUpdateHandlers()
	tg.routeUpdate(context.Background(), nil, telegramCallbackUpdate(42))

	select {
	case msg := <-tg.chanSend:
		if msg.ChatID != int64(42) {
			t.Fatalf("unexpected refresh chat: %v", msg.ChatID)
		}
		if msg.ParseMode != models.ParseModeHTML {
			t.Fatalf("interactive response lost HTML formatting: %q", msg.ParseMode)
		}
		if !strings.Contains(msg.Text, "-3.50 (-1.25%)") {
			t.Fatalf("refreshed orders do not contain calculated PnL: %s", msg.Text)
		}
	default:
		t.Fatal("official bot refresh did not enqueue a response")
	}
}

func TestTelegramUnchangedRefreshDoesNotDuplicateMessage(t *testing.T) {
	oldManager := orderManager
	orderManager = &telegramOrderManagerStub{}
	t.Cleanup(func() { orderManager = oldManager })

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/answerCallbackQuery") {
			fmt.Fprint(w, `{"ok":true,"result":true}`)
			return
		}
		fmt.Fprint(w, `{"ok":false,"error_code":400,"description":"Bad Request: message is not modified"}`)
	}))
	defer server.Close()

	client, err := bot.New("1:test", bot.WithServerURL(server.URL), bot.WithSkipGetMe())
	if err != nil {
		t.Fatalf("create telegram client: %v", err)
	}
	tg := newTelegramTestInstance(42)
	tg.handleCallbackQuery(context.Background(), client, telegramCallbackUpdate(42))

	select {
	case msg := <-tg.chanSend:
		t.Fatalf("unchanged refresh should not send a duplicate message: %#v", msg)
	default:
	}
}

func TestTelegramOrdersKeepPlaceholderWhenPriceUnavailable(t *testing.T) {
	oldManager := orderManager
	orderManager = &telegramOrderManagerStub{orders: []*OrderInfo{{ID: 13, Symbol: "SOL/USDT:USDT"}}}
	t.Cleanup(func() { orderManager = oldManager })

	text := newTelegramTestInstance(42).getOrdersList()
	if !strings.Contains(text, "Calculating...") && !strings.Contains(text, "计算中...") {
		t.Fatalf("missing unavailable PnL placeholder: %s", text)
	}
}

func TestTelegramQueuedNotificationsSendArbitraryContentAsPlainText(t *testing.T) {
	var gotText, gotParseMode string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		gotText = r.FormValue("text")
		gotParseMode = r.FormValue("parse_mode")
		w.Header().Set("Content-Type", "application/json")
		if gotParseMode == string(models.ParseModeHTML) {
			fmt.Fprint(w, `{"ok":false,"error_code":400,"description":"Bad Request: can't parse entities"}`)
			return
		}
		fmt.Fprint(w, `{"ok":true,"result":{"message_id":1,"date":1,"chat":{"id":42,"type":"private"}}}`)
	}))
	defer server.Close()

	client, err := bot.New("1:test", bot.WithServerURL(server.URL), bot.WithSkipGetMe())
	if err != nil {
		t.Fatalf("create telegram client: %v", err)
	}
	tg := newTelegramTestInstance(42)
	tg.bot = client
	tg.ctx = context.Background()
	contents := []map[string]string{
		{"content": `error: value is <nil> & price > stop`},
		{"content": `<b>tag-like text</b> and "quotes"`},
	}

	if remaining := makeDoSendMsgTelegram(tg)(contents); remaining != nil {
		t.Fatalf("queued notifications were rejected and retained for retry: %#v", remaining)
	}
	wantText := contents[0]["content"] + "\n\n---\n\n" + contents[1]["content"]
	if gotText != wantText {
		t.Fatalf("notification content changed:\n got: %q\nwant: %q", gotText, wantText)
	}
	if gotParseMode != "" {
		t.Fatalf("arbitrary notification content must not use parse mode, got %q", gotParseMode)
	}
}
