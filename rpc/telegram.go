package rpc

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	utils2 "github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/banbox/banexg/utils"
	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// Telegram 表示Telegram Bot推送渠道，实现了 IWebHook 接口的核心功能
// 其配置继承自 webHookItem，额外支持 token 和 chat_id 字段
// 示例配置（位于 rpc_channels.* 节点下）：
//
//   [rpc_channels.telegram_notice]
//   type = "telegram"                    # 渠道类型，对应 ChlType
//   token = "BOT_TOKEN"                  # 必填，Telegram Bot Token
//   chat_id = "CHAT_ID"                  # 必填，聊天ID（可以是用户ID或群组ID）
//   proxy = "http://127.0.0.1:7897"      # 可选，代理地址
//   msg_types = ["status", "exception"]
//   retry_delay = 30
//   min_intv_secs = 5
//
// 通过 SendMsg -> Queue -> doSendMsgs 的链路实现异步批量发送与失败重试
// 实际发送调用 Telegram Bot API 的 sendMessage 接口。

var (
	telegramInstances = make(map[string]*Telegram)
	telegramMutex     sync.RWMutex
	dashBot           *utils2.ClientIO // 通过官方机器人管理
	// 订单管理接口，由外部注入实现，避免循环依赖
	orderManager OrderManagerInterface
	// 钱包信息提供者，由外部注入，避免循环依赖
	walletProvider WalletInfoProvider
	reUUID4        = regexp.MustCompile(`^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-4[a-fA-F0-9]{3}-[89abAB][a-fA-F0-9]{3}-[a-fA-F0-9]{12}$`)
)

type BotSecret struct {
	Secret string `json:"secret"`
	Pid    int    `json:"pid"`
	Port   int    `json:"port"`
}

// OrderInfo 订单信息结构
type OrderInfo struct {
	ID          int64   `json:"id"`
	Symbol      string  `json:"symbol"`
	Short       bool    `json:"short"`
	Price       float64 `json:"price"`
	Amount      float64 `json:"amount"`
	Profit      float64 `json:"profit"`
	ProfitRate  float64 `json:"profit_rate"`
	ProfitValid bool    `json:"profit_valid"`
	EnterTag    string  `json:"enter_tag"`
	Account     string  `json:"account"`
}

// OrderManagerInterface 订单管理接口，避免循环依赖
type OrderManagerInterface interface {
	GetActiveOrders(account string) ([]*OrderInfo, error)
	CloseOrder(account string, orderID int64) error
	CloseAllOrders(account string) (int, int, error) // success count, failed count, error
	GetOrderStats(account string) (longCount, shortCount int, err error)
	DisableTrading(account string, hours int) (int64, error)
	EnableTrading(account string) error
}

// SetOrderManager 设置订单管理器（由外部调用）
func SetOrderManager(mgr OrderManagerInterface) {
	orderManager = mgr
}

// WalletInfoProvider 钱包信息提供者接口
type WalletInfoProvider interface {
	// 返回 单账户: 总额(法币), 可用(法币), 未实现盈亏(法币)
	GetSummary(account string) (totalLegal float64, availableLegal float64, unrealizedPOLLegal float64)
}

// SetWalletInfoProvider 设置钱包信息提供者（由外部调用）
func SetWalletInfoProvider(p WalletInfoProvider) {
	walletProvider = p
}

type Telegram struct {
	*WebHook
	token         string
	chatId        int64
	secret        string
	bot           *bot.Bot
	ctx           context.Context
	cancel        context.CancelFunc
	chanSend      chan *bot.SendMessageParams
	handlers      []updateHandler
	mu            sync.RWMutex
	activeAccount string // 当前激活的账户
}

// updateHandler 消息处理器
type updateHandler struct {
	handlerType handlerType
	matchType   matchType
	pattern     string
	handler     handlerFunc
	re          *regexp.Regexp
}

// handlerType 处理器类型
type handlerType int

const (
	handlerTypeMessageText handlerType = iota
	handlerTypeCallbackQueryData
)

const (
	separatorLine     = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	maxTelegramMsgLen = 4096
)

// matchType 匹配类型
type matchType int

const (
	matchTypeExact    matchType = iota // 精确匹配
	matchTypePrefix                    // 前缀匹配
	matchTypeContains                  // 包含匹配
	matchTypeCommand                   // 命令匹配（任意位置）
	matchTypeRegexp                    // 正则匹配
)

// handlerFunc 处理函数类型
type handlerFunc func(ctx context.Context, b *bot.Bot, update *models.Update)

// NewTelegram 构造函数，基于通用 WebHook 创建 Telegram 发送实例
func NewTelegram(name string, item map[string]interface{}) *Telegram {
	hook := NewWebHook(name, item)

	res := &Telegram{
		WebHook:       hook,
		token:         utils.GetMapVal(item, "token", ""),
		secret:        utils.GetMapVal(item, "secret", ""),
		chanSend:      make(chan *bot.SendMessageParams, 10),
		activeAccount: config.DefAcc, // 初始化为默认账户
	}
	if hook.Disable {
		return res
	}
	ctx, cancel := context.WithCancel(context.Background())
	res.ctx = ctx
	res.cancel = cancel
	res.doSendMsgs = makeDoSendMsgTelegram(res)
	res.setupUpdateHandlers()
	err := initCustomTgBot(res, name, item)
	if err != nil {
		if err.Code == errs.CodeParamRequired {
			err = initDashBot(res, name, item)
			if err != nil {
				log.Error("init telegram fail", zap.Error(err))
				return res
			}
		} else {
			panic(err)
		}
	}
	go res.loopSend()
	return res
}

func initDashBot(res *Telegram, name string, item map[string]interface{}) *errs.Error {
	if dashBot != nil {
		if secretV, ok := dashBot.GetData("secret"); ok {
			res.secret = secretV.(string)
		}
		return nil
	}
	sessionSecret, err2 := getSessionSecret(res.Proxy)
	if err2 != nil {
		return err2
	}
	// 创建ClientIO连接
	ioClient, err2 := utils2.NewClientIO("www.banbot.site:6788", sessionSecret)
	if err2 != nil {
		return err2
	}
	dashBot = ioClient
	if res.secret != "" {
		if !reUUID4.MatchString(res.secret) {
			log.Warn("rpc_channels." + name + ".secret must be uuid v4 format, reset to random")
			res.secret = ""
		}
	}
	if res.secret == "" {
		res.secret = uuid.New().String()
	}
	dashBot.SetData(res.secret, "secret")
	dashBot.ReInitConn = func() {
		sessionSecret, err2 = getSessionSecret(res.Proxy)
		if err2 != nil {
			log.Error("re-initDashBot fail, get sess secret fail", zap.Error(err2))
			return
		}
		ioClient.SetAesKey(sessionSecret)
		err2 = dashBot.WriteMsg(&utils2.IOMsg{
			Action:    "init",
			Data:      config.Name,
			NoEncrypt: true,
		})
		if err2 != nil {
			log.Error("re-initDashBot fail", zap.Error(err2))
		}
	}
	dashBot.Listens["getSecret"] = func(msg *utils2.IOMsgRaw) {
		info := BotSecret{
			Secret: res.secret,
			Pid:    os.Getpid(),
		}
		if config.APIServer != nil {
			info.Port = config.APIServer.Port
		}
		err2 = ioClient.WriteMsg(&utils2.IOMsg{
			Action: "onGetSecret",
			Data:   info,
		})
		if err2 != nil {
			log.Error("send onGetSecret fail", zap.Error(err2))
		} else {
			log.Info("Manage Your Bot On https://t.me/trade_banbot?start=" + res.secret)
		}
	}
	dashBot.Listens["telegram_cmd"] = func(msg *utils2.IOMsgRaw) {
		log.Warn("telegram_cmd", zap.String("str", string(msg.Data)))
		var update models.Update
		err := utils.Unmarshal(msg.Data, &update, utils.JsonNumDefault)
		if err != nil {
			log.Error("parse telegram_cmd fail", zap.Error(err))
			return
		}
		if update.Message == nil && update.CallbackQuery == nil {
			log.Warn("invalid tg msg", zap.String("str", string(msg.Data)))
			return
		}
		if update.Message != nil {
			res.chatId = update.Message.Chat.ID
		} else {
			chatID, _, _, _ := callbackMessageTarget(&update)
			if chatID != 0 {
				res.chatId = chatID
			} else {
				res.chatId = update.CallbackQuery.From.ID
			}
		}
		res.routeUpdate(res.ctx, nil, &update)
	}

	go func() {
		err := dashBot.RunForever()
		if err != nil {
			log.Warn("read client fail", zap.String("remote", dashBot.GetRemote()),
				zap.String("err", err.Message()))
		}
	}()
	return dashBot.WriteMsg(&utils2.IOMsg{
		Action:    "init",
		Data:      config.Name,
		NoEncrypt: true,
	})
}

func getSessionSecret(proxy string) (string, *errs.Error) {
	genUrl := "https://www.banbot.site/api/banconn/token"

	// 构建请求体
	reqBody := map[string]string{
		"name": config.Name,
	}
	reqData, err := utils.MarshalString(reqBody)
	if err != nil {
		return "", errs.New(errs.CodeRunTime, err)
	}

	// 发送POST请求
	client, req, err := prepareRequest("POST", genUrl, reqData, proxy)
	if err != nil {
		return "", errs.New(errs.CodeRunTime, err)
	}
	req.Header.Set("Content-Type", "application/json")

	rsp := utils2.DoHttp(client, req)
	if rsp.Error != nil {
		return "", rsp.Error
	}

	if rsp.Status != 200 {
		return "", errs.NewMsg(errs.CodeRunTime, "failed to get banconn token, status: %d", rsp.Status)
	}

	// 解析响应
	var result struct {
		Success bool   `json:"success"`
		Token   string `json:"token"`
		IP      string `json:"ip"`
		Name    string `json:"name"`
		Expires string `json:"expires"`
	}

	if err = utils.UnmarshalString(rsp.Content, &result, utils.JsonNumDefault); err != nil {
		return "", errs.New(errs.CodeRunTime, err)
	}

	if !result.Success {
		return "", errs.NewMsg(errs.CodeRunTime, "failed to get banconn token")
	}
	return result.Token, nil
}

func initCustomTgBot(res *Telegram, name string, item map[string]interface{}) *errs.Error {
	if res.token == "" {
		return errs.NewMsg(errs.CodeParamRequired, "token is required")
	}

	chatIdV, ok := item["chat_id"]
	if !ok {
		return errs.NewMsg(errs.CodeParamRequired, "%s.chat_id is required", name)
	}

	switch v := chatIdV.(type) {
	case int:
		res.chatId = int64(v)
	case int64:
		res.chatId = v
	case string:
		chatId, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return errs.NewMsg(errs.CodeParamInvalid, "%s.chat_id is invalid, must be a number: %v", name, err)
		}
		res.chatId = chatId
	default:
		return errs.NewMsg(errs.CodeParamInvalid, "%s.chat_id must be a number", name)
	}

	// 创建bot实例
	httpClient := createWebHookClient(res.Proxy)
	botInstance, err := bot.New(res.token, bot.WithHTTPClient(30*time.Second, httpClient))
	if err != nil {
		return errs.New(errs.CodeRunTime, err)
	}
	res.bot = botInstance
	res.setupCommandHandlers()

	// 注册到全局实例管理器
	telegramMutex.Lock()
	telegramInstances[name] = res
	telegramMutex.Unlock()

	return nil
}

// Close 关闭Telegram客户端
func (t *Telegram) Close() {
	if t.cancel != nil {
		t.cancel()
	}

	// 从全局实例管理器中移除
	telegramMutex.Lock()
	for name, instance := range telegramInstances {
		if instance == t {
			delete(telegramInstances, name)
			break
		}
	}
	telegramMutex.Unlock()
}

// registerHandler 注册消息处理器
func (t *Telegram) registerHandler(hType handlerType, pattern string, mType matchType, handler handlerFunc) {
	t.mu.Lock()
	defer t.mu.Unlock()

	h := updateHandler{
		handlerType: hType,
		matchType:   mType,
		pattern:     pattern,
		handler:     handler,
	}

	// 如果是正则匹配，编译正则表达式
	if mType == matchTypeRegexp {
		h.re = regexp.MustCompile(pattern)
	}

	t.handlers = append(t.handlers, h)
}

// matchUpdate 检查处理器是否匹配更新
func (h *updateHandler) matchUpdate(update *models.Update) bool {
	var data string
	var entities []models.MessageEntity

	// 根据处理器类型提取数据
	switch h.handlerType {
	case handlerTypeMessageText:
		if update.Message == nil {
			return false
		}
		data = update.Message.Text
		entities = update.Message.Entities
	case handlerTypeCallbackQueryData:
		if update.CallbackQuery == nil {
			return false
		}
		data = update.CallbackQuery.Data
	default:
		return false
	}

	// 根据匹配类型进行匹配
	switch h.matchType {
	case matchTypeExact:
		return data == h.pattern
	case matchTypePrefix:
		return strings.HasPrefix(data, h.pattern)
	case matchTypeContains:
		return strings.Contains(data, h.pattern)
	case matchTypeCommand:
		// 匹配命令实体（任意位置）
		for _, e := range entities {
			if e.Type == models.MessageEntityTypeBotCommand {
				cmd := data[e.Offset+1 : e.Offset+e.Length]
				if cmd == h.pattern {
					return true
				}
			}
		}
		return false
	case matchTypeRegexp:
		if h.re != nil {
			return h.re.MatchString(data)
		}
		return false
	}

	return false
}

// findUpdateHandler 查找匹配的处理器
func (t *Telegram) findUpdateHandler(update *models.Update) handlerFunc {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for _, h := range t.handlers {
		if h.matchUpdate(update) {
			return h.handler
		}
	}

	return nil
}

// setupUpdateHandlers registers handlers shared by the official and custom bots.
func (t *Telegram) setupUpdateHandlers() {
	// 注册命令处理器
	t.registerHandler(handlerTypeMessageText, "/orders", matchTypeExact, t.handleOrdersCommand)
	t.registerHandler(handlerTypeMessageText, "/close", matchTypePrefix, t.handleCloseCommand)
	t.registerHandler(handlerTypeMessageText, "/status", matchTypeExact, t.handleStatusCommand)
	t.registerHandler(handlerTypeMessageText, "/wallet", matchTypeExact, t.handleWalletCommand)
	t.registerHandler(handlerTypeMessageText, "/disable", matchTypePrefix, t.handleDisableCommand)
	t.registerHandler(handlerTypeMessageText, "/enable", matchTypeExact, t.handleEnableCommand)
	t.registerHandler(handlerTypeMessageText, "/account", matchTypeExact, t.handleAccountCommand)
	t.registerHandler(handlerTypeMessageText, "/switch", matchTypePrefix, t.handleSwitchCommand)
	t.registerHandler(handlerTypeMessageText, "/help", matchTypeExact, t.handleHelpCommand)
	t.registerHandler(handlerTypeMessageText, "/menu", matchTypeExact, t.handleMenuCommand)

	// 注册键盘按钮处理器
	viewOrders := config.GetLangMsg("view_orders", "📊 查看订单")
	tradingStatus := config.GetLangMsg("trading_status", "📈 开单状态")
	viewWallet := config.GetLangMsg("view_wallet", "👛 查看钱包")
	disableTrading := config.GetLangMsg("disable_trading", "🚫 禁止开单")
	enableTrading := config.GetLangMsg("enable_trading", "✅ 启用开单")
	closeAllOrders := config.GetLangMsg("close_all_orders", "❌ 平仓所有")
	switchAccount := config.GetLangMsg("switch_account", "🔄 切换账户")

	t.registerHandler(handlerTypeMessageText, viewOrders, matchTypeExact, t.handleOrdersCommand)
	t.registerHandler(handlerTypeMessageText, tradingStatus, matchTypeExact, t.handleStatusCommand)
	t.registerHandler(handlerTypeMessageText, viewWallet, matchTypeExact, t.handleWalletCommand)
	t.registerHandler(handlerTypeMessageText, disableTrading, matchTypeExact, t.handleDisableCommand)
	t.registerHandler(handlerTypeMessageText, enableTrading, matchTypeExact, t.handleEnableCommand)
	t.registerHandler(handlerTypeMessageText, closeAllOrders, matchTypeExact, t.handleCloseAllCommand)
	t.registerHandler(handlerTypeMessageText, switchAccount, matchTypeExact, t.handleAccountCommand)

	// 注册内联键盘回调处理器（前缀匹配所有回调）
	t.registerHandler(handlerTypeCallbackQueryData, "", matchTypePrefix, t.handleCallbackQuery)
}

func (t *Telegram) loopSend() {
	defer func() {
		log.Info("loopSend for telegram exit")
	}()
	for {
		select {
		case <-t.ctx.Done():
			return
		case msg := <-t.chanSend:
			err := t.send(msg)
			if err != nil {
				log.Error("send telegram msg fail", zap.Error(err))
			}
		}
	}
}

func (t *Telegram) send(msg *bot.SendMessageParams) error {
	// Telegram消息长度限制
	if len(msg.Text) > maxTelegramMsgLen {
		msg.Text = msg.Text[:maxTelegramMsgLen-3] + "..."
	}
	log.Info("telegram sending message", zap.String("text", msg.Text), zap.Int64("chat_id", t.chatId), zap.Bool("off", t.bot == nil))
	var err error
	if t.bot == nil {
		// 通过官方机器人发送
		err2 := dashBot.WriteMsg(&utils2.IOMsg{
			Action: "telegram",
			Data:   msg,
		})
		if err2 != nil {
			err = err2
		}
	} else {
		// 使用go-telegram/bot库发送消息
		_, err = t.bot.SendMessage(t.ctx, msg)
	}
	return err
}

// makeDoSendMsgTelegram 返回批量Telegram消息发送函数，符合 WebHook.doSendMsgs 的签名要求
func makeDoSendMsgTelegram(t *Telegram) func([]map[string]string) []map[string]string {
	return func(msgList []map[string]string) []map[string]string {
		if t.bot == nil && dashBot == nil {
			log.Debug("skip send telegram msg, no valid channel")
			return nil
		}
		var msgArr []string
		var charLen, offset int
		for i, msg := range msgList {
			content, _ := msg["content"]
			if content == "" {
				continue
			}
			if i > 0 && charLen+len(content) > maxTelegramMsgLen {
				break
			}
			msgArr = append(msgArr, content)
			charLen += len(content) + 7
			offset = i + 1
		}

		text := strings.Join(msgArr, "\n\n---\n\n")
		err := t.send(&bot.SendMessageParams{
			ChatID:    t.chatId,
			Text:      text,
			ParseMode: models.ParseModeHTML,
		})
		if err != nil {
			log.Error("telegram send msg fail", zap.String("text", text),
				zap.Int64("chat_id", t.chatId), zap.Error(err))
			return msgList
		}

		log.Debug("telegram send msg success", zap.Int("count", len(msgList)))
		if offset < len(msgList) {
			return msgList[offset:]
		}
		return nil
	}
}

// setupCommandHandlers 设置Telegram Bot命令处理器
func (t *Telegram) setupCommandHandlers() {
	t.bot.RegisterHandler(bot.HandlerTypeMessageText, "", bot.MatchTypePrefix, func(ctx context.Context, b *bot.Bot, update *models.Update) {
		t.routeUpdate(ctx, b, update)
	})
	t.bot.RegisterHandler(bot.HandlerTypeCallbackQueryData, "", bot.MatchTypePrefix, func(ctx context.Context, b *bot.Bot, update *models.Update) {
		t.routeUpdate(ctx, b, update)
	})
	// 启动Bot更新监听
	go func() {
		log.Info("Starting Telegram bot command listener", zap.Int64("chat_id", t.chatId))
		defer func() {
			if r := recover(); r != nil {
				log.Error("Telegram bot panic", zap.Any("panic", r))
			}
		}()
		t.bot.Start(t.ctx)
		log.Info("Telegram bot stopped")
	}()
}

func (t *Telegram) routeUpdate(ctx context.Context, b *bot.Bot, update *models.Update) {
	if update == nil {
		return
	}
	if update.Message == nil && update.CallbackQuery != nil {
		update.Message = update.CallbackQuery.Message.Message
	}
	handler := t.findUpdateHandler(update)
	if handler == nil {
		log.Warn("no handler for telegram update")
		return
	}
	handler(ctx, b, update)
}

// handleOrdersCommand 处理 /orders 命令 - 获取订单列表
func (t *Telegram) handleOrdersCommand(ctx context.Context, b *bot.Bot, update *models.Update) {
	if !t.isAuthorized(update) {
		return
	}

	response := t.getOrdersList()
	kb := t.buildOrdersInlineKeyboard()

	t.chanSend <- &bot.SendMessageParams{
		ChatID:      update.Message.Chat.ID,
		Text:        response,
		ParseMode:   models.ParseModeHTML,
		ReplyMarkup: kb,
	}
}

// handleCloseCommand 处理 /close 命令 - 强制平仓订单
func (t *Telegram) handleCloseCommand(ctx context.Context, b *bot.Bot, update *models.Update) {
	if !t.isAuthorized(update) {
		return
	}

	parts := strings.Fields(update.Message.Text)
	if len(parts) < 2 {
		response, err := config.ReadLangFile(config.ShowLangCode, "close_order_tip.txt")
		if err != nil {
			log.Error("read lang file fail: close_order_tip.txt", zap.Error(err))
			response = "/close [OrderID|all]"
		}
		t.sendResponse(update, response)
		return
	}

	orderID := parts[1]
	response := t.closeOrders(orderID)
	t.sendResponse(update, response)
}

// handleCloseAllCommand 处理 /closeall
func (t *Telegram) handleCloseAllCommand(ctx context.Context, b *bot.Bot, update *models.Update) {
	if !t.isAuthorized(update) {
		return
	}

	response := t.closeAllOrders()
	t.sendResponse(update, response)
}

// handleStatusCommand 处理 /status 命令 - 获取开单状态
func (t *Telegram) handleStatusCommand(ctx context.Context, b *bot.Bot, update *models.Update) {
	if !t.isAuthorized(update) {
		return
	}

	response := t.getTradingStatus()
	t.sendResponse(update, response)
}

// handleDisableCommand 处理 /disable 命令 - 禁止开单
func (t *Telegram) handleDisableCommand(ctx context.Context, b *bot.Bot, update *models.Update) {
	if !t.isAuthorized(update) {
		return
	}

	parts := strings.Fields(update.Message.Text)
	hours := 1 // 默认1小时

	if len(parts) >= 2 {
		if h, err := strconv.Atoi(parts[1]); err == nil && h > 0 && h <= 24 {
			hours = h
		}
	}

	response := t.disableTrading(hours)
	t.sendResponse(update, response)
}

// handleEnableCommand 处理 /enable 命令 - 启用开单
func (t *Telegram) handleEnableCommand(ctx context.Context, b *bot.Bot, update *models.Update) {
	if !t.isAuthorized(update) {
		return
	}

	response := t.enableTrading()
	t.sendResponse(update, response)
}

// handleHelpCommand 处理 /help 命令 - 显示帮助信息
func (t *Telegram) handleHelpCommand(ctx context.Context, b *bot.Bot, update *models.Update) {
	if !t.isAuthorized(update) {
		return
	}

	response, err := config.ReadLangFile(config.ShowLangCode, "telegram_help.txt")
	if err != nil {
		log.Error("read lang file fail: telegram_help.txt", zap.Error(err))
		response = "🤖 <b>BanBot Telegram Commands Help</b>"
	}

	t.sendResponse(update, response)
}

// handleMenuCommand 处理 /menu 命令 - 显示主菜单
func (t *Telegram) handleMenuCommand(ctx context.Context, b *bot.Bot, update *models.Update) {
	if !t.isAuthorized(update) {
		return
	}

	// 创建 Reply Keyboard（显示在键盘上）
	viewOrders := config.GetLangMsg("view_orders", "📊 查看订单")
	tradingStatus := config.GetLangMsg("trading_status", "📈 开单状态")
	viewWallet := config.GetLangMsg("view_wallet", "👛 查看钱包")
	closeAllOrders := config.GetLangMsg("close_all_orders", "❌ 平仓所有")
	disableTrading := config.GetLangMsg("disable_trading", "🚫 禁止开单")
	enableTrading := config.GetLangMsg("enable_trading", "✅ 启用开单")
	switchAccount := config.GetLangMsg("switch_account", "🔄 切换账户")

	kb := &models.ReplyKeyboardMarkup{
		Keyboard: [][]models.KeyboardButton{
			{
				{Text: viewOrders},
				{Text: tradingStatus},
			},
			{
				{Text: viewWallet},
				{Text: closeAllOrders},
			},
			{
				{Text: disableTrading},
				{Text: enableTrading},
			},
			{
				{Text: switchAccount},
			},
		},
		ResizeKeyboard:  true,
		OneTimeKeyboard: false,
	}

	menuText := `🎛️ <b>BanBot Menu</b>`
	t.chanSend <- &bot.SendMessageParams{
		ChatID:      update.Message.Chat.ID,
		Text:        menuText,
		ParseMode:   models.ParseModeHTML,
		ReplyMarkup: kb,
	}
}

// handleCallbackQuery 处理内联键盘回调
func (t *Telegram) handleCallbackQuery(ctx context.Context, b *bot.Bot, update *models.Update) {
	if update.CallbackQuery == nil {
		return
	}
	if !t.isAuthorized(update) {
		return
	}
	if b != nil {
		_, err := b.AnswerCallbackQuery(ctx, &bot.AnswerCallbackQueryParams{
			CallbackQueryID: update.CallbackQuery.ID,
		})
		if err != nil {
			log.Warn("answer telegram callback fail", zap.Error(err))
		}
	}
	data := update.CallbackQuery.Data
	if data != "action:orders" && update.Message == nil {
		log.Warn("telegram callback message is inaccessible", zap.String("data", data))
		return
	}

	// 处理不同的回调数据
	switch data {
	case "action:orders":
		t.handleOrdersCallback(ctx, b, update)
	case "action:status":
		t.handleStatusCommand(ctx, b, update)
	case "action:disable":
		t.handleDisableCommand(ctx, b, update)
	case "action:enable":
		t.handleEnableCommand(ctx, b, update)
	case "action:wallet":
		t.handleWalletCommand(ctx, b, update)
	case "action:close_all":
		t.handleCloseAllCommand(ctx, b, update)
	case "action:refresh":
		t.handleMenuCommand(ctx, b, update)
	case "action:account":
		t.handleAccountCommand(ctx, b, update)
	default:
		if strings.HasPrefix(data, "close:") {
			update.Message.Text = strings.ReplaceAll(data, ":", " ")
			t.handleCloseCommand(ctx, b, update)
		} else if strings.HasPrefix(data, "switch:") {
			// 处理账户切换回调
			account := strings.TrimPrefix(data, "switch:")
			t.switchAccount(account)
			response := config.GetLangMsg("account_switched", "✅ 已切换到账户: <code>%s</code>")
			t.chanSend <- &bot.SendMessageParams{
				ChatID:    update.Message.Chat.ID,
				Text:      fmt.Sprintf(response, account),
				ParseMode: models.ParseModeHTML,
			}
		}
	}
}

func (t *Telegram) handleOrdersCallback(ctx context.Context, b *bot.Bot, update *models.Update) {
	response := t.getOrdersList()
	kb := t.buildOrdersInlineKeyboard()
	chatID, messageID, inlineMessageID, ok := callbackMessageTarget(update)
	if b != nil && ok {
		params := &bot.EditMessageTextParams{
			InlineMessageID: inlineMessageID,
			Text:            response,
			ParseMode:       models.ParseModeHTML,
			ReplyMarkup:     kb,
		}
		if chatID != 0 {
			params.ChatID = chatID
			params.MessageID = messageID
		}
		_, err := b.EditMessageText(ctx, params)
		if err == nil || isTelegramMessageNotModified(err) {
			return
		}
		log.Warn("refresh telegram orders in place fail", zap.Error(err))
	}
	if chatID == 0 {
		log.Warn("refresh telegram orders fail: callback has no chat")
		return
	}
	t.chanSend <- &bot.SendMessageParams{
		ChatID:      chatID,
		Text:        response,
		ParseMode:   models.ParseModeHTML,
		ReplyMarkup: kb,
	}
}

func isTelegramMessageNotModified(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "message is not modified")
}

func callbackMessageTarget(update *models.Update) (int64, int, string, bool) {
	if update == nil || update.CallbackQuery == nil {
		return 0, 0, "", false
	}
	query := update.CallbackQuery
	if query.Message.Message != nil {
		return query.Message.Message.Chat.ID, query.Message.Message.ID, "", true
	}
	if query.Message.InaccessibleMessage != nil {
		msg := query.Message.InaccessibleMessage
		return msg.Chat.ID, msg.MessageID, "", true
	}
	if query.InlineMessageID != "" {
		return 0, 0, query.InlineMessageID, true
	}
	return 0, 0, "", false
}

// isAuthorized 检查用户是否有权限使用命令
func (t *Telegram) isAuthorized(update *models.Update) bool {
	if update == nil {
		return false
	}
	if update.CallbackQuery != nil {
		userID := update.CallbackQuery.From.ID
		chatID, _, _, _ := callbackMessageTarget(update)
		if chatID == t.chatId || userID == t.chatId {
			return true
		}
		log.Warn("Unauthorized telegram callback attempt",
			zap.Int64("user_id", userID),
			zap.Int64("chat_id", chatID),
			zap.Int64("authorized_chat_id", t.chatId))
		return false
	}
	if update.Message == nil || update.Message.From == nil {
		return false
	}

	// 检查是否是配置的chat_id
	userID := update.Message.From.ID
	chatID := update.Message.Chat.ID

	// 如果是私聊，检查用户ID；如果是群聊，检查群ID
	if chatID == t.chatId || userID == t.chatId {
		return true
	}

	log.Warn("Unauthorized telegram command attempt",
		zap.Int64("user_id", userID),
		zap.Int64("chat_id", chatID),
		zap.Int64("authorized_chat_id", t.chatId))
	return false
}

// sendResponse 发送响应消息
func (t *Telegram) sendResponse(update *models.Update, response string) {
	t.chanSend <- &bot.SendMessageParams{
		ChatID:    update.Message.Chat.ID,
		Text:      response,
		ParseMode: models.ParseModeHTML,
	}
}

// getOrdersList 获取订单列表
func (t *Telegram) getOrdersList() string {
	var response strings.Builder
	title := config.GetLangMsg("current_orders_title", "📊 当前订单列表")
	response.WriteString(title + "\n")
	response.WriteString(separatorLine + "\n\n")

	// 显示当前激活账户
	activeAccountLabel := config.GetLangMsg("active_account_label", "🎯 当前账户:")
	response.WriteString(fmt.Sprintf("<b>%s</b> <code>%s</code>\n\n", activeAccountLabel, t.activeAccount))

	if orderManager == nil {
		notInitialized := config.GetLangMsg("order_manager_not_initialized", "❌ 订单管理器未初始化")
		response.WriteString(notInitialized + "\n")
		response.WriteString(separatorLine)
		return response.String()
	}

	// 提前获取所有语言标签，避免循环中重复获取
	directionLong := config.GetLangMsg("direction_long", "📈 多")
	directionShort := config.GetLangMsg("direction_short", "📉 空")
	priceLabel := config.GetLangMsg("price_label", "💰 价格:")
	quantityLabel := config.GetLangMsg("quantity_label", "数量:")
	pnlLabel := config.GetLangMsg("pnl_label", "📊 盈亏:")
	tagLabel := config.GetLangMsg("tag_label", "标签:")
	calculating := config.GetLangMsg("calculating", "计算中...")

	// 只查询当前激活账户的订单
	orders, err := orderManager.GetActiveOrders(t.activeAccount)
	if err != nil {
		log.Error("Failed to get orders", zap.String("account", t.activeAccount), zap.Error(err))
		errorMsg := config.GetLangMsg("get_orders_failed", "❌ 获取订单失败:")
		response.WriteString(fmt.Sprintf("%s %s\n", errorMsg, err.Error()))
		response.WriteString(separatorLine)
		return response.String()
	}

	if len(orders) == 0 {
		noActiveOrders := config.GetLangMsg("no_active_orders", "暂无活跃订单")
		response.WriteString(noActiveOrders + "\n")
	} else {
		for _, order := range orders {
			direction := directionLong
			if order.Short {
				direction = directionShort
			}

			pnl := calculating
			if order.ProfitValid {
				pnl = fmt.Sprintf("%+.2f (%+.2f%%)", order.Profit, order.ProfitRate*100)
			}
			response.WriteString(fmt.Sprintf(
				"• <code>%d</code> %s <code>%s</code>\n"+
					"  %s <code>%.5f</code> | %s <code>%.4f</code>\n"+
					"  %s <code>%s</code> | %s <code>%s</code>\n\n",
				order.ID,
				direction,
				order.Symbol,
				priceLabel, order.Price, quantityLabel, order.Amount,
				pnlLabel, pnl, tagLabel, order.EnterTag,
			))
		}

		totalLabel := config.GetLangMsg("total_label", "总计")
		activeOrdersCount := config.GetLangMsg("active_orders_count", "个活跃订单")
		response.WriteString(fmt.Sprintf("%s: <b>%d</b> %s", totalLabel, len(orders), activeOrdersCount))
	}

	response.WriteString("\n" + separatorLine)

	return response.String()
}

// buildOrdersInlineKeyboard 构建订单列表对应的内联键盘（每单平仓 + 批量操作）
func (t *Telegram) buildOrdersInlineKeyboard() *models.InlineKeyboardMarkup {
	var rows = make([][]models.InlineKeyboardButton, 0)
	if orderManager != nil {
		// 使用当前激活账户
		orders, err := orderManager.GetActiveOrders(t.activeAccount)
		if err == nil && len(orders) > 0 {
			closePositionFormat := config.GetLangMsg("close_position_format", "❌ 平仓 %d")
			for _, od := range orders {
				rows = append(rows, []models.InlineKeyboardButton{{
					Text:         fmt.Sprintf(closePositionFormat, od.ID),
					CallbackData: fmt.Sprintf("close:%d", od.ID),
				}})
			}
			closeAllOrdersBtn := config.GetLangMsg("close_all_orders_button", "❌ 平仓所有订单")
			refreshOrdersBtn := config.GetLangMsg("refresh_orders", "🔄 刷新订单")
			rows = append(rows, []models.InlineKeyboardButton{
				{Text: closeAllOrdersBtn, CallbackData: "action:close_all"},
				{Text: refreshOrdersBtn, CallbackData: "action:orders"},
			})
		} else {
			refreshOrdersBtn := config.GetLangMsg("refresh_orders", "🔄 刷新订单")
			rows = append(rows, []models.InlineKeyboardButton{
				{Text: refreshOrdersBtn, CallbackData: "action:orders"},
			})
		}
	}
	return &models.InlineKeyboardMarkup{InlineKeyboard: rows}
}

// closeOrders 平仓订单
func (t *Telegram) closeOrders(orderID string) string {
	if orderID == "all" {
		return t.closeAllOrders()
	}

	if orderManager == nil {
		errorLabel := config.GetLangMsg("error_label", "❌ 错误")
		notInitialized := config.GetLangMsg("order_manager_not_initialized", "订单管理器未初始化")
		return fmt.Sprintf("%s: %s", errorLabel, notInitialized)
	}

	// 解析订单ID
	id, err := strconv.ParseInt(orderID, 10, 64)
	if err != nil {
		errorLabel := config.GetLangMsg("error_label", "❌ 错误")
		invalidOrderID := config.GetLangMsg("invalid_order_id", "无效的订单ID")
		return fmt.Sprintf("%s: %s", errorLabel, invalidOrderID)
	}

	// 使用当前激活账户平仓
	err = orderManager.CloseOrder(t.activeAccount, id)
	if err == nil {
		closeSuccessTitle := config.GetLangMsg("close_success_title", "✅ 平仓成功")
		orderIDLabel := config.GetLangMsg("order_id_label", "📊 订单ID:")
		accountTarget := config.GetLangMsg("account_target", "🎯 账户:")
		timeLabel := config.GetLangMsg("time_label", "⏰ 时间:")
		closeRequestSubmitted := config.GetLangMsg("close_request_submitted", "已提交平仓请求，请等待执行完成。")
		return fmt.Sprintf("%s\n\n%s <code>%d</code>\n%s <code>%s</code>\n%s %s\n\n%s",
			closeSuccessTitle, orderIDLabel, id, accountTarget, t.activeAccount,
			timeLabel, time.Now().Format("15:04:05"), closeRequestSubmitted)
	}

	orderNotFoundTitle := config.GetLangMsg("order_not_found_title", "❌ 订单未找到")
	orderIDLabel := config.GetLangMsg("order_id_label", "📊 订单ID:")
	timeLabel := config.GetLangMsg("time_label", "⏰ 时间:")
	checkOrderIDTip := config.GetLangMsg("check_order_id_tip", "请检查订单ID是否正确，或使用 <code>/orders</code> 查看当前活跃订单。")
	return fmt.Sprintf("%s\n\n%s <code>%d</code>\n%s %s\n\n%s",
		orderNotFoundTitle, orderIDLabel, id, timeLabel, time.Now().Format("15:04:05"), checkOrderIDTip)
}

// closeAllOrders 平仓所有订单
func (t *Telegram) closeAllOrders() string {
	var response strings.Builder
	batchCloseResultTitle := config.GetLangMsg("batch_close_result_title", "🔄 批量平仓结果")
	response.WriteString(batchCloseResultTitle + "\n")
	response.WriteString(separatorLine + "\n\n")

	// 显示当前激活账户
	activeAccountLabel := config.GetLangMsg("active_account_label", "🎯 当前账户:")
	response.WriteString(fmt.Sprintf("<b>%s</b> <code>%s</code>\n\n", activeAccountLabel, t.activeAccount))

	if orderManager == nil {
		notInitialized := config.GetLangMsg("order_manager_not_initialized", "❌ 订单管理器未初始化")
		response.WriteString(notInitialized + "\n")
		response.WriteString(separatorLine)
		return response.String()
	}

	getOrdersFailed := config.GetLangMsg("get_orders_failed", "❌ 获取订单失败:")
	successLabel := config.GetLangMsg("success_label", "✅ 成功:")
	failedLabel := config.GetLangMsg("failed_label", "❌ 失败:")

	// 只平仓当前激活账户的订单
	successCount, failedCount, err := orderManager.CloseAllOrders(t.activeAccount)
	if err != nil {
		response.WriteString(fmt.Sprintf("%s %s\n", getOrdersFailed, err.Error()))
		response.WriteString(separatorLine)
		return response.String()
	}

	response.WriteString(fmt.Sprintf("%s %d | %s %d\n", successLabel, successCount, failedLabel, failedCount))
	response.WriteString(separatorLine)

	return response.String()
}

// getTradingStatus 获取交易状态
func (t *Telegram) getTradingStatus() string {
	var response strings.Builder
	tradingStatusTitle := config.GetLangMsg("trading_status_title", "📊 交易状态")
	response.WriteString(tradingStatusTitle + "\n")
	response.WriteString(separatorLine + "\n\n")

	// 显示当前激活账户
	activeAccountLabel := config.GetLangMsg("active_account_label", "🎯 当前账户:")
	response.WriteString(fmt.Sprintf("<b>%s</b> <code>%s</code>\n\n", activeAccountLabel, t.activeAccount))

	nowMS := btime.TimeMS()

	// 提前获取所有语言标签
	statusLabel := config.GetLangMsg("status_label", "状态:")
	tradingDisabledStatus := config.GetLangMsg("trading_disabled_status", "开单已禁用")
	tradingNormalStatus := config.GetLangMsg("trading_normal_status", "开单正常")
	remainingLabel := config.GetLangMsg("remaining_label", "剩余:")
	hoursFormat := config.GetLangMsg("hours_format", "%d小时%d分钟")
	minutesFormat := config.GetLangMsg("minutes_format", "%d分钟")
	longOrderLabel := config.GetLangMsg("long_order_label", "多单:")
	shortOrderLabel := config.GetLangMsg("short_order_label", "空单:")

	// 检查当前激活账户是否被禁用
	if untilMS, exists := core.NoEnterUntil[t.activeAccount]; exists && nowMS < untilMS {
		remainingMS := untilMS - nowMS
		remaining := time.Duration(remainingMS) * time.Millisecond
		response.WriteString(fmt.Sprintf("🚫 <b>%s</b> %s\n", statusLabel, tradingDisabledStatus))
		// Inline format duration
		hours := int(remaining.Hours())
		minutes := int(remaining.Minutes()) % 60
		var durationStr string
		if hours > 0 {
			durationStr = fmt.Sprintf(hoursFormat, hours, minutes)
		} else {
			durationStr = fmt.Sprintf(minutesFormat, minutes)
		}
		response.WriteString(fmt.Sprintf("⏰ <b>%s</b> %s\n", remainingLabel, durationStr))
	} else {
		response.WriteString(fmt.Sprintf("✅ <b>%s</b> %s\n", statusLabel, tradingNormalStatus))
	}

	// 获取当前账户的订单数量
	if orderManager != nil {
		longCount, shortCount, err := orderManager.GetOrderStats(t.activeAccount)
		if err == nil {
			response.WriteString(fmt.Sprintf("📈 <b>%s</b> %d | 📉 <b>%s</b> %d\n", longOrderLabel, longCount, shortOrderLabel, shortCount))
		}
	}

	response.WriteString("\n" + separatorLine)

	return response.String()
}

// disableTrading 禁用交易
func (t *Telegram) disableTrading(hours int) string {
	if orderManager == nil {
		errorLabel := config.GetLangMsg("error_label", "❌ 错误")
		notInitialized := config.GetLangMsg("order_manager_not_initialized", "订单管理器未初始化")
		return fmt.Sprintf("%s: %s", errorLabel, notInitialized)
	}
	untilMS, err := orderManager.DisableTrading(t.activeAccount, hours)
	if err != nil {
		errorLabel := config.GetLangMsg("error_label", "❌ 错误")
		return fmt.Sprintf("%s: %s", errorLabel, err.Error())
	}

	format := config.GetLangMsg("trading_disabled_format", "🚫 <b>开单已禁用</b>\n\n🎯 <b>账户:</b> <code>%s</code>\n⏰ <b>禁用时长:</b> %d 小时\n📅 <b>恢复时间:</b> %s\n\n使用 <code>/enable</code> 可提前恢复开单")
	disabledUntil := time.Unix(untilMS/1000, (untilMS%1000)*1000000)
	return fmt.Sprintf(format, t.activeAccount, hours, disabledUntil.Format("2006-01-02 15:04:05"))
}

// enableTrading 启用交易
func (t *Telegram) enableTrading() string {
	if orderManager == nil {
		errorLabel := config.GetLangMsg("error_label", "❌ 错误")
		notInitialized := config.GetLangMsg("order_manager_not_initialized", "订单管理器未初始化")
		return fmt.Sprintf("%s: %s", errorLabel, notInitialized)
	}
	if err := orderManager.EnableTrading(t.activeAccount); err != nil {
		errorLabel := config.GetLangMsg("error_label", "❌ 错误")
		return fmt.Sprintf("%s: %s", errorLabel, err.Error())
	}

	format := config.GetLangMsg("trading_enabled_message", "✅ <b>开单已恢复</b>\n\n🎯 <b>账户:</b> <code>%s</code>\n\n该账户的交易功能已重新启用")
	return fmt.Sprintf(format, t.activeAccount)
}

// IsTradingDisabled 检查指定账户是否被禁用交易（供外部调用）
func (t *Telegram) IsTradingDisabled(account string) bool {
	if untilMS, exists := core.NoEnterUntil[account]; exists {
		return btime.TimeMS() < untilMS
	}
	return false
}

// handleWalletCommand 处理 /wallet 命令 - 显示钱包信息
func (t *Telegram) handleWalletCommand(ctx context.Context, b *bot.Bot, update *models.Update) {
	if !t.isAuthorized(update) {
		return
	}

	response := t.getWalletSummary()
	t.sendResponse(update, response)
}

// getWalletSummary 获取钱包汇总信息
func (t *Telegram) getWalletSummary() string {
	var bld strings.Builder
	walletTitle := config.GetLangMsg("wallet_summary_title", "👛 钱包汇总")
	bld.WriteString(fmt.Sprintf("<b>%s</b>\n", walletTitle))
	bld.WriteString(separatorLine + "\n\n")

	// 显示当前激活账户
	activeAccountLabel := config.GetLangMsg("active_account_label", "🎯 当前账户:")
	bld.WriteString(fmt.Sprintf("<b>%s</b> <code>%s</code>\n\n", activeAccountLabel, t.activeAccount))

	// 提前获取所有语言标签
	totalAmount := config.GetLangMsg("total_amount", "💼 总额:")
	availableAmount := config.GetLangMsg("available_amount", "💰 可用:")
	unrealizedPnl := config.GetLangMsg("unrealized_pnl", "📊 未实现盈亏:")
	notInitialized := config.GetLangMsg("wallet_provider_not_initialized_full", "❌ 钱包提供者未初始化")

	// 只查询当前激活账户的钱包信息
	var total, ava, upol float64
	if walletProvider != nil {
		total, ava, upol = walletProvider.GetSummary(t.activeAccount)
		bld.WriteString(fmt.Sprintf("<b>%s</b> <code>%.2f</code>\n", totalAmount, total))
		bld.WriteString(fmt.Sprintf("<b>%s</b> <code>%.2f</code>\n", availableAmount, ava))
		bld.WriteString(fmt.Sprintf("<b>%s</b> <code>%.2f</code>\n", unrealizedPnl, upol))
	} else {
		bld.WriteString(fmt.Sprintf("%s\n", notInitialized))
	}

	bld.WriteString(separatorLine)

	return bld.String()
}

// handleAccountCommand 处理 /account 命令 - 显示账户列表和切换选项
func (t *Telegram) handleAccountCommand(ctx context.Context, b *bot.Bot, update *models.Update) {
	if !t.isAuthorized(update) {
		return
	}

	response := t.getAccountList()
	kb := t.buildAccountInlineKeyboard()

	t.chanSend <- &bot.SendMessageParams{
		ChatID:      update.Message.Chat.ID,
		Text:        response,
		ParseMode:   models.ParseModeHTML,
		ReplyMarkup: kb,
	}
}

// handleSwitchCommand 处理 /switch <account> 命令 - 切换账户
func (t *Telegram) handleSwitchCommand(ctx context.Context, b *bot.Bot, update *models.Update) {
	if !t.isAuthorized(update) {
		return
	}

	parts := strings.Fields(update.Message.Text)
	if len(parts) < 2 {
		response := config.GetLangMsg("switch_usage", "用法: <code>/switch [账户名]</code>\n\n使用 <code>/account</code> 查看所有可用账户")
		t.sendResponse(update, response)
		return
	}

	account := parts[1]
	if _, exists := config.Accounts[account]; !exists {
		response := config.GetLangMsg("account_not_found", "❌ 账户 <code>%s</code> 不存在\n\n使用 <code>/account</code> 查看所有可用账户")
		t.sendResponse(update, fmt.Sprintf(response, account))
		return
	}

	t.switchAccount(account)
	response := config.GetLangMsg("account_switched", "✅ 已切换到账户: <code>%s</code>")
	t.sendResponse(update, fmt.Sprintf(response, account))
}

// getAccountList 获取账户列表
func (t *Telegram) getAccountList() string {
	var response strings.Builder
	title := config.GetLangMsg("account_list_title", "📋 账户列表")
	response.WriteString(title + "\n")
	response.WriteString(separatorLine + "\n\n")

	activeAccountLabel := config.GetLangMsg("active_account_label", "🎯 当前账户:")
	response.WriteString(fmt.Sprintf("<b>%s</b> <code>%s</code>\n\n", activeAccountLabel, t.activeAccount))

	availableAccountsLabel := config.GetLangMsg("available_accounts_label", "可用账户:")
	response.WriteString(fmt.Sprintf("<b>%s</b>\n", availableAccountsLabel))

	for account, cfg := range config.Accounts {
		if cfg.NoTrade {
			continue
		}
		if account == t.activeAccount {
			response.WriteString(fmt.Sprintf("  ✅ <code>%s</code> (当前)\n", account))
		} else {
			response.WriteString(fmt.Sprintf("  • <code>%s</code>\n", account))
		}
	}

	response.WriteString("\n" + separatorLine)
	return response.String()
}

// buildAccountInlineKeyboard 构建账户切换的内联键盘
func (t *Telegram) buildAccountInlineKeyboard() *models.InlineKeyboardMarkup {
	var rows = make([][]models.InlineKeyboardButton, 0)

	for account, cfg := range config.Accounts {
		if cfg.NoTrade {
			continue
		}
		if account != t.activeAccount {
			rows = append(rows, []models.InlineKeyboardButton{{
				Text:         fmt.Sprintf("🔄 切换到 %s", account),
				CallbackData: fmt.Sprintf("switch:%s", account),
			}})
		}
	}

	if len(rows) > 0 {
		refreshBtn := config.GetLangMsg("refresh_accounts", "🔄 刷新")
		rows = append(rows, []models.InlineKeyboardButton{
			{Text: refreshBtn, CallbackData: "action:account"},
		})
	}

	return &models.InlineKeyboardMarkup{InlineKeyboard: rows}
}

// switchAccount 切换当前激活账户
func (t *Telegram) switchAccount(account string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := config.Accounts[account]; exists {
		t.activeAccount = account
		log.Info("Telegram bot switched account", zap.String("account", account))
	}
}
