package rpc

import (
	"context"
	"fmt"
	utils2 "github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"
	"github.com/google/uuid"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/log"
	"github.com/banbox/banexg/utils"
	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
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

// OrderInfo 订单信息结构
type OrderInfo struct {
	ID       int64   `json:"id"`
	Symbol   string  `json:"symbol"`
	Short    bool    `json:"short"`
	Price    float64 `json:"price"`
	Amount   float64 `json:"amount"`
	EnterTag string  `json:"enter_tag"`
	Account  string  `json:"account"`
}

// OrderManagerInterface 订单管理接口，避免循环依赖
type OrderManagerInterface interface {
	GetActiveOrders(account string) ([]*OrderInfo, error)
	CloseOrder(account string, orderID int64) error
	CloseAllOrders(account string) (int, int, error) // success count, failed count, error
	GetOrderStats(account string) (longCount, shortCount int, err error)
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
	token    string
	chatId   int64
	secret   string
	bot      *bot.Bot
	ctx      context.Context
	cancel   context.CancelFunc
	chanSend chan *bot.SendMessageParams
}

// NewTelegram 构造函数，基于通用 WebHook 创建 Telegram 发送实例
func NewTelegram(name string, item map[string]interface{}) *Telegram {
	hook := NewWebHook(name, item)

	res := &Telegram{
		WebHook: hook,
		token:   utils.GetMapVal(item, "token", ""),
		secret:  utils.GetMapVal(item, "secret", ""),
	}
	if hook.Disable {
		return res
	}
	ctx, cancel := context.WithCancel(context.Background())
	res.ctx = ctx
	res.cancel = cancel
	res.doSendMsgs = makeDoSendMsgTelegram(res)
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
		err2 = ioClient.WriteMsg(&utils2.IOMsg{
			Action: "onGetSecret",
			Data:   res.secret,
		})
		if err2 != nil {
			log.Error("send onGetSecret fail", zap.Error(err2))
		}
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

	if chatIdV, ok := item["chat_id"]; ok {
		if chatIdInt, ok := chatIdV.(int); ok {
			res.chatId = int64(chatIdInt)
		} else if chatIdInt64, ok := chatIdV.(int64); ok {
			res.chatId = chatIdInt64
		} else if chatIdStr, ok := chatIdV.(string); ok {
			chatId, err := strconv.ParseInt(chatIdStr, 10, 64)
			if err != nil {
				return errs.NewMsg(errs.CodeParamInvalid, "%s.chat_id is invalid, must be a number: %v", name, err)
			}
			res.chatId = chatId
		} else {
			return errs.NewMsg(errs.CodeParamInvalid, "%s.chat_id must be a number", name)
		}
	} else {
		return errs.NewMsg(errs.CodeParamRequired, "%s.chat_id is required", name)
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

func (t *Telegram) loopSend() {
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
	// Telegram消息长度限制为4096字符
	if len(msg.Text) > 4096 {
		msg.Text = msg.Text[:4093] + "..."
	}
	log.Debug("telegram sending message", zap.String("text", msg.Text), zap.Int64("chat_id", t.chatId))
	var err error
	if t.bot == nil {
		// 通过官方机器人发送
		var msgStr string
		msgStr, err = utils.MarshalString(msg)
		if err != nil {
			return err
		}
		err2 := dashBot.WriteMsg(&utils2.IOMsg{
			Action: "telegram",
			Data:   msgStr,
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
			if i > 0 && charLen+len(content) > 4096 {
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
	// 注册命令处理器
	t.bot.RegisterHandler(bot.HandlerTypeMessageText, "/orders", bot.MatchTypeExact, t.handleOrdersCommand)
	t.bot.RegisterHandler(bot.HandlerTypeMessageText, "/close", bot.MatchTypePrefix, t.handleCloseCommand)
	t.bot.RegisterHandler(bot.HandlerTypeMessageText, "/status", bot.MatchTypeExact, t.handleStatusCommand)
	// 钱包
	t.bot.RegisterHandler(bot.HandlerTypeMessageText, "/wallet", bot.MatchTypeExact, t.handleWalletCommand)
	t.bot.RegisterHandler(bot.HandlerTypeMessageText, "/disable", bot.MatchTypePrefix, t.handleDisableCommand)
	t.bot.RegisterHandler(bot.HandlerTypeMessageText, "/enable", bot.MatchTypeExact, t.handleEnableCommand)
	t.bot.RegisterHandler(bot.HandlerTypeMessageText, "/help", bot.MatchTypeExact, t.handleHelpCommand)
	t.bot.RegisterHandler(bot.HandlerTypeMessageText, "/menu", bot.MatchTypeExact, t.handleMenuCommand)

	// 注册键盘按钮处理器
	viewOrders := config.GetLangMsg("view_orders", "📊 查看订单")
	tradingStatus := config.GetLangMsg("trading_status", "📈 开单状态")
	viewWallet := config.GetLangMsg("view_wallet", "👛 查看钱包")
	disableTrading := config.GetLangMsg("disable_trading", "🚫 禁止开单")
	enableTrading := config.GetLangMsg("enable_trading", "✅ 启用开单")
	closeAllOrders := config.GetLangMsg("close_all_orders", "❌ 平仓所有")
	refreshMenu := config.GetLangMsg("refresh_menu", "🔄 刷新菜单")
	hideMenu := config.GetLangMsg("hide_menu", "❌ 隐藏菜单")

	t.bot.RegisterHandler(bot.HandlerTypeMessageText, viewOrders, bot.MatchTypeExact, t.handleKeyboardOrdersCommand)
	t.bot.RegisterHandler(bot.HandlerTypeMessageText, tradingStatus, bot.MatchTypeExact, t.handleKeyboardStatusCommand)
	t.bot.RegisterHandler(bot.HandlerTypeMessageText, viewWallet, bot.MatchTypeExact, t.handleKeyboardWalletCommand)
	t.bot.RegisterHandler(bot.HandlerTypeMessageText, disableTrading, bot.MatchTypeExact, t.handleKeyboardDisableCommand)
	t.bot.RegisterHandler(bot.HandlerTypeMessageText, enableTrading, bot.MatchTypeExact, t.handleKeyboardEnableCommand)
	t.bot.RegisterHandler(bot.HandlerTypeMessageText, closeAllOrders, bot.MatchTypeExact, t.handleKeyboardCloseAllCommand)
	t.bot.RegisterHandler(bot.HandlerTypeMessageText, refreshMenu, bot.MatchTypeExact, t.handleMenuCommand)
	t.bot.RegisterHandler(bot.HandlerTypeMessageText, hideMenu, bot.MatchTypeExact, t.handleHideMenuCommand)

	// 注册内联键盘回调处理器
	t.bot.RegisterHandler(bot.HandlerTypeCallbackQueryData, "", bot.MatchTypePrefix, t.handleCallbackQuery)

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
		t.sendResponse(b, update, response)
		return
	}

	orderID := parts[1]
	response := t.closeOrders(orderID)
	t.sendResponse(b, update, response)
}

// handleStatusCommand 处理 /status 命令 - 获取开单状态
func (t *Telegram) handleStatusCommand(ctx context.Context, b *bot.Bot, update *models.Update) {
	if !t.isAuthorized(update) {
		return
	}

	response := t.getTradingStatus()
	t.sendResponse(b, update, response)
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
	t.sendResponse(b, update, response)
}

// handleEnableCommand 处理 /enable 命令 - 启用开单
func (t *Telegram) handleEnableCommand(ctx context.Context, b *bot.Bot, update *models.Update) {
	if !t.isAuthorized(update) {
		return
	}

	response := t.enableTrading()
	t.sendResponse(b, update, response)
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

	t.sendResponse(b, update, response)
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
	refreshMenu := config.GetLangMsg("refresh_menu", "🔄 刷新菜单")
	hideMenu := config.GetLangMsg("hide_menu", "❌ 隐藏菜单")

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
				{Text: refreshMenu},
			},
			{
				{Text: hideMenu},
			},
		},
		ResizeKeyboard:  true,
		OneTimeKeyboard: false,
	}

	menuText, err := config.ReadLangFile(config.ShowLangCode, "telegram_menu.txt")
	if err != nil {
		log.Error("read lang file fail: telegram_menu.txt", zap.Error(err))
		// 使用默认菜单文本
		menuText = `🎛️ <b>BanBot Menu</b>`
	}

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

	if !t.isAuthorizedCallback(update) {
		return
	}

	data := update.CallbackQuery.Data

	// 先回应回调查询
	processing := config.GetLangMsg("processing", "处理中...")
	_, err := b.AnswerCallbackQuery(ctx, &bot.AnswerCallbackQueryParams{
		CallbackQueryID: update.CallbackQuery.ID,
		Text:            processing,
	})
	if err != nil {
		log.Error("Failed to answer callback query", zap.Error(err))
	}

	// 处理不同的回调数据
	switch data {
	case "action:orders":
		t.handleOrdersCallback(ctx, b, update)
	case "action:status":
		t.handleStatusCallback(ctx, b, update)
	case "action:disable":
		t.handleDisableCallback(ctx, b, update)
	case "action:enable":
		t.handleEnableCallback(ctx, b, update)
	case "action:wallet":
		t.handleWalletCallback(ctx, b, update)
	case "action:close_all":
		t.handleCloseAllCallback(ctx, b, update)
	case "action:refresh":
		t.handleMenuCallback(ctx, b, update)
	default:
		if strings.HasPrefix(data, "close:") {
			t.handleCloseOrderCallback(ctx, b, update, data)
		}
	}
}

// isAuthorizedCallback 检查回调查询用户是否有权限
func (t *Telegram) isAuthorizedCallback(update *models.Update) bool {
	if update.CallbackQuery == nil {
		return false
	}

	userID := update.CallbackQuery.From.ID
	return userID == t.chatId
}

// isAuthorized 检查用户是否有权限使用命令
func (t *Telegram) isAuthorized(update *models.Update) bool {
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
func (t *Telegram) sendResponse(b *bot.Bot, update *models.Update, response string) {
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
	response.WriteString("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")

	if orderManager == nil {
		notInitialized := config.GetLangMsg("order_manager_not_initialized", "❌ 订单管理器未初始化")
		response.WriteString(notInitialized + "\n")
		response.WriteString("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		return response.String()
	}

	totalOrders := 0

	// 遍历所有账户
	for account := range config.Accounts {
		orders, err := orderManager.GetActiveOrders(account)
		if err != nil {
			log.Error("Failed to get orders", zap.String("account", account), zap.Error(err))
			continue
		}

		if len(orders) == 0 {
			continue
		}

		accountLabel := config.GetLangMsg("account_label", "账户:")
		response.WriteString(fmt.Sprintf("🏷️ <b>%s</b> <code>%s</code>\n", accountLabel, account))

		for _, order := range orders {
			totalOrders++

			// 订单方向
			directionLong := config.GetLangMsg("direction_long", "📈 多")
			directionShort := config.GetLangMsg("direction_short", "📉 空")
			direction := directionLong
			if order.Short {
				direction = directionShort
			}

			// 格式化订单信息
			priceLabel := config.GetLangMsg("price_label", "💰 价格:")
			quantityLabel := config.GetLangMsg("quantity_label", "数量:")
			pnlLabel := config.GetLangMsg("pnl_label", "📊 盈亏:")
			tagLabel := config.GetLangMsg("tag_label", "标签:")
			calculating := config.GetLangMsg("calculating", "计算中...")
			response.WriteString(fmt.Sprintf(
				"• <code>%d</code> %s <code>%s</code>\n"+
					"  %s <code>%.5f</code> | %s <code>%.4f</code>\n"+
					"  %s <code>%s</code> | %s <code>%s</code>\n\n",
				order.ID,
				direction,
				order.Symbol,
				priceLabel, order.Price, quantityLabel, order.Amount,
				pnlLabel, calculating, tagLabel, order.EnterTag,
			))
		}
	}

	if totalOrders == 0 {
		noActiveOrders := config.GetLangMsg("no_active_orders", "暂无活跃订单")
		response.WriteString(noActiveOrders + "\n")
	} else {
		totalLabel := config.GetLangMsg("total_label", "总计")
		activeOrdersCount := config.GetLangMsg("active_orders_count", "个活跃订单")
		response.WriteString(fmt.Sprintf("%s: <b>%d</b> %s", totalLabel, totalOrders, activeOrdersCount))
	}

	response.WriteString("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	return response.String()
}

// buildOrdersInlineKeyboard 构建订单列表对应的内联键盘（每单平仓 + 批量操作）
func (t *Telegram) buildOrdersInlineKeyboard() *models.InlineKeyboardMarkup {
	var rows [][]models.InlineKeyboardButton
	if orderManager != nil {
		orders, err := orderManager.GetActiveOrders("default")
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

	// 先尝试默认账户
	defaultAccount := "default"
	err = orderManager.CloseOrder(defaultAccount, id)
	if err == nil {
		closeSuccessTitle := config.GetLangMsg("close_success_title", "✅ 平仓成功")
		orderIDLabel := config.GetLangMsg("order_id_label", "📊 订单ID:")
		accountTarget := config.GetLangMsg("account_target", "🎯 账户:")
		timeLabel := config.GetLangMsg("time_label", "⏰ 时间:")
		closeRequestSubmitted := config.GetLangMsg("close_request_submitted", "已提交平仓请求，请等待执行完成。")
		return fmt.Sprintf("%s\n\n%s <code>%d</code>\n%s <code>%s</code>\n%s %s\n\n%s",
			closeSuccessTitle, orderIDLabel, id, accountTarget, defaultAccount,
			timeLabel, time.Now().Format("15:04:05"), closeRequestSubmitted)
	}

	// 如果默认账户中没有找到，再查找其他账户
	for account := range config.Accounts {
		if account == defaultAccount {
			continue // 跳过已经尝试过的默认账户
		}
		err := orderManager.CloseOrder(account, id)
		if err == nil {
			closeSuccessTitle := config.GetLangMsg("close_success_title", "✅ 平仓成功")
			orderIDLabel := config.GetLangMsg("order_id_label", "📊 订单ID:")
			accountTarget := config.GetLangMsg("account_target", "🎯 账户:")
			timeLabel := config.GetLangMsg("time_label", "⏰ 时间:")
			closeRequestSubmitted := config.GetLangMsg("close_request_submitted", "已提交平仓请求，请等待执行完成。")
			return fmt.Sprintf("%s\n\n%s <code>%d</code>\n%s <code>%s</code>\n%s %s\n\n%s",
				closeSuccessTitle, orderIDLabel, id, accountTarget, account,
				timeLabel, time.Now().Format("15:04:05"), closeRequestSubmitted)
		}
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
	response.WriteString("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")

	if orderManager == nil {
		notInitialized := config.GetLangMsg("order_manager_not_initialized", "❌ 订单管理器未初始化")
		response.WriteString(notInitialized + "\n")
		response.WriteString("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		return response.String()
	}

	totalClosed := 0
	totalFailed := 0

	accountLabel := config.GetLangMsg("account_label", "账户:")
	getOrdersFailed := config.GetLangMsg("get_orders_failed", "❌ 获取订单失败:")
	successLabel := config.GetLangMsg("success_label", "✅ 成功:")
	failedLabel := config.GetLangMsg("failed_label", "❌ 失败:")
	statisticsLabel := config.GetLangMsg("statistics_label", "📊 统计:")

	for account := range config.Accounts {
		response.WriteString(fmt.Sprintf("🏷️ <b>%s</b> <code>%s</code>\n", accountLabel, account))

		successCount, failedCount, err := orderManager.CloseAllOrders(account)
		if err != nil {
			response.WriteString(fmt.Sprintf("  %s %s\n", getOrdersFailed, err.Error()))
			continue
		}

		totalClosed += successCount
		totalFailed += failedCount

		response.WriteString(fmt.Sprintf("  %s %d | %s %d\n", successLabel, successCount, failedLabel, failedCount))
		response.WriteString("\n")
	}

	successCountLabel := config.GetLangMsg("success_count_label", "成功")
	failedCountLabel := config.GetLangMsg("failed_count_label", "失败")
	response.WriteString(fmt.Sprintf("%s %s %d | %s %d", statisticsLabel, successCountLabel, totalClosed, failedCountLabel, totalFailed))
	response.WriteString("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	return response.String()
}

// getOrdersListWithKeyboard 获取订单列表并返回是否有订单的标志
func (t *Telegram) getOrdersListWithKeyboard(account string) (string, bool) {
	var response strings.Builder
	activeOrdersTitle := config.GetLangMsg("active_orders_title", "📊 活跃订单列表")
	response.WriteString(activeOrdersTitle + "\n")
	response.WriteString("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")

	if orderManager == nil {
		notInitialized := config.GetLangMsg("order_manager_not_initialized", "❌ 订单管理器未初始化")
		response.WriteString(notInitialized + "\n")
		response.WriteString("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		return response.String(), false
	}

	hasOrders := false
	totalOrders := 0

	// 获取指定账户的订单
	orders, err := orderManager.GetActiveOrders(account)
	if err != nil {
		log.Error("Failed to get orders", zap.String("account", account), zap.Error(err))
		getAccountOrdersFailed := config.GetLangMsg("get_account_orders_failed", "❌ 获取账户 %s 订单失败: %v")
		response.WriteString(fmt.Sprintf(getAccountOrdersFailed, account, err) + "\n")
	} else if len(orders) > 0 {
		hasOrders = true
		accountLabel := config.GetLangMsg("account_label", "账户:")
		response.WriteString(fmt.Sprintf("🏷️ <b>%s</b> %s\n\n", accountLabel, account))

		directionLong := config.GetLangMsg("direction_long_order", "📈 多单")
		directionShort := config.GetLangMsg("direction_short_order", "📉 空单")
		priceLabel := config.GetLangMsg("price_label", "💰 价格:")
		quantityLabel := config.GetLangMsg("quantity_label", "数量:")
		tagLabel := config.GetLangMsg("tag_label", "🏷️ 标签:")

		for _, order := range orders {
			totalOrders++
			direction := directionLong
			if order.Short {
				direction = directionShort
			}

			response.WriteString(fmt.Sprintf("• <code>%d</code> %s <code>%s</code>\n", order.ID, direction, order.Symbol))
			response.WriteString(fmt.Sprintf("  %s <code>%.5f</code> | %s <code>%.4f</code>\n", priceLabel, order.Price, quantityLabel, order.Amount))
			if order.EnterTag != "" {
				response.WriteString(fmt.Sprintf("  %s <code>%s</code>\n", tagLabel, order.EnterTag))
			}
			response.WriteString("\n")
		}
	}

	if totalOrders == 0 {
		noActiveOrdersEmoji := config.GetLangMsg("no_active_orders_emoji", "📭 暂无活跃订单")
		response.WriteString(noActiveOrdersEmoji + "\n")
		response.WriteString("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	} else {
		response.WriteString("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
		totalLabel := config.GetLangMsg("total_label", "总计")
		activeOrdersCount := config.GetLangMsg("active_orders_count", "个活跃订单")
		response.WriteString(fmt.Sprintf("📊 <b>%s:</b> %d %s", totalLabel, totalOrders, activeOrdersCount))
	}

	return response.String(), hasOrders
}

// getTradingStatus 获取交易状态
func (t *Telegram) getTradingStatus() string {
	var response strings.Builder
	tradingStatusTitle := config.GetLangMsg("trading_status_title", "📊 交易状态")
	response.WriteString(tradingStatusTitle + "\n")
	response.WriteString("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")

	nowMS := btime.TimeMS()

	for account := range config.Accounts {
		accountLabel := config.GetLangMsg("account_label", "账户:")
		response.WriteString(fmt.Sprintf("🏷️ <b>%s</b> <code>%s</code>\n", accountLabel, account))

		// 检查是否被禁用
		if untilMS, exists := core.NoEnterUntil[account]; exists && nowMS < untilMS {
			remainingMS := untilMS - nowMS
			remaining := time.Duration(remainingMS) * time.Millisecond
			statusLabel := config.GetLangMsg("status_label", "状态:")
			tradingDisabledStatus := config.GetLangMsg("trading_disabled_status", "开单已禁用")
			remainingLabel := config.GetLangMsg("remaining_label", "剩余:")
			response.WriteString(fmt.Sprintf("  🚫 <b>%s</b> %s\n", statusLabel, tradingDisabledStatus))
			response.WriteString(fmt.Sprintf("  ⏰ <b>%s</b> %s\n", remainingLabel, formatDuration(remaining)))
		} else {
			statusLabel := config.GetLangMsg("status_label", "状态:")
			tradingNormalStatus := config.GetLangMsg("trading_normal_status", "开单正常")
			response.WriteString(fmt.Sprintf("  ✅ <b>%s</b> %s\n", statusLabel, tradingNormalStatus))
		}

		// 获取当前订单数量
		if orderManager != nil {
			longCount, shortCount, err := orderManager.GetOrderStats(account)
			if err == nil {
				longOrderLabel := config.GetLangMsg("long_order_label", "多单:")
				shortOrderLabel := config.GetLangMsg("short_order_label", "空单:")
				response.WriteString(fmt.Sprintf("  📈 <b>%s</b> %d | 📉 <b>%s</b> %d\n", longOrderLabel, longCount, shortOrderLabel, shortCount))
			}
		}

		response.WriteString("\n")
	}

	response.WriteString("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	return response.String()
}

// disableTrading 禁用交易
func (t *Telegram) disableTrading(hours int) string {
	untilMS := btime.TimeMS() + int64(hours)*3600*1000

	// 对所有账户禁用交易
	for account := range config.Accounts {
		core.NoEnterUntil[account] = untilMS
	}

	format := config.GetLangMsg("trading_disabled_format", "🚫 <b>开单已禁用</b>\n\n⏰ <b>禁用时长:</b> %d 小时\n📅 <b>恢复时间:</b> %s\n\n使用 <code>/enable</code> 可提前恢复开单")
	disabledUntil := time.Unix(untilMS/1000, (untilMS%1000)*1000000)
	return fmt.Sprintf(format, hours, disabledUntil.Format("2006-01-02 15:04:05"))
}

// enableTrading 启用交易
func (t *Telegram) enableTrading() string {
	// 清除所有账户的禁用状态
	for account := range config.Accounts {
		delete(core.NoEnterUntil, account)
	}

	return config.GetLangMsg("trading_enabled_message", "✅ <b>开单已恢复</b>\n\n所有账户的交易功能已重新启用")
}

// IsTradingDisabled 检查指定账户是否被禁用交易（供外部调用）
func (t *Telegram) IsTradingDisabled(account string) bool {
	if untilMS, exists := core.NoEnterUntil[account]; exists {
		return btime.TimeMS() < untilMS
	}
	return false
}

// formatDuration 格式化时间间隔
func formatDuration(d time.Duration) string {
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60

	if hours > 0 {
		format := config.GetLangMsg("hours_format", "%d小时%d分钟")
		return fmt.Sprintf(format, hours, minutes)
	}
	format := config.GetLangMsg("minutes_format", "%d分钟")
	return fmt.Sprintf(format, minutes)
}

// handleOrdersCallback 处理查看订单回调
func (t *Telegram) handleOrdersCallback(ctx context.Context, b *bot.Bot, update *models.Update) {
	ordersList, hasOrders := t.getOrdersListWithKeyboard("default")

	// 创建键盘，包含每个订单的单独平仓按钮
	var rows [][]models.InlineKeyboardButton
	if hasOrders {
		// 从文本里解析订单ID太复杂，这里直接重新获取订单构建按钮
		if orderManager != nil {
			orders, err := orderManager.GetActiveOrders("default")
			if err == nil {
				closePositionFormat := config.GetLangMsg("close_position_format", "❌ 平仓 %d")
				for _, od := range orders {
					btn := models.InlineKeyboardButton{Text: fmt.Sprintf(closePositionFormat, od.ID), CallbackData: fmt.Sprintf("close:%d", od.ID)}
					rows = append(rows, []models.InlineKeyboardButton{btn})
				}
			}
		}
		// 追加操作行
		closeAllOrdersBtn := config.GetLangMsg("close_all_orders_button", "❌ 平仓所有订单")
		refreshOrdersBtn := config.GetLangMsg("refresh_orders", "🔄 刷新订单")
		backToMenuBtn := config.GetLangMsg("back_to_menu", "🔙 返回菜单")
		rows = append(rows, []models.InlineKeyboardButton{
			{Text: closeAllOrdersBtn, CallbackData: "action:close_all"},
			{Text: refreshOrdersBtn, CallbackData: "action:orders"},
		})
		rows = append(rows, []models.InlineKeyboardButton{{Text: backToMenuBtn, CallbackData: "action:refresh"}})
	} else {
		refreshOrdersBtn := config.GetLangMsg("refresh_orders", "🔄 刷新订单")
		backToMenuBtn := config.GetLangMsg("back_to_menu", "🔙 返回菜单")
		rows = append(rows, []models.InlineKeyboardButton{
			{Text: refreshOrdersBtn, CallbackData: "action:orders"},
			{Text: backToMenuBtn, CallbackData: "action:refresh"},
		})
	}
	kb := &models.InlineKeyboardMarkup{InlineKeyboard: rows}

	_, err := b.EditMessageText(ctx, &bot.EditMessageTextParams{
		ChatID:      update.CallbackQuery.Message.Message.Chat.ID,
		MessageID:   update.CallbackQuery.Message.Message.ID,
		Text:        ordersList,
		ParseMode:   models.ParseModeHTML,
		ReplyMarkup: kb,
	})
	if err != nil {
		log.Error("Failed to edit message with orders", zap.Error(err))
	}
}

// handleStatusCallback 处理查看状态回调
func (t *Telegram) handleStatusCallback(ctx context.Context, b *bot.Bot, update *models.Update) {
	status := t.getTradingStatus()

	refreshStatus := config.GetLangMsg("refresh_status", "🔄 刷新状态")
	backToMenu := config.GetLangMsg("back_to_menu", "🔙 返回菜单")
	kb := &models.InlineKeyboardMarkup{
		InlineKeyboard: [][]models.InlineKeyboardButton{
			{
				{Text: refreshStatus, CallbackData: "action:status"},
				{Text: backToMenu, CallbackData: "action:refresh"},
			},
		},
	}

	_, err := b.EditMessageText(ctx, &bot.EditMessageTextParams{
		ChatID:      update.CallbackQuery.Message.Message.Chat.ID,
		MessageID:   update.CallbackQuery.Message.Message.ID,
		Text:        status,
		ParseMode:   models.ParseModeHTML,
		ReplyMarkup: kb,
	})
	if err != nil {
		log.Error("Failed to edit message with status", zap.Error(err))
	}
}

// handleDisableCallback 处理禁止开单回调
func (t *Telegram) handleDisableCallback(ctx context.Context, b *bot.Bot, update *models.Update) {
	// 默认禁用1小时
	hours := 1
	t.disableTrading(hours)

	format := config.GetLangMsg("trading_disabled_callback", "🚫 <b>交易已禁用</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n⏰ <b>禁用时长:</b> %s\n🕒 <b>恢复时间:</b> %s\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	oneHour := config.GetLangMsg("one_hour", "1小时")
	untilMS := btime.TimeMS() + int64(hours)*3600*1000
	disabledUntil := time.Unix(untilMS/1000, (untilMS%1000)*1000000)
	response := fmt.Sprintf(format, oneHour, disabledUntil.Format("2006-01-02 15:04:05"))

	enableImmediately := config.GetLangMsg("enable_immediately", "✅ 立即启用")
	backToMenu := config.GetLangMsg("back_to_menu", "🔙 返回菜单")
	kb := &models.InlineKeyboardMarkup{
		InlineKeyboard: [][]models.InlineKeyboardButton{
			{
				{Text: enableImmediately, CallbackData: "action:enable"},
				{Text: backToMenu, CallbackData: "action:refresh"},
			},
		},
	}

	_, err := b.EditMessageText(ctx, &bot.EditMessageTextParams{
		ChatID:      update.CallbackQuery.Message.Message.Chat.ID,
		MessageID:   update.CallbackQuery.Message.Message.ID,
		Text:        response,
		ParseMode:   models.ParseModeHTML,
		ReplyMarkup: kb,
	})
	if err != nil {
		log.Error("Failed to edit message with disable status", zap.Error(err))
	}
}

// handleEnableCallback 处理启用开单回调
func (t *Telegram) handleEnableCallback(ctx context.Context, b *bot.Bot, update *models.Update) {
	t.enableTrading()

	format := config.GetLangMsg("trading_enabled_callback", "✅ <b>交易已启用</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n🎯 <b>状态:</b> 交易功能已恢复正常\n⏰ <b>时间:</b> %s\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	response := fmt.Sprintf(format, time.Now().Format("2006-01-02 15:04:05"))

	disableTrading := config.GetLangMsg("disable_trading_callback", "🚫 禁用交易")
	backToMenu := config.GetLangMsg("back_to_menu", "🔙 返回菜单")
	kb := &models.InlineKeyboardMarkup{
		InlineKeyboard: [][]models.InlineKeyboardButton{
			{
				{Text: disableTrading, CallbackData: "action:disable"},
				{Text: backToMenu, CallbackData: "action:refresh"},
			},
		},
	}

	_, err := b.EditMessageText(ctx, &bot.EditMessageTextParams{
		ChatID:      update.CallbackQuery.Message.Message.Chat.ID,
		MessageID:   update.CallbackQuery.Message.Message.ID,
		Text:        response,
		ParseMode:   models.ParseModeHTML,
		ReplyMarkup: kb,
	})
	if err != nil {
		log.Error("Failed to edit message with enable status", zap.Error(err))
	}
}

// handleCloseAllCallback 处理平仓所有订单回调
func (t *Telegram) handleCloseAllCallback(ctx context.Context, b *bot.Bot, update *models.Update) {
	result := t.closeAllOrders()

	viewOrders := config.GetLangMsg("view_orders_button", "📊 查看订单")
	backToMenu := config.GetLangMsg("back_to_menu", "🔙 返回菜单")
	kb := &models.InlineKeyboardMarkup{
		InlineKeyboard: [][]models.InlineKeyboardButton{
			{
				{Text: viewOrders, CallbackData: "action:orders"},
				{Text: backToMenu, CallbackData: "action:refresh"},
			},
		},
	}

	// FIXME: Replace this placeholder with proper keyboard

	_, err := b.EditMessageText(ctx, &bot.EditMessageTextParams{
		ChatID:      update.CallbackQuery.Message.Message.Chat.ID,
		MessageID:   update.CallbackQuery.Message.Message.ID,
		Text:        result,
		ParseMode:   models.ParseModeHTML,
		ReplyMarkup: kb,
	})
	if err != nil {
		log.Error("Failed to edit message with close all result", zap.Error(err))
	}
}

// handleMenuCallback 处理返回菜单回调
func (t *Telegram) handleMenuCallback(ctx context.Context, b *bot.Bot, update *models.Update) {
	viewOrders := config.GetLangMsg("view_orders_button", "📊 查看订单")
	tradingStatus := config.GetLangMsg("trading_status_button", "📈 开单状态")
	viewWallet := config.GetLangMsg("view_wallet_button", "👛 查看钱包")
	closeAll := config.GetLangMsg("close_all_button", "❌ 平仓所有")
	disableTrading := config.GetLangMsg("disable_trading_callback", "🚫 禁用交易")
	enableTrading := config.GetLangMsg("enable_trading_callback", "✅ 启用开单")

	kb := &models.InlineKeyboardMarkup{
		InlineKeyboard: [][]models.InlineKeyboardButton{
			{
				{Text: viewOrders, CallbackData: "action:orders"},
				{Text: tradingStatus, CallbackData: "action:status"},
			},
			{
				{Text: viewWallet, CallbackData: "action:wallet"},
				{Text: closeAll, CallbackData: "action:close_all"},
			},
			{
				{Text: disableTrading, CallbackData: "action:disable"},
				{Text: enableTrading, CallbackData: "action:enable"},
			},
		},
	}

	menuText, err := config.ReadLangFile(config.ShowLangCode, "telegram_menu.txt")
	if err != nil {
		log.Error("read lang file fail: telegram_menu_default.txt", zap.Error(err))
		menuText = `🎛️ <b>BanBot Menu</b>`
	}

	_, err = b.EditMessageText(ctx, &bot.EditMessageTextParams{
		ChatID:      update.CallbackQuery.Message.Message.Chat.ID,
		MessageID:   update.CallbackQuery.Message.Message.ID,
		Text:        menuText,
		ParseMode:   models.ParseModeHTML,
		ReplyMarkup: kb,
	})
	if err != nil {
		log.Error("Failed to edit message with menu", zap.Error(err))
	}
}

// handleCloseOrderCallback 处理平仓特定订单回调
func (t *Telegram) handleCloseOrderCallback(ctx context.Context, b *bot.Bot, update *models.Update, data string) {
	// 解析订单ID：close:12345
	parts := strings.Split(data, ":")
	if len(parts) != 2 {
		return
	}

	orderIDStr := parts[1]

	result := t.closeOrders(orderIDStr)

	viewOrders := config.GetLangMsg("view_orders_button", "📊 查看订单")
	backToMenu := config.GetLangMsg("back_to_menu", "🔙 返回菜单")
	kb := &models.InlineKeyboardMarkup{
		InlineKeyboard: [][]models.InlineKeyboardButton{
			{
				{Text: viewOrders, CallbackData: "action:orders"},
				{Text: backToMenu, CallbackData: "action:refresh"},
			},
		},
	}

	// FIXME: Replace this placeholder with proper keyboard

	_, editErr := b.EditMessageText(ctx, &bot.EditMessageTextParams{
		ChatID:      update.CallbackQuery.Message.Message.Chat.ID,
		MessageID:   update.CallbackQuery.Message.Message.ID,
		Text:        result,
		ParseMode:   models.ParseModeHTML,
		ReplyMarkup: kb,
	})
	if editErr != nil {
		log.Error("Failed to edit message with close order result", zap.Error(editErr))
	}
}

// 键盘按钮处理函数

// handleKeyboardOrdersCommand 处理键盘"查看订单"按钮
func (t *Telegram) handleKeyboardOrdersCommand(ctx context.Context, b *bot.Bot, update *models.Update) {
	if !t.isAuthorized(update) {
		return
	}

	ordersList, hasOrders := t.getOrdersListWithKeyboard("default")

	var rows [][]models.InlineKeyboardButton
	if hasOrders && orderManager != nil {
		orders, err := orderManager.GetActiveOrders("default")
		if err == nil {
			closePositionFormat := config.GetLangMsg("close_position_format", "❌ 平仓 %d")
			for _, od := range orders {
				btn := models.InlineKeyboardButton{Text: fmt.Sprintf(closePositionFormat, od.ID), CallbackData: fmt.Sprintf("close:%d", od.ID)}
				rows = append(rows, []models.InlineKeyboardButton{btn})
			}
		}
		closeAllOrdersBtn := config.GetLangMsg("close_all_orders_button", "❌ 平仓所有订单")
		refreshOrdersBtn := config.GetLangMsg("refresh_orders", "🔄 刷新订单")
		backToMenuBtn := config.GetLangMsg("back_to_menu", "🔙 返回菜单")
		rows = append(rows, []models.InlineKeyboardButton{
			{Text: closeAllOrdersBtn, CallbackData: "action:close_all"},
			{Text: refreshOrdersBtn, CallbackData: "action:orders"},
		})
		rows = append(rows, []models.InlineKeyboardButton{{Text: backToMenuBtn, CallbackData: "action:refresh"}})
	}

	t.chanSend <- &bot.SendMessageParams{
		ChatID:      update.Message.Chat.ID,
		Text:        ordersList,
		ParseMode:   models.ParseModeHTML,
		ReplyMarkup: &models.InlineKeyboardMarkup{InlineKeyboard: rows},
	}
}

// handleKeyboardStatusCommand 处理键盘"开单状态"按钮
func (t *Telegram) handleKeyboardStatusCommand(ctx context.Context, b *bot.Bot, update *models.Update) {
	if !t.isAuthorized(update) {
		return
	}

	status := t.getTradingStatus()
	t.sendResponse(b, update, status)
}

// handleKeyboardDisableCommand 处理键盘"禁止开单"按钮
func (t *Telegram) handleKeyboardDisableCommand(ctx context.Context, b *bot.Bot, update *models.Update) {
	if !t.isAuthorized(update) {
		return
	}

	response := t.disableTrading(1) // 默认禁用1小时
	t.sendResponse(b, update, response)
}

// handleKeyboardEnableCommand 处理键盘"启用开单"按钮
func (t *Telegram) handleKeyboardEnableCommand(ctx context.Context, b *bot.Bot, update *models.Update) {
	if !t.isAuthorized(update) {
		return
	}

	response := t.enableTrading()
	t.sendResponse(b, update, response)
}

// handleKeyboardCloseAllCommand 处理键盘"平仓所有"按钮
func (t *Telegram) handleKeyboardCloseAllCommand(ctx context.Context, b *bot.Bot, update *models.Update) {
	if !t.isAuthorized(update) {
		return
	}

	response := t.closeAllOrders()
	t.sendResponse(b, update, response)
}

// handleHideMenuCommand 处理"隐藏菜单"按钮
func (t *Telegram) handleHideMenuCommand(ctx context.Context, b *bot.Bot, update *models.Update) {
	if !t.isAuthorized(update) {
		return
	}

	menuHiddenTitle := config.GetLangMsg("menu_hidden_title", "🔄 菜单已隐藏")
	menuHiddenTip := config.GetLangMsg("menu_hidden_tip", "使用 <code>/menu</code> 命令可以重新显示菜单。")
	text := fmt.Sprintf("<b>%s</b>\n\n%s", menuHiddenTitle, menuHiddenTip)

	// 发送隐藏键盘的消息
	t.chanSend <- &bot.SendMessageParams{
		ChatID:    update.Message.Chat.ID,
		Text:      text,
		ParseMode: models.ParseModeHTML,
		ReplyMarkup: &models.ReplyKeyboardRemove{
			RemoveKeyboard: true,
		},
	}
}

// handleWalletCommand 处理 /wallet 命令 - 显示钱包信息
func (t *Telegram) handleWalletCommand(ctx context.Context, b *bot.Bot, update *models.Update) {
	if !t.isAuthorized(update) {
		return
	}

	response := t.getWalletSummary()
	t.sendResponse(b, update, response)
}

// handleKeyboardWalletCommand 处理键盘"查看钱包"按钮
func (t *Telegram) handleKeyboardWalletCommand(ctx context.Context, b *bot.Bot, update *models.Update) {
	if !t.isAuthorized(update) {
		return
	}

	response := t.getWalletSummary()
	// 带内联按钮：刷新与返回
	refreshWallet := config.GetLangMsg("refresh_wallet_button", "🔄 刷新钱包")
	backToMenu := config.GetLangMsg("back_to_menu", "🔙 返回菜单")
	kb := &models.InlineKeyboardMarkup{
		InlineKeyboard: [][]models.InlineKeyboardButton{
			{
				{Text: refreshWallet, CallbackData: "action:wallet"},
				{Text: backToMenu, CallbackData: "action:refresh"},
			},
		},
	}

	t.chanSend <- &bot.SendMessageParams{
		ChatID:      update.Message.Chat.ID,
		Text:        response,
		ParseMode:   models.ParseModeHTML,
		ReplyMarkup: kb,
	}
}

// handleWalletCallback 处理查看钱包回调
func (t *Telegram) handleWalletCallback(ctx context.Context, b *bot.Bot, update *models.Update) {
	response := t.getWalletSummary()
	refreshWallet := config.GetLangMsg("refresh_wallet_button", "🔄 刷新钱包")
	backToMenu := config.GetLangMsg("back_to_menu", "🔙 返回菜单")
	kb := &models.InlineKeyboardMarkup{
		InlineKeyboard: [][]models.InlineKeyboardButton{
			{
				{Text: refreshWallet, CallbackData: "action:wallet"},
				{Text: backToMenu, CallbackData: "action:refresh"},
			},
		},
	}

	_, err := b.EditMessageText(ctx, &bot.EditMessageTextParams{
		ChatID:      update.CallbackQuery.Message.Message.Chat.ID,
		MessageID:   update.CallbackQuery.Message.Message.ID,
		Text:        response,
		ParseMode:   models.ParseModeHTML,
		ReplyMarkup: kb,
	})
	if err != nil {
		log.Error("Failed to edit message with wallet summary", zap.Error(err))
	}
}

// getWalletSummary 获取钱包汇总信息
func (t *Telegram) getWalletSummary() string {
	var bld strings.Builder
	walletTitle := config.GetLangMsg("wallet_summary_title", "👛 钱包汇总")
	separator := config.GetLangMsg("wallet_summary_separator", "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	bld.WriteString(fmt.Sprintf("<b>%s</b>\n", walletTitle))
	bld.WriteString(separator + "\n\n")

	totalAll := 0.0
	avaAll := 0.0
	upolAll := 0.0

	for account := range config.Accounts {
		var total, ava, upol float64
		if walletProvider != nil {
			total, ava, upol = walletProvider.GetSummary(account)
		} else {
			accountLabel := config.GetLangMsg("account_label", "账户:")
			notInitialized := config.GetLangMsg("wallet_provider_not_initialized_full", "❌ 钱包提供者未初始化")
			bld.WriteString(fmt.Sprintf("🏷️ <b>%s</b> <code>%s</code>\n", accountLabel, account))
			bld.WriteString(fmt.Sprintf("  %s\n\n", notInitialized))
			continue
		}

		totalAll += total
		avaAll += ava
		upolAll += upol

		accountLabel := config.GetLangMsg("account_label", "账户:")
		totalAmount := config.GetLangMsg("total_amount", "💼 总额:")
		availableAmount := config.GetLangMsg("available_amount", "💰 可用:")
		unrealizedPnl := config.GetLangMsg("unrealized_pnl", "📊 未实现盈亏:")

		bld.WriteString(fmt.Sprintf("🏷️ <b>%s</b> <code>%s</code>\n", accountLabel, account))
		bld.WriteString(fmt.Sprintf("  <b>%s</b> <code>%.2f</code>\n", totalAmount, total))
		bld.WriteString(fmt.Sprintf("  <b>%s</b> <code>%.2f</code>\n", availableAmount, ava))
		bld.WriteString(fmt.Sprintf("  <b>%s</b> <code>%.2f</code>\n\n", unrealizedPnl, upol))
	}

	totalSummary := config.GetLangMsg("total_summary", "📈 合计")
	totalAmount := config.GetLangMsg("total_amount", "💼 总额:")
	availableAmount := config.GetLangMsg("available_amount", "💰 可用:")
	unrealizedPnl := config.GetLangMsg("unrealized_pnl", "📊 未实现盈亏:")

	bld.WriteString(separator + "\n")
	bld.WriteString(fmt.Sprintf("<b>%s</b>\n", totalSummary))
	bld.WriteString(fmt.Sprintf("  <b>%s</b> <code>%.2f</code>\n", totalAmount, totalAll))
	bld.WriteString(fmt.Sprintf("  <b>%s</b> <code>%.2f</code>\n", availableAmount, avaAll))
	bld.WriteString(fmt.Sprintf("  <b>%s</b> <code>%.2f</code>\n", unrealizedPnl, upolAll))
	bld.WriteString(separator)

	return bld.String()
}

// addCloseButtonsToOrdersList 为订单列表添加单独平仓按钮
func (t *Telegram) addCloseButtonsToOrdersList(account string) string {
	var response strings.Builder
	activeOrdersTitle := config.GetLangMsg("active_orders_title", "📊 活跃订单列表")
	separator := config.GetLangMsg("wallet_summary_separator", "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	response.WriteString(fmt.Sprintf("<b>%s</b>\n", activeOrdersTitle))
	response.WriteString(separator + "\n\n")

	if orderManager == nil {
		notInitialized := config.GetLangMsg("order_manager_not_initialized", "❌ 订单管理器未初始化")
		response.WriteString(notInitialized + "\n")
		response.WriteString(separator)
		return response.String()
	}

	totalOrders := 0

	// 获取指定账户的订单
	orders, err := orderManager.GetActiveOrders(account)
	if err != nil {
		log.Error("Failed to get orders", zap.String("account", account), zap.Error(err))
		getAccountOrdersFailed := config.GetLangMsg("get_account_orders_failed", "❌ 获取账户 %s 订单失败: %v")
		response.WriteString(fmt.Sprintf(getAccountOrdersFailed, account, err) + "\n")
	} else if len(orders) > 0 {
		accountLabel := config.GetLangMsg("account_label", "账户:")
		response.WriteString(fmt.Sprintf("🏷️ <b>%s</b> %s\n\n", accountLabel, account))

		directionLong := config.GetLangMsg("direction_long_order", "📈 多单")
		directionShort := config.GetLangMsg("direction_short_order", "📉 空单")
		priceLabel := config.GetLangMsg("price_label", "💰 价格:")
		quantityLabel := config.GetLangMsg("quantity_label", "数量:")
		tagLabel := config.GetLangMsg("tag_label", "🏷️ 标签:")
		closeCommandTip := config.GetLangMsg("close_command_tip", "💡 平仓命令:")
		closeOrderFormat := config.GetLangMsg("close_order_format", "/close %d")

		for _, order := range orders {
			totalOrders++
			direction := directionLong
			if order.Short {
				direction = directionShort
			}

			response.WriteString(fmt.Sprintf("• <code>%d</code> %s <code>%s</code>\n", order.ID, direction, order.Symbol))
			response.WriteString(fmt.Sprintf("  %s <code>%.5f</code> | %s <code>%.4f</code>\n", priceLabel, order.Price, quantityLabel, order.Amount))
			if order.EnterTag != "" {
				response.WriteString(fmt.Sprintf("  %s <code>%s</code>\n", tagLabel, order.EnterTag))
			}
			response.WriteString(fmt.Sprintf("  %s <code>%s</code>\n\n", closeCommandTip, fmt.Sprintf(closeOrderFormat, order.ID)))
		}
	}

	if totalOrders == 0 {
		noActiveOrdersFull := config.GetLangMsg("no_active_orders_full", "📭 暂无活跃订单")
		response.WriteString(fmt.Sprintf("<b>%s</b>\n", noActiveOrdersFull))
		response.WriteString(separator)
	} else {
		response.WriteString(separator + "\n")
		totalOrdersFormat := config.GetLangMsg("total_orders_format", "📊 总计: %d 个活跃订单")
		closeTipMessage := config.GetLangMsg("close_tip_message", "💡 提示: 点击上方平仓命令或直接输入 <code>/close [订单ID]</code> 来平仓指定订单")
		response.WriteString(fmt.Sprintf("<b>%s</b>\n", fmt.Sprintf(totalOrdersFormat, totalOrders)))
		response.WriteString(fmt.Sprintf("<b>%s</b>", closeTipMessage))
	}

	return response.String()
}
