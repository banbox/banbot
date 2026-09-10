package rpc

import (
	"fmt"
	"maps"
	"sync"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	utils2 "github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/banbox/banexg/utils"
	"go.uber.org/zap"
)

var (
	channels      = make([]IWebHook, 0, 2)
	channelsMu    sync.RWMutex
	rpcClosed     bool
	rpcReady      bool
	rpcGeneration uint64
	rpcInitMu     sync.Mutex
	rpcStopDone   = closedRPCSignal()
	rpcJoined     bool
)

func closedRPCSignal() chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}

type rpcStopper interface {
	Stop()
}

type rpcJoiner interface {
	Join()
}

func stopChannel(channel IWebHook) {
	if channel == nil {
		return
	}
	if stopper, ok := channel.(rpcStopper); ok {
		stopper.Stop()
		return
	}
	channel.SetDisable(true)
}

func joinChannel(channel IWebHook) {
	if channel == nil {
		return
	}
	if joiner, ok := channel.(rpcJoiner); ok {
		joiner.Join()
		return
	}
	channel.CleanUp()
}

func stopAndJoinChannels(channels []IWebHook) {
	for _, channel := range channels {
		stopChannel(channel)
	}
	for _, channel := range channels {
		joinChannel(channel)
	}
}

// InitRPC initializes the current RPC session. It is idempotent while the
// session is open and starts a new generation after Stop or CleanUp.
func InitRPC() *errs.Error {
	rpcInitMu.Lock()
	defer rpcInitMu.Unlock()

	channelsMu.RLock()
	ready, closed := rpcReady, rpcClosed
	channelsMu.RUnlock()
	if ready && !closed {
		return nil
	}
	if ready && closed {
		joinRPCSession()
	}

	channelsMu.Lock()
	if rpcReady && !rpcClosed {
		channelsMu.Unlock()
		return nil
	}
	rpcGeneration++
	generation := rpcGeneration
	rpcReady = true
	rpcClosed = false
	channels = make([]IWebHook, 0, 2)
	rpcStopDone = closedRPCSignal()
	rpcJoined = false
	channelsMu.Unlock()

	return initWebHooksForGeneration(generation)
}

func initWebHooks() *errs.Error {
	return InitRPC()
}

func buildChannels(cfg *config.Config, accounts map[string]*config.AccountConfig, newTelegram func(string, map[string]interface{}) *Telegram) ([]IWebHook, *errs.Error) {
	if len(cfg.RPCChannels) == 0 {
		log.Info("no channels, skip send rpc msg")
		return nil, nil
	}
	// 解析accounts中的rpc配置
	accChls := make([]map[string]interface{}, 0)
	for accName, acc := range accounts {
		if acc.NoTrade {
			continue
		}
		for i, rawChl := range acc.RPCChannels {
			chl := maps.Clone(rawChl)
			chlName := utils.GetMapVal(chl, "name", "")
			if chlName == "" {
				return nil, errs.NewMsg(core.ErrBadConfig, "`name` is required in accounts.%s.rpc_channels[%d]", accName, i)
			}
			chl["_acc"] = accName
			if _, ok := chl["accounts"]; !ok {
				chl["accounts"] = []string{accName}
			}
			accChls = append(accChls, chl)
		}
	}
	items := maps.Clone(cfg.RPCChannels)
	for _, chl := range accChls {
		chlName := utils.PopMapVal(chl, "name", "")
		acc := utils.PopMapVal(chl, "_acc", "")
		base, _ := items[chlName]
		if base == nil {
			return nil, errs.NewMsg(core.ErrBadConfig, "channel `%s.%s` not exists", acc, chlName)
		}
		chlCfg := maps.Clone(base)
		maps.Copy(chlCfg, chl)
		items[fmt.Sprintf("%s_%s", chlName, acc)] = chlCfg
	}
	newChannels := make([]IWebHook, 0, len(items))
	for name, item := range items {
		chlType := utils.GetMapVal(item, "type", "")
		var channel IWebHook
		switch chlType {
		case "wework":
			channel = NewWeWork(name, item)
		case "mail", "email":
			channel = NewEmail(name, item)
		case "telegram":
			channel = newTelegram(name, item)
		default:
			err := errs.NewMsg(core.ErrBadConfig, "RPCChannel not support: %v", chlType)
			stopAndJoinChannels(newChannels)
			return nil, err
		}
		if channel.IsDisable() {
			continue
		}
		newChannels = append(newChannels, channel)
	}
	return newChannels, nil
}

func initWebHooksForGeneration(generation uint64) *errs.Error {
	cfg := config.Data
	cfg.RPCChannels = config.RPCChannels
	newChannels, err := buildChannels(&cfg, config.Accounts, NewTelegram)
	if err != nil {
		return err
	}
	channelsMu.Lock()
	valid := rpcReady && !rpcClosed && rpcGeneration == generation
	if valid {
		channels = append(channels, newChannels...)
	}
	channelCount := len(channels)
	channelsMu.Unlock()
	if !valid {
		stopAndJoinChannels(newChannels)
		return nil
	}
	for _, channel := range newChannels {
		go channel.ConsumeForever()
	}
	if channelCount == 0 {
		log.Info("no channels, skip send rpc msg")
	}
	return nil
}

func SendMsg(msg map[string]interface{}) {
	channelsMu.RLock()
	ready := rpcReady && !rpcClosed
	channelsMu.RUnlock()
	if !ready {
		err := InitRPC()
		if err != nil {
			log.Error("init rpc fail", zap.Error(err))
		}
	}
	channelsMu.RLock()
	if !rpcReady || rpcClosed {
		channelsMu.RUnlock()
		return
	}
	chls := append([]IWebHook(nil), channels...)
	channelsMu.RUnlock()
	if len(chls) == 0 {
		return
	}
	account := utils.GetMapVal(msg, "account", "")
	botName := config.Name
	if account != "" {
		botName += "/" + account
	}
	msg["name"] = botName
	msgType := utils.GetMapVal(msg, "type", "")
	item, ok := config.Webhook[msgType]
	if !ok {
		log.Error(fmt.Sprintf("webhook for %v not found!", msgType))
		return
	}
	var payload = make(map[string]string)
	for key, val := range item {
		payload[key] = utils2.FormatWithMap(val, msg)
	}
	for _, chl := range chls {
		chl.SendMsg(msgType, account, payload)
	}
}

// Stop closes RPC admission and signals every channel without waiting for
// callbacks already running in those channels.
func Stop() {
	channelsMu.Lock()
	if rpcClosed {
		channelsMu.Unlock()
		return
	}
	rpcClosed = true
	stopDone := make(chan struct{})
	rpcStopDone = stopDone
	chls := append([]IWebHook(nil), channels...)
	channelsMu.Unlock()
	for _, chl := range chls {
		stopChannel(chl)
	}
	close(stopDone)
}

func joinRPCSession() {
	channelsMu.RLock()
	if !rpcClosed {
		channelsMu.RUnlock()
		return
	}
	generation := rpcGeneration
	chls := append([]IWebHook(nil), channels...)
	stopDone := rpcStopDone
	channelsMu.RUnlock()
	if stopDone != nil {
		<-stopDone
	}
	if rpcJoined {
		return
	}
	for _, chl := range chls {
		joinChannel(chl)
	}
	rpcJoined = true
	channelsMu.Lock()
	if rpcGeneration == generation && rpcClosed {
		channels = make([]IWebHook, 0, 2)
	}
	channelsMu.Unlock()
}

// Join waits for every RPC channel stopped by Stop. It is a no-op while RPC
// remains open; callers that need shutdown completion should call CleanUp.
func Join() {
	rpcInitMu.Lock()
	defer rpcInitMu.Unlock()
	joinRPCSession()
}

func CleanUp() {
	rpcInitMu.Lock()
	defer rpcInitMu.Unlock()
	Stop()
	joinRPCSession()
}
