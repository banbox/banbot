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
	channels = make([]IWebHook, 0, 2)
	initOnce sync.Once
)

func initWebHooks() *errs.Error {
	if len(config.RPCChannels) == 0 {
		log.Info("no channels, skip send rpc msg")
		return nil
	}
	// 解析accounts中的rpc配置
	accChls := make([]map[string]interface{}, 0)
	for accName, acc := range config.Accounts {
		for i, chl := range acc.RPCChannels {
			chlName := utils.GetMapVal(chl, "name", "")
			if chlName == "" {
				return errs.NewMsg(core.ErrBadConfig, "`name` is required in accounts.%s.rpc_channels[%d]", accName, i)
			}
			chl["_acc"] = accName
			if _, ok := chl["accounts"]; !ok {
				chl["accounts"] = []string{accName}
			}
			accChls = append(accChls, chl)
		}
	}
	items := maps.Clone(config.RPCChannels)
	for _, chl := range accChls {
		chlName := utils.PopMapVal(chl, "name", "")
		acc := utils.PopMapVal(chl, "_acc", "")
		base, _ := items[chlName]
		if base == nil {
			return errs.NewMsg(core.ErrBadConfig, "channel `%s.%s` not exists", acc, chlName)
		}
		chlCfg := maps.Clone(base)
		maps.Copy(chlCfg, chl)
		items[fmt.Sprintf("%s_%s", chlName, acc)] = chlCfg
	}
	for name, item := range items {
		chlType := utils.GetMapVal(item, "type", "")
		var channel IWebHook
		switch chlType {
		case "wework":
			channel = NewWeWork(name, item)
		case "mail", "email":
			channel = NewEmail(name, item)
		case "telegram":
			channel = NewTelegram(name, item)
		default:
			return errs.NewMsg(core.ErrBadConfig, "RPCChannel not support: %v", chlType)
		}
		if channel.IsDisable() {
			continue
		}
		go channel.ConsumeForever()
		channels = append(channels, channel)
	}
	if len(channels) == 0 {
		log.Info("no channels, skip send rpc msg")
	}
	return nil
}

func SendMsg(msg map[string]interface{}) {
	initOnce.Do(func() {
		err := initWebHooks()
		if err != nil {
			log.Error("init rpc fail", zap.Error(err))
		}
	})
	if len(channels) == 0 {
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
	for _, chl := range channels {
		chl.SendMsg(msgType, account, payload)
	}
}

func CleanUp() {
	for _, chl := range channels {
		chl.SetDisable(true)
	}
	for _, chl := range channels {
		chl.CleanUp()
	}
	channels = make([]IWebHook, 0, 2)
}
