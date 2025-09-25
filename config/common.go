package config

import (
	"fmt"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/banbox/banexg/utils"
	"go.uber.org/zap"
)

func GetExchangeProxy(exgId string) (string, *errs.Error) {
	params, ok := Exchange.Items[exgId]
	if !ok {
		return "", errs.NewMsg(errs.CodeParamInvalid, "no exchange found: %v", exgId)
	}
	proxyUrl := utils.GetMapVal(params, "proxy", "")
	if proxyUrl == "" {
		proxyUrl = utils.GetSystemEnvProxy()
		if proxyUrl == "" {
			prx, err := utils.GetSystemProxy()
			if err != nil {
				log.Error("GetSystemProxy fail, skip", zap.Error(err))
			} else if prx != nil {
				proxyUrl = fmt.Sprintf("%s://%s:%s", prx.Protocol, prx.Host, prx.Port)
			}
		}
	} else if proxyUrl == "no" {
		proxyUrl = ""
	}
	return proxyUrl, nil
}
