package utils

import (
	"io"
	"net/http"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

func DoHttp(client *http.Client, req *http.Request) *banexg.HttpRes {
	rsp, err_ := client.Do(req)
	if err_ != nil {
		return &banexg.HttpRes{Error: errs.New(core.ErrNetReadFail, err_)}
	}
	defer rsp.Body.Close()
	var result = banexg.HttpRes{Status: rsp.StatusCode, Headers: rsp.Header}
	rspData, err := io.ReadAll(rsp.Body)
	if err != nil {
		result.Error = errs.New(core.ErrNetReadFail, err)
		return &result
	}
	result.Content = string(rspData)
	cutLen := min(len(result.Content), 3000)
	bodyShort := zap.String("body", result.Content[:cutLen])
	log.Debug("rsp", zap.String("method", req.Method), zap.String("url", req.RequestURI),
		zap.Int("status", result.Status), zap.Int("len", len(result.Content)),
		zap.Object("head", banexg.HttpHeader(result.Headers)), bodyShort)
	if result.Status >= 400 {
		result.Error = errs.NewMsg(result.Status, "%s  %v", req.URL, result.Content)
	}
	return &result
}
