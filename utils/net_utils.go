package utils

import (
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
	"io"
	"net/http"
)

func DoHttp(client *http.Client, req *http.Request) *banexg.HttpRes {
	rsp, err_ := client.Do(req)
	if err_ != nil {
		return &banexg.HttpRes{Error: errs.New(core.ErrNetReadFail, err_)}
	}
	var result = banexg.HttpRes{Status: rsp.StatusCode, Headers: rsp.Header}
	rspData, err := io.ReadAll(rsp.Body)
	defer func() {
		cerr := rsp.Body.Close()
		// Only overwrite the retured error if the original error was nil and an
		// error occurred while closing the body.
		if err == nil && cerr != nil {
			err = cerr
		}
	}()
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
