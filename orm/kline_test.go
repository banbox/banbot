package orm

import (
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
	"testing"
)

func initApp() *errs.Error {
	var args config.CmdArgs
	err := config.LoadConfig(&args)
	if err != nil {
		return err
	}
	config.Args.SetLog(true)
	err = exg.Setup()
	if err != nil {
		return err
	}
	return Setup()
}

func TestGetKrange(t *testing.T) {
	t.Skip("integration test (requires database)")
	err := initApp()
	if err != nil {
		panic(err)
	}
	_, conn, err := Conn(nil)
	if err != nil {
		panic(err)
	}
	defer conn.Release()
	start, stop := PubQ().GetKlineRange(12, "1m")
	log.Info("krange", zap.Int64("start", start), zap.Int64("stop", stop))
}
