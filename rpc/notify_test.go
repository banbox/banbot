package rpc

import (
	"fmt"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
	"testing"
)

func TestTrySendExc(t *testing.T) {
	err := config.LoadConfig(&config.CmdArgs{})
	if err != nil {
		t.Fatal(err)
	}
	err = core.Setup()
	if err != nil {
		t.Fatal(err)
	}
	for count := 1; count <= 3; count++ {
		msg := fmt.Sprintf("this is tpl: %d", count)
		log.Info("try send", zap.String("key", "testMsg"), zap.String("text", msg))
		TrySendExc("testMsg", msg)
	}
}
