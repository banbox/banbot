package rpc

import (
	"fmt"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
	"math"
	"math/rand"
	"testing"
	"time"
)

func TestTrySendExc(t *testing.T) {
	err := config.LoadConfig(&config.CmdArgs{})
	if err != nil {
		panic(err)
	}
	err = core.Setup()
	if err != nil {
		panic(err)
	}
	key := "testMsg"
	text := "this is tpl:"
	count := 0
	for {
		count += 1
		msg := fmt.Sprintf("%s %v", text, count)
		log.Info("try send", zap.String("key", key), zap.String("text", msg))
		TrySendExc(key, msg)
		waitSecs := int(math.Round(rand.Float64() * 10))
		time.Sleep(time.Duration(waitSecs) * time.Second)
	}
}
