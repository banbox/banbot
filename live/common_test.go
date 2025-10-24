package live

import (
	"fmt"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banexg/log"
	"github.com/banbox/cron/v3"
	log2 "log"
	"os"
	"testing"
	"time"
)

func TestCron(t *testing.T) {
	logger := cron.VerbosePrintfLogger(log2.New(os.Stdout, "cron: ", log2.LstdFlags))
	loc, _ := time.LoadLocation("Asia/Shanghai")
	bntpClock := cron.NewNtpClock(loc, "zh-CN")
	c := cron.New(cron.WithSeconds(), cron.WithLogger(logger), cron.WithClock(bntpClock))
	//c := cron.New(cron.WithSeconds(), cron.WithLogger(logger))
	c.AddFunc("0 * * * * *", func() {
		realTime := btime.UTCStamp()      // correct utc timestamp
		sysTime := time.Now().UnixMilli() // local system timestamp
		log.Info(fmt.Sprintf("system: %d, real: %d", sysTime, realTime))
	})
	c.Start()
	time.Sleep(time.Minute * 3)
}
