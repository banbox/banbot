package biz

import (
	"context"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/log"
	"github.com/banbox/banexg/utils"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"os"
	"testing"
	"time"
)

func TestDataServer(t *testing.T) {
	if os.Getenv("BANBOT_TEST_DATA_SERVER") == "" {
		t.Skip("set BANBOT_TEST_DATA_SERVER=1 to run external gRPC data server integration test")
	}
	const maxMsgSize = 100 * 1024 * 1024
	addr := "127.0.0.1:6789"
	creds := grpc.WithTransportCredentials(insecure.NewCredentials())
	conn, err_ := grpc.NewClient(addr, creds, grpc.WithDefaultCallOptions(
		grpc.MaxCallSendMsgSize(maxMsgSize),
		grpc.MaxCallRecvMsgSize(maxMsgSize),
	))
	if err_ != nil {
		panic(err_)
	}
	client := NewFeaFeederClient(conn)
	ctx := context.Background()
	tfMSecs := int64(utils.TFToSecs("1M") * 1000)
	now := time.Now()
	endMS := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, btime.UTCLocale).UnixMilli()
	pairs := []string{"AAVE/USDT:USDT", "ACH/USDT:USDT", "ADA/USDT:USDT", "AGIX/USDT:USDT", "ALGO/USDT:USDT", "ANKR/USDT:USDT", "APE/USDT:USDT", "API3/USDT:USDT", "APT/USDT:USDT", "AR/USDT:USDT", "ARB/USDT:USDT", "ARPA/USDT:USDT", "ASTR/USDT:USDT", "ATOM/USDT:USDT", "AUCTION/USDT:USDT", "AVAX/USDT:USDT", "AXL/USDT:USDT", "AXS/USDT:USDT", "BADGER/USDT:USDT", "BAKE/USDT:USDT", "BCH/USDT:USDT", "BEAMX/USDT:USDT", "BEL/USDT:USDT", "BICO/USDT:USDT", "BLUR/USDT:USDT", "BNB/USDT:USDT", "BNX/USDT:USDT", "BOND/USDT:USDT", "BSV/USDT:USDT", "BTC/USDT:USDT"}
	res, err_ := client.SubFeatures(ctx, &SubReq{
		Exchange: "binance",
		Market:   banexg.MarketLinear,
		Codes:    pairs,
		Start:    endMS - tfMSecs/4,
		End:      endMS, // first day of month
		Task:     "aifea2",
		Sample:   10,
	})
	if err_ != nil {
		panic(err_)
	}
	for {
		data, err_ := res.Recv()
		if err_ != nil {
			if err_ == io.EOF {
				break
			}
			panic(err_)
		}
		barSecs := data.Mats["bar"].Data[0]
		barShape := data.Mats["bar"].Shape
		barDate := btime.ToDateStr(int64(barSecs*1000), core.DefaultDateFmt)
		log.Info("receive", zap.Int("keys", len(data.Mats)), zap.String("date", barDate),
			zap.Int32s("bar_shape", barShape), zap.Strings("codes", data.Codes))
	}
}
