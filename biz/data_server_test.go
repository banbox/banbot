package biz

import (
	"context"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/internal/testutil"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/banbox/banexg/utils"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"strings"
	"testing"
	"time"
)

func TestDataServerResolvesRequestWithOwnedRuntime(t *testing.T) {
	state := orm.NewSymbolStateWithIdentity("test-exchange", banexg.MarketSpot)
	if err := state.SetExSymbols([]*orm.ExSymbol{{ID: 7, Exchange: "test-exchange", Market: banexg.MarketSpot, Symbol: "BTC/USDT"}}); err != nil {
		t.Fatal(err)
	}
	exchange := &banexg.Exchange{ExgInfo: &banexg.ExgInfo{
		ID: "test-exchange", MarketType: banexg.MarketSpot,
		Markets: banexg.MarketMap{"BTC/USDT": {Symbol: "BTC/USDT", Type: banexg.MarketSpot, Spot: true}},
	}}
	var gotName, gotMarket string
	cleaned := 0
	server, err := NewDataServer(func(_ context.Context, name, market string) (*data.RuntimeDeps, func(), *errs.Error) {
		gotName, gotMarket = name, market
		return &data.RuntimeDeps{Symbols: state, Exchange: exchange}, func() { cleaned++ }, nil
	}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	RegisterFeaGenerator("data-server-test", func([]*orm.ExSymbol, *SubReq, FeaFeeder_SubFeaturesServer) error { return nil })
	exs, _, cleanup, resolveErr := server.resolveFeatures(context.Background(), &SubReq{
		Exchange: "test-exchange", Market: banexg.MarketSpot, Codes: []string{"BTC/USDT", "BTC/USDT"}, Task: "data-server-test",
	})
	if resolveErr != nil {
		t.Fatal(resolveErr)
	}
	if gotName != "test-exchange" || gotMarket != banexg.MarketSpot {
		t.Fatalf("factory identity = %s/%s", gotName, gotMarket)
	}
	if len(exs) != 1 || exs[0].ID != 7 {
		t.Fatalf("symbols = %#v", exs)
	}
	cleanup()
	if cleaned != 1 {
		t.Fatalf("cleanup calls = %d, want 1", cleaned)
	}
}

func TestDataServerReleasesRuntimeWhenTaskIsUnknown(t *testing.T) {
	cleaned := 0
	server, err := NewDataServer(func(context.Context, string, string) (*data.RuntimeDeps, func(), *errs.Error) {
		return &data.RuntimeDeps{Symbols: orm.NewSymbolState(), Exchange: &banexg.Exchange{ExgInfo: &banexg.ExgInfo{ID: "test", MarketType: banexg.MarketSpot}}}, func() { cleaned++ }, nil
	}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, resolveErr := server.resolveFeatures(context.Background(), &SubReq{Exchange: "test", Market: banexg.MarketSpot, Task: "not-registered"})
	if resolveErr == nil || !strings.Contains(resolveErr.Error(), "unsupported data task") {
		t.Fatalf("error = %v", resolveErr)
	}
	if cleaned != 1 {
		t.Fatalf("cleanup calls = %d, want 1", cleaned)
	}
}

func TestFeatureGeneratorSnapshotCannotMutateRegistry(t *testing.T) {
	const task = "data-server-registry-snapshot-test"
	gen := func([]*orm.ExSymbol, *SubReq, FeaFeeder_SubFeaturesServer) error { return nil }
	RegisterFeaGenerator(task, gen)
	if got, ok := GetFeaGenerator(task); !ok || got == nil {
		t.Fatal("registered feature generator was not found")
	}
	snapshot := SnapshotFeaGenerators()
	delete(snapshot, task)
	if _, ok := GetFeaGenerator(task); !ok {
		t.Fatal("snapshot mutation changed feature generator registry")
	}
}

func TestDataServerRejectsMismatchedRuntimeIdentity(t *testing.T) {
	cleaned := false
	server, err := NewDataServer(func(context.Context, string, string) (*data.RuntimeDeps, func(), *errs.Error) {
		return &data.RuntimeDeps{
			Symbols:  orm.NewSymbolStateWithIdentity("other", banexg.MarketSpot),
			Exchange: &banexg.Exchange{ExgInfo: &banexg.ExgInfo{ID: "other", MarketType: banexg.MarketSpot}},
		}, func() { cleaned = true }, nil
	}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, resolveErr := server.resolveFeatures(context.Background(), &SubReq{
		Exchange: "requested", Market: banexg.MarketSpot, Task: "unused",
	})
	if resolveErr == nil || !strings.Contains(resolveErr.Error(), "does not match request") {
		t.Fatalf("mismatched runtime identity error = %v", resolveErr)
	}
	if !cleaned {
		t.Fatal("mismatched request runtime was not cleaned up")
	}
}

func TestDataServerServesOwnedRuntimeAndStops(t *testing.T) {
	state := orm.NewSymbolStateWithIdentity("serve-exchange", banexg.MarketSpot)
	if err := state.SetExSymbols([]*orm.ExSymbol{{ID: 8, Exchange: "serve-exchange", Market: banexg.MarketSpot, Symbol: "ETH/USDT"}}); err != nil {
		t.Fatal(err)
	}
	exchange := &banexg.Exchange{ExgInfo: &banexg.ExgInfo{
		ID: "serve-exchange", MarketType: banexg.MarketSpot,
		Markets: banexg.MarketMap{"ETH/USDT": {Symbol: "ETH/USDT", Type: banexg.MarketSpot, Spot: true}},
	}}
	cleaned := make(chan struct{}, 1)
	server, err := NewDataServer(func(context.Context, string, string) (*data.RuntimeDeps, func(), *errs.Error) {
		return &data.RuntimeDeps{Symbols: state, Exchange: exchange}, func() { cleaned <- struct{}{} }, nil
	}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	RegisterFeaGenerator("data-server-serve-test", func([]*orm.ExSymbol, *SubReq, FeaFeeder_SubFeaturesServer) error { return nil })
	serveDone := make(chan *errs.Error, 1)
	go func() { serveDone <- server.Serve(context.Background(), "127.0.0.1:0") }()
	deadline := time.Now().Add(time.Second)
	for server.Address() == "" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	addr := server.Address()
	if addr == "" {
		server.Stop()
		t.Fatal("data server did not bind")
	}
	conn, dialErr := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if dialErr != nil {
		server.Stop()
		t.Fatal(dialErr)
	}
	defer conn.Close()
	client := NewFeaFeederClient(conn)
	stream, streamErr := client.SubFeatures(context.Background(), &SubReq{Exchange: "serve-exchange", Market: banexg.MarketSpot, Codes: []string{"ETH/USDT"}, Task: "data-server-serve-test"})
	if streamErr != nil {
		server.Stop()
		t.Fatal(streamErr)
	}
	if _, recvErr := stream.Recv(); recvErr != io.EOF {
		server.Stop()
		t.Fatalf("receive error = %v, want EOF", recvErr)
	}
	select {
	case <-cleaned:
	case <-time.After(time.Second):
		server.Stop()
		t.Fatal("request runtime cleanup was not called")
	}
	server.Stop()
	if serveErr := <-serveDone; serveErr != nil {
		t.Fatal(serveErr)
	}
}

func TestDataServerStopAndJoinWaitForActiveHandler(t *testing.T) {
	state := orm.NewSymbolStateWithIdentity("stop-exchange", banexg.MarketSpot)
	if err := state.SetExSymbols([]*orm.ExSymbol{{ID: 9, Exchange: "stop-exchange", Market: banexg.MarketSpot, Symbol: "SOL/USDT"}}); err != nil {
		t.Fatal(err)
	}
	exchange := &banexg.Exchange{ExgInfo: &banexg.ExgInfo{
		ID: "stop-exchange", MarketType: banexg.MarketSpot,
		Markets: banexg.MarketMap{"SOL/USDT": {Symbol: "SOL/USDT", Type: banexg.MarketSpot, Spot: true}},
	}}
	server, err := NewDataServer(func(context.Context, string, string) (*data.RuntimeDeps, func(), *errs.Error) {
		return &data.RuntimeDeps{Symbols: state, Exchange: exchange}, func() {}, nil
	}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	RegisterFeaGenerator("data-server-stop-test", func([]*orm.ExSymbol, *SubReq, FeaFeeder_SubFeaturesServer) error {
		close(started)
		server.Stop() // A callback must be able to stop its owner without self-waiting.
		<-release
		return nil
	})
	serveDone := make(chan *errs.Error, 1)
	go func() { serveDone <- server.Serve(context.Background(), "127.0.0.1:0") }()
	deadline := time.Now().Add(time.Second)
	for server.Address() == "" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if addr := server.Address(); addr == "" {
		server.Stop()
		t.Fatal("data server did not bind")
	} else {
		conn, dialErr := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if dialErr != nil {
			server.Stop()
			t.Fatal(dialErr)
		}
		defer conn.Close()
		if _, streamErr := NewFeaFeederClient(conn).SubFeatures(context.Background(), &SubReq{Exchange: "stop-exchange", Market: banexg.MarketSpot, Codes: []string{"SOL/USDT"}, Task: "data-server-stop-test"}); streamErr != nil {
			server.Stop()
			t.Fatal(streamErr)
		}
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		server.Stop()
		t.Fatal("feature handler did not start")
	}
	joinDone := make(chan struct{})
	go func() { server.Join(); close(joinDone) }()
	select {
	case <-joinDone:
		t.Fatal("Join returned while feature handler was active")
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	select {
	case <-joinDone:
	case <-time.After(time.Second):
		t.Fatal("Join did not wait for feature handler")
	}
	if serveErr := <-serveDone; serveErr != nil {
		t.Fatal(serveErr)
	}
	if serveErr := server.Serve(context.Background(), "127.0.0.1:0"); serveErr == nil {
		t.Fatal("second Serve unexpectedly succeeded")
	}
}

func TestDataServerStopBeforeServeDoesNotBind(t *testing.T) {
	server, err := NewDataServer(func(context.Context, string, string) (*data.RuntimeDeps, func(), *errs.Error) {
		return nil, nil, nil
	}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	server.Stop()
	server.Join()
	if serveErr := server.Serve(context.Background(), "127.0.0.1:0"); serveErr != nil {
		t.Fatal(serveErr)
	}
	if address := server.Address(); address != "" {
		t.Fatalf("stopped server bound %s", address)
	}
}

func TestDataServer(t *testing.T) {
	testutil.RequireIntegration(t)
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
