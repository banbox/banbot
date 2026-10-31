package runtime

import (
	"errors"
	"net"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm"
)

func TestHubPublicProviderStartsWithoutLocalAccountAndStopsOnSourceError(t *testing.T) {
	for _, mode := range []string{core.RunModeData, core.RunModeLive} {
		t.Run(mode, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			accepted := make(chan net.Conn, 1)
			go func() {
				conn, err := listener.Accept()
				if err == nil {
					accepted <- conn
				}
			}()
			process := NewProcess()
			defer process.Close()
			rt, err := process.NewRuntime(Options{Mode: mode, Env: core.RunEnvDryRun, HubMarketData: true, Exchange: &observerRuntimeExchange{}, Config: &config.Config{Exchange: &config.ExchangeConfig{Name: "observer"}, MarketType: "linear", SpiderAddr: listener.Addr().String()}})
			if err != nil {
				t.Fatal(err)
			}
			provider, providerErr := rt.NewFactorLiveProvider(func(*orm.DataSeries) {}, nil)
			if providerErr != nil {
				t.Fatal("remote provider required a local owner", providerErr)
			}
			conn := <-accepted
			defer conn.Close()
			defer provider.Stop()
			if rt.SharedExecution() != nil {
				t.Fatal("public runtime created a local account")
			}
			primary := errors.New("remote public source failed")
			failures := make(chan error, 2)
			rt.failFactorLive(nil, nil, failures, primary)
			if err := <-failures; !errors.Is(err, primary) {
				t.Fatal("source failure lost", err)
			}
			select {
			case <-rt.Context().Done():
			default:
				t.Fatal("remote source failure left runtime active")
			}
		})
	}
}

func TestHubPublicProviderRejectsBacktestAndLocalOwner(t *testing.T) {
	process := NewProcess()
	defer process.Close()
	for _, options := range []Options{
		{HubMarketData: true, Mode: core.RunModeBackTest, Env: core.RunEnvDryRun, Exchange: &observerRuntimeExchange{}},
		{HubMarketData: true, Mode: core.RunModeData, Env: core.RunEnvDryRun, Exchange: &observerRuntimeExchange{}, SharedExecution: &execution.SharedExecutionOptions{}},
	} {
		if rt, err := process.NewRuntime(options); err == nil {
			rt.Close()
			t.Fatal("invalid remote public composition admitted")
		}
	}
}
