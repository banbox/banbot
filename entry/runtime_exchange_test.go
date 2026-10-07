package entry

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type explicitExchangeStub struct {
	banexg.BanExchange
	info           *banexg.ExgInfo
	contract       bool
	loadCalls      int
	initCalls      int
	bracketLoads   int
	loadBracketErr *errs.Error
}

func (e *explicitExchangeStub) Info() *banexg.ExgInfo { return e.info }

func (e *explicitExchangeStub) IsContract(string) bool { return e.contract }

func (e *explicitExchangeStub) LoadMarkets(bool, map[string]interface{}) (banexg.MarketMap, *errs.Error) {
	e.loadCalls++
	return banexg.MarketMap{}, nil
}

func (e *explicitExchangeStub) InitLeverageBrackets() *errs.Error {
	e.initCalls++
	return nil
}

func (e *explicitExchangeStub) LoadLeverageBrackets(bool, map[string]interface{}) *errs.Error {
	e.bracketLoads++
	return e.loadBracketErr
}

func TestInitializeExplicitExchangeInitializesContractBrackets(t *testing.T) {
	contract := &explicitExchangeStub{info: &banexg.ExgInfo{MarketType: banexg.MarketLinear}, contract: true}
	if err := initializeExplicitExchange(contract, nil, true, core.RunModeBackTest, nil); err != nil {
		t.Fatal(err)
	}
	if contract.loadCalls != 1 || contract.bracketLoads != 0 || contract.initCalls != 1 {
		t.Fatalf("offline contract calls markets=%d brackets=%d init=%d", contract.loadCalls, contract.bracketLoads, contract.initCalls)
	}

	spot := &explicitExchangeStub{info: &banexg.ExgInfo{MarketType: banexg.MarketSpot}}
	if err := initializeExplicitExchange(spot, nil, true, core.RunModeBackTest, nil); err != nil {
		t.Fatal(err)
	}
	if spot.loadCalls != 1 || spot.initCalls != 0 {
		t.Fatalf("spot calls load=%d init=%d", spot.loadCalls, spot.initCalls)
	}

	online := &explicitExchangeStub{info: &banexg.ExgInfo{MarketType: banexg.MarketLinear}, contract: true}
	if err := initializeExplicitExchange(online, nil, false, core.RunModeLive, nil); err != nil {
		t.Fatal(err)
	}
	if online.loadCalls != 1 || online.bracketLoads != 1 || online.initCalls != 0 {
		t.Fatalf("online contract calls markets=%d brackets=%d init=%d", online.loadCalls, online.bracketLoads, online.initCalls)
	}
}

func TestNormalizeDevChildConfigMatchesSelectedMarket(t *testing.T) {
	cfg := &config.Config{Exchange: &config.ExchangeConfig{Name: "parent"}, MarketType: banexg.MarketLinear, ContractType: banexg.MarketFuture}
	normalizeDevChildConfig(cfg, "spot-exchange", banexg.MarketSpot)
	if cfg.Exchange.Name != "spot-exchange" || cfg.MarketType != banexg.MarketSpot || cfg.ContractType != "" {
		t.Fatalf("spot child config = %#v", cfg)
	}
	normalizeDevChildConfig(cfg, "linear-exchange", banexg.MarketLinear)
	if cfg.Exchange.Name != "linear-exchange" || cfg.MarketType != banexg.MarketLinear || cfg.ContractType != banexg.MarketSwap {
		t.Fatalf("linear child config = %#v", cfg)
	}
}

func TestInitializeExplicitExchangeUsesSnapshotBracketsAndLogsFallback(t *testing.T) {
	snapshot := explicitMarketSnapshotFixture(t, false)
	configured := &explicitExchangeStub{info: &banexg.ExgInfo{ID: "binance", MarketType: banexg.MarketLinear}, contract: true}
	if err := initializeExplicitExchange(configured, snapshot, false, core.RunModeBackTest, nil); err != nil {
		t.Fatal(err)
	}
	if configured.bracketLoads != 0 || configured.initCalls != 1 {
		t.Fatalf("snapshot calls brackets=%d init=%d", configured.bracketLoads, configured.initCalls)
	}
	liveConfigured := &explicitExchangeStub{info: &banexg.ExgInfo{MarketType: banexg.MarketLinear}, contract: true}
	if err := initializeExplicitExchange(liveConfigured, snapshot, false, core.RunModeLive, nil); err != nil {
		t.Fatal(err)
	}
	if liveConfigured.bracketLoads != 1 || liveConfigured.initCalls != 0 {
		t.Fatalf("live snapshot calls brackets=%d init=%d", liveConfigured.bracketLoads, liveConfigured.initCalls)
	}

	var output bytes.Buffer
	logger := zap.New(zapcore.NewCore(zapcore.NewConsoleEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(&output), zap.ErrorLevel))
	fallback := &explicitExchangeStub{info: &banexg.ExgInfo{MarketType: banexg.MarketLinear}, contract: true}
	fallback.loadBracketErr = errs.NewMsg(errs.CodeRunTime, "unavailable")
	if err := initializeExplicitExchange(fallback, nil, false, core.RunModeLive, logger); err != nil {
		t.Fatal(err)
	}
	if fallback.bracketLoads != 1 || fallback.initCalls != 1 {
		t.Fatalf("fallback calls brackets=%d init=%d", fallback.bracketLoads, fallback.initCalls)
	}
	if !bytes.Contains(output.Bytes(), []byte("maint margin calculation may have large deviation")) {
		t.Fatalf("missing fallback warning: %s", output.String())
	}
}

func explicitMarketSnapshotFixture(t *testing.T, invalidHash bool) *config.Snapshot {
	t.Helper()
	const symbol = "BTC/USDT:USDT"
	data, err := json.Marshal(map[string]any{
		"exchange": "binance", "market_type": banexg.MarketLinear,
		"markets": banexg.MarketMap{symbol: {
			ID: "BTCUSDT", Symbol: symbol, Linear: true, Contract: true,
			Precision: &banexg.Precision{Price: 2, Amount: 3},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "markets.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(data))
	if invalidHash {
		hash = strings.Repeat("0", 64)
	}
	return config.NewSnapshotWithDirs(&config.Config{Exchange: &config.ExchangeConfig{
		Name: "binance", Items: map[string]map[string]interface{}{
			"binance": {"market_snapshot": "@markets.json", "market_snapshot_sha256": hash},
		},
	}, MarketType: banexg.MarketLinear}, root, "")
}

func TestInitializeExplicitExchangeOfflineSnapshotAvoidsMarketRequest(t *testing.T) {
	for _, mode := range []string{core.RunModeData, core.RunModeBackTest} {
		t.Run(mode, func(t *testing.T) {
			exchange := &explicitExchangeStub{info: &banexg.ExgInfo{ID: "binance", MarketType: banexg.MarketLinear}, contract: true}
			if err := initializeExplicitExchange(exchange, explicitMarketSnapshotFixture(t, false), true, mode, nil); err != nil {
				t.Fatal(err)
			}
			if exchange.loadCalls != 0 || exchange.bracketLoads != 0 || exchange.initCalls != 1 || exchange.info.Markets["BTC/USDT:USDT"] == nil {
				t.Fatalf("snapshot initialization called exchange markets=%d brackets=%d init=%d; markets=%#v", exchange.loadCalls, exchange.bracketLoads, exchange.initCalls, exchange.info.Markets)
			}
		})
	}
}

func TestInitializeExplicitExchangeRejectsInvalidSnapshotHash(t *testing.T) {
	exchange := &explicitExchangeStub{info: &banexg.ExgInfo{ID: "binance", MarketType: banexg.MarketLinear}, contract: true}
	err := initializeExplicitExchange(exchange, explicitMarketSnapshotFixture(t, true), true, core.RunModeData, nil)
	if err == nil || !strings.Contains(err.Error(), "market_snapshot_sha256 mismatch") {
		t.Fatalf("invalid snapshot hash error = %v", err)
	}
	if exchange.loadCalls != 0 || exchange.initCalls != 0 || exchange.bracketLoads != 0 {
		t.Fatal("invalid snapshot fell back to exchange initialization")
	}
}
