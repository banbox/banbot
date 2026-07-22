package orm

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

type historicalMarketSnapshot struct {
	Exchange   string           `json:"exchange"`
	MarketType string           `json:"market_type"`
	Markets    banexg.MarketMap `json:"markets"`
}

func hasConfiguredMarketSnapshot() bool {
	if !core.BackTestMode || config.Exchange == nil {
		return false
	}
	return config.Exchange.Items[config.Exchange.Name]["market_snapshot"] != nil
}

func applyConfiguredMarketSnapshot(exchange banexg.BanExchange, markets banexg.MarketMap) *errs.Error {
	if !core.BackTestMode || config.Exchange == nil {
		return nil
	}
	options := config.Exchange.Items[config.Exchange.Name]
	rawPath, _ := options["market_snapshot"].(string)
	if rawPath == "" {
		return nil
	}
	expectedHash, _ := options["market_snapshot_sha256"].(string)
	if decoded, err := hex.DecodeString(expectedHash); err != nil || len(decoded) != sha256.Size {
		return errs.NewMsg(core.ErrBadConfig, "market_snapshot_sha256 is required")
	}
	if !strings.HasPrefix(rawPath, "@") {
		return errs.NewMsg(core.ErrBadConfig, "market_snapshot must use a BanDataDir-relative @ path")
	}
	path := config.ParsePath(rawPath)
	root, err := filepath.EvalSymlinks(filepath.Clean(config.GetDataDir()))
	if err != nil {
		return errs.New(core.ErrIOReadFail, err)
	}
	resolved, err := filepath.EvalSymlinks(filepath.Clean(path))
	if err != nil {
		return errs.New(core.ErrIOReadFail, err)
	}
	rel, err := filepath.Rel(root, resolved)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return errs.NewMsg(core.ErrBadConfig, "market_snapshot escapes BanDataDir")
	}
	data, err := os.ReadFile(resolved)
	if err != nil {
		return errs.New(core.ErrIOReadFail, err)
	}
	actualHash := fmt.Sprintf("%x", sha256.Sum256(data))
	if actualHash != expectedHash {
		return errs.NewMsg(core.ErrBadConfig, "market_snapshot_sha256 mismatch")
	}
	var snapshot historicalMarketSnapshot
	if err = json.Unmarshal(data, &snapshot); err != nil {
		return errs.New(errs.CodeUnmarshalFail, err)
	}
	count, err := mergeHistoricalMarketSnapshot(exchange.Info(), markets, &snapshot)
	if err != nil {
		return errs.New(core.ErrBadConfig, err)
	}
	log.Info("historical market snapshot applied", zap.String("path", rawPath),
		zap.String("sha256", actualHash), zap.Int("markets", count))
	return nil
}

func mergeHistoricalMarketSnapshot(info *banexg.ExgInfo, markets banexg.MarketMap,
	snapshot *historicalMarketSnapshot,
) (int, error) {
	if info == nil || snapshot == nil || len(snapshot.Markets) == 0 {
		return 0, fmt.Errorf("market snapshot is empty")
	}
	if snapshot.Exchange != info.ID || snapshot.MarketType != info.MarketType {
		return 0, fmt.Errorf("market snapshot identity mismatch")
	}
	count := 0
	for symbol, historical := range snapshot.Markets {
		if historical == nil || historical.Symbol != symbol || historical.Precision == nil {
			continue
		}
		marketType := historicalMarketType(historical)
		if marketType != info.MarketType {
			continue
		}
		precision := *historical.Precision
		if precision.ModeAmount == 0 {
			precision.ModeAmount = banexg.PrecModeDecimalPlace
		}
		if precision.ModePrice == 0 {
			precision.ModePrice = banexg.PrecModeDecimalPlace
		}
		if precision.ModeBase == 0 {
			precision.ModeBase = banexg.PrecModeDecimalPlace
		}
		if precision.ModeQuote == 0 {
			precision.ModeQuote = banexg.PrecModeDecimalPlace
		}
		current := markets[symbol]
		if current == nil {
			copy := *historical
			copy.Type = marketType
			copy.Active = false
			copy.Precision = &precision
			copy.Limits = cloneMarketLimits(historical.Limits)
			current = &copy
			markets[symbol] = current
		} else {
			current.Precision = &precision
			current.Limits = cloneMarketLimits(historical.Limits)
			if historical.ContractSize > 0 {
				current.ContractSize = historical.ContractSize
			}
		}
		count++
	}
	if count == 0 {
		return 0, fmt.Errorf("market snapshot has no %s markets", info.MarketType)
	}
	rebuildMarketIndexes(info, markets)
	return count, nil
}

func historicalMarketType(market *banexg.Market) string {
	switch {
	case market.Linear:
		return banexg.MarketLinear
	case market.Inverse:
		return banexg.MarketInverse
	case market.Option:
		return banexg.MarketOption
	case market.Spot:
		return banexg.MarketSpot
	default:
		return market.Type
	}
}

func cloneMarketLimits(limits *banexg.MarketLimits) *banexg.MarketLimits {
	if limits == nil {
		return nil
	}
	copy := *limits
	clone := func(value *banexg.LimitRange) *banexg.LimitRange {
		if value == nil {
			return nil
		}
		result := *value
		return &result
	}
	copy.Leverage = clone(limits.Leverage)
	copy.Amount = clone(limits.Amount)
	copy.Price = clone(limits.Price)
	copy.Cost = clone(limits.Cost)
	copy.Market = clone(limits.Market)
	return &copy
}

func rebuildMarketIndexes(info *banexg.ExgInfo, markets banexg.MarketMap) {
	byID := make(banexg.MarketArrMap)
	for _, market := range markets {
		byID[market.ID] = append(byID[market.ID], market)
	}
	info.MarketsLock.Lock()
	info.MarketsByIdLock.Lock()
	info.Markets = markets
	info.MarketsById = byID
	info.MarketsByIdLock.Unlock()
	info.MarketsLock.Unlock()
}
