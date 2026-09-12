package exg

import (
	"maps"
	"slices"
	"time"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/bex"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/bntp"
	"github.com/go-viper/mapstructure/v2"
)

func Setup() *errs.Error {
	if Default != nil {
		return nil
	}
	exgCfg := config.Exchange
	if exgCfg == nil {
		return errs.NewMsg(core.ErrBadConfig, "exchange is required")
	}
	if config.NTPLangCode != "" {
		bntp.LangCode = config.NTPLangCode
	}
	var err *errs.Error
	Default, err = GetWith(exgCfg.Name, core.Market, core.ContractType)
	core.IsContract = banexg.IsContract(core.Market)
	return err
}

func create(name, market, contractType string) (banexg.BanExchange, *errs.Error) {
	return createConfigured(name, market, contractType, config.Exchange, config.Accounts, config.BakAccounts, core.RunEnv, core.NetDisable)
}

func NewForRuntime(snapshot *config.Snapshot, netDisable bool) (banexg.BanExchange, *errs.Error) {
	if snapshot == nil || snapshot.View() == nil || snapshot.View().Exchange == nil {
		return nil, errs.NewMsg(core.ErrBadConfig, "runtime exchange configuration is required")
	}
	cfg := snapshot.View()
	accounts := make(map[string]*config.AccountConfig)
	backups := make(map[string]*config.AccountConfig)
	for _, name := range slices.Sorted(maps.Keys(cfg.Accounts)) {
		account := cfg.Accounts[name]
		if account == nil {
			continue
		}
		if account.NoTrade {
			backups[name] = account
		} else if cfg.Env == core.RunEnvProd {
			accounts[name] = account
		} else if accounts["default"] == nil || name == "default" {
			accounts["default"] = account
		}
	}
	if cfg.Env != core.RunEnvProd && len(accounts) == 0 {
		accounts["default"] = &config.AccountConfig{}
	}
	return createConfigured(cfg.Exchange.Name, cfg.MarketType, cfg.ContractType, cfg.Exchange, accounts, backups, cfg.Env, netDisable)
}

func createConfigured(name, market, contractType string, exchangeConfig *config.ExchangeConfig,
	accounts, backups map[string]*config.AccountConfig, env string, netDisable bool) (banexg.BanExchange, *errs.Error) {
	var exgOpts map[string]interface{}
	if exchangeConfig != nil {
		exgOpts = exchangeConfig.Items[name]
		if exgOpts == nil && exchangeConfig.Name == name {
			exgOpts = exchangeConfig.Items[exchangeConfig.Name]
		}
	}
	var options = map[string]interface{}{}
	for key, val := range exgOpts {
		key = utils.SnakeToCamel(key)
		if key == banexg.OptFees {
			var target = make(map[string]map[string]float64)
			err_ := mapstructure.Decode(val, &target)
			if err_ != nil {
				return nil, errs.New(core.ErrBadConfig, err_)
			}
			options[key] = target
		} else {
			options[key] = val
		}
	}
	accs := map[string]map[string]interface{}{}
	var defAcc string
	for _, key := range slices.Sorted(maps.Keys(backups)) {
		acc := backups[key]
		sec := acc.GetApiSecretFor(name, env)
		accs[key] = map[string]interface{}{
			banexg.OptApiKey:    sec.APIKey,
			banexg.OptApiSecret: sec.APISecret,
			banexg.OptPassword:  sec.Password,
			banexg.OptNoTrade:   true,
		}
		defAcc = key
	}
	for _, key := range slices.Sorted(maps.Keys(accounts)) {
		acc := accounts[key]
		if acc.NoTrade {
			continue
		}
		sec := acc.GetApiSecretFor(name, env)
		accs[key] = map[string]interface{}{
			banexg.OptApiKey:    sec.APIKey,
			banexg.OptApiSecret: sec.APISecret,
			banexg.OptPassword:  sec.Password,
		}
		defAcc = key
	}
	if len(accs) > 0 {
		options[banexg.OptAccCreds] = accs
		if defAcc != "" {
			options[banexg.OptAccName] = defAcc
		}
	}
	if market != "" {
		options[banexg.OptMarketType] = market
	}
	if contractType != "" {
		options[banexg.OptContractType] = contractType
	}
	if env == core.RunEnvTest {
		options[banexg.OptEnv] = env
	}
	exchange, err := bex.New(name, options)
	if err != nil {
		return exchange, err
	}
	if netDisable {
		exchange.SetNetDisable(true)
	}
	return &BotExchange{BanExchange: exchange}, nil
}

func GetWith(name, market, contractType string) (banexg.BanExchange, *errs.Error) {
	if contractType == "" {
		contractType = core.ContractType
	}
	exgMapLock.Lock()
	defer exgMapLock.Unlock()
	cacheKey := name + "@" + market + "@" + contractType
	client, ok := exgMap[cacheKey]
	var err *errs.Error
	if !ok {
		client, err = create(name, market, contractType)
		if err != nil {
			return nil, err
		}
		exgMap[cacheKey] = client
	} else {
		err = client.SetMarketType(market, contractType)
	}
	return client, err
}

func precNum(exchange banexg.BanExchange, symbol string, num float64, source string) (float64, *errs.Error) {
	if exchange == nil {
		if Default == nil {
			return 0, errs.NewMsg(core.ErrExgNotInit, "exchange not loaded")
		}
		exchange = Default
	}
	market, err := exchange.GetMarket(symbol)
	if err != nil {
		return 0, err
	}
	var res float64
	if source == "cost" {
		res, err = exchange.PrecCost(market, num)
	} else if source == "price" {
		res, err = exchange.PrecPrice(market, num)
	} else if source == "amount" {
		// For contract markets with ContractSize != 1, convert coin amount to contracts,
		// apply precision, then convert back to coins.
		// OKX uses contract units for derivatives (sz = number of contracts)
		if market.Contract && market.ContractSize > 0 && market.ContractSize != 1 {
			num = num / market.ContractSize
			res, err = exchange.PrecAmount(market, num)
			if err == nil {
				res = res * market.ContractSize
			}
		} else {
			res, err = exchange.PrecAmount(market, num)
		}
	} else if source == "fee" {
		res, err = exchange.PrecFee(market, num)
	} else {
		panic("invalid source to prec: " + source)
	}
	return res, err
}

func PrecCost(exchange banexg.BanExchange, symbol string, cost float64) (float64, *errs.Error) {
	return precNum(exchange, symbol, cost, "cost")
}

func PrecPrice(exchange banexg.BanExchange, symbol string, price float64) (float64, *errs.Error) {
	return precNum(exchange, symbol, price, "price")
}

func PrecAmount(exchange banexg.BanExchange, symbol string, amount float64) (float64, *errs.Error) {
	return precNum(exchange, symbol, amount, "amount")
}

func GetLeverage(symbol string, notional float64, account string) (float64, float64) {
	return Default.GetLeverage(symbol, notional, account)
}

func GetOdBook(pair string) (*banexg.OrderBook, *errs.Error) {
	book, ok := core.GetOdBook(pair)
	if !ok || book == nil || book.TimeStamp+config.OdBookTtl < btime.TimeMS() {
		var err *errs.Error
		book, err = Default.FetchOrderBook(pair, 1000, nil)
		if err != nil {
			return nil, err
		}
		core.SetOdBook(pair, book)
	}
	return book, nil
}

func GetTickers24Hr() (map[string]*banexg.Ticker, *errs.Error) {
	tickersMap := core.GetCacheVal("tickers", map[string]*banexg.Ticker{})
	if len(tickersMap) > 0 {
		return tickersMap, nil
	}
	tickers, err := Default.FetchTickers(nil, nil)
	if err != nil {
		return nil, err
	}
	for _, t := range tickers {
		tickersMap[t.Symbol] = t
	}
	expires := time.Second * 3600
	core.Cache.SetWithTTL("tickers", tickersMap, 1, expires)
	return tickersMap, nil
}
