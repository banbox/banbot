package orm

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/sasha-s/go-deadlock"
	"go.uber.org/zap"
)

var (
	keySymbolMap  = make(map[string]*ExSymbol)
	idSymbolMap   = make(map[int32]*ExSymbol)
	marketMap     = make(map[string]int)
	pairsMap      = make(map[string]*ExSymbol) // 获取1m的品种
	hourPairsMap  = make(map[string]*ExSymbol) // 只获取1h及以上的品种
	symbolLock    deadlock.Mutex
	tryListIds    = make(map[int32]bool)
	tryListLock   deadlock.Mutex
	hourPairsLock deadlock.Mutex
	maxSid        int32 // in-memory sid counter, protected by symbolLock
	aggRulesMu    sync.RWMutex
	aggRules      = map[string]AggRuleFunc{
		"min":   aggMin,
		"max":   aggMax,
		"last":  aggLast,
		"first": aggFirst,
		"sum":   aggSum,
		"avg":   aggAvg,
		"mid":   aggMid,
	}
)

type AggRuleFunc func(rows []*DataRecord, field SeriesField) (any, error)

func cacheExSymbol(exs *ExSymbol) {
	if exs == nil {
		return
	}
	if exs.ID > maxSid {
		maxSid = exs.ID
	}
	idSymbolMap[exs.ID] = exs
	key := exSymbolKey(exs.Exchange, exs.Market, exs.Symbol, exs.ExgReal)
	if cur, ok := keySymbolMap[key]; ok {
		if cur.ExgReal == "" || exs.ExgReal != "" {
			return
		}
	}
	keySymbolMap[key] = exs
	market := fmt.Sprintf("%s:%s", exs.Exchange, exs.Market)
	marketMap[market] = marketMap[market] + 1
}

func exSymbolKey(exchange, market, symbol string, _ ...string) string {
	return fmt.Sprintf("%s:%s:%s", exchange, market, symbol)
}

func findExSymbol(exchange, market, symbol string) *ExSymbol {
	if item, ok := keySymbolMap[exSymbolKey(exchange, market, symbol)]; ok {
		return item
	}
	return nil
}

func (q *Queries) LoadExgSymbols(exgName string) *errs.Error {
	ctx := context.Background()
	exsList, err := q.ListSymbols(ctx, exgName)
	if err != nil {
		return NewDbErr(core.ErrDbReadFail, err)
	}
	for _, exs := range exsList {
		cacheExSymbol(exs)
	}
	return nil
}

func GetExSymbols(exgName, market string) map[int32]*ExSymbol {
	var res = make(map[int32]*ExSymbol)
	for _, exs := range keySymbolMap {
		if exgName != "" && exs.Exchange != exgName {
			continue
		}
		if market != "" && exs.Market != market {
			continue
		}
		res[exs.ID] = exs
	}
	return res
}

func GetExSymbolMap(exgName, market string) map[string]*ExSymbol {
	var res = make(map[string]*ExSymbol)
	for _, exs := range keySymbolMap {
		if exgName != "" && exs.Exchange != exgName {
			continue
		}
		if market != "" && exs.Market != market {
			continue
		}
		if cur, ok := res[exs.Symbol]; !ok || cur.ExgReal != "" && exs.ExgReal == "" {
			res[exs.Symbol] = exs
		}
	}
	return res
}

func GetSymbolByID(id int32) *ExSymbol {
	item, ok := idSymbolMap[id]
	if !ok {
		return nil
	}
	return item
}

func GetExSymbolCur(symbol string) (*ExSymbol, *errs.Error) {
	return GetExSymbol(exg.Default, symbol)
}

func GetExSymbol(exchange banexg.BanExchange, symbol string) (*ExSymbol, *errs.Error) {
	market, err := exchange.GetMarket(symbol)
	// It is not immediately exited here, it may be delisted, and it is returned empty, but there is historical data, you can try to get it from the cache below
	// 这里不立即退出，可能退市了这里返回空，但有历史数据，可尝试从下面缓存获取
	exgInfo := exchange.Info()
	var marketType = exgInfo.MarketType
	if market != nil {
		marketType = market.Type
	}
	item := findExSymbol(exgInfo.ID, marketType, symbol)
	if item == nil {
		if err == nil {
			err = errs.NewMsg(core.ErrInvalidSymbol, "%s not exist in %d cache", symbol, len(keySymbolMap))
		}
		return nil, err
	}
	return item, nil
}

func GetExSymbol2(exgName, market, symbol string, exgReal ...string) *ExSymbol {
	return findExSymbol(exgName, market, symbol)
}

func EnsureExSymbol(exchange, market, symbol string, exgReal ...string) (*ExSymbol, error) {
	if exchange == "" {
		return nil, fmt.Errorf("exchange is required")
	}
	if market == "" {
		return nil, fmt.Errorf("market is required")
	}
	if symbol == "" {
		return nil, fmt.Errorf("symbol is required")
	}
	if item := GetExSymbol2(exchange, market, symbol, exgReal...); item != nil {
		return item, nil
	}
	exs := &ExSymbol{
		Exchange: exchange,
		Market:   market,
		Symbol:   symbol,
	}
	if len(exgReal) > 0 {
		exs.ExgReal = exgReal[0]
	}
	if err := EnsureSymbols([]*ExSymbol{exs}, exchange); err != nil {
		return nil, err
	}
	if item := GetExSymbol2(exchange, market, symbol, exgReal...); item != nil {
		return item, nil
	}
	return nil, fmt.Errorf("ensure exsymbol failed for %s:%s:%s", exchange, market, symbol)
}

func makeExSymbolFromAdd(id int32, item AddSymbolsParams) *ExSymbol {
	return &ExSymbol{
		ID:       id,
		Exchange: item.Exchange,
		ExgReal:  item.ExgReal,
		Market:   item.Market,
		Symbol:   item.Symbol,
		Combined: item.Combined,
		ListMs:   item.ListMs,
		DelistMs: item.DelistMs,
		AggRules: item.AggRules,
	}
}

func EnsureExgSymbols(exchange banexg.BanExchange) *errs.Error {
	_, err := LoadMarkets(exchange, false)
	if err != nil {
		return err
	}
	exInfo := exchange.Info()
	exgId := exInfo.ID
	marMap := exchange.GetCurMarkets()
	exsList := make([]*ExSymbol, 0, len(marMap))
	for symbol, market := range marMap {
		exsList = append(exsList, &ExSymbol{
			Exchange: exgId,
			Market:   market.Type,
			Symbol:   symbol,
			Combined: market.Combined,
			ListMs:   market.Created,
			DelistMs: market.Expiry,
		})
	}
	err = EnsureSymbols(exsList, exgId)
	if err != nil {
		return err
	}
	if len(exInfo.Markets) == 0 {
		// China Futures needs to call LoadMarkets again after EnsureSymbols to pass in symbols for the loading to be successful
		// 中国期货需要在EnsureSymbols后再次调用LoadMarkets传入symbols才能加载成功
		_, err = LoadMarkets(exchange, false)
	} else {
		// Mark the coins that are not returned by the exchange as delisted
		var editList []*ExSymbol
		for _, exs := range idSymbolMap {
			if exs.Exchange != exInfo.ID || exs.Market != exInfo.MarketType || exs.DelistMs > 0 {
				continue
			}
			if _, ok := exInfo.Markets[exs.Symbol]; !ok {
				exs.DelistMs = btime.UTCStamp()
				editList = append(editList, exs)
			}
		}
		if len(editList) > 0 {
			pq, conn2, err2 := Conn(nil)
			if err2 != nil {
				return err2
			}
			defer conn2.Release()
			for _, exs := range editList {
				err_ := pq.SetListMS(context.Background(), SetListMSParams{
					ID:       exs.ID,
					ListMs:   exs.ListMs,
					DelistMs: exs.DelistMs,
				})
				if err_ != nil {
					return NewDbErr(core.ErrDbExecFail, err_)
				}
			}
		}
	}
	return err
}

func EnsureCurSymbols(symbols []string) *errs.Error {
	exsList := make([]*ExSymbol, 0, len(symbols))
	exgId := config.Exchange.Name
	marketType := core.Market
	marMap, err := LoadMarkets(exg.Default, false)
	if err != nil {
		return err
	}
	for _, symbol := range symbols {
		mar, ok := marMap[symbol]
		if !ok {
			return errs.NewMsg(core.ErrInvalidSymbol, "symbol %s not found", symbol)
		}
		exsList = append(exsList, &ExSymbol{
			Exchange: exgId,
			Market:   marketType,
			Symbol:   symbol,
			Combined: mar.Combined,
			ListMs:   mar.Created,
			DelistMs: mar.Expiry,
		})
	}
	return EnsureSymbols(exsList, exgId)
}

func EnsureSymbols(symbols []*ExSymbol, exchanges ...string) *errs.Error {
	var err *errs.Error
	var exgNames = make(map[string]bool)
	for _, exs := range symbols {
		exgNames[exs.Exchange] = true
	}
	for _, name := range exchanges {
		exgNames[name] = true
	}
	pq, conn2, err2 := Conn(nil)
	if err2 != nil {
		return err2
	}
	defer conn2.Release()
	if len(keySymbolMap) == 0 {
		// Not yet loaded, load the information of all the underlying assets of the specified exchange
		// 尚未加载，加载指定交易所所有标的信息
		for exgId := range exgNames {
			err = pq.LoadExgSymbols(exgId)
			if err != nil {
				return err
			}
		}
	}
	// Check symbols that need to be inserted
	// 检查需要插入的标的
	adds := map[string]*ExSymbol{}
	for _, exs := range symbols {
		key := exSymbolKey(exs.Exchange, exs.Market, exs.Symbol)
		if item, ok := keySymbolMap[key]; !ok {
			adds[key] = exs
		} else {
			exs.ID = item.ID
			exs.ListMs = item.ListMs
			exs.DelistMs = item.DelistMs
			exs.Combined = item.Combined
			exs.AggRules = item.AggRules
		}
	}
	if len(adds) == 0 {
		return nil
	}
	// Lock, reload, and add the data that needs to be added
	// 加锁，重新加载，然后添加需要添加的数据
	symbolLock.Lock()
	defer symbolLock.Unlock()
	for exgId := range exgNames {
		err = pq.LoadExgSymbols(exgId)
		if err != nil {
			return err
		}
	}
	argList := make([]AddSymbolsParams, 0, len(adds))
	for _, item := range adds {
		key := exSymbolKey(item.Exchange, item.Market, item.Symbol)
		if _, ok := keySymbolMap[key]; ok {
			continue
		}
		argList = append(argList, AddSymbolsParams{Exchange: item.Exchange, ExgReal: item.ExgReal,
			Market: item.Market, Symbol: item.Symbol, Combined: item.Combined, ListMs: item.ListMs, DelistMs: item.DelistMs,
			AggRules: item.AggRules})
	}
	_, err_ := pq.AddSymbols(context.Background(), argList)
	if err_ != nil {
		errMsg := err_.Error()
		if strings.Contains(errMsg, "SQLSTATE 22001") {
			log.Error("save fail, data too lang", zap.Error(err_), zap.Any("data", argList))
		}
		return NewDbErr(core.ErrDbExecFail, err_)
	}
	for exgId := range exgNames {
		err = pq.LoadExgSymbols(exgId)
		if err != nil {
			return err
		}
	}
	// 刷新Sid
	for _, exs := range symbols {
		item := findExSymbol(exs.Exchange, exs.Market, exs.Symbol)
		if item == nil {
			continue
		}
		exs.ID = item.ID
		exs.ListMs = item.ListMs
		exs.DelistMs = item.DelistMs
		exs.Combined = item.Combined
		exs.AggRules = item.AggRules
	}
	return nil
}

func LoadAllExSymbols() *errs.Error {
	ctx := context.Background()
	pq, conn2, err2 := Conn(ctx)
	if err2 != nil {
		return err2
	}
	defer conn2.Release()
	exgList, err_ := pq.ListExchanges(ctx)
	if err_ != nil {
		return NewDbErr(core.ErrDbReadFail, err_)
	}
	for _, exgId := range exgList {
		err := pq.LoadExgSymbols(exgId)
		if err != nil {
			return err
		}
	}
	return nil
}

/*
GetAllExSymbols
Gets all the objects that have been loaded into the cache
获取已加载到缓存的所有标的
*/
func GetAllExSymbols() map[int32]*ExSymbol {
	return idSymbolMap
}

func (s *ExSymbol) GetValidStart(startMS int64) int64 {
	return max(s.ListMs, startMS)
}

func (s *ExSymbol) ToShort() string {
	slashArr := strings.Split(s.Symbol, "/")
	if len(slashArr) == 1 {
		// 非数字货币，直接返回
		return s.Symbol
	}
	comArr := strings.Split(slashArr[1], ":")
	if len(comArr) == 1 {
		// 现货，直接返回
		return s.Symbol
	}
	base, quote, settle := slashArr[0], comArr[0], comArr[1]
	if !strings.HasPrefix(settle, quote) {
		// 是币本位合约，直接返回
		return s.Symbol
	}
	if quote == settle {
		return fmt.Sprintf("%s/%s.P", base, quote)
	} else {
		suffix := settle[len(quote)+1:]
		return fmt.Sprintf("%s/%s.%s", base, quote, suffix)
	}
}

func (s *ExSymbol) AggRule(col string) string {
	rules := s.AggRuleMap()
	if rule, ok := rules[col]; ok {
		return normalizeAggRule(rule)
	}
	return "last"
}

func (s *ExSymbol) AggRuleMap() map[string]string {
	if s == nil || strings.TrimSpace(s.AggRules) == "" {
		return nil
	}
	var rules map[string]string
	if err := json.Unmarshal([]byte(s.AggRules), &rules); err != nil {
		return nil
	}
	for col, rule := range rules {
		rules[col] = normalizeAggRule(rule)
	}
	return rules
}

func (s *ExSymbol) SetAggRules(rules map[string]string) error {
	if len(rules) == 0 {
		s.AggRules = ""
		return nil
	}
	clean := make(map[string]string, len(rules))
	for col, rule := range rules {
		col = strings.TrimSpace(col)
		if col == "" {
			continue
		}
		rule = strings.ToLower(strings.TrimSpace(rule))
		if rule == "" {
			rule = "last"
		}
		clean[col] = rule
	}
	data, err := json.Marshal(clean)
	if err != nil {
		return err
	}
	s.AggRules = string(data)
	return nil
}

func normalizeAggRule(rule string) string {
	rule = strings.ToLower(strings.TrimSpace(rule))
	aggRulesMu.RLock()
	_, ok := aggRules[rule]
	aggRulesMu.RUnlock()
	if ok {
		return rule
	}
	return "last"
}

func RegisterAggRule(name string, fn AggRuleFunc) bool {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" || fn == nil {
		return false
	}
	aggRulesMu.Lock()
	aggRules[name] = fn
	aggRulesMu.Unlock()
	return true
}

func GetAggRuleFunc(name string) (AggRuleFunc, bool) {
	raw := strings.ToLower(strings.TrimSpace(name))
	name = normalizeAggRule(raw)
	if name != raw {
		return nil, false
	}
	aggRulesMu.RLock()
	fn, ok := aggRules[name]
	aggRulesMu.RUnlock()
	return fn, ok
}

func aggFirst(rows []*DataRecord, field SeriesField) (any, error) {
	return aggEdge(rows, field, true)
}

func aggLast(rows []*DataRecord, field SeriesField) (any, error) {
	return aggEdge(rows, field, false)
}

func aggEdge(rows []*DataRecord, field SeriesField, first bool) (any, error) {
	for i := 0; i < len(rows); i++ {
		idx := i
		if !first {
			idx = len(rows) - 1 - i
		}
		if rows[idx] == nil || rows[idx].Values == nil {
			continue
		}
		if val, ok := rows[idx].Values[field.Name]; ok {
			return val, nil
		}
	}
	return nil, fmt.Errorf("series field %q has no values", field.Name)
}

func aggMin(rows []*DataRecord, field SeriesField) (any, error) {
	val, ok, err := aggFloatSeed(rows, field)
	if err != nil || !ok {
		return val, err
	}
	for _, row := range rows {
		cur, ok, err := aggFloatValue(row, field)
		if err != nil {
			return nil, err
		}
		if ok && cur < val {
			val = cur
		}
	}
	return aggNumericResult(field, val), nil
}

func aggMax(rows []*DataRecord, field SeriesField) (any, error) {
	val, ok, err := aggFloatSeed(rows, field)
	if err != nil || !ok {
		return val, err
	}
	for _, row := range rows {
		cur, ok, err := aggFloatValue(row, field)
		if err != nil {
			return nil, err
		}
		if ok && cur > val {
			val = cur
		}
	}
	return aggNumericResult(field, val), nil
}

func aggSum(rows []*DataRecord, field SeriesField) (any, error) {
	total := 0.0
	seen := false
	for _, row := range rows {
		val, ok, err := aggFloatValue(row, field)
		if err != nil {
			return nil, err
		}
		if ok {
			total += val
			seen = true
		}
	}
	if !seen {
		return nil, fmt.Errorf("series field %q has no values", field.Name)
	}
	return aggNumericResult(field, total), nil
}

func aggAvg(rows []*DataRecord, field SeriesField) (any, error) {
	total := 0.0
	count := 0
	for _, row := range rows {
		val, ok, err := aggFloatValue(row, field)
		if err != nil {
			return nil, err
		}
		if ok {
			total += val
			count++
		}
	}
	if count == 0 {
		return nil, fmt.Errorf("series field %q has no values", field.Name)
	}
	return total / float64(count), nil
}

func aggMid(rows []*DataRecord, field SeriesField) (any, error) {
	minVal, ok, err := aggFloatSeed(rows, field)
	if err != nil || !ok {
		return minVal, err
	}
	maxVal := minVal
	for _, row := range rows {
		val, ok, err := aggFloatValue(row, field)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		if val < minVal {
			minVal = val
		}
		if val > maxVal {
			maxVal = val
		}
	}
	return (minVal + maxVal) / 2, nil
}

func aggFloatSeed(rows []*DataRecord, field SeriesField) (float64, bool, error) {
	for _, row := range rows {
		val, ok, err := aggFloatValue(row, field)
		if err != nil || ok {
			return val, ok, err
		}
	}
	return 0, false, fmt.Errorf("series field %q has no values", field.Name)
}

func aggFloatValue(row *DataRecord, field SeriesField) (float64, bool, error) {
	if row == nil || row.Values == nil {
		return 0, false, nil
	}
	val, ok := row.Values[field.Name]
	if !ok {
		return 0, false, nil
	}
	num, err := seriesFloatAny(val)
	if err != nil {
		return 0, false, err
	}
	return num, true, nil
}

func aggNumericResult(field SeriesField, val float64) any {
	if field.Type == "int" {
		return int64(val)
	}
	return val
}

func InitListDates() *errs.Error {
	pq, conn2, err2 := Conn(nil)
	if err2 != nil {
		return err2
	}
	defer conn2.Release()
	exchange := exg.Default
	exInfo := exchange.Info()
	exsList := GetExSymbols(exInfo.ID, exInfo.MarketType)
	marketMap := exchange.GetCurMarkets()
	for _, exs := range exsList {
		if exs.ListMs > 0 && exs.DelistMs > 0 {
			continue
		}
		mar, ok := marketMap[exs.Symbol]
		if !ok {
			continue
		}
		changed := false
		if exs.DelistMs == 0 && mar.Expiry > 0 {
			exs.DelistMs = mar.Expiry
			changed = true
		}
		if exs.ListMs == 0 && mar.Created > 0 {
			// 只有合约有Created字段，现货需从k线计算
			exs.ListMs = mar.Created
			changed = true
		}
		if changed {
			err_ := pq.SetListMS(context.Background(), SetListMSParams{
				ID:       exs.ID,
				ListMs:   exs.ListMs,
				DelistMs: exs.DelistMs,
			})
			if err_ != nil {
				return NewDbErr(core.ErrDbExecFail, err_)
			}
		}
	}
	return nil
}

func EnsureListDates(sess *Queries, exchange banexg.BanExchange, exsMap map[int32]*ExSymbol, exsList []*ExSymbol) *errs.Error {
	exInfo := exchange.Info()
	if exInfo.MarketType != banexg.MarketSpot {
		return nil
	}
	tryListLock.Lock()
	defer tryListLock.Unlock()
	var emptys = make([]*ExSymbol, 0, (len(exsMap)+len(exsList))/4)
	for _, v := range exsMap {
		if v.ListMs == 0 {
			if _, ok := tryListIds[v.ID]; !ok {
				emptys = append(emptys, v)
			}
		}
	}
	for _, v := range exsList {
		if v.ListMs == 0 {
			if _, ok := tryListIds[v.ID]; !ok {
				emptys = append(emptys, v)
			}
		}
	}
	if len(emptys) == 0 {
		return nil
	}
	hasFetch := !core.NetDisable && exchange.HasApi(banexg.ApiFetchOHLCV, exInfo.MarketType)
	var prgBar *utils.PrgBar
	cacheNum := len(emptys)
	if cacheNum > 10 && hasFetch {
		costSecs := float64(cacheNum) / 6
		log.Info("calculating listDates for new symbols", zap.Int("num", cacheNum),
			zap.Int("secs", int(costSecs)))
		prgBar = utils.NewPrgBar(cacheNum, "InitListDates")
		defer prgBar.Close()
	}
	var err *errs.Error
	for _, exs := range emptys {
		tryListIds[exs.ID] = true
		if prgBar != nil {
			prgBar.Add(1)
		}
		startMS := core.MSMinStamp
		if hasFetch {
			var klines []*banexg.Kline
			klines, err = exchange.FetchOHLCV(exs.Symbol, "1m", startMS, 1, nil)
			if len(klines) > 0 {
				exs.ListMs = klines[0].Time
			}
		} else {
			var rows []*DataSeries
			rows, err = sess.QueryOHLCV(exs, "1m", startMS, 0, 1, false)
			if len(rows) > 0 {
				exs.ListMs = rows[0].TimeMS
			}
		}
		if err != nil {
			return err
		}
		if exs.ListMs > 0 {
			err_ := sess.SetListMS(context.Background(), SetListMSParams{
				ID:       exs.ID,
				ListMs:   exs.ListMs,
				DelistMs: exs.DelistMs,
			})
			if err_ != nil {
				return NewDbErr(core.ErrDbExecFail, err_)
			}
		}
	}
	return nil
}

func ParseShort(exgName, short string) (*ExSymbol, *errs.Error) {
	slashArr := strings.Split(short, "/")
	var symbol string
	var market = banexg.MarketSpot
	if len(slashArr) > 1 {
		// 对数字货币 BTC/USDT:USDT BTC/USDT.P BTC/USDT.2309
		dotArr := strings.Split(slashArr[1], ".")
		quote := dotArr[0]
		var suffix = ""
		if len(dotArr) > 1 {
			suffix = quote
			market = banexg.MarketLinear
			if !strings.EqualFold(dotArr[1], "p") {
				suffix += "-" + dotArr[1]
			}
		} else {
			comArr := strings.Split(quote, ":")
			if len(comArr) > 1 {
				quote, suffix = comArr[0], comArr[1]
				if strings.HasPrefix(suffix, quote) {
					market = banexg.MarketLinear
				} else {
					market = banexg.MarketInverse
				}
			}
		}
		if market == banexg.MarketSpot {
			symbol = fmt.Sprintf("%s/%s", slashArr[0], quote)
		} else {
			symbol = fmt.Sprintf("%s/%s:%s", slashArr[0], quote, suffix)
		}
	} else {
		symbol = short
	}
	item := findExSymbol(exgName, market, symbol)
	if item == nil {
		exgMarket := fmt.Sprintf("%s:%s", exgName, market)
		pairNum, _ := marketMap[exgMarket]
		if pairNum == 0 {
			pq2, conn2, err2 := Conn(nil)
			if err2 != nil {
				return nil, err2
			}
			err := pq2.LoadExgSymbols(exgName)
			conn2.Release()
			if err != nil {
				return nil, err
			}
			item = findExSymbol(exgName, market, symbol)
			if item != nil {
				return item, nil
			}
			pairNum, _ = marketMap[exgMarket]
		}
		err := errs.NewMsg(core.ErrInvalidSymbol, "%s not exist in %d cache for %s", symbol, pairNum, exgMarket)
		return nil, err
	}
	return item, nil
}

func AddHourSymbol(exs *ExSymbol) {
	hourPairsLock.Lock()
	hourPairsMap[exs.Symbol] = exs
	hourPairsLock.Unlock()
}

func Sub1mSymbol(pair string) {
	hourPairsLock.Lock()
	pairsMap[pair] = nil
	hourPairsLock.Unlock()
}

func ResetSubSymbol() {
	hourPairsLock.Lock()
	hourPairsMap = make(map[string]*ExSymbol)
	pairsMap = make(map[string]*ExSymbol)
	hourPairsLock.Unlock()
}

func GetHourOnlySymbols() map[int32]*ExSymbol {
	hourPairsLock.Lock()
	res := make(map[int32]*ExSymbol)
	for _, exs := range hourPairsMap {
		if _, ok := pairsMap[exs.Symbol]; !ok {
			res[exs.ID] = exs
		}
	}
	hourPairsLock.Unlock()
	return res
}
