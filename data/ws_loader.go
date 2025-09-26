package data

import (
	"archive/zip"
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/exg"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"
	"github.com/sasha-s/go-deadlock"
	"github.com/shirou/gopsutil/v4/cpu"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/banbox/banexg"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

type TradeBatch []*banexg.Trade

func (b TradeBatch) TimeMS() int64 {
	if len(b) > 0 {
		return b[0].Timestamp
	}
	return 0
}

type WsDataLoader struct {
	cacheDir   string
	mu         sync.RWMutex
	httpClient *http.Client

	tasks     map[*WsSymbol]chan *errs.Error
	lockTasks deadlock.Mutex

	chanDown  chan *WsSymbol // 下载任务队列
	chanSplit chan *WsSymbol // 待分割任务
}

type WsSymbol struct {
	ExgId     string
	Market    string
	WsType    string
	Symbol    string
	RawSymbol string
	Date      string
	Hour      int
	dataType  string
}

func (info *WsSymbol) FillDefaults() *errs.Error {
	if info.ExgId == "" {
		info.ExgId = config.Exchange.Name
	}
	if info.RawSymbol == "" || info.Market == "" {
		exchange, err := exg.GetWith(info.ExgId, info.Market, "")
		if err != nil {
			return err
		}
		mkt, err := exchange.GetMarket(info.Symbol)
		if err != nil {
			return err
		}
		info.Market = mkt.Type
		info.RawSymbol = mkt.ID
	}
	if info.dataType == "" {
		wsType := ""
		switch info.WsType {
		case core.WsSubKLine:
			wsType = "klines"
		case core.WsSubDepth:
			wsType = "bookTicker"
		case core.WsSubTrade:
			wsType = "aggTrades"
		}
		info.dataType = wsType
	}
	return nil
}

func (info *WsSymbol) String() string {
	return fmt.Sprintf("%s_%s %s %s %s %d", info.ExgId, info.Market, info.WsType, info.Symbol, info.Date, info.Hour)
}

func (info *WsSymbol) MidPath() string {
	exgMarket := fmt.Sprintf("%s_%s", info.ExgId, info.Market)
	return filepath.Join(exgMarket, info.dataType, info.RawSymbol, info.Date)
}

func (info *WsSymbol) DownUrl() string {
	if info.ExgId == "binance" {
		// https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2025-08-03.zip
		prefix := "https://data.binance.vision/data"
		market := banexg.MarketSpot
		if info.Market == banexg.MarketLinear {
			market = "futures/um"
		} else if info.Market == banexg.MarketInverse {
			market = "futures/cm"
		} else if info.Market == banexg.MarketOption {
			market = banexg.MarketOption
		}
		name := fmt.Sprintf("%s-%s-%s.zip", info.RawSymbol, info.dataType, info.Date)
		return fmt.Sprintf("%s/%s/daily/%s/%s/%s", prefix, market, info.dataType, info.RawSymbol, name)
	}
	panic("exchange not support: " + info.ExgId)
}

func NewWsDataLoader() (*WsDataLoader, *errs.Error) {
	proxyUrl, err := config.GetExchangeProxy(config.Exchange.Name)
	if err != nil {
		return nil, err
	}
	client := &http.Client{
		Timeout: 120 * time.Second,
	}
	proxy, err_ := url.Parse(proxyUrl)
	if err_ != nil {
		return nil, errs.New(errs.CodeParamInvalid, err_)
	}
	client.Transport = &http.Transport{
		Proxy: http.ProxyURL(proxy),
	}
	loader := &WsDataLoader{
		cacheDir:   filepath.Join(config.GetDataDir(), "wscache"),
		httpClient: client,
		tasks:      make(map[*WsSymbol]chan *errs.Error),
		chanDown:   make(chan *WsSymbol, 100),
		chanSplit:  make(chan *WsSymbol, 100),
	}
	logicCpu, err_ := cpu.Counts(true)
	if err_ != nil {
		log.Warn("get cpu logic num fail, use 4", zap.Error(err_))
		logicCpu = 4
	} else if logicCpu > 8 {
		logicCpu = 8
	}
	for range logicCpu {
		go loader.downloadWorker()
	}
	physicalCounts, err_ := cpu.Counts(false)
	if err_ != nil {
		log.Warn("get cpu physical num fail, use 4", zap.Error(err_))
		physicalCounts = 4
	} else if physicalCounts > 8 {
		physicalCounts = 8
	}
	for range physicalCounts {
		go loader.splitWorker()
	}
	log.Info("start hft data loader workers", zap.Int("download", logicCpu), zap.Int("split", physicalCounts))

	return loader, nil
}

// GetCachePath returns the cache path for a specific symbol, date and hour
func (l *WsDataLoader) GetCachePath(info *WsSymbol, tryRoot bool) (string, bool) {
	err := info.FillDefaults()
	if err != nil {
		log.Error("fill defaults for ws symbol fail", zap.Error(err))
	}
	suffix := ".bin"
	parentDir := filepath.Join(l.cacheDir, info.MidPath())
	path := filepath.Join(parentDir, fmt.Sprintf("%02d%s", info.Hour, suffix))
	if utils.Exists(path) {
		return path, true
	}
	if !tryRoot {
		return path, false
	}
	// hour不存在时尝试按日期读取
	datePath := parentDir + suffix
	if utils.Exists(datePath) {
		return datePath, true
	}
	return datePath, false
}

// LoadTrades loads trades for a specific hour from cache
func (l *WsDataLoader) LoadTrades(info *WsSymbol) ([]*banexg.Trade, *errs.Error) {
	cachePath, isCached := l.GetCachePath(info, true)
	if !isCached {
		wait := l.submitTask(info)
		err := <-wait
		if err != nil {
			return nil, err
		}
		cachePath, _ = l.GetCachePath(info, true)
	}
	if strings.HasSuffix(cachePath, ".zip") {
		return l.loadTradeZip(cachePath, info)
	} else if strings.HasSuffix(cachePath, ".bin") {
		timeStart := btime.UTCStamp()
		trades, err_ := loadBinaryTrades(cachePath)
		if err_ != nil {
			return nil, errs.New(errs.CodeIOReadFail, err_)
		}
		for _, t := range trades {
			t.Symbol = info.Symbol
		}
		cost := btime.UTCStamp() - timeStart
		log.Debug("load bin ws trades ok", zap.Int("num", len(trades)), zap.Int64("cost", cost),
			zap.String("path", cachePath))
		return trades, nil
	} else {
		return nil, errs.NewMsg(errs.CodeRunTime, "unknown format: %v", cachePath)
	}
}

func (l *WsDataLoader) loadTradeZip(path string, info *WsSymbol) ([]*banexg.Trade, *errs.Error) {
	timeStart := btime.UTCStamp()
	trades := make([]*banexg.Trade, 0, 1000)
	err := ReadZipCSVs(path, nil, func(inPath string, fid int, fileRaw *zip.File, arg interface{}) *errs.Error {
		// Pre-allocate trades slice with estimated capacity
		file, err := fileRaw.Open()
		if err != nil {
			return errs.New(errs.CodeIOReadFail, err)
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		buf := make([]byte, 0, 256*1024)
		scanner.Buffer(buf, 256*1024)

		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				continue
			}

			// Use fast parsing without full string split
			trade := parseCsvWsTrade(line)
			if trade != nil {
				trade.Symbol = info.Symbol
				trades = append(trades, trade)
			}
		}

		if err = scanner.Err(); err != nil {
			return errs.New(errs.CodeIOReadFail, err)
		}
		return nil
	}, nil)
	if err != nil {
		return nil, err
	}
	cost := btime.UTCStamp() - timeStart
	log.Debug("load csv ws trades cost", zap.Int("num", len(trades)), zap.Int64("cost", cost))
	return trades, nil
}

// SplitBigZip extracts and splits a zip file into hourly
func (l *WsDataLoader) SplitBigZip(zipPath string, info *WsSymbol) *errs.Error {
	startTime := time.Now()
	lineCount := 0

	log.Debug("try split big zip", zap.String("job", info.String()))

	err := ReadZipCSVs(zipPath, nil, func(inPath string, fid int, file *zip.File, arg interface{}) *errs.Error {
		if !strings.HasSuffix(file.Name, ".csv") {
			return nil
		}
		rc, err := file.Open()
		if err != nil {
			return errs.New(errs.CodeIOReadFail, err)
		}
		defer rc.Close()

		scanner := bufio.NewScanner(rc)
		buf := make([]byte, 0, 1024*1024)
		scanner.Buffer(buf, 1024*1024)

		prevHour := 0
		var lines []*banexg.Trade

		for scanner.Scan() {
			line := scanner.Text()
			lineCount++

			if line == "" {
				continue
			}
			item := parseCsvWsTrade(line)
			hour := int(item.Timestamp / 3600000 % 24)
			if item.Timestamp > 0 {
				if hour != prevHour {
					if len(lines) > 0 {
						info.Hour = prevHour
						path, _ := l.GetCachePath(info, false)
						err = writeBinaryTrades(lines, path)
						if err != nil {
							return errs.New(errs.CodeIOWriteFail, err)
						}
					}
					prevHour = hour
					lines = nil
				}
				lines = append(lines, item)
			}
		}
		if len(lines) > 0 {
			info.Hour = prevHour
			path, _ := l.GetCachePath(info, false)
			err = writeBinaryTrades(lines, path)
			if err != nil {
				return errs.New(errs.CodeIOWriteFail, err)
			}
		}

		if err = scanner.Err(); err != nil {
			return errs.New(errs.CodeIOReadFail, err)
		}
		return nil
	}, nil)

	if err != nil {
		return err
	}

	elapsed := time.Since(startTime)
	log.Debug("split ws data ok",
		zap.String("symbol", info.Symbol),
		zap.String("date", info.Date),
		zap.Int("raw_num", lineCount),
		zap.Duration("cost_time", elapsed))

	return nil
}

func (l *WsDataLoader) submitTask(info *WsSymbol) chan *errs.Error {
	l.lockTasks.Lock()
	taskChan := make(chan *errs.Error)
	l.tasks[info] = taskChan
	l.chanDown <- info
	l.lockTasks.Unlock()
	return taskChan
}

func (l *WsDataLoader) markTaskDone(info *WsSymbol, err *errs.Error) {
	l.lockTasks.Lock()
	taskChan := l.tasks[info]
	taskChan <- err
	delete(l.tasks, info)
	l.lockTasks.Unlock()
}

func (l *WsDataLoader) downloadWorker() {
	for {
		select {
		case <-core.Ctx.Done():
			return
		case info := <-l.chanDown:
			tmpPath, err := l.downloadJob(info)
			if tmpPath != "" {
				l.chanSplit <- info
			} else {
				l.markTaskDone(info, err)
			}
		}
	}
}

func (l *WsDataLoader) splitWorker() {
	for {
		select {
		case <-core.Ctx.Done():
			return
		case info := <-l.chanSplit:
			filePath, _ := l.GetCachePath(info, true)
			tmpFile := filePath + ".tmp"
			err := l.SplitBigZip(tmpFile, info)
			err_ := os.Remove(tmpFile)
			if err_ != nil {
				log.Error("remove zip tmp fail", zap.Error(err_))
			}
			l.markTaskDone(info, err)
		}
	}
}

// 下载高频数据任务，如果需要分割，返回的第一个string不为空
func (l *WsDataLoader) downloadJob(info *WsSymbol) (string, *errs.Error) {
	filePath, exist := l.GetCachePath(info, true)
	if exist {
		return "", nil
	}

	tmpFile := filePath + ".tmp"
	downUrl := info.DownUrl()
	log.Debug("download", zap.String("ws", info.String()))
	resp, err := l.httpClient.Get(downUrl)
	if err != nil {
		return "", errs.New(errs.CodeNetFail, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", errs.NewMsg(errs.CodeRunTime, "download failed: %d %s", resp.StatusCode, downUrl)
	}
	folder := filepath.Dir(tmpFile)
	err = utils.EnsureDir(folder, 0755)
	if err != nil {
		return "", errs.New(errs.CodeIOWriteFail, err)
	}

	// Write to temporary file
	out, err := os.Create(tmpFile)
	if err != nil {
		return "", errs.New(errs.CodeIOWriteFail, err)
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		os.Remove(tmpFile)
		return "", errs.New(errs.CodeIOWriteFail, err)
	}

	return tmpFile, nil
}

// parseCsvWsTrade parses trade line without full string split
func parseCsvWsTrade(line string) *banexg.Trade {
	var fields = strings.Split(line, ",")
	if len(fields) == 6 {
		return parseWsTradeRow(fields)
	} else if len(fields) == 7 {
		return parseWsAggTradeRow(fields)
	}
	return nil
}

func parseWsAggTradeRow(fields []string) *banexg.Trade {
	//agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker
	price, _ := strconv.ParseFloat(fields[1], 64)
	amount, _ := strconv.ParseFloat(fields[2], 64)
	timestamp, _ := strconv.ParseInt(fields[5], 10, 64)
	// true：主动卖出，false: 主动买入
	isBuyerMaker, _ := strconv.ParseBool(fields[6])
	side := banexg.OdSideBuy
	if isBuyerMaker {
		side = banexg.OdSideSell
	}

	return &banexg.Trade{
		ID:        fields[0],
		Price:     price,
		Amount:    amount,
		Timestamp: timestamp,
		Side:      side,
	}
}

func parseWsTradeRow(fields []string) *banexg.Trade {
	//id,price,qty,quote_qty,time,is_buyer_maker
	price, _ := strconv.ParseFloat(fields[1], 64)
	amount, _ := strconv.ParseFloat(fields[2], 64)
	cost, _ := strconv.ParseFloat(fields[3], 64)
	timestamp, _ := strconv.ParseInt(fields[4], 10, 64)
	// true：主动卖出，false: 主动买入
	isBuyerMaker, _ := strconv.ParseBool(fields[5])
	side := banexg.OdSideBuy
	if isBuyerMaker {
		side = banexg.OdSideSell
	}

	return &banexg.Trade{
		ID:        fields[0],
		Price:     price,
		Amount:    amount,
		Cost:      cost,
		Timestamp: timestamp,
		Side:      side,
	}
}

// Trade二进制格式 (固定长度，便于快速读取)
// ID: 8 bytes (uint64)
// Price: 8 bytes (float64)
// Amount: 8 bytes (float64)
// Timestamp: 8 bytes (int64)
// Side: 1 byte (0=buy, 1=sell)
// Total: 33 bytes per trade
const tradeBinarySize = 33

// loadBinaryTrades loads trades from binary file
func loadBinaryTrades(cachePath string) ([]*banexg.Trade, error) {
	file, err := os.Open(cachePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Use buffered reader for better performance
	reader := bufio.NewReaderSize(file, 1024*1024) // 1MB buffer

	// Read trade count
	var count uint32
	if err = binary.Read(reader, binary.LittleEndian, &count); err != nil {
		return nil, err
	}

	// Read all data at once
	totalSize := int(count) * tradeBinarySize
	bigBuf := make([]byte, totalSize)
	if _, err = io.ReadFull(reader, bigBuf); err != nil {
		return nil, err
	}

	// Pre-allocate trades slice
	trades := make([]*banexg.Trade, count)

	// Batch decode
	for i := uint32(0); i < count; i++ {
		offset := int(i) * tradeBinarySize
		trades[i] = decodeTrade(bigBuf[offset : offset+tradeBinarySize])
	}

	return trades, nil
}

// writeBinaryTrades writes trades to binary file
func writeBinaryTrades(trades []*banexg.Trade, cachePath string) error {
	// Create directory if needed
	dir := filepath.Dir(cachePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	file, err := os.Create(cachePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Use buffered writer
	writer := bufio.NewWriterSize(file, 1024*1024) // 1MB buffer
	defer writer.Flush()

	// Write trade count
	if err = binary.Write(writer, binary.LittleEndian, uint32(len(trades))); err != nil {
		return err
	}

	// Pre-allocate big buffer for all trades
	totalSize := len(trades) * tradeBinarySize
	bigBuf := make([]byte, totalSize)

	// Batch encode
	for i, trade := range trades {
		offset := i * tradeBinarySize
		encodeTrade(trade, bigBuf[offset:offset+tradeBinarySize])
	}

	// Write all at once
	if _, err = writer.Write(bigBuf); err != nil {
		return err
	}

	return nil
}

// encodeTrade encodes a trade to binary format
func encodeTrade(trade *banexg.Trade, buf []byte) {
	// ID as uint64
	var idNum uint64
	fmt.Sscanf(trade.ID, "%d", &idNum)
	binary.LittleEndian.PutUint64(buf[0:8], idNum)
	binary.LittleEndian.PutUint64(buf[8:16], math.Float64bits(trade.Price))
	binary.LittleEndian.PutUint64(buf[16:24], math.Float64bits(trade.Amount))
	binary.LittleEndian.PutUint64(buf[24:32], uint64(trade.Timestamp))

	// Side (0=buy, 1=sell)
	if trade.Side == banexg.OdSideSell {
		buf[32] = 1
	} else {
		buf[32] = 0
	}
}

// decodeTrade decodes a trade from binary format
func decodeTrade(buf []byte) *banexg.Trade {
	trade := &banexg.Trade{}

	idNum := binary.LittleEndian.Uint64(buf[0:8])
	trade.ID = fmt.Sprintf("%d", idNum)
	trade.Price = math.Float64frombits(binary.LittleEndian.Uint64(buf[8:16]))
	trade.Amount = math.Float64frombits(binary.LittleEndian.Uint64(buf[16:24]))
	trade.Timestamp = int64(binary.LittleEndian.Uint64(buf[24:32]))

	// Side
	if buf[32] == 1 {
		trade.Side = banexg.OdSideSell
	} else {
		trade.Side = banexg.OdSideBuy
	}

	return trade
}
