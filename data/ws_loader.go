package data

import (
	"archive/zip"
	"bufio"
	"context"
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
	deps       *RuntimeDeps
	ctx        context.Context
	cancel     context.CancelFunc
	stopOnce   sync.Once
	workerWait sync.WaitGroup
	stopped    bool
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
	// ArchiveURL overrides the exchange-specific archive endpoint when set.
	ArchiveURL string
}

func (info *WsSymbol) FillDefaults() *errs.Error {
	return info.fillDefaults(nil)
}

func (info *WsSymbol) fillDefaults(deps *RuntimeDeps) *errs.Error {
	if info.ExgId == "" {
		if deps == nil {
			info.ExgId = config.Exchange.Name
		} else {
			info.ExgId, _ = deps.identity()
			if info.ExgId == "" && deps.exchange() != nil {
				info.ExgId = deps.exchange().Info().ID
			}
		}
	}
	if info.RawSymbol == "" || info.Market == "" {
		var exchange banexg.BanExchange
		var err *errs.Error
		if deps == nil {
			exchange, err = exg.GetWith(info.ExgId, info.Market, "")
		} else {
			exchange = deps.exchange()
			if exchange == nil {
				return errs.NewMsg(core.ErrBadConfig, "runtime exchange is required")
			}
		}
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
	url, _ := info.archiveURL()
	return url
}

func (info *WsSymbol) archiveURL() (string, *errs.Error) {
	return info.archiveURLForDeps(nil)
}

func (info *WsSymbol) archiveURLForDeps(deps *RuntimeDeps) (string, *errs.Error) {
	if info == nil {
		return "", errs.NewMsg(errs.CodeParamInvalid, "ws symbol is nil")
	}
	if info.ArchiveURL != "" {
		return info.ArchiveURL, nil
	}
	if deps != nil {
		return exg.BuildArchiveURLForExchange(deps.exchange(), info.Market, info.dataType, info.RawSymbol, info.Date)
	}
	return exg.BuildArchiveURL(info.ExgId, info.Market, info.dataType, info.RawSymbol, info.Date)
}

func NewWsDataLoader() (*WsDataLoader, *errs.Error) {
	return newWsDataLoader(nil)
}

// NewWsDataLoaderWithRuntimeDeps binds cache resolution, worker lifetime, and
// HTTP requests to one runtime. A nil dependency set preserves the legacy
// package facade.
func NewWsDataLoaderWithRuntimeDeps(deps *RuntimeDeps) (*WsDataLoader, *errs.Error) {
	if deps == nil {
		return NewWsDataLoader()
	}
	return newWsDataLoader(deps)
}

func newWsDataLoader(deps *RuntimeDeps) (*WsDataLoader, *errs.Error) {
	client := banexg.NewHttpClient()
	client.Timeout = 120 * time.Second
	var parent context.Context
	if deps == nil {
		parent = core.Ctx
	} else {
		parent = deps.context()
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	var dataDir string
	if deps == nil {
		dataDir = config.GetDataDir()
	} else {
		dataDir = deps.dataDir()
	}
	loader := &WsDataLoader{
		cacheDir:   filepath.Join(dataDir, "wscache"),
		deps:       deps,
		ctx:        ctx,
		cancel:     cancel,
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
		loader.workerWait.Add(1)
		go func() {
			defer loader.workerWait.Done()
			loader.downloadWorker()
		}()
	}
	physicalCounts, err_ := cpu.Counts(false)
	if err_ != nil {
		log.Warn("get cpu physical num fail, use 4", zap.Error(err_))
		physicalCounts = 4
	} else if physicalCounts > 8 {
		physicalCounts = 8
	}
	for range physicalCounts {
		loader.workerWait.Add(1)
		go func() {
			defer loader.workerWait.Done()
			loader.splitWorker()
		}()
	}
	log.Info("start hft data loader workers", zap.Int("download", logicCpu), zap.Int("split", physicalCounts))

	return loader, nil
}

// GetCachePath returns the cache path for a specific symbol, date and hour
func (l *WsDataLoader) GetCachePath(info *WsSymbol, tryRoot bool) (string, bool) {
	err := info.fillDefaults(l.deps)
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
		select {
		case err := <-wait:
			if err != nil {
				return nil, err
			}
		case <-l.context().Done():
			l.cancelTask(info, wait)
			return nil, l.canceledError()
		}
		cachePath, _ = l.GetCachePath(info, true)
	}
	if strings.HasSuffix(cachePath, ".zip") {
		return l.loadTradeZip(cachePath, info)
	} else if strings.HasSuffix(cachePath, ".bin") {
		timeStart := l.nowMS()
		trades, err_ := loadBinaryTrades(cachePath)
		if err_ != nil {
			return nil, errs.New(errs.CodeIOReadFail, err_)
		}
		for _, t := range trades {
			t.Symbol = info.Symbol
		}
		cost := l.nowMS() - timeStart
		log.Debug("load bin ws trades ok", zap.Int("num", len(trades)), zap.Int64("cost", cost),
			zap.String("path", cachePath))
		return trades, nil
	} else {
		return nil, errs.NewMsg(errs.CodeRunTime, "unknown format: %v", cachePath)
	}
}

func (l *WsDataLoader) loadTradeZip(path string, info *WsSymbol) ([]*banexg.Trade, *errs.Error) {
	timeStart := l.nowMS()
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
	cost := l.nowMS() - timeStart
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
	taskChan := make(chan *errs.Error, 1)
	if l == nil {
		completeTask(taskChan, errs.NewMsg(errs.CodeCancel, "ws data loader is nil"))
		return taskChan
	}
	ctx := l.context()
	l.lockTasks.Lock()
	if l.stopped {
		l.lockTasks.Unlock()
		completeTask(taskChan, l.canceledError())
		return taskChan
	}
	if l.tasks == nil {
		l.tasks = make(map[*WsSymbol]chan *errs.Error)
	}
	l.tasks[info] = taskChan
	l.lockTasks.Unlock()
	if l.chanDown == nil {
		l.cancelTask(info, taskChan)
		return taskChan
	}
	select {
	case l.chanDown <- info:
	case <-ctx.Done():
		l.cancelTask(info, taskChan)
	}
	return taskChan
}

func (l *WsDataLoader) context() context.Context {
	if l == nil {
		return context.Background()
	}
	if l.ctx != nil {
		return l.ctx
	}
	if l.deps != nil {
		if ctx := l.deps.context(); ctx != nil {
			return ctx
		}
	}
	if core.Ctx != nil {
		return core.Ctx
	}
	return context.Background()
}

func (l *WsDataLoader) nowMS() int64 {
	if l.deps != nil {
		return l.deps.utcStamp()
	}
	return btime.UTCStamp()
}

func (l *WsDataLoader) markTaskDone(info *WsSymbol, err *errs.Error) {
	l.lockTasks.Lock()
	var taskChan chan *errs.Error
	if l.tasks != nil {
		taskChan = l.tasks[info]
		delete(l.tasks, info)
	}
	l.lockTasks.Unlock()
	completeTask(taskChan, err)
}

func completeTask(taskChan chan *errs.Error, err *errs.Error) {
	if taskChan == nil {
		return
	}
	select {
	case taskChan <- err:
	default:
	}
}

func (l *WsDataLoader) cancelTask(info *WsSymbol, taskChan chan *errs.Error) {
	if l == nil {
		return
	}
	l.lockTasks.Lock()
	if l.tasks != nil && l.tasks[info] == taskChan {
		delete(l.tasks, info)
	}
	l.lockTasks.Unlock()
	completeTask(taskChan, l.canceledError())
}

func (l *WsDataLoader) canceledError() *errs.Error {
	if err := l.context().Err(); err != nil {
		return errs.New(errs.CodeCancel, err)
	}
	return errs.NewMsg(errs.CodeCancel, "ws data loader stopped")
}

// Stop closes task admission and cancels queued or active work without waiting
// for workers. Call Join when the lifecycle owner needs completion.
func (l *WsDataLoader) Stop() *errs.Error {
	if l == nil {
		return nil
	}
	l.stopOnce.Do(func() {
		l.lockTasks.Lock()
		l.stopped = true
		pending := make([]chan *errs.Error, 0, len(l.tasks))
		for info, taskChan := range l.tasks {
			delete(l.tasks, info)
			pending = append(pending, taskChan)
		}
		l.lockTasks.Unlock()
		if l.cancel != nil {
			l.cancel()
		}
		cancelErr := l.canceledError()
		for _, taskChan := range pending {
			completeTask(taskChan, cancelErr)
		}
	})
	return nil
}

// Join stops admission and waits for every loader worker to exit.
func (l *WsDataLoader) Join() {
	if l == nil {
		return
	}
	l.Stop()
	l.workerWait.Wait()
}

func (l *WsDataLoader) downloadWorker() {
	ctx := l.context()
	for {
		select {
		case <-ctx.Done():
			return
		case info, ok := <-l.chanDown:
			if !ok {
				return
			}
			if ctx.Err() != nil {
				l.markTaskDone(info, l.canceledError())
				continue
			}
			tmpPath, err := l.downloadJob(info)
			if ctx.Err() != nil {
				l.markTaskDone(info, l.canceledError())
				continue
			}
			if tmpPath != "" {
				select {
				case l.chanSplit <- info:
				case <-ctx.Done():
					l.markTaskDone(info, l.canceledError())
				}
			} else {
				l.markTaskDone(info, err)
			}
		}
	}
}

func (l *WsDataLoader) splitWorker() {
	ctx := l.context()
	for {
		select {
		case <-ctx.Done():
			return
		case info, ok := <-l.chanSplit:
			if !ok {
				return
			}
			if ctx.Err() != nil {
				l.markTaskDone(info, l.canceledError())
				continue
			}
			filePath, _ := l.GetCachePath(info, true)
			tmpFile := filePath + ".tmp"
			err := l.SplitBigZip(tmpFile, info)
			if ctx.Err() != nil {
				err = l.canceledError()
			}
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
	downUrl, downErr := info.archiveURLForDeps(l.deps)
	if downErr != nil {
		return "", downErr
	}
	log.Debug("download", zap.String("ws", info.String()))
	request, err := http.NewRequestWithContext(l.context(), http.MethodGet, downUrl, nil)
	if err != nil {
		return "", errs.New(errs.CodeNetFail, err)
	}
	resp, err := l.httpClient.Do(request)
	if err != nil {
		if ctxErr := l.context().Err(); ctxErr != nil {
			return "", errs.New(errs.CodeCancel, ctxErr)
		}
		return "", errs.New(errs.CodeNetFail, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNotFound {
			return "", errs.NewMsg(errs.CodeDataNotFound, "invalid : %s", downUrl)
		}
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
		if ctxErr := l.context().Err(); ctxErr != nil {
			return "", errs.New(errs.CodeCancel, ctxErr)
		}
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
