package orm

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/sasha-s/go-deadlock"
	"go.uber.org/zap"

	"github.com/banbox/banbot/core"

	"github.com/banbox/banbot/config"
	utils2 "github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/utils"
	"google.golang.org/protobuf/proto"
)

type ExportKlineJob struct {
	*ExSymbol
	TimeFrame string
	StartMS   int64
	StopMS    int64
	HasInfo   bool
}

type ExportTask struct {
	jobs   []*ExportKlineJob
	exInfo *EXInfo
}

// KDataTransferDeps owns the runtime resources used by protobuf kline
// import/export. Symbols must contain the complete stored catalog referenced
// by the transfer configuration, not merely the configured trading pairs.
type KDataTransferDeps struct {
	Context      context.Context
	Storage      *Storage
	Symbols      *SymbolState
	Logger       *zap.Logger
	KlineOptions *KlineRuntimeOptions
}

func (d KDataTransferDeps) context() context.Context {
	if d.Context != nil {
		return d.Context
	}
	return context.Background()
}

func (d KDataTransferDeps) logger() *zap.Logger {
	if d.Logger != nil {
		return d.Logger
	}
	return zap.NewNop()
}

func (d KDataTransferDeps) conn() (*Queries, func(), *errs.Error) {
	if d.Storage == nil {
		return nil, nil, errs.NewMsg(core.ErrDbConnFail, "data transfer storage is required")
	}
	if d.Symbols == nil {
		return nil, nil, errs.NewMsg(core.ErrBadConfig, "data transfer symbol state is required")
	}
	q, conn, err := d.Storage.Conn(d.context())
	if err != nil {
		return nil, nil, err
	}
	q = q.WithSeriesSymbolState(d.Symbols)
	if d.KlineOptions != nil {
		q = q.WithKlineRuntimeOptions(*d.KlineOptions)
	}
	return q, conn.Release, nil
}

const maxOutFSize = 1024 * 1024 * 1024

func ExportKData(configFile string, outputDir string, numWorkers int, pb *utils2.StagedPrg) *errs.Error {
	return ExportKDataWithDeps(configFile, outputDir, numWorkers, KDataTransferDeps{Storage: CurrentStorage(), Symbols: loadDefaultSymbolState()}, pb)
}

func ExportKDataWithDeps(configFile string, outputDir string, numWorkers int, deps KDataTransferDeps, pb *utils2.StagedPrg) *errs.Error {
	cfg, err := config.GetExportConfig(configFile)
	if err != nil {
		return err
	}
	if _, statErr := os.Stat(filepath.Join(outputDir, "exInfo1.dat")); statErr == nil {
		return errs.NewMsg(core.ErrBadConfig, "export output already contains an archive; use a fresh directory")
	}
	task, err := genExportTaskWithDeps(deps, cfg, pb)
	if err != nil {
		return err
	}

	if err_ := utils2.EnsureDir(outputDir, 0755); err_ != nil {
		return errs.New(errs.CodeIOWriteFail, err_)
	}

	// 先导出辅助数据
	file, err_ := utils2.CreateNumFile(outputDir, "exInfo", "dat")
	if err_ != nil {
		return errs.New(errs.CodeIOWriteFail, err_)
	}
	err = dumpProto(task.exInfo, file)
	closeErr := file.Close()
	if err == nil && closeErr != nil {
		err = errs.New(errs.CodeIOWriteFail, closeErr)
	}
	if err != nil {
		return err
	}
	deps.logger().Info("export basic info ok")

	if err := runExportKlinesWithDeps(deps, task.jobs, outputDir, numWorkers, pb); err != nil {
		return err
	}
	files, _ := filepath.Glob(filepath.Join(outputDir, "kline*.dat"))
	// No marker is published for an empty or failed export.
	legacyInfo := EXInfo{Symbols: task.exInfo.Symbols}
	if err := validateTransferArchive(outputDir, &legacyInfo, files); err != nil {
		return errs.New(core.ErrBadConfig, err)
	}
	if err := writeTransferManifest(outputDir, files); err != nil {
		return errs.New(errs.CodeIOWriteFail, err)
	}
	return nil
}

func genExpAdjFactors(deps KDataTransferDeps, items []*config.MarketSymbolsRange) ([]*AdjFactorBlock, *errs.Error) {
	var adjFactors []*AdjFactorBlock
	ctx := deps.context()
	sess, release, err2 := deps.conn()
	if err2 != nil {
		return nil, err2
	}
	defer release()

	for _, adjCfg := range items {
		startMS, stopMS, err_ := config.ParseTimeRange(adjCfg.TimeRange)
		if err_ != nil {
			return nil, errs.New(errs.CodeRunTime, err_)
		}

		exchanges, markets := parseExgMarkets(deps.Symbols, adjCfg.Exchange, adjCfg.Market)
		for _, exchange := range exchanges {
			for _, market := range markets {
				exsList, err := parseExSymbols(deps.Symbols, exchange, adjCfg.ExgReal, market, adjCfg.Symbols)
				if err != nil {
					return nil, err
				}
				for _, exs := range exsList {
					facs, err_ := sess.GetAdjFactors(ctx, exs.ID)
					if err_ != nil {
						return nil, errs.New(core.ErrDbReadFail, err_)
					}
					for _, f := range facs {
						if f.StartMs >= startMS && f.StartMs <= stopMS {
							adjFactors = append(adjFactors, &AdjFactorBlock{
								Sid:     f.Sid,
								SubId:   f.SubID,
								StartMs: f.StartMs,
								Factor:  f.Factor,
							})
						}
					}
				}
			}
		}
	}
	return adjFactors, nil
}

func genExpCalendars(deps KDataTransferDeps, items []*config.MarketRange) ([]*CalendarBlock, *errs.Error) {
	var calendars []*CalendarBlock
	calSess, release, err2 := deps.conn()
	if err2 != nil {
		return nil, err2
	}
	defer release()

	for _, calCfg := range items {
		startMS, stopMS, err := config.ParseTimeRange(calCfg.TimeRange)
		if err != nil {
			return nil, errs.New(errs.CodeRunTime, err)
		}

		exchanges, markets := parseExgMarkets(deps.Symbols, calCfg.Exchange, calCfg.Market)
		allExList := deps.Symbols.GetExSymbolsByID("", "")
		allowExgs := make(map[string]bool)
		allowMarket := make(map[string]bool)
		for _, exchange := range exchanges {
			allowExgs[exchange] = true
		}
		for _, market := range markets {
			allowMarket[market] = true
		}
		realExgs := make(map[string]bool)
		for _, exs := range allExList {
			if _, ok := allowExgs[exs.Exchange]; !ok {
				continue
			}
			if _, ok := allowMarket[exs.Market]; !ok {
				continue
			}
			if calCfg.ExgReal != "" && exs.ExgReal != calCfg.ExgReal {
				continue
			}
			if exs.ExgReal != "" {
				realExgs[exs.ExgReal] = true
			} else {
				realExgs[exs.Exchange] = true
			}
		}
		for exchange := range realExgs {
			cals, err := calSess.GetCalendars(exchange, startMS, stopMS)
			if err != nil {
				return nil, err
			}
			if len(cals) == 0 {
				continue
			}
			times := make([]int64, 0, len(cals)*2)
			for _, it := range cals {
				times = append(times, it[0], it[1])
			}
			calendars = append(calendars, &CalendarBlock{
				Name:  exchange,
				Times: times,
			})
		}
	}
	return calendars, nil
}

func genExportTaskWithDeps(deps KDataTransferDeps, cfg *config.ExportConfig, pb *utils2.StagedPrg) (*ExportTask, *errs.Error) {
	adjFactors, err := genExpAdjFactors(deps, cfg.AdjFactors)
	if err != nil {
		return nil, err
	}

	calendars, err := genExpCalendars(deps, cfg.Calendars)
	if err != nil {
		return nil, err
	}

	jobs, exsBlockMap, err := genExportKlines(deps.Symbols, cfg.Klines, adjFactors)
	if err != nil {
		return nil, err
	}

	symbols := make([]*ExSymbolBlock, 0, len(exsBlockMap))
	for _, exs := range exsBlockMap {
		symbols = append(symbols, &ExSymbolBlock{
			Id:       exs.ID,
			Exchange: exs.Exchange,
			ExgReal:  exs.ExgReal,
			Market:   exs.Market,
			Symbol:   exs.Symbol,
			ListMs:   exs.ListMs,
			DelistMs: exs.DelistMs,
			Combined: exs.Combined,
			AggRules: exs.AggRules,
		})
	}

	// Generate kHoles for each ExSymbol+TimeFrame combination
	kHoles, err := genExpKHoles(deps, jobs, pb)
	if err != nil {
		return nil, err
	}

	return &ExportTask{
		jobs: jobs,
		exInfo: &EXInfo{
			ArchiveVersion: transferArchiveVersion,
			Symbols:        symbols,
			KHoles:         kHoles,
			AdjFactors:     adjFactors,
			Calendars:      calendars,
		},
	}, nil
}

func genExportKlines(symbols *SymbolState, items []*config.MarketTFSymbolsRange, adjs []*AdjFactorBlock) ([]*ExportKlineJob, map[int32]*ExSymbol, *errs.Error) {
	var tasks []*ExportKlineJob
	var exsResMap = make(map[int32]*ExSymbol)

	// Default timeframes if not specified
	defaultTFs := make([]string, 0, len(aggList))
	for _, agg := range aggList {
		defaultTFs = append(defaultTFs, agg.TimeFrame)
	}

	// Process klines configuration
	for _, kCfg := range items {
		startMS, stopMS, err_ := config.ParseTimeRange(kCfg.TimeRange)
		if err_ != nil {
			return nil, nil, errs.New(errs.CodeRunTime, err_)
		}

		exchanges, markets := parseExgMarkets(symbols, kCfg.Exchange, kCfg.Market)
		for _, exchange := range exchanges {
			hasInfo := config.ExchangeUsesOpaqueSymbols(exchange)
			for _, market := range markets {
				exsList, err := parseExSymbols(symbols, exchange, kCfg.ExgReal, market, kCfg.Symbols)
				if err != nil {
					return nil, nil, err
				}

				timeframes := kCfg.TimeFrames
				if len(timeframes) == 0 {
					timeframes = defaultTFs
				}

				for _, exs := range exsList {
					exsResMap[exs.ID] = exs
					for _, tf := range timeframes {
						tasks = append(tasks, &ExportKlineJob{
							ExSymbol:  exs,
							TimeFrame: tf,
							StartMS:   startMS,
							StopMS:    stopMS,
							HasInfo:   hasInfo,
						})
					}
				}
			}
		}
	}

	if len(adjs) > 0 {
		allExSymbols := symbols.GetExSymbolsByID("", "")
		for _, a := range adjs {
			exsResMap[a.Sid] = allExSymbols[a.Sid]
			exsResMap[a.SubId] = allExSymbols[a.SubId]
		}
	}

	// Sort tasks by Exchange, Market, TimeFrame (from large to small), Symbol
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].Exchange != tasks[j].Exchange {
			return tasks[i].Exchange < tasks[j].Exchange
		}
		if tasks[i].ExgReal != tasks[j].ExgReal {
			return tasks[i].ExgReal < tasks[j].ExgReal
		}
		if tasks[i].Market != tasks[j].Market {
			return tasks[i].Market < tasks[j].Market
		}
		if tasks[i].TimeFrame != tasks[j].TimeFrame {
			// Sort timeframes from large to small
			iTFSecs := utils.TFToSecs(tasks[i].TimeFrame)
			jTFSecs := utils.TFToSecs(tasks[j].TimeFrame)
			if iTFSecs != jTFSecs {
				return iTFSecs > jTFSecs
			}
		}
		return tasks[i].Symbol < tasks[j].Symbol
	})
	return tasks, exsResMap, nil
}

func genExpKHoles(deps KDataTransferDeps, jobs []*ExportKlineJob, pb *utils2.StagedPrg) ([]*KHoleBlock, *errs.Error) {
	rangeMap := make(map[string]*config.TimeTuple)
	var kHoles []*KHoleBlock
	holeSess, release, err2 := deps.conn()
	if err2 != nil {
		return nil, err2
	}
	defer release()

	// Find min startMS and max stopMS for each ExSymbol+TimeFrame
	for _, task := range jobs {
		key := fmt.Sprintf("%d_%s", task.ID, task.TimeFrame)
		if r, exists := rangeMap[key]; exists {
			if task.StartMS < r.StartMS {
				r.StartMS = task.StartMS
			}
			if task.StopMS > r.EndMS {
				r.EndMS = task.StopMS
			}
			rangeMap[key] = r
		} else {
			rangeMap[key] = &config.TimeTuple{StartMS: task.StartMS, EndMS: task.StopMS}
		}
	}

	pBar := utils2.NewPrgBar(len(jobs), "kHoles")
	if pb != nil {
		pBar.PrgCbs = append(pBar.PrgCbs, func(done int, total int) {
			pb.SetProgress("holes", float64(done)/float64(total))
		})
	}
	defer pBar.Close()

	// Query kHoles for each ExSymbol+TimeFrame
	for _, task := range jobs {
		pBar.Add(1)
		key := fmt.Sprintf("%d_%s", task.ID, task.TimeFrame)
		r := rangeMap[key]
		ranges, err := holeSess.ListSRanges(deps.context(), task.ID, "kline_"+task.TimeFrame, task.TimeFrame, r.StartMS, r.EndMS)
		if err != nil {
			return nil, errs.New(core.ErrDbReadFail, err)
		}
		if len(ranges) == 0 {
			continue
		}
		items := make([]int64, 0, len(ranges)/10)
		for _, rg := range ranges {
			if rg.HasData {
				continue
			}
			items = append(items, rg.StartMs, rg.StopMs)
		}
		if len(items) > 0 {
			kHoles = append(kHoles, &KHoleBlock{
				Sid:       task.ID,
				Timeframe: task.TimeFrame,
				Holes:     items,
			})
		}
	}
	return kHoles, nil
}

func parseExgMarkets(symbols *SymbolState, exgName, market string) ([]string, []string) {
	all := symbols.GetExSymbolsByID("", "")
	exgSet, marketSet := make(map[string]bool), make(map[string]bool)
	for _, item := range all {
		exgSet[item.Exchange] = true
		marketSet[item.Market] = true
	}
	exchanges := expandWildcard(exgName, utils2.KeysOfMap(exgSet))
	markets := expandWildcard(market, utils2.KeysOfMap(marketSet))
	return exchanges, markets
}

func parseExSymbols(state *SymbolState, exgName, exgReal, market string, symbols []string) ([]*ExSymbol, *errs.Error) {
	var exsList []*ExSymbol
	exsMap := state.GetExSymbolMap(exgName, market)
	if len(symbols) == 0 {
		exsList = make([]*ExSymbol, 0, len(exsMap))
		for _, exs := range exsMap {
			if exgReal != "" && exs.ExgReal != exgReal {
				continue
			}
			exsList = append(exsList, exs)
		}
	} else {
		exsList = make([]*ExSymbol, 0, len(symbols))
		for _, pair := range symbols {
			if exs, ok := exsMap[pair]; ok {
				if exgReal != "" && exs.ExgReal != exgReal {
					continue
				}
				exsList = append(exsList, exs)
			} else {
				return nil, errs.NewMsg(core.ErrBadConfig, "symbol not found: %s", pair)
			}
		}
	}
	return exsList, nil
}

// Helper function to expand wildcards
func expandWildcard(val string, allVals []string) []string {
	if val == "" {
		return allVals
	}
	return []string{val}
}

func exportKlines(sess *Queries, ctx context.Context, task *ExportKlineJob, outDir string, file *os.File, pBar *utils2.PrgBar) (*os.File, *errs.Error) {
	tfMS := int64(utils.TFToSecs(task.TimeFrame)) * 1000
	if tfMS <= 0 {
		return file, errs.NewMsg(core.ErrBadConfig, "invalid export timeframe %s", task.TimeFrame)
	}
	pJob := pBar.NewJob(int((task.StopMS - task.StartMS) / tfMS))
	defer pJob.Done()
	fields, err_ := transferColumns(ctx, sess, task.TimeFrame)
	if err_ != nil {
		return file, errs.New(core.ErrDbReadFail, err_)
	}
	for start := task.StartMS; start < task.StopMS; {
		if err := ctx.Err(); err != nil {
			return file, errs.New(errs.CodeRunTime, err)
		}
		rows, err := readTransferRows(ctx, sess, task, fields, start, 5000)
		if err != nil {
			return file, err
		}
		if len(rows) == 0 {
			break
		}
		block := &KlineBlock{ExsId: task.ID, Timeframe: task.TimeFrame}
		if err_ := encodeTransferBlock(block, fields, rows, sess.isQuestDB()); err_ != nil {
			return file, errs.New(core.ErrMarshalFail, err_)
		}
		if file == nil {
			file, err_ = utils2.CreateNumFile(outDir, "kline", "dat")
			if err_ != nil {
				return nil, errs.New(errs.CodeIOWriteFail, err_)
			}
		}
		if err := dumpProto(block, file); err != nil {
			return file, err
		}
		pJob.Add(len(rows))
		next := rows[len(rows)-1].EndMS
		if next <= start {
			return file, errs.NewMsg(core.ErrBadConfig, "export made no progress")
		}
		start = next
		stat, err_ := file.Stat()
		if err_ != nil {
			return file, errs.New(errs.CodeIOWriteFail, err_)
		}
		if stat.Size() > maxOutFSize {
			if err_ := file.Close(); err_ != nil {
				return nil, errs.New(errs.CodeIOWriteFail, err_)
			}
			file = nil
		}
	}
	return file, nil
}

func newKlineBlock(exsID int32, tf string, size int) *KlineBlock {
	return &KlineBlock{
		Start:     0,
		End:       0,
		ExsId:     exsID,
		Timeframe: tf,
		Open:      make([]float64, 0, size),
		High:      make([]float64, 0, size),
		Low:       make([]float64, 0, size),
		Close:     make([]float64, 0, size),
		Volume:    make([]float64, 0, size),
		Info:      make([]float64, 0, size),
	}
}

/*
Append
添加连续K线到末尾，如果中间缺失，则返回缺失的后面K线，创建新Block
*/
func (b *KlineBlock) Append(klines []*banexg.Kline, tfMSec int64, hasInfo bool) []*banexg.Kline {
	if len(klines) == 0 {
		return nil
	}
	if b.Start == 0 {
		// 为空时，允许第一个K线
		b.Start = klines[0].Time
		b.End = b.Start
	}
	for i, k := range klines {
		if b.End == k.Time {
			b.End = k.Time + tfMSec
			b.Open = append(b.Open, k.Open)
			b.High = append(b.High, k.High)
			b.Low = append(b.Low, k.Low)
			b.Close = append(b.Close, k.Close)
			b.Volume = append(b.Volume, k.Volume)
			if hasInfo {
				b.Info = append(b.Info, k.BuyVolume)
			}
		} else {
			return klines[i:]
		}
	}
	return nil
}

func (b *KlineBlock) Dump(file *os.File) *errs.Error {
	if len(b.Open) == 0 {
		return nil
	}
	err := dumpProto(b, file)
	if err == nil {
		b.Start = 0
		b.End = 0
		size := cap(b.Open)
		b.Open = make([]float64, 0, size)
		b.High = make([]float64, 0, size)
		b.Low = make([]float64, 0, size)
		b.Close = make([]float64, 0, size)
		b.Volume = make([]float64, 0, size)
		b.Info = make([]float64, 0, size)
	}
	return err
}

func dumpProto(b proto.Message, file io.Writer) *errs.Error {
	var err2 *errs.Error
	data, err := proto.Marshal(b)
	if err != nil {
		err2 = errs.New(core.ErrMarshalFail, err)
	} else {
		sizeBuf, err := utils2.IntToBytes(uint32(len(data)))
		if err != nil {
			err2 = errs.New(core.ErrRunTime, err)
		} else {
			if n, writeErr := file.Write(sizeBuf); writeErr != nil || n != len(sizeBuf) {
				err = writeErr
				if err == nil {
					err = io.ErrShortWrite
				}
				err2 = errs.New(errs.CodeIOWriteFail, err)
			} else {
				if n, writeErr := file.Write(data); writeErr != nil || n != len(data) {
					err = writeErr
					if err == nil {
						err = io.ErrShortWrite
					}
					err2 = errs.New(errs.CodeIOWriteFail, err)
				}
			}
		}
	}
	return err2
}

func runExportKlinesWithDeps(deps KDataTransferDeps, jobs []*ExportKlineJob, outputDir string, numWorkers int, pb *utils2.StagedPrg) *errs.Error {
	ctx, cancel := context.WithCancel(deps.context())
	defer cancel()
	numWorkers = min(len(jobs), max(1, numWorkers))

	// Create worker pool
	var wg sync.WaitGroup
	jobCh := make(chan *ExportKlineJob)
	errCh := make(chan error, 2*numWorkers+2)
	pBar := utils2.NewPrgBar(len(jobs)*core.StepTotal, "kline")
	if pb != nil {
		pBar.PrgCbs = append(pBar.PrgCbs, func(done int, total int) {
			pb.SetProgress("kline", float64(done)/float64(total))
		})
	}
	defer pBar.Close()

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var file *os.File
			sess, release, err := deps.conn()
			if err != nil {
				errCh <- fmt.Errorf("error get db session: %v", err.Short())
				cancel()
				return
			}
			defer func() {
				release()
				if file != nil {
					if closeErr := file.Close(); closeErr != nil {
						errCh <- closeErr
						cancel()
					}
				}
			}()

			for job := range jobCh {
				file, err = exportKlines(sess, ctx, job, outputDir, file, pBar)
				if err != nil {
					errCh <- fmt.Errorf("error exporting %s %s: %v", job.Symbol, job.TimeFrame, err)
					cancel()
					return
				}
			}
		}()
	}

	// Send jobs to workers
	go func() {
		defer close(jobCh)
		for _, job := range jobs {
			select {
			case <-ctx.Done():
				return
			case jobCh <- job:
			}
		}
	}()

	wg.Wait()
	deps.logger().Info("export kline done")

	// Check for errors
	select {
	case err_ := <-errCh:
		return errs.New(errs.CodeRunTime, err_)
	default:
		if ctx.Err() != nil {
			return errs.New(errs.CodeRunTime, ctx.Err())
		}
		return nil
	}
}

func ImportData(dataDir string, numWorkers int, pb *utils2.StagedPrg) *errs.Error {
	options := LegacyKlineRuntimeOptions()
	return ImportDataWithDeps(dataDir, numWorkers, KDataTransferDeps{Storage: CurrentStorage(), Symbols: loadDefaultSymbolState(), KlineOptions: &options}, pb)
}

func ImportDataWithDeps(dataDir string, numWorkers int, deps KDataTransferDeps, pb *utils2.StagedPrg) *errs.Error {
	if deps.KlineOptions == nil {
		return errs.NewMsg(core.ErrBadConfig, "data import kline runtime options are required")
	}
	// 首先读取并处理exInfo文件
	exInfoPath := filepath.Join(dataDir, "exInfo1.dat")
	exInfoFile, err_ := os.Open(exInfoPath)
	if err_ != nil {
		return errs.New(errs.CodeIOReadFail, err_)
	}
	defer exInfoFile.Close()

	exInfo := &EXInfo{}
	if err := readProtoMessage(exInfoFile, exInfo); err != nil {
		return err
	}

	files, globErr := filepath.Glob(filepath.Join(dataDir, "kline*.dat"))
	if globErr != nil {
		return errs.New(errs.CodeIOReadFail, globErr)
	}
	if err := validateTransferArchive(dataDir, exInfo, files); err != nil {
		return errs.New(core.ErrBadConfig, err)
	}

	if exInfo.ArchiveVersion > 0 {
		for _, item := range exInfo.Symbols {
			if old := deps.Symbols.GetExSymbol2(item.Exchange, item.Market, item.Symbol); old != nil && (old.Combined != item.Combined || old.AggRules != item.AggRules) {
				return errs.NewMsg(core.ErrBadConfig, "symbol metadata conflict for %s", item.Symbol)
			}
		}
	}
	idMap, err := importSymbolsWithDeps(deps, exInfo.Symbols)
	if err != nil {
		return err
	}
	// 创建数据库连接和导入管理器
	if err = importAdjFactorsWithDeps(deps, idMap, exInfo.AdjFactors); err != nil {
		return err
	}
	if err = importCalendarsWithDeps(deps, exInfo.Calendars); err != nil {
		return err
	}

	// Get all .dat files in the directory
	files, err_ = filepath.Glob(filepath.Join(dataDir, "kline*.dat"))
	if err_ != nil {
		return errs.New(errs.CodeIOReadFail, err_)
	}
	if len(files) == 0 {
		return errs.NewMsg(core.ErrBadConfig, "no .dat files found in directory: %s", dataDir)
	}

	// Create worker pool
	numWorkers = max(1, numWorkers)
	var wg sync.WaitGroup
	taskCh := make(chan string)
	errCh := make(chan error, numWorkers+2)
	insRanges := make(map[int32]map[string][]MSRange)
	insLock := deadlock.Mutex{}
	ctx, cancel := context.WithCancel(deps.context())
	defer cancel()
	pBar := utils2.NewPrgBar(len(files)*core.StepTotal, "kLine")
	if pb != nil {
		pBar.PrgCbs = append(pBar.PrgCbs, func(done int, total int) {
			pb.SetProgress("kline", float64(done)/float64(total))
		})
	}
	defer pBar.Close()

	updateRange := func(sid int32, tf string, start, end int64) {
		if start <= 0 || end <= start {
			return
		}
		insLock.Lock()
		defer insLock.Unlock()
		tfMap := insRanges[sid]
		if tfMap == nil {
			tfMap = make(map[string][]MSRange)
			insRanges[sid] = tfMap
		}
		tfMap[tf] = append(tfMap[tf], MSRange{Start: start, Stop: end})
	}

	runFile := func(path string) {
		file, err_ := os.Open(path)
		pJob := pBar.NewJob(300)
		defer pJob.Done()
		if err_ != nil {
			errCh <- fmt.Errorf("error opening file %s: %v", path, err_)
			cancel()
			return
		}
		defer file.Close()
		sess2, release, err := deps.conn()
		if err != nil {
			errCh <- fmt.Errorf("error get db session: %v", err)
			cancel()
			return
		}
		defer release()

		for {
			block := KlineBlock{}
			err = readProtoMessage(file, &block)
			if err != nil {
				errCh <- fmt.Errorf("error reading block from %s: %v", path, err)
				cancel()
				return
			}
			if len(block.Open) == 0 && len(block.Series) == 0 {
				break
			}
			pJob.Add(1)
			sid, mapped := idMap[block.ExsId]
			if !mapped || sid <= 0 {
				errCh <- fmt.Errorf("unknown archive symbol id %d", block.ExsId)
				cancel()
				return
			}
			if err := importKlines(sess2, ctx, sid, &block); err != nil {
				errCh <- fmt.Errorf("error importing klines for symbol %d: %v", block.ExsId, err)
				cancel()
				return
			}

			rows, _, _, decodeErr := decodeTransferBlock(&block, sid)
			if decodeErr != nil {
				errCh <- decodeErr
				cancel()
				return
			}
			for _, row := range rows {
				updateRange(sid, block.Timeframe, row.TimeMS, row.EndMS)
			}
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range taskCh {
				if ctx.Err() != nil {
					return
				}
				runFile(path)
			}
		}()
	}

	// Read blocks from all files and send to workers
	go func() {
		defer close(taskCh)
		for _, filePath := range files {
			select {
			case <-ctx.Done():
				return
			case taskCh <- filePath:
			}
		}
	}()

	wg.Wait()

	select {
	case workerErr := <-errCh:
		return errs.New(errs.CodeRunTime, workerErr)
	default:
	}
	if ctx.Err() != nil {
		return errs.New(errs.CodeRunTime, ctx.Err())
	}
	// Large imports can outlive a server-side connection. Acquire a fresh
	// connection for range metadata after all worker writes have completed.
	sess, release, err := deps.conn()
	if err != nil {
		return err
	}
	defer release()

	itemNum := 0
	for _, tfMap := range insRanges {
		for range tfMap {
			itemNum += 1
		}
	}
	pBar2 := utils2.NewPrgBar(itemNum, "kRange")
	if pb != nil {
		pBar2.PrgCbs = append(pBar2.PrgCbs, func(done int, total int) {
			pb.SetProgress("range", float64(done)/float64(total))
		})
	}
	defer pBar2.Close()
	for sid, tfMap := range insRanges {
		for tf, rawRanges := range tfMap {
			newRanges := mergeMSRanges(rawRanges)
			if len(newRanges) == 0 {
				pBar2.Add(1)
				continue
			}
			newStart, newEnd := newRanges[0].Start, newRanges[len(newRanges)-1].Stop
			oldStart, oldEnd := sess.GetKlineRange(sid, tf)

			// sranges replaces kinfo/khole: record imported data ranges (non-contiguous allowed).
			for _, r := range newRanges {
				if err := sess.UpdateSRanges(ctx, sid, "kline_"+tf, tf, r.Start, r.Stop, true); err != nil {
					return errs.New(core.ErrDbExecFail, err)
				}
			}
			// Scan the whole imported span to record holes (including gaps between non-contiguous blocks).
			if err := sess.updateKHoles(sid, tf, newStart, newEnd, false); err != nil {
				return err
			}

			// If new data is disjoint from existing data, mark holes in the gap between them.
			if oldStart > 0 && oldEnd > 0 && (newEnd < oldStart || newStart > oldEnd) {
				if err := sess.updateKHoles(sid, tf, min(newStart, oldStart), max(newEnd, oldEnd), false); err != nil {
					return err
				}
			}
			pBar2.Add(1)
		}
	}

	// Check for errors
	select {
	case err := <-errCh:
		return errs.New(errs.CodeRunTime, err)
	default:
		return nil
	}
}

func readProtoMessage(file *os.File, msg proto.Message) *errs.Error {
	// 读取消息大小
	sizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(file, sizeBuf); err != nil {
		if err == io.EOF {
			return nil
		}
		return errs.New(errs.CodeIOReadFail, err)
	}
	size := binary.LittleEndian.Uint32(sizeBuf)
	if size == 0 {
		return errs.NewMsg(errs.CodeIOReadFail, "zero-length archive frame")
	}

	// 读取消息内容
	msgBuf := make([]byte, size)
	if _, err := io.ReadFull(file, msgBuf); err != nil {
		return errs.New(errs.CodeIOReadFail, err)
	}

	// 解析消息
	if err := proto.Unmarshal(msgBuf, msg); err != nil {
		return errs.New(errs.CodeUnmarshalFail, err)
	}

	return nil
}

func tryInsertKlines(sess *Queries, tf string, sid int32, klines []*banexg.Kline) *errs.Error {
	if len(klines) == 0 {
		return nil
	}
	// insert would update if duplicate in questdb
	_, err := sess.InsertKLines(tf, sid, klines)
	return err
}

func importKlines(sess *Queries, ctx context.Context, sid int32, block *KlineBlock) *errs.Error {
	if sid <= 0 {
		return errs.NewMsg(core.ErrBadConfig, "import target sid must be positive")
	}
	rows, fields, quest, err := decodeTransferBlock(block, sid)
	if err != nil {
		return errs.New(core.ErrBadConfig, err)
	}
	if err := ctx.Err(); err != nil {
		return errs.New(errs.CodeRunTime, err)
	}
	if err := ensureTransferColumns(ctx, sess, block.Timeframe, fields, quest); err != nil {
		return errs.New(core.ErrDbExecFail, err)
	}
	for start := 0; start < len(rows); {
		end := start + 1
		for end < len(rows) && end-start < 1000 && rows[end].TimeMS == rows[end-1].EndMS {
			end++
		}
		if err := ctx.Err(); err != nil {
			return errs.New(errs.CodeRunTime, err)
		}
		if _, err := sess.InsertOHLCVSeriesContext(ctx, block.Timeframe, sid, rows[start:end]); err != nil {
			return err
		}
		start = end
	}
	return nil
}

func importSymbolsWithDeps(deps KDataTransferDeps, items []*ExSymbolBlock) (map[int32]int32, *errs.Error) {
	olds := deps.Symbols.GetExSymbolsByID("", "")
	idMap := make(map[int32]int32)
	var addItems []*ExSymbolBlock
	var addExs []*ExSymbol
	oldMap := make(map[string]*ExSymbol)
	for _, old := range olds {
		key := fmt.Sprintf("%s_%s_%s", old.Exchange, old.Market, old.Symbol)
		oldMap[key] = old
	}
	for _, it := range items {
		key := fmt.Sprintf("%s_%s_%s", it.Exchange, it.Market, it.Symbol)
		if old, ok := oldMap[key]; ok {
			if it.Combined != old.Combined || it.AggRules != old.AggRules {
				return nil, errs.NewMsg(core.ErrBadConfig, "symbol metadata conflict for %s", it.Symbol)
			}
			idMap[it.Id] = old.ID
		} else {
			addItems = append(addItems, it)
			addExs = append(addExs, &ExSymbol{
				Exchange: it.Exchange,
				ExgReal:  it.ExgReal,
				Market:   it.Market,
				Symbol:   it.Symbol,
				ListMs:   it.ListMs,
				DelistMs: it.DelistMs,
				Combined: it.Combined,
				AggRules: it.AggRules,
			})
		}
	}
	if len(addExs) > 0 {
		q, release, err := deps.conn()
		if err != nil {
			return nil, err
		}
		defer release()
		adds := make([]AddSymbolsParams, 0, len(addExs))
		for _, item := range addExs {
			adds = append(adds, AddSymbolsParams{Exchange: item.Exchange, ExgReal: item.ExgReal, Market: item.Market, Symbol: item.Symbol, ListMs: item.ListMs, DelistMs: item.DelistMs, Combined: item.Combined, AggRules: item.AggRules})
		}
		if _, err := q.AddSymbols(deps.context(), adds); err != nil {
			return nil, errs.New(core.ErrDbExecFail, err)
		}
		deps.logger().Info("symbols import ok", zap.Int("num", len(addExs)))
		for i, exs := range addExs {
			stored := deps.Symbols.GetExSymbol2(exs.Exchange, exs.Market, exs.Symbol)
			if stored == nil || stored.ID == 0 {
				return nil, errs.NewMsg(errs.CodeRunTime, "add ExSymbol fail: %v", exs.Symbol)
			}
			idMap[addItems[i].Id] = stored.ID
		}
	}
	return idMap, nil
}

func importAdjFactorsWithDeps(deps KDataTransferDeps, idMap map[int32]int32, items []*AdjFactorBlock) *errs.Error {
	if len(items) == 0 {
		return nil
	}
	adjSess, release, err2 := deps.conn()
	if err2 != nil {
		return err2
	}
	defer release()
	idArr := make(map[int32][]*AdjFactor)
	for _, it := range items {
		oldId, ok := idMap[it.Sid]
		if !ok {
			return errs.NewMsg(errs.CodeRunTime, "sid unknown: %v", it.Sid)
		}
		oldSubId, ok := idMap[it.SubId]
		if !ok {
			return errs.NewMsg(errs.CodeRunTime, "subId unknown: %v", it.SubId)
		}
		arr, _ := idArr[oldId]
		idArr[oldId] = append(arr, &AdjFactor{
			Sid:     oldId,
			SubID:   oldSubId,
			StartMs: it.StartMs,
			Factor:  it.Factor,
		})
	}
	addNum := 0
	defer func() { deps.logger().Info("adjFactors import ok", zap.Int("num", addNum)) }()
	for sid, arr := range idArr {
		olds, err := adjSess.GetAdjFactors(deps.context(), sid)
		if err != nil {
			return errs.New(core.ErrDbReadFail, err)
		}
		var valids = make([]*AdjFactor, 0, len(arr))
		known := make(map[string]bool, len(olds))
		for _, old := range olds {
			known[fmt.Sprintf("%d:%d:%d:%g", old.Sid, old.SubID, old.StartMs, old.Factor)] = true
		}
		for _, item := range arr {
			key := fmt.Sprintf("%d:%d:%d:%g", item.Sid, item.SubID, item.StartMs, item.Factor)
			if !known[key] {
				valids = append(valids, item)
			}
		}
		var adds = make([]AddAdjFactorsParams, 0, len(valids))
		for _, v := range valids {
			adds = append(adds, AddAdjFactorsParams{
				Sid:     v.Sid,
				SubID:   v.SubID,
				StartMs: v.StartMs,
				Factor:  v.Factor,
			})
		}
		if len(adds) > 0 {
			_, err_ := adjSess.AddAdjFactors(deps.context(), adds)
			if err_ != nil {
				return errs.New(core.ErrDbExecFail, err_)
			}
			addNum += len(adds)
		}
	}
	return nil
}

func importCalendarsWithDeps(deps KDataTransferDeps, cals []*CalendarBlock) *errs.Error {
	if len(cals) == 0 {
		return nil
	}
	calSess, release, err2 := deps.conn()
	if err2 != nil {
		return err2
	}
	defer release()
	for _, cal := range cals {
		if len(cal.Times) == 0 {
			continue
		}
		items, err := calSess.GetCalendarsWithContext(deps.context(), cal.Name, cal.Times[0], cal.Times[len(cal.Times)-1])
		if err != nil {
			return err
		}

		// 创建一个map来存储已存在的区间
		startMS, endMS := int64(0), int64(0)
		if len(items) > 0 {
			startMS = items[0][0]
			endMS = items[len(items)-1][0]
		}

		// 处理新的日历数据
		var newCalendars []AddCalendarsParams
		for i := 0; i < len(cal.Times); i += 2 {
			start := cal.Times[i]
			end := cal.Times[i+1]

			if startMS == 0 || end <= startMS || start >= endMS {
				newCalendars = append(newCalendars, AddCalendarsParams{
					Name:    cal.Name,
					StartMs: start,
					StopMs:  end,
				})
			}
		}

		// 添加新的日历数据
		if len(newCalendars) > 0 {
			_, err_ := calSess.AddCalendars(deps.context(), newCalendars)
			if err_ != nil {
				return errs.New(core.ErrDbExecFail, err_)
			}
			deps.logger().Info("import calendars", zap.String("exg", cal.Name), zap.Int("num", len(newCalendars)))
		}
	}
	return nil
}

type IfParam struct {
	Cond bool
	Val  interface{}
	Tpl  string
}

func BuildQuery(b *strings.Builder, params []interface{}, start int, fields []IfParam) ([]interface{}, int) {
	for _, f := range fields {
		if f.Cond {
			b.WriteString(fmt.Sprintf(f.Tpl, start))
			params = append(params, f.Val)
			start++
		}
	}
	return params, start
}
