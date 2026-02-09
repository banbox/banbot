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

	"github.com/banbox/banexg/log"
	"github.com/sasha-s/go-deadlock"
	"go.uber.org/zap"

	"github.com/banbox/banbot/core"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/exg"
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
}

type ExportTask struct {
	jobs   []*ExportKlineJob
	exInfo *EXInfo
}

const maxOutFSize = 1024 * 1024 * 1024

func ExportKData(configFile string, outputDir string, numWorkers int, pb *utils2.StagedPrg) *errs.Error {
	cfg, err := config.GetExportConfig(configFile)
	if err != nil {
		return err
	}
	task, err := genExportTask(cfg, pb)
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
	file.Close()
	if err != nil {
		return err
	}
	log.Info("export basic info ok")

	return runExportKlines(task.jobs, outputDir, numWorkers, pb)
}

func genExpAdjFactors(items []*config.MarketSymbolsRange) ([]*AdjFactorBlock, *errs.Error) {
	var adjFactors []*AdjFactorBlock

	for _, adjCfg := range items {
		startMS, stopMS, err_ := config.ParseTimeRange(adjCfg.TimeRange)
		if err_ != nil {
			return nil, errs.New(errs.CodeRunTime, err_)
		}

		exchanges, markets := parseExgMarkets(adjCfg.Exchange, adjCfg.Market)
		for _, exchange := range exchanges {
			for _, market := range markets {
				exsList, err := parseExSymbols(exchange, adjCfg.ExgReal, market, adjCfg.Symbols)
				if err != nil {
					return nil, err
				}
				for _, exs := range exsList {
					facs, err_ := PubQ().GetAdjFactors(context.Background(), exs.ID)
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

func genExpCalendars(items []*config.MarketRange) ([]*CalendarBlock, *errs.Error) {
	var calendars []*CalendarBlock

	for _, calCfg := range items {
		startMS, stopMS, err := config.ParseTimeRange(calCfg.TimeRange)
		if err != nil {
			return nil, errs.New(errs.CodeRunTime, err)
		}

		exchanges, markets := parseExgMarkets(calCfg.Exchange, calCfg.Market)
		allExList := GetAllExSymbols()
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
			cals, err := PubQ().GetCalendars(exchange, startMS, stopMS)
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

func genExportTask(cfg *config.ExportConfig, pb *utils2.StagedPrg) (*ExportTask, *errs.Error) {
	adjFactors, err := genExpAdjFactors(cfg.AdjFactors)
	if err != nil {
		return nil, err
	}

	calendars, err := genExpCalendars(cfg.Calendars)
	if err != nil {
		return nil, err
	}

	jobs, exsBlockMap, err := genExportKlines(cfg.Klines, adjFactors)
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
		})
	}

	// Generate kHoles for each ExSymbol+TimeFrame combination
	kHoles, err := genExpKHoles(jobs, pb)
	if err != nil {
		return nil, err
	}

	return &ExportTask{
		jobs: jobs,
		exInfo: &EXInfo{
			Symbols:    symbols,
			KHoles:     kHoles,
			AdjFactors: adjFactors,
			Calendars:  calendars,
		},
	}, nil
}

func genExportKlines(items []*config.MarketTFSymbolsRange, adjs []*AdjFactorBlock) ([]*ExportKlineJob, map[int32]*ExSymbol, *errs.Error) {
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

		exchanges, markets := parseExgMarkets(kCfg.Exchange, kCfg.Market)
		for _, exchange := range exchanges {
			for _, market := range markets {
				exsList, err := parseExSymbols(exchange, kCfg.ExgReal, market, kCfg.Symbols)
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
						})
					}
				}
			}
		}
	}

	if len(adjs) > 0 {
		allExSymbols := GetAllExSymbols()
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

func genExpKHoles(jobs []*ExportKlineJob, pb *utils2.StagedPrg) ([]*KHoleBlock, *errs.Error) {
	rangeMap := make(map[string]*config.TimeTuple)
	var kHoles []*KHoleBlock

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
		ranges, err := PubQ().ListSRanges(context.Background(), task.ID, "kline_"+task.TimeFrame, task.TimeFrame, r.StartMS, r.EndMS)
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

func parseExgMarkets(exgName, market string) ([]string, []string) {
	exchanges := expandWildcard(exgName, utils2.KeysOfMap(exg.AllowExgIds))
	markets := expandWildcard(market, []string{banexg.MarketSpot, banexg.MarketLinear, banexg.MarketInverse})
	return exchanges, markets
}

func parseExSymbols(exgName, exgReal, market string, symbols []string) ([]*ExSymbol, *errs.Error) {
	var exsList []*ExSymbol
	exsMap := GetExSymbolMap(exgName, market)
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
	exs := task.ExSymbol
	tfMSecs := int64(utils.TFToSecs(task.TimeFrame) * 1000)
	totalKLineNum := int((task.StopMS - task.StartMS) / tfMSecs)
	pJob := pBar.NewJob(totalKLineNum)
	defer pJob.Done()
	if totalKLineNum <= 0 {
		return file, nil
	}

	// 分批获取数据，每批最多5000条
	var err_ error
	var err *errs.Error
	var klines []*banexg.Kline
	const batchSize, maxKlineSize = 5000, 655360
	startMS := task.StartMS
	block := newKlineBlock(exs.ID, task.TimeFrame, min(batchSize, totalKLineNum))
	hasInfo := task.Exchange == "china"
	if file == nil {
		file, err_ = utils2.CreateNumFile(outDir, "kline", "dat")
		if err_ != nil {
			return nil, errs.New(errs.CodeIOWriteFail, err_)
		}
	}

	for startMS < task.StopMS {
		select {
		case <-ctx.Done():
			return file, errs.New(errs.CodeRunTime, ctx.Err())
		default:
		}

		_, klines, err = sess.GetOHLCV(exs, task.TimeFrame, startMS, task.StopMS, batchSize, false)
		if err != nil {
			return file, err
		}
		if len(klines) == 0 {
			break
		}
		curEnd := klines[len(klines)-1].Time + tfMSecs
		pJob.Add(int((curEnd - startMS) / tfMSecs))
		startMS = curEnd

		for len(klines) > 0 {
			klines = block.Append(klines, tfMSecs, hasInfo)
			if len(klines) > 0 || len(block.Open) > maxKlineSize {
				err = block.Dump(file)
				if err != nil {
					return file, err
				}
				if info, err_ := file.Stat(); err_ != nil || info.Size() > maxOutFSize {
					file.Close()
					file, err_ = utils2.CreateNumFile(outDir, "kline", "dat")
					if err_ != nil {
						return nil, errs.New(errs.CodeIOWriteFail, err_)
					}
				}
			}
		}
	}
	err = block.Dump(file)
	if err != nil {
		return file, err
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

func dumpProto(b proto.Message, file *os.File) *errs.Error {
	var err2 *errs.Error
	data, err := proto.Marshal(b)
	if err != nil {
		err2 = errs.New(core.ErrMarshalFail, err)
	} else {
		sizeBuf, err := utils2.IntToBytes(uint32(len(data)))
		if err != nil {
			err2 = errs.New(core.ErrRunTime, err)
		} else {
			if _, err = file.Write(sizeBuf); err != nil {
				err2 = errs.New(errs.CodeIOWriteFail, err)
			} else {
				if _, err = file.Write(data); err != nil {
					err2 = errs.New(errs.CodeIOWriteFail, err)
				}
			}
		}
	}
	return err2
}

func runExportKlines(jobs []*ExportKlineJob, outputDir string, numWorkers int, pb *utils2.StagedPrg) *errs.Error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	numWorkers = min(len(jobs), max(1, numWorkers))

	// Create worker pool
	var wg sync.WaitGroup
	jobCh := make(chan *ExportKlineJob)
	errCh := make(chan error, numWorkers+2)
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
			var file *os.File
			sess, conn, err := Conn(nil)
			if err != nil {
				errCh <- fmt.Errorf("error get db session: %v", err.Short())
				cancel()
				return
			}
			defer func() {
				wg.Done()
				conn.Release()
				if file != nil {
					_ = file.Close()
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
		for _, job := range jobs {
			select {
			case <-ctx.Done():
				return
			case jobCh <- job:
			}
		}
		close(jobCh)
	}()

	wg.Wait()
	log.Info("export kline done")

	// Check for errors
	select {
	case err_ := <-errCh:
		return errs.New(errs.CodeRunTime, err_)
	default:
		return nil
	}
}

func ImportData(dataDir string, numWorkers int, pb *utils2.StagedPrg) *errs.Error {
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

	idMap, err := importSymbols(exInfo.Symbols)
	if err != nil {
		return err
	}
	// 创建数据库连接和导入管理器
	sess, conn, err := Conn(nil)
	if err != nil {
		return err
	}
	defer conn.Release()

	if err = importAdjFactors(idMap, exInfo.AdjFactors); err != nil {
		return err
	}
	if err = importCalendars(exInfo.Calendars); err != nil {
		return err
	}

	// Get all .dat files in the directory
	files, err_ := filepath.Glob(filepath.Join(dataDir, "kline*.dat"))
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
	ctx, cancel := context.WithCancel(context.Background())
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
		sess2, conn2, err := Conn(nil)
		if err != nil {
			errCh <- fmt.Errorf("error get db session: %v", err)
			cancel()
			return
		}
		defer conn2.Release()

		for {
			block := KlineBlock{}
			err = readProtoMessage(file, &block)
			if err != nil {
				errCh <- fmt.Errorf("error reading block from %s: %v", path, err)
				cancel()
				return
			}
			if len(block.Open) == 0 {
				break
			}
			pJob.Add(1)
			sid := idMap[block.ExsId]
			updateRange(sid, block.Timeframe, block.Start, block.End)
			if err := importKlines(sess2, ctx, sid, &block); err != nil {
				errCh <- fmt.Errorf("error importing klines for symbol %d: %v", block.ExsId, err)
				cancel()
				return
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
				runFile(path)
			}
		}()
	}

	// Read blocks from all files and send to workers
	go func() {
		for _, filePath := range files {
			select {
			case <-ctx.Done():
				return
			case taskCh <- filePath:
			}
		}
		close(taskCh)
	}()

	wg.Wait()

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
			oldStart, oldEnd := PubQ().GetKlineRange(sid, tf)

			// sranges replaces kinfo/khole: record imported data ranges (non-contiguous allowed).
			for _, r := range newRanges {
				if err := PubQ().UpdateSRanges(context.Background(), sid, "kline_"+tf, tf, r.Start, r.Stop, true); err != nil {
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
	const batchSize = 1000
	klines := make([]*banexg.Kline, 0, batchSize)
	startMS := block.Start
	var tfMSecs = int64(utils.TFToSecs(block.Timeframe) * 1000)
	for i := 0; i < len(block.Open); i++ {
		bar := &banexg.Kline{
			Time:   startMS,
			Open:   block.Open[i],
			High:   block.High[i],
			Low:    block.Low[i],
			Close:  block.Close[i],
			Volume: block.Volume[i],
		}
		if len(block.Info) > i {
			bar.BuyVolume = block.Info[i]
		}
		klines = append(klines, bar)
		if len(klines) >= batchSize {
			select {
			case <-ctx.Done():
				return nil
			default:
			}
			err := tryInsertKlines(sess, block.Timeframe, sid, klines)
			if err != nil {
				return err
			}
			klines = make([]*banexg.Kline, 0, batchSize)
		}
		startMS += tfMSecs
	}
	// 处理剩余的K线数据
	if len(klines) > 0 {
		err := tryInsertKlines(sess, block.Timeframe, sid, klines)
		if err != nil {
			return err
		}
	}
	return nil
}

func importSymbols(items []*ExSymbolBlock) (map[int32]int32, *errs.Error) {
	olds := GetAllExSymbols()
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
			})
		}
	}
	if len(addExs) > 0 {
		err := EnsureSymbols(addExs)
		if err != nil {
			return nil, err
		}
		log.Info("symbols import ok", zap.Int("num", len(addExs)))
		for i, exs := range addExs {
			if exs.ID == 0 {
				return nil, errs.NewMsg(errs.CodeRunTime, "add ExSymbol fail: %v", exs.Symbol)
			}
			idMap[addItems[i].Id] = exs.ID
		}
	}
	return idMap, nil
}

func importAdjFactors(idMap map[int32]int32, items []*AdjFactorBlock) *errs.Error {
	if len(items) == 0 {
		return nil
	}
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
	defer log.Info("adjFactors import ok", zap.Int("num", addNum))
	for sid, arr := range idArr {
		olds, err := GetAdjs(sid)
		if err != nil {
			return err
		}
		var valids = make([]*AdjFactor, 0, len(arr))
		if len(olds) == 0 {
			start := olds[0].StartMS
			end := olds[len(olds)-1].StopMS
			for _, v := range arr {
				if v.StartMs < start || v.StartMs >= end {
					valids = append(valids, v)
				}
			}
		} else {
			valids = arr
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
			_, err_ := PubQ().AddAdjFactors(context.Background(), adds)
			if err_ != nil {
				return errs.New(core.ErrDbExecFail, err_)
			}
			addNum += len(adds)
		}
	}
	return nil
}

func importCalendars(cals []*CalendarBlock) *errs.Error {
	if len(cals) == 0 {
		return nil
	}
	for _, cal := range cals {
		if len(cal.Times) == 0 {
			continue
		}
		items, err := PubQ().GetCalendars(cal.Name, cal.Times[0], cal.Times[len(cal.Times)-1])
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
			_, err_ := PubQ().AddCalendars(context.Background(), newCalendars)
			if err_ != nil {
				return errs.New(core.ErrDbExecFail, err_)
			}
			log.Info("import calendars", zap.String("exg", cal.Name), zap.Int("num", len(newCalendars)))
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
