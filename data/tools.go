package data

import (
	"archive/zip"
	"encoding/csv"
	"fmt"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	utils2 "github.com/banbox/banexg/utils"
	"github.com/sasha-s/go-deadlock"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

/*
The problem with tick data
1. In the compressed tick package every day, a symbol file may have multiple symbol data, which coincide with other valid ticks, and need to be deduplicated
2. When there is a night session, the date of the night session data is actually the previous day
tick数据的问题
1. 每天的压缩tick包内，一个symbol文件可能有多个symbol的数据，且和其他有效的tick重合，需要去重
2. 有夜盘时，夜盘数据的日期实际是前一天
*/

var (
	ConcurNum = 5 // 并发处理的数量
	// The following are used in Build1mWithTicks to build 1m candlesticks from ticks
	// 下面几个在Build1mWithTicks中使用，从tick构建1m K线
	symKLines = make(map[string][]*banexg.Kline) // 1M data for the current year, the key is the contract ID 当前年的1m数据，键是合约ID
	klineLock deadlock.Mutex                     //symKlines的并发读写锁
	timeMsMin = int64(0)
	timeMsMax = int64(0)
)

type FuncConvert func(inPath string, file *zip.File, writer *zip.Writer) *errs.Error

type FuncReadZipItem func(inPath string, fid int, file *zip.File, arg interface{}) *errs.Error

type FuncTickBar func(inPath string, row []string) (string, int64, [5]float64)

func zipConvert(inPath, outPath, suffix string, convert FuncConvert, pBar *utils.PrgBar) *errs.Error {
	r, err := zip.OpenReader(inPath)
	if err != nil {
		pBar.Add(core.StepTotal)
		return errs.New(errs.CodeIOReadFail, err)
	}
	defer r.Close()
	if err = utils.EnsureDir(filepath.Dir(outPath), 0755); err != nil {
		pBar.Add(core.StepTotal)
		return errs.New(errs.CodeIOWriteFail, err)
	}
	out, err := os.Create(outPath)
	if err != nil {
		pBar.Add(core.StepTotal)
		return errs.New(errs.CodeIOWriteFail, err)
	}
	defer out.Close()
	zipWriter := zip.NewWriter(out)
	defer zipWriter.Close()
	bar := pBar.NewJob(len(r.File))
	defer bar.Done()
	for _, f := range r.File {
		bar.Add(1)
		if f.FileInfo().IsDir() || !strings.HasSuffix(f.Name, suffix) {
			continue
		}
		err2 := convert(inPath, f, zipWriter)
		if err2 != nil {
			return err2
		}
	}
	return nil
}

/*
FindPathNames
Finds all file paths of the specified type for a given path

An array of paths is returned, with the first being the parent directory followed by the relative child paths
查找给定路径所有指定类型的文件路径

返回路径数组，第一个是父目录，后续是相对子路径
*/
func FindPathNames(inPath, suffix string) ([]string, *errs.Error) {
	inPath = filepath.Clean(inPath)
	info, err_ := os.Stat(inPath)
	if err_ != nil {
		return nil, errs.New(errs.CodeIOReadFail, err_)
	}
	var result []string
	if info.IsDir() {
		result = append(result, inPath)
		err_ = filepath.WalkDir(inPath, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			name := d.Name()
			if strings.HasSuffix(name, suffix) {
				path = filepath.Clean(path)
				subName := strings.Replace(path, inPath, "", -1)
				result = append(result, subName[1:])
			}
			return nil
		})
		if err_ != nil {
			return nil, errs.New(errs.CodeIOReadFail, err_)
		}
	} else if strings.HasSuffix(inPath, suffix) {
		result = append(result, filepath.Dir(inPath), info.Name())
	}
	return result, nil
}

func convertFiles(inPath, outPath, srcSuffix string, makeOutPath func(string, string) string, convert FuncConvert) *errs.Error {
	names, err := FindPathNames(inPath, ".zip")
	if err != nil {
		return err
	}
	var dirPath = names[0]
	names = names[1:]
	pBar := utils.NewPrgBar(len(names)*core.StepTotal, "")
	defer pBar.Close()
	return utils.ParallelRun(names, ConcurNum, func(_ int, name string) *errs.Error {
		fileOutPath := makeOutPath(outPath, name)
		_, err_ := os.Stat(fileOutPath)
		if err_ == nil {
			// 已存在，跳过
			pBar.Add(core.StepTotal)
			return nil
		}
		fileInPath := filepath.Join(dirPath, name)
		return zipConvert(fileInPath, fileOutPath, srcSuffix, convert, pBar)
	})
}

func isRawContract(name string) bool {
	if strings.Contains(name, "&") {
		// 跳过价差数据
		return false
	}
	// 检查是否以价差前缀开始
	if name[0] == 'S' || name[0] == 'I' {
		parts := strings.Split(name, " ")
		prefix := strings.ToUpper(parts[0])
		if prefix == "SPD" || prefix == "SPC" || prefix == "SP" || prefix == "IPS" {
			return false
		}
	}
	// 检查是否最后一个字母是数字
	parts := strings.Split(name, ".")
	cleanName := parts[0]
	if len(parts) > 1 {
		cleanName = parts[len(parts)-2]
	}
	if cleanName == "README" || strings.HasSuffix(cleanName, "efp") {
		// 跳过期货转现货efp
		return false
	}
	if strings.HasSuffix(cleanName, "TAS") {
		// 以结算价交易
		return true
	}
	if cleanName == "IMCI" {
		return true
	}
	lastChar := cleanName[len(cleanName)-1]
	if lastChar < '0' || lastChar > '9' {
		// 文件名不是以数字结尾的，不是合约数据，跳过
		log.Info("skip non tick", zap.String("name", name))
		return false
	}
	return true
}

func RunFormatTick(args *config.CmdArgs) *errs.Error {
	if args.InPath == "" {
		return errs.NewMsg(errs.CodeParamRequired, "--in is required")
	}
	if args.OutPath == "" {
		return errs.NewMsg(errs.CodeParamRequired, "--out is required")
	}
	layout := "20060102 15:04:05"
	loc, err_ := time.LoadLocation("Asia/Shanghai")
	if err_ != nil {
		return errs.New(errs.CodeRunTime, err_)
	}
	handleEntry := func(inPath string, file *zip.File, out *zip.Writer) *errs.Error {
		if !isRawContract(file.Name) {
			return nil
		}
		fReader, err_ := file.Open()
		if err_ != nil {
			return errs.New(errs.CodeIOReadFail, err_)
		}
		rows, err_ := csv.NewReader(fReader).ReadAll()
		if err_ != nil {
			return errs.New(errs.CodeIOReadFail, err_)
		}
		items := make([][]string, 0)
		for _, row := range rows {
			if len(row) < 13 {
				log.Error("columns invalid", zap.Int("num", len(row)), zap.String("name", inPath))
				continue
			}
			if row[0] == "TradingDay" {
				continue
			}
			// TradingDay,InstrumentID,UpdateTime,UpdateMillisec,LastPrice,Volume,BidPrice1,BidVolume1,
			// AskPrice1,AskVolume1,AveragePrice,Turnover,OpenInterest
			// 24年之前15列，之后13列，最后两列是：UpperLimitPrice,LowerLimitPrice
			dateStr := row[0] + " " + row[2]
			timeObj, err_ := time.ParseInLocation(layout, dateStr, loc)
			if err_ != nil {
				return errs.New(errs.CodeRunTime, err_)
			}
			milliSecs, _ := strconv.ParseInt(row[3], 10, 64)
			timeMS := timeObj.UnixMilli() + milliSecs
			bidPrice1, _ := strconv.ParseFloat(row[6], 64)
			askPrice1, _ := strconv.ParseFloat(row[8], 64)
			avgPrice, _ := strconv.ParseFloat(row[10], 64)
			if bidPrice1 == 0 && askPrice1 == 0 && avgPrice == 0 {
				continue
			}
			// InstrumentID,Time,LastPrice,Volume,BidPrice1,BidVolume1,
			// AskPrice1,AskVolume1,AveragePrice,Turnover,OpenInterest,UpperLimitPrice,LowerLimitPrice
			item := append([]string{row[1], strconv.FormatInt(timeMS, 10)}, row[4:]...)
			items = append(items, item)
		}
		if len(items) == 0 {
			return nil
		}
		outWriter, err_ := out.Create(file.Name)
		if err_ != nil {
			return errs.New(errs.CodeIOWriteFail, err_)
		}
		csvWriter := csv.NewWriter(outWriter)
		err_ = csvWriter.WriteAll(items)
		if err_ != nil {
			return errs.New(errs.CodeIOWriteFail, err_)
		}
		csvWriter.Flush()
		return nil
	}
	makeOutPath := func(dirPath, name string) string {
		name = strings.ReplaceAll(name, "marketdatacsv", "")
		return filepath.Join(dirPath, name)
	}
	return convertFiles(args.InPath, args.OutPath, ".csv", makeOutPath, handleEntry)
}

func Build1mWithTicks(args *config.CmdArgs) *errs.Error {
	return build1mWithTicks(args)
}

func build1mWithTicks(args *config.CmdArgs) *errs.Error {
	if args.InPath == "" {
		return errs.NewMsg(errs.CodeParamRequired, "--in is required")
	}
	if args.OutPath == "" {
		return errs.NewMsg(errs.CodeParamRequired, "--out is required")
	}
	names, err := FindPathNames(args.InPath, ".zip")
	if err != nil {
		return err
	}
	if err_ := utils.EnsureDir(args.OutPath, 0755); err_ != nil {
		return errs.New(errs.CodeIOWriteFail, err_)
	}
	var dirPath = names[0]
	names = names[1:]
	if timeMsMin > 0 || timeMsMax > 0 {
		log.Info("enable time filter", zap.Int64("min", timeMsMin), zap.Int64("maz", timeMsMax))
	}
	// Input: There is only a folder of years in the root directory, and a zip package for each trading day under each year folder, and the tick data of the day is stored in the contract name in each zip
	// Output: A ZIP archive in the root directory every year, and each zip stores 1M data of the contract in CSV with the contract name
	// 输入：根目录下只有年份的文件夹，每个年份文件夹下每个交易日一个zip压缩包，每个zip内以合约名称存储当日tick数据
	// 输出：根目录下每年一个zip压缩包，每个zip内以合约名称csv存储当年该合约的1m数据
	layout := "20060102 15:04:05"
	loc, err_ := time.LoadLocation("Asia/Shanghai")
	if err_ != nil {
		return errs.New(errs.CodeRunTime, err_)
	}
	dayMSecs := int64(utils2.TFToSecs("1d") * 1000)
	nightMSecs := int64(3600 * 10 * 1000)  // utc时间，10小时后，即北京18:00后
	nightZeroMs := int64(3600 * 16 * 1000) // utc时间，16小时前，即北京24:00前
	tickBar := func(inPath string, row []string) (string, int64, [5]float64) {
		var symbol string
		var timeMS int64
		var arr [5]float64
		if len(row) >= 13 && len(row[0]) == 8 && len(row[2]) == 8 {
			// TradingDay,InstrumentID,UpdateTime,UpdateMillisec,LastPrice,Volume,BidPrice1,BidVolume1,
			// AskPrice1,AskVolume1,AveragePrice,Turnover,OpenInterest,[UpperLimitPrice,LowerLimitPrice]
			bidPrice1, _ := strconv.ParseFloat(row[6], 64)
			askPrice1, _ := strconv.ParseFloat(row[8], 64)
			avgPrice, _ := strconv.ParseFloat(row[10], 64)
			if bidPrice1 == 0 && askPrice1 == 0 && avgPrice == 0 {
				return "", 0, [5]float64{0, 0, 0, 0, 0}
			}
			symbol = row[1]
			dateStr := row[0] + " " + row[2]
			timeObj, err_ := time.ParseInLocation(layout, dateStr, loc)
			if err_ != nil {
				log.Error("invalid time", zap.String("date", dateStr), zap.String("name", inPath))
				return "", 0, [5]float64{0, 0, 0, 0, 0}
			}
			milliSecs, _ := strconv.ParseInt(row[3], 10, 64)
			timeMS = timeObj.UnixMilli() + milliSecs
			price, _ := strconv.ParseFloat(row[4], 64)
			volume, _ := strconv.ParseFloat(row[5], 64)
			turnOver, _ := strconv.ParseFloat(row[11], 64)
			openInt, _ := strconv.ParseFloat(row[12], 64)
			arr = [5]float64{price, volume, avgPrice, turnOver, openInt}
		} else {
			// InstrumentID,Time,LastPrice,Volume,BidPrice1,BidVolume1,
			// AskPrice1,AskVolume1,AveragePrice,Turnover,OpenInterest,[UpperLimitPrice,LowerLimitPrice]
			symbol = row[0]
			timeMS, _ = strconv.ParseInt(row[1], 10, 64)
			if timeMS == 0 {
				return "", 0, [5]float64{0, 0, 0, 0, 0}
			}
			price, _ := strconv.ParseFloat(row[2], 64)
			volume, _ := strconv.ParseFloat(row[3], 64)
			avgPrice, _ := strconv.ParseFloat(row[8], 64)
			turnOver, _ := strconv.ParseFloat(row[9], 64)
			openInt, _ := strconv.ParseFloat(row[10], 64)
			arr = [5]float64{price, volume, avgPrice, turnOver, openInt}
		}
		off := timeMS % dayMSecs
		if off > nightMSecs && off < nightZeroMs {
			// Night trading time, and before 24 o'clock, is the day before; It doesn't have to be a day, it could be Monday, it needs to be minus two days, 1 day for simplicity
			// 夜盘时间，且24点前，是日前的；不一定是一天，可能是周一，需要减两天，简单起见1天
			timeMS -= dayMSecs
		}
		return symbol, timeMS, arr
	}
	// 按年对文件名分组
	nameGrps := make([][]string, 0)
	var tmpNames []string
	oldYear := ""
	for _, name := range names {
		year := filepath.Base(filepath.Dir(name))
		if year != oldYear {
			if len(tmpNames) > 0 {
				nameGrps = append(nameGrps, tmpNames)
			}
			oldYear = year
			tmpNames = []string{name}
		} else {
			tmpNames = append(tmpNames, name)
		}
	}
	if len(tmpNames) > 0 {
		nameGrps = append(nameGrps, tmpNames)
	}
	// 每组分别处理消费
	for _, names = range nameGrps {
		year := filepath.Base(filepath.Dir(names[0]))
		outPath := fmt.Sprintf("%s/%s.zip", args.OutPath, year)
		if _, err_ := os.Stat(outPath); err_ == nil {
			// 已存在，跳过
			continue
		}
		log.Info("calc 1m kline from ticks", zap.String("year", year))
		totalNum := len(names) * core.StepTotal
		pBar := utils.NewPrgBar(totalNum, "tickTo1m")
		err = utils.ParallelRun(names, ConcurNum, func(_ int, name string) *errs.Error {
			if timeMsMin > 0 || timeMsMax > 0 {
				// 过滤范围之外的数据
				cleanName := strings.Split(filepath.Base(name), ".")[0]
				cleanName = cleanName[len(cleanName)-8:]
				timeObj, err_ := time.ParseInLocation("20060102", cleanName, loc)
				if err_ == nil {
					timeMS := timeObj.UnixMilli()
					if timeMsMin > 0 && timeMS < timeMsMin {
						return nil
					}
					if timeMsMax > 0 && timeMS > timeMsMax {
						return nil
					}
				}
			}
			fileInPath := filepath.Join(dirPath, name)
			symbolDones := make(map[string]map[string]int)
			dayKlines := make(map[string][]*banexg.Kline)
			err = ReadZipCSVs(fileInPath, pBar, func(inPath string, fid int, file *zip.File, arg interface{}) *errs.Error {
				return build1mSymbolTick(inPath, fid, file, symbolDones, dayKlines, tickBar)
			}, nil)
			var b strings.Builder
			for symbol, data := range symbolDones {
				if len(data) < 2 {
					continue
				}
				b.WriteString(fmt.Sprintf("%s \t", symbol))
				for fname, num := range data {
					b.WriteString(fmt.Sprintf("%s:%v ", fname, num))
				}
				b.WriteString("\t")
			}
			if b.Len() > 0 {
				fmt.Printf("check duplicate: %s  %s\n", name, b.String())
			}
			// For the same target, on the same day, tick data may exist in multiple files, choose the one with the highest volume
			// 同一个标的，在同一天，tick数据可能存在于多个文件，选择成交量最高的那个
			symbolVolMap := make(map[string]map[string]float64)
			for key, items := range dayKlines {
				sumVol := float64(0)
				for _, k := range items {
					sumVol += k.Volume
				}
				parts := strings.Split(key, "_")
				symbol := strings.Join(parts[:len(parts)-1], "_")
				data, exist := symbolVolMap[symbol]
				if !exist {
					data = make(map[string]float64)
					symbolVolMap[symbol] = data
				}
				data[parts[len(parts)-1]] = sumVol
			}
			klineLock.Lock()
			for key, data := range symbolVolMap {
				suffix, vol := "", -1.0
				for sf, volume := range data {
					if volume > vol {
						suffix = sf
						vol = volume
					}
				}
				oldData, _ := symKLines[key]
				symKLines[key] = append(oldData, dayKlines[key+"_"+suffix]...)
			}
			klineLock.Unlock()
			return err
		})
		pBar.Close()
		log.Info("save 1m kline", zap.String("year", year))
		klineLock.Lock()
		saveYear1m(args.OutPath, year)
		symKLines = make(map[string][]*banexg.Kline)
		klineLock.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

func saveYear1m(outDir, year string) {
	if len(symKLines) == 0 || year == "" {
		return
	}
	outPath := fmt.Sprintf("%s/%s.zip", outDir, year)
	out, err := os.Create(outPath)
	if err != nil {
		log.Error("create 1m zip fail", zap.String("year", year), zap.Error(err))
		return
	}
	defer out.Close()
	zipWriter := zip.NewWriter(out)
	defer zipWriter.Close()
	sylDupMap := make(map[string][2]int)
	for symbol, items := range symKLines {
		outWriter, err_ := zipWriter.Create(symbol + ".csv")
		if err_ != nil {
			log.Error("create in zip fail", zap.String("year", year),
				zap.String("symbol", symbol), zap.Error(err))
			continue
		}
		sort.Slice(items, func(i, j int) bool {
			return items[i].Time < items[j].Time
		})
		rows := make([][]string, 0, len(items))
		duplicateNum := 0
		lastMS := int64(0)
		afterVol := false // 过滤前面成交量为0的K线
		for _, bar1m := range items {
			if bar1m.Time == lastMS {
				duplicateNum += 1
				continue
			}
			if !afterVol && bar1m.Volume == 0 {
				continue
			}
			afterVol = true
			lastMS = bar1m.Time
			fltArr := []float64{bar1m.Open, bar1m.High, bar1m.Low, bar1m.Close, bar1m.Volume, bar1m.Info}
			row := make([]string, 0, 7)
			row = append(row, strconv.FormatInt(bar1m.Time, 10))
			for _, val := range fltArr {
				valStr := strconv.FormatFloat(math.Round(val*1000)/1000, 'f', -1, 64)
				row = append(row, valStr)
			}
			rows = append(rows, row)
		}
		if duplicateNum > 0 {
			sylDupMap[symbol] = [2]int{len(rows), duplicateNum}
		}
		csvWriter := csv.NewWriter(outWriter)
		err_ = csvWriter.WriteAll(rows)
		if err_ != nil {
			log.Error("write to zip fail", zap.String("year", year),
				zap.String("symbol", symbol), zap.Int("num", len(items)), zap.Error(err))
		}
		csvWriter.Flush()
	}
	if len(sylDupMap) > 0 {
		var b strings.Builder
		for syl, tup := range sylDupMap {
			b.WriteString(fmt.Sprintf("%s num=%v dup=%v\t\t", syl, tup[0], tup[1]))
		}
		fmt.Print(b.String())
	}
}

// ReadZipCSVs arg is for handle
func ReadZipCSVs(inPath string, pBar *utils.PrgBar, handle FuncReadZipItem, arg interface{}) *errs.Error {
	r, err := zip.OpenReader(inPath)
	if err != nil {
		if pBar != nil {
			pBar.Add(core.StepTotal)
		}
		return errs.New(errs.CodeIOReadFail, err)
	}
	defer r.Close()
	var bar *utils.PrgBarJob
	if pBar != nil {
		bar = pBar.NewJob(len(r.File))
		defer bar.Done()
	}
	for i, f := range r.File {
		if bar != nil {
			bar.Add(1)
		}
		if f.FileInfo().IsDir() || !strings.HasSuffix(f.Name, ".csv") {
			continue
		}
		err2 := handle(inPath, i, f, arg)
		if err2 != nil {
			return err2
		}
	}
	return nil
}

type tkInfo struct {
	symbol   string
	timeMS   int64   // 13位毫秒时间戳
	price    float64 // latest price 最新价格
	volume   float64 // Cumulative daily volume 累计日成交量
	avgPrice float64 // 平均价格
	turnOver float64 // 换手率
	openInt  float64 // 持仓量
}

func build1mSymbolTick(inPath string, fid int, file *zip.File, dones map[string]map[string]int,
	dayKlines map[string][]*banexg.Kline, tickBar FuncTickBar) *errs.Error {
	if !isRawContract(file.Name) {
		return nil
	}
	fReader, err_ := file.Open()
	if err_ != nil {
		return errs.New(errs.CodeIOReadFail, err_)
	}
	rows, err_ := csv.NewReader(fReader).ReadAll()
	if err_ != nil {
		return errs.New(errs.CodeIOReadFail, err_)
	}
	// 文件名内可能有多个symbol的tick信息，这里进行排序
	var rawSyls = make(map[string]bool)
	var counts = make(map[string]int)
	ticks := make([]*tkInfo, 0, len(rows))
	for _, row := range rows {
		symbol, timeMS, arr := tickBar(inPath, row)
		if timeMS == 0 {
			continue
		}
		// 检查是否是原始合约编号，跳过组合数据
		isRaw, rawChk := rawSyls[symbol]
		if !rawChk {
			isRaw = isRawContract(symbol)
			rawSyls[symbol] = isRaw
		}
		if !isRaw {
			continue
		}
		count, _ := counts[symbol]
		counts[symbol] = count + 1
		ticks = append(ticks, &tkInfo{symbol: symbol, timeMS: timeMS, price: arr[0], volume: arr[1],
			avgPrice: arr[2], turnOver: arr[3], openInt: arr[4]})
	}
	for key, num := range counts {
		items, _ := dones[key]
		if items == nil {
			items = make(map[string]int)
			dones[key] = items
		}
		items[file.Name] = num
	}
	if len(ticks) == 0 {
		return nil
	}
	sort.Slice(ticks, func(i, j int) bool {
		a, b := ticks[i], ticks[j]
		if a.symbol != b.symbol {
			return a.symbol <= b.symbol
		} else {
			return a.timeMS <= b.timeMS
		}
	})
	sumVol := ticks[0].volume
	var bar1m *banexg.Kline
	var oldKey string
	saveBar := func() {
		if bar1m == nil {
			return
		}
		barRows, _ := dayKlines[oldKey]
		if barRows == nil {
			barRows = make([]*banexg.Kline, 0, 2000)
		}
		newSumVol := bar1m.Volume
		bar1m.Volume -= sumVol
		dayKlines[oldKey] = append(barRows, bar1m)
		sumVol = newSumVol
	}
	oldMinMS := int64(0)
	minGapMSecs := int64(300000) // 间隔超过5分钟，且成交量下降，是切换盘口
	// 对排序后的tick合并为K线
	for _, t := range ticks {
		curMinMS := utils2.AlignTfMSecs(t.timeMS, 60000)
		price, volume := t.price, t.volume
		keyValid := strings.HasPrefix(oldKey, t.symbol)
		if bar1m == nil || oldMinMS == 0 || curMinMS > oldMinMS || !keyValid {
			saveBar()
			if !keyValid {
				oldKey = fmt.Sprintf("%s_%v", t.symbol, fid)
				sumVol = volume
			}
			if volume < sumVol {
				if curMinMS-oldMinMS >= minGapMSecs {
					// 日盘/夜盘切换，累计成交量归0
					sumVol = volume
				} else {
					// 有些数据时间错了，提早了2分钟，但成交量正确，跳过异常数据
					timeStr := btime.ToDateStr(curMinMS, "")
					log.Warn("skip volume invalid", zap.Float64("old", sumVol), zap.Float64("new", volume),
						zap.String("time", timeStr), zap.String("nm", oldKey))
					continue
				}
			}
			oldMinMS = curMinMS
			bar1m = &banexg.Kline{Time: curMinMS, Open: price, High: price, Low: price, Close: price,
				Volume: volume, Info: t.openInt}
		} else {
			if volume < sumVol {
				if volume == 0 && t.avgPrice == 0 && t.turnOver == 0 {
					// 部分tick，成交量、均价、换手率均为0，是无效的，需要跳过
					continue
				}
				timeStr := btime.ToDateStr(curMinMS, "")
				log.Warn("volume may invalid", zap.Float64("old", sumVol), zap.Float64("new", volume),
					zap.String("time", timeStr), zap.String("nm", oldKey))
				continue
			}
			if volume > bar1m.Volume {
				if bar1m.Volume <= sumVol {
					// 开盘价取时间段内第一次有成交量的价格
					bar1m.Open = price
					bar1m.High = price
					bar1m.Low = price
				} else if price > bar1m.High {
					bar1m.High = price
				} else if price < bar1m.Low {
					bar1m.Low = price
				}
				bar1m.Close = price
				bar1m.Volume = volume
				bar1m.Info = t.openInt
			}
		}
	}
	saveBar()
	return nil
}

/*
CalcFilePerfs calc sharpe/sortino ratio for input data
*/
func CalcFilePerfs(args *config.CmdArgs) *errs.Error {
	path := args.InPath
	if path == "" {
		return errs.NewMsg(errs.CodeParamRequired, "--in is required")
	}
	if args.OutPath == "" {
		return errs.NewMsg(errs.CodeParamRequired, "--out is required")
	}
	ext := strings.ToLower(filepath.Ext(path))
	var rows [][]string
	var err *errs.Error
	if ext == ".csv" {
		rows, err = utils.ReadCSV(path)
	} else if ext == ".xlsx" {
		rows, err = utils.ReadXlsx(path, "")
	} else {
		return errs.NewMsg(errs.CodeRunTime, "invalid file type: %s, expect csv/xlsx", ext)
	}
	if err != nil {
		return err
	}
	if len(rows) <= 1 {
		log.Warn("file empty, skip CalcFilePerfs", zap.Int("rowNum", len(rows)))
		return nil
	}
	var names = rows[0]
	var startCol = 1
	isPrice := args.InType != "ratio"
	var cols [][]string
	for range names {
		cols = append(cols, make([]string, 0, len(rows)))
	}
	for _, row := range rows {
		for i, text := range row {
			cols[i] = append(cols[i], text)
		}
	}
	// convert string to Decimal for all cols
	var colData [][]decimal.Decimal
	for _, col := range cols[startCol:] {
		data := make([]decimal.Decimal, 0, len(col)+2)
		for _, text := range col[1:] {
			val, _ := decimal.NewFromString(text)
			data = append(data, val)
		}
		colData = append(colData, data)
	}
	if isPrice {
		var resData [][]decimal.Decimal
		val1 := decimal.NewFromInt(1)
		val0 := decimal.NewFromInt(0)
		for _, col := range colData {
			var res []decimal.Decimal
			for i, v := range col {
				if i == 0 {
					continue
				}
				pv := col[i-1]
				if pv.Equal(val0) {
					res = append(res, val0)
				} else {
					res = append(res, v.Div(pv).Sub(val1))
				}
			}
			resData = append(resData, res)
		}
		colData = resData
	}
	// calculate sharpe/sortino
	type Col struct {
		name string
		data []decimal.Decimal
		raw  []string
	}
	var items []*Col
	val0 := decimal.NewFromInt(0)
	for i, col := range colData {
		sharpe, err_ := utils.DecSharpeRatio(col, val0)
		if err_ != nil {
			return errs.New(errs.CodeRunTime, err_)
		}
		sortino, err_ := utils.DecSortinoRatio(col, val0)
		if err_ != nil {
			sortino = decimal.NewFromFloat(0)
		}
		items = append(items, &Col{
			name: names[startCol+i],
			data: append(col, sharpe, sortino),
			raw:  cols[startCol+i][1:],
		})
	}
	// sort columns by sharpe ratio
	slices.SortFunc(items, func(a, b *Col) int {
		av := a.data[len(a.data)-2]
		bv := b.data[len(b.data)-2]
		sub, _ := av.Sub(bv).Float64()
		return int(sub * 100)
	})
	var outRows [][]string
	var heads = []string{""}
	for _, item := range items {
		heads = append(heads, item.name)
	}
	outRows = append(outRows, heads)
	rowId := 0
	for {
		row := make([]string, 0, len(items)+2)
		row = append(row, rows[rowId+1][0])
		for _, item := range items {
			row = append(row, item.raw[rowId])
		}
		outRows = append(outRows, row)
		rowId += 1
		if rowId >= len(items[0].raw) {
			row = make([]string, 0, len(items)+2)
			row = append(row, "sharpe")
			for _, item := range items {
				row = append(row, item.data[len(item.data)-2].String())
			}
			outRows = append(outRows, row)
			row = make([]string, 0, len(items)+2)
			row = append(row, "sortino")
			for _, item := range items {
				row = append(row, item.data[len(item.data)-1].String())
			}
			outRows = append(outRows, row)
			break
		}
	}
	return utils.WriteCsvFile(args.OutPath, outRows, false)
}
