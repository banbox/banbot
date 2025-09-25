package opt

import (
	"bufio"
	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
)

type BTSection struct {
	StartMS int64
	EndMS   int64
	PairMap map[string]bool
}

/*
RunSimBT
Perform rolling simulation backtest, extract trading symbols for each time range from the log file, export orders & enters2.html

执行滚动模拟回测，从日志文件中提取每个区间的交易品种，并进行回测；导出订单记录和enters2.html
*/
func RunSimBT(args *config.CmdArgs) *errs.Error {
	if args.InPath == "" {
		log.Warn("-in is required")
		return nil
	}
	// load config
	configPath := filepath.Join(args.InPath, "config.yml")
	if !utils.Exists(configPath) {
		return errs.NewMsg(errs.CodeIOReadFail, "not a valid backtest report dir, `config.yml` not exist!")
	}
	args.Configs = append(args.Configs, configPath)
	core.SetRunMode(core.RunModeBackTest)
	err := biz.SetupComsExg(args)
	if err != nil {
		return err
	}
	config.PairMgr = &config.PairMgrConfig{}

	logPath := filepath.Join(args.InPath, "out.log")
	sections, err := parseLogFile(logPath)
	if err != nil {
		return err
	}

	// 收集所有订单
	var allOrders []*ormo.InOutOrder
	for i, sec := range sections {
		log.Info("run section", zap.Int64("start", sec.StartMS), zap.Int64("end", sec.EndMS))

		// 更新配置
		config.TimeRange.StartMS = sec.StartMS
		config.TimeRange.EndMS = sec.EndMS
		config.Pairs = utils.KeysOfMap(sec.PairMap)

		// 执行回测
		core.BotRunning = true
		biz.ResetVars()
		bt, err := NewBackTest(true, "")
		if err != nil {
			return err
		}
		bt.Run()

		// 收集订单
		allOrders = append(allOrders, ormo.HistODs...)

		// 输出进度
		log.Info("finished", zap.Int("current", i+1), zap.Int("total", len(sections)),
			zap.Int("orders", len(ormo.HistODs)))
	}

	// 保存订单
	err = saveOrders(allOrders, filepath.Join(args.InPath, "orders2"))
	if err != nil {
		return err
	}
	// 生成enters并保存
	outPath := filepath.Join(args.InPath, "enters2.html")
	err = DumpEnterTagCumProfits(outPath, allOrders, 600)
	if err != nil {
		return err
	}

	log.Info("roll backtest finished",
		zap.Int("total_sections", len(sections)),
		zap.Int("total_orders", len(allOrders)))
	return nil
}

// parseLogFile 解析日志文件，提取每个回测区间的信息
func parseLogFile(logPath string) ([]*BTSection, *errs.Error) {
	file, err := os.Open(logPath)
	if err != nil {
		return nil, errs.New(errs.CodeIOReadFail, err)
	}
	defer file.Close()

	var sections []*BTSection
	var sec *BTSection

	bulkReg := regexp.MustCompile(`bulk down (\w+) \d+ pairs [\d-]+ \d+:\d+:\d+-(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})`)
	pairReg := regexp.MustCompile(`(\w+)\(\d+\): (.+)`)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		// 解析区间结束时间
		if matches := bulkReg.FindStringSubmatch(line); len(matches) > 2 {
			startTime, err := time.Parse("2006-01-02 15:04:05", matches[2])
			if err != nil {
				continue
			}
			startMS := startTime.UnixMilli()

			// 如果是新区间，创建新的section
			if sec == nil || sec.StartMS != startMS && len(sec.PairMap) > 0 {
				if sec != nil {
					sec.EndMS = startMS
					sections = append(sections, sec)
				}
				sec = &BTSection{
					StartMS: startMS,
					PairMap: make(map[string]bool),
				}
			} else if sec.StartMS > startMS {
				sec.StartMS = startMS
			}
		} else if sec != nil {
			// 解析交易品种
			if matches = pairReg.FindStringSubmatch(line); len(matches) > 2 {
				quote := matches[1]
				suffix := "/" + quote
				codes := strings.Split(matches[2], " ")
				isInverse := false
				if core.Market == banexg.MarketLinear {
					suffix += ":" + quote
				} else if core.Market == banexg.MarketInverse {
					isInverse = true
				}
				for _, p := range codes {
					if p != "" {
						pair := p + suffix
						if isInverse {
							pair += ":" + p
						}
						sec.PairMap[pair] = true
					}
				}
			}
		}
	}

	if sec != nil {
		sec.EndMS = config.TimeRange.EndMS
		sections = append(sections, sec)
	}
	return sections, nil
}

// saveOrders 保存订单到CSV和GOB文件
func saveOrders(orders []*ormo.InOutOrder, outPath string) *errs.Error {
	// 保存为CSV
	csvPath := outPath + ".csv"
	if err := DumpOrdersCSV(orders, csvPath); err != nil {
		return errs.New(errs.CodeIOWriteFail, err)
	}

	// 保存为GOB
	err2 := ormo.DumpOrdersGob(outPath + ".gob")
	if err2 != nil {
		return err2
	}
	return nil
}
