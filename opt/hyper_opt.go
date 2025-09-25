package opt

import (
	"bytes"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/anyongjin/go-bayesopt"
	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/goods"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/c-bata/goptuna"
	"github.com/c-bata/goptuna/cmaes"
	"github.com/c-bata/goptuna/tpe"
	"go.uber.org/zap"
)

type FuncOptTask func(params map[string]float64) (float64, *errs.Error)

/*
RunBTOverOpt
Backtesting mode based on continuous parameter tuning. Approach the real situation and avoid using future information to adjust parameters for backtesting.
基于持续调参的回测模式。接近实盘情况，避免使用未来信息调参回测。
*/
func RunBTOverOpt(args *config.CmdArgs) *errs.Error {
	t, err := newRollBtOpt(args)
	if err != nil || t == nil {
		return err
	}
	var allHisOds []*ormo.InOutOrder
	var lastWal map[string]float64
	var lastRes *BTResult
	lastPols := config.RunPolicy
	pbar := utils.NewPrgBar(int((t.allEndMs-t.curMs)/1000), "BtOpt")
	defer pbar.Close()
	backPols := config.RunPolicy
	for t.curMs < t.allEndMs {
		pbar.Add(int(t.runMSecs / 1000))
		config.RunPolicy = backPols
		polStr, err := t.next(args.PairPicker)
		if err != nil {
			return err
		}
		biz.ResetVars()
		polList, err := parseRunPolicies(polStr)
		if err != nil {
			return err
		}
		if len(polList) == 0 {
			log.Warn("no RunPolicy for ", zap.Int64("start", t.dateRange.StartMS/1000),
				zap.Int64("end", t.curMs/1000))
			t.curMs += t.runMSecs
			continue
		}
		applyOptPolicies(lastPols, polList, args.Alpha)
		lastPols = config.RunPolicy
		wallets := biz.GetWallets(config.DefAcc)
		core.BotRunning = true
		t.dateRange.StartMS = t.curMs
		t.dateRange.EndMS = t.curMs + t.runMSecs
		outDir := filepath.Join(t.outDir, args.Picker)
		bt, err := NewBackTest(false, outDir)
		if err != nil {
			return err
		}
		if lastWal != nil {
			wallets.SetWallets(lastWal)
		}
		if lastRes != nil {
			bt.BTResult = lastRes
		}
		ormo.HistODs = allHisOds
		bt.Run()
		lastRes = bt.BTResult
		allHisOds = ormo.HistODs
		lastWal = wallets.DumpAvas()
		t.curMs += t.runMSecs
	}
	err = t.dumpConfig()
	if err != nil {
		return err
	}
	log.Info("Rolling Optimization Backtesting finished", zap.String("at", t.outDir))
	return nil
}

func RunRollBTPicker(args *config.CmdArgs) *errs.Error {
	t, err := newRollBtOpt(args)
	if err != nil || t == nil {
		return err
	}
	pbar := utils.NewPrgBar(int((t.allEndMs-t.curMs)/1000), "RollPicker")
	defer pbar.Close()
	pickers, err := getTestPickers(args.Picker)
	if err != nil {
		return err
	}
	var rows, rows2 [][]string
	head := []string{"date"}
	head = append(head, pickers...)
	rows = append(rows, head)
	rows2 = append(rows2, head)
	for t.curMs < t.allEndMs {
		pbar.Add(int(t.runMSecs / 1000))
		scores := make([]float64, 0, len(pickers))
		row := []string{btime.ToDateStr(t.curMs, "2006-01-02")}
		row2 := []string{row[0]}
		items := make([]*ValItem, 0, len(pickers))
		log.Info("test pickers for", zap.String("dt", row[0]))
		backPols := config.RunPolicy
		for i, picker := range pickers {
			t.args.Picker = picker
			config.RunPolicy = backPols
			polStr, err := t.next(args.PairPicker)
			if err != nil {
				return err
			}
			biz.ResetVars()
			polList, err := parseRunPolicies(polStr)
			if err != nil {
				return err
			}
			if len(polList) == 0 {
				log.Warn("no RunPolicy for ", zap.Int64("start", t.dateRange.StartMS/1000),
					zap.Int64("end", t.curMs/1000))
				t.curMs += t.runMSecs
				continue
			}
			err = config.SetRunPolicy(true, polList...)
			if err != nil {
				return err
			}
			core.BotRunning = true
			t.dateRange.StartMS = t.curMs
			t.dateRange.EndMS = t.curMs + t.runMSecs
			bt, err := NewBackTest(true, "")
			if err != nil {
				return err
			}
			bt.Run()
			score := bt.Score()
			scores = append(scores, score)
			row = append(row, strconv.FormatFloat(score, 'f', 1, 64))
			items = append(items, &ValItem{Tag: picker, Score: score, Order: i})
		}
		slices.SortFunc(items, func(a, b *ValItem) int {
			return int(b.Score - a.Score)
		})
		for i, it := range items {
			it.Res = i
		}
		slices.SortFunc(items, func(a, b *ValItem) int {
			return a.Order - b.Order
		})
		for _, it := range items {
			row2 = append(row2, strconv.Itoa(it.Res))
		}
		log.Info("scores", zap.Strings("r", row))
		rows = append(rows, row)
		rows2 = append(rows2, row2)
		t.curMs += t.runMSecs
	}
	err = t.dumpConfig()
	if err != nil {
		return err
	}
	csvPath := filepath.Join(t.outDir, "pickerScores.csv")
	err = utils.WriteCsvFile(csvPath, rows, false)
	if err != nil {
		return err
	}
	csvPath = filepath.Join(t.outDir, "pickerRanks.csv")
	err = utils.WriteCsvFile(csvPath, rows2, false)
	if err != nil {
		return err
	}
	log.Info("Test Pickers finished", zap.String("at", t.outDir))
	return nil
}

func btOptHash(args *config.CmdArgs) string {
	raws := []string{
		args.Sampler,
		args.RunPeriod,
		args.ReviewPeriod,
		strconv.FormatBool(args.EachPairs),
		strconv.Itoa(args.OptRounds),
	}
	ymlData, err := config.DumpYaml(true)
	if ymlData != nil {
		raws = append(raws, string(ymlData))
	} else {
		log.Warn("dump config yaml fail", zap.Error(err))
	}
	for _, p := range config.RunPolicy {
		raws = append(raws, p.Key())
	}
	res := utils.MD5([]byte(strings.Join(raws, "")))
	return res[:10]
}

func pickFromExists(path string, picker, pairPicker string) (string, *errs.Error) {
	paths, err_ := utils.GetFilesWithPrefix(path)
	if err_ != nil {
		return "", errs.New(errs.CodeIOReadFail, err_)
	}
	if len(paths) == 0 {
		return "", nil
	}
	return collectOptLog(paths, 0, picker, pairPicker)
}

/*
applyOptPolicies
Update strategy group parameters using EMA to avoid significant differences in parameters before and after rolling backtesting
使用EMA更新策略组参数，避免滚动回测前后参数差异较大
*/
func applyOptPolicies(olds, pols []*config.RunPolicyConfig, alpha float64) {
	if alpha >= 1 {
		_ = config.SetRunPolicy(true, pols...)
		return
	}
	var data = make(map[string]*config.RunPolicyConfig)
	for _, p := range olds {
		data[p.Key()] = p
	}
	var res = make([]*config.RunPolicyConfig, 0, len(pols))
	for _, p := range pols {
		key := p.Key()
		old, _ := data[key]
		if old == nil {
			log.Warn("no match old", zap.String("for", key))
			res = append(res, p)
		} else {
			item := p.Clone()
			for k, v := range item.Params {
				oldV, ok := old.Params[k]
				if ok {
					item.Params[k] = v*alpha + oldV*(1-alpha)
				}
			}
			res = append(res, item)
		}
	}
	_ = config.SetRunPolicy(true, res...)
}

func RunOptimize(args *config.CmdArgs) *errs.Error {
	if args.OutPath == "" {
		log.Warn("-out is required")
		return nil
	}
	args.LogLevel = "warn"
	core.SetRunMode(core.RunModeBackTest)
	err := biz.SetupComsExg(args)
	if err != nil {
		return err
	}
	cfgStr, err := runOptimize(args, 0)
	if err != nil {
		return err
	}
	fmt.Print(cfgStr)
	return nil
}

func runOptimize(args *config.CmdArgs, minScore float64) (string, *errs.Error) {
	var err *errs.Error
	btime.CurTimeMS = config.TimeRange.StartMS
	// 列举所有标的
	allPairs := config.Pairs
	if len(allPairs) == 0 {
		goods.ShowLog = false
		allPairs, err = goods.RefreshPairList(btime.TimeMS())
		if err != nil {
			return "", err
		}
	}
	var logOuts []string
	groups := config.RunPolicy
	if len(groups) <= 1 || args.Concur <= 1 {
		logOuts = append(logOuts, args.OutPath)
		file, err_ := os.Create(args.OutPath)
		if err_ != nil {
			return "", errs.New(errs.CodeIOWriteFail, err_)
		}
		for _, gp := range groups {
			// Bayesian optimization is carried out separately for each strategy, long and short, to find the best parameters
			// 针对每个策略、多空单独进行贝叶斯优化，寻找最佳参数
			err = optAndPrint(gp.Clone(), args, allPairs, file)
			if err != nil {
				file.Close()
				return "", err
			}
		}
		file.Close()
		sortOptLogs(args.OutPath)
	} else {
		// Multi-process execution to improve speed.
		// 多进程执行，提高速度。
		log.Warn("running optimize", zap.Int("num", len(groups)), zap.Int("rounds", args.OptRounds))
		var cmds = []string{"optimize", "-opt-rounds"}
		cmds = append(cmds, strconv.Itoa(args.OptRounds), "-sampler", args.Sampler)
		if args.EachPairs {
			cmds = append(cmds, "-each-pairs")
		}
		for _, p := range args.Configs {
			cmds = append(cmds, "-config", p)
		}
		if args.Picker != "" {
			cmds = append(cmds, "-picker", args.Picker)
		}
		startStr := strconv.FormatInt(config.TimeRange.StartMS/1000, 10)
		endStr := strconv.FormatInt(config.TimeRange.EndMS/1000, 10)
		err = utils.ParallelRun(groups, args.Concur, func(i int, pol *config.RunPolicyConfig) *errs.Error {
			core.Sleep(time.Millisecond * time.Duration(1000*rand.Float64()+100*float64(i)))
			iStr := strconv.Itoa(i + 1)
			cfgFile, err_ := os.CreateTemp("", "ban_opt"+iStr)
			if err_ != nil {
				log.Warn("write temp config fail", zap.Error(err_))
				return nil
			}
			defer os.Remove(cfgFile.Name())
			cfgFile.WriteString(fmt.Sprintf("time_start: \"%s\"\n", startStr))
			cfgFile.WriteString(fmt.Sprintf("time_end: \"%s\"\n", endStr))
			cfgFile.WriteString("run_policy:\n")
			cfgFile.WriteString(pol.ToYaml())
			cfgFile.Close()
			curCmds := append(cmds, "-config", cfgFile.Name())
			outPath := args.OutPath + "." + iStr
			curCmds = append(curCmds, "-out", outPath)
			logOuts = append(logOuts, outPath)
			log.Warn("runing: " + strings.Join(curCmds, " "))
			var out bytes.Buffer
			excPath, err_ := os.Executable()
			if err_ != nil {
				return errs.New(errs.CodeRunTime, err_)
			}
			cmd := exec.Command(excPath, curCmds...)
			cmd.Stdout = &out
			cmd.Stderr = &out
			err_ = cmd.Run()
			fmt.Println(out.String())
			if err_ != nil {
				return errs.New(errs.CodeRunTime, err_)
			}
			return nil
		})
		if err != nil {
			return "", err
		}
	}
	return collectOptLog(logOuts, minScore, args.Picker, args.PairPicker)
}

func sortOptLogs(path string) {
	// 重新读取文件并对loss行排序
	content, err_ := os.ReadFile(path)
	if err_ != nil {
		log.Warn("read output file fail", zap.Error(err_))
		return
	}
	reg := regexp.MustCompile(`^loss:\s*(-?\d+\.\d+)`)
	lines := utils.SplitLines(string(content))
	lines = append(lines, "") // 确保末尾是loss行也能触发排序
	var lossLines []*core.StrFloat
	inLossArea := false
	var result = make([]string, 0, len(lines))
	for _, line := range lines {
		if strings.HasPrefix(line, "loss: ") {
			matches := reg.FindStringSubmatch(line)
			if len(matches) > 1 {
				lossVal, err_ := strconv.ParseFloat(matches[1], 64)
				if err_ == nil {
					lossLines = append(lossLines, &core.StrFloat{
						Str: line,
						Val: lossVal,
					})
					if !inLossArea {
						inLossArea = true
					}
					continue
				}
			}
		}
		if inLossArea {
			inLossArea = false
			sort.SliceStable(lossLines, func(i, j int) bool {
				return lossLines[i].Val < lossLines[j].Val
			})
			for _, it := range lossLines {
				result = append(result, it.Str)
			}
			lossLines = nil
		}
		result = append(result, line)
	}
	// 这里不用再次检查inLossArea，因为前面已添加末尾空行
	err_ = os.WriteFile(path, []byte(strings.Join(result, "\n")), 0644)
	if err_ != nil {
		log.Warn("write sorted file fail", zap.Error(err_))
	}
}

/*
optAndPrint optimize for raw policy group;
write one or multiple optimize result to file.
*/
func optAndPrint(pol *config.RunPolicyConfig, args *config.CmdArgs, allPairs []string, file *os.File) *errs.Error {
	file.WriteString(fmt.Sprintf("# run hyper optimize: %v, rounds: %v\n", args.Sampler, args.OptRounds))
	startDt := btime.ToDateStr(config.TimeRange.StartMS, "")
	endDt := btime.ToDateStr(config.TimeRange.EndMS, "")
	file.WriteString(fmt.Sprintf("# date range: %v - %v\n", startDt, endDt))
	var res []*GroupScore
	if args.EachPairs {
		pairs := pol.Pairs
		if len(pairs) == 0 {
			pairs = allPairs
		}
		res = make([]*GroupScore, 0, len(pairs))
		for _, p := range pairs {
			pol.Pairs = []string{p}
			item := optForGroup(pol, args.Sampler, args.Picker, args.OptRounds, file)
			if item != nil {
				res = append(res, item)
			}
		}
		sort.Slice(res, func(i, j int) bool {
			return res[i].Score > res[j].Score
		})
	} else {
		item := optForGroup(pol, args.Sampler, args.Picker, args.OptRounds, file)
		if item != nil {
			res = append(res, item)
		}
	}
	for _, gp := range res {
		if gp.Score <= 0 {
			break
		}
		file.WriteString(fmt.Sprintf("\n  # score: %.2f\n", gp.Score))
		for _, p := range gp.Items {
			file.WriteString(p.ToYaml())
		}
	}
	return nil
}

/*
optForGroup
Optimize the hyperparameters of a policy and automatically search for the best combination of long, short, and both.
对某个策略超参数调优，自动搜索long/short/both的最佳组合。
*/
func optForGroup(pol *config.RunPolicyConfig, method, picker string, rounds int, flog *os.File) *GroupScore {
	groups := make([]*config.RunPolicyConfig, 0, 3)
	var long, short, both *config.RunPolicyConfig
	if pol.Dirt == "any" {
		long = pol.Clone()
		long.Dirt = "long"
		short = pol.Clone()
		short.Dirt = "short"
		both = pol.Clone()
		both.Dirt = ""
		groups = append(groups, long, short, both)
	} else {
		groups = append(groups, pol.Clone())
	}
	var bestOdNum = 0
	var bestScore = -999.0
	var bestPols []*config.RunPolicyConfig
	for _, p := range groups {
		config.RunPolicy = []*config.RunPolicyConfig{p}
		optForPol(p, method, picker, rounds, flog)
		if p.Score > bestScore {
			bestOdNum = p.MaxOpen
			bestScore = p.Score
			bestPols = []*config.RunPolicyConfig{p}
		}
	}
	if len(groups) == 1 {
		return &GroupScore{groups, bestScore}
	}
	// long,short,both分别评估
	minScore := min(long.Score, short.Score)
	maxScore := max(long.Score, short.Score)
	if minScore > 0 && maxScore > 0 {
		// 检查组合的是否优于long/short/both
		flog.WriteString("\n========== union long/short ============\n")
		_ = config.SetRunPolicy(true, long, short)
		bt, loss := runBTOnce()
		line := fmt.Sprintf("loss: %5.2f \t%v\n", loss, bt.BriefLine())
		flog.WriteString(line)
		log.Warn(line)
		curScore := -loss
		odNumRate := float64(bt.OrderNum) / float64(bestOdNum)
		scoreRate := curScore / bestScore
		if scoreRate > 1.25 || scoreRate > 1.1 && odNumRate < 1.5 {
			bestScore = curScore
			bestOdNum = bt.OrderNum
			bestPols = []*config.RunPolicyConfig{long, short}
		}
	}
	if minScore < 0 || maxScore > minScore*5 {
		// The long and short returns are seriously unbalanced, the parameters with high fixed returns remain unchanged, and the parameters with low returns are fine-tuned to find the best score of the combination
		// 多空收益严重不均衡，固定收益高的参数不变，微调收益低的参数，寻找组合最佳分数
		_ = config.SetRunPolicy(true, long, short)
		var unionScore float64
		if long.Score > short.Score {
			optForPol(short, method, picker, rounds, flog)
			unionScore = short.Score
		} else {
			optForPol(long, method, picker, rounds, flog)
			unionScore = long.Score
		}
		if unionScore > bestScore {
			return &GroupScore{[]*config.RunPolicyConfig{long, short}, unionScore}
		}
	}
	if len(bestPols) > 0 {
		return &GroupScore{bestPols, bestScore}
	}
	return nil
}

type GroupScore struct {
	Items []*config.RunPolicyConfig
	Score float64
}

/*
optForPol
Optimize policy tasks and support bayes, tpe, and cames
Before calling this method, you need to set 'config. RunPolicy`
对策略任务执行优化，支持bayes/tpe/cames等
调用此方法前需要设置 `config.RunPolicy`
*/
func optForPol(pol *config.RunPolicyConfig, method, picker string, rounds int, flog *os.File) {
	title := pol.Key()
	// 重置PairParams，避免影响传入参数
	pol.PairParams = make(map[string]map[string]float64)
	pol.Score = -998
	_ = strat.New(pol)
	params := pol.HyperParams()
	if len(params) == 0 {
		log.Warn("no hyper params, skip optimize", zap.String("strat", title))
		return
	}
	detailDir := filepath.Join(filepath.Dir(flog.Name()), "detail")
	err_ := utils.EnsureDir(detailDir, 0755)
	if err_ != nil {
		log.Warn("create detail dir fail", zap.String("path", detailDir), zap.Error(err_))
		return
	}
	flog.WriteString(fmt.Sprintf("\n============== %s =============\n", title))
	var resList = make([]*OptInfo, 0, rounds)
	runOptJob := func(data map[string]float64) (float64, *errs.Error) {
		jobId := utils.RandomStr(6)
		ints := make(map[string]bool)
		for k, v := range data {
			pol.Params[k] = v
			ints[k] = pol.IsInt(k)
		}
		bt, loss := runBTOnce()
		o := &OptInfo{Score: -loss, Params: data, Ints: ints, BTResult: bt.BTResult, ID: jobId}
		line := o.ToLine()
		flog.WriteString(line + "\n")
		log.Warn(line)
		bt.dumpDetail(filepath.Join(detailDir, jobId+".json"))
		o.BTResult.DelBigObjects()
		resList = append(resList, o)
		return loss, nil
	}
	var err *errs.Error
	if method == "bayes" {
		err = runBayes(rounds, params, runOptJob)
	} else {
		err = runGOptuna(method, rounds, params, runOptJob)
	}
	best := calcBestBy(resList, picker)
	if best.BTResult == nil {
		best.ID = utils.RandomStr(6)
		best.runGetBtResult(pol)
		best.dumpDetail(filepath.Join(detailDir, best.ID+".json"))
		line := fmt.Sprintf("[%s] %s", picker, best.ToLine())
		flog.WriteString(line + "\n")
		log.Warn(line)
	}
	if err != nil {
		log.Error("optimize fail", zap.String("job", title), zap.Error(err))
	}
	pol.Params = best.Params
	pol.Score = best.Score
	pol.MaxOpen = best.OrderNum
}

func runBTOnce() (*BackTest, float64) {
	core.BotRunning = true
	biz.ResetVars()
	bt, err := NewBackTest(true, "")
	if err != nil {
		panic(err)
	}
	bt.Run()
	var loss = -bt.Score()
	return bt, loss
}

func runGOptuna(name string, rounds int, params []*core.Param, loop FuncOptTask) *errs.Error {
	var sampler goptuna.Sampler
	var options []goptuna.StudyOption
	var seed = int64(0)
	if name == "random" {
		sampler = goptuna.NewRandomSampler(goptuna.RandomSamplerOptionSeed(seed))
	} else if name == "cmaes" {
		sampler = goptuna.NewRandomSampler(goptuna.RandomSamplerOptionSeed(seed))
		rs := cmaes.NewSampler(cmaes.SamplerOptionSeed(seed))
		options = append(options, goptuna.StudyOptionRelativeSampler(rs))
	} else if name == "ipop-cmaes" {
		sampler = goptuna.NewRandomSampler(goptuna.RandomSamplerOptionSeed(seed))
		rs := cmaes.NewSampler(cmaes.SamplerOptionSeed(seed),
			cmaes.SamplerOptionIPop(2))
		options = append(options, goptuna.StudyOptionRelativeSampler(rs))
	} else if name == "bipop-cmaes" {
		sampler = goptuna.NewRandomSampler(goptuna.RandomSamplerOptionSeed(seed))
		rs := cmaes.NewSampler(cmaes.SamplerOptionSeed(seed),
			cmaes.SamplerOptionBIPop(2))
		options = append(options, goptuna.StudyOptionRelativeSampler(rs))
	} else if name == "tpe" {
		sampler = tpe.NewSampler()
	} else {
		panic("invalid sampler")
	}
	options = append(options, goptuna.StudyOptionSampler(sampler))
	study, err_ := goptuna.CreateStudy("optimize", options...)
	if err_ != nil {
		return errs.New(errs.CodeRunTime, err_)
	}
	err_ = study.Optimize(func(trial goptuna.Trial) (float64, error) {
		var data = make(map[string]float64)
		for _, p := range params {
			minVal, maxVal := p.OptSpace()
			var val float64
			var valid bool
			for i := 0; i < 100; i++ {
				val, _ = trial.SuggestFloat(p.Name, minVal, maxVal)
				val, valid = p.ToRegular(val)
				if valid {
					break
				}
			}
			data[p.Name] = val
		}
		score, err := loop(data)
		if err != nil {
			return 0, err
		}
		return score, nil
	}, rounds)
	if err_ != nil {
		return errs.New(errs.CodeRunTime, err_)
	}
	return nil
}

func runBayes(rounds int, params []*core.Param, loop FuncOptTask) *errs.Error {
	bysParams := make([]bayesopt.Param, 0, len(params))
	for _, p := range params {
		minVal, maxVal := p.OptSpace()
		bysParams = append(bysParams, bayesopt.UniformParam{
			Name: p.Name,
			Min:  minVal,
			Max:  maxVal,
		})
	}
	options := []bayesopt.OptimizerOption{
		bayesopt.WithParallel(1),
		bayesopt.WithRounds(rounds),
		bayesopt.WithRandomRounds(rounds / 2),
	}
	opt := bayesopt.New(bysParams, options...)
	_, _, err_ := opt.Optimize(func(m map[bayesopt.Param]float64) float64 {
		var data = make(map[string]float64)
		for k, v := range m {
			data[k.GetName()] = v
		}
		for _, p := range params {
			data[p.Name], _ = p.ToRegular(data[p.Name])
		}
		score, _ := loop(data)
		return score
	})
	if err_ != nil {
		return errs.New(errs.CodeRunTime, err_)
	}
	err_ = opt.ExplorationErr()
	if err_ != nil {
		log.Warn("bayes early stop", zap.String("err", err_.Error()))
	}
	return nil
}

/*
CollectOptLog
Collect and analyze the logs generated by RunOptimize
Sorts all policy tasks in reverse score order of output.
收集分析RunOptimize生成的日志
将所有策略任务按分数倒序排列输出。
*/
func CollectOptLog(args *config.CmdArgs) *errs.Error {
	if args.InPath == "" {
		log.Warn("-in is required")
		return nil
	}
	info, err_ := os.Stat(args.InPath)
	if err_ != nil {
		return errs.New(errs.CodeIOReadFail, err_)
	}
	if !info.IsDir() {
		sortOptLogs(args.InPath)
		return nil
	}
	core.SetRunMode(core.RunModeBackTest)
	err := biz.SetupComsExg(args)
	if err != nil {
		return err
	}
	paths := make([]string, 0)
	filepath.WalkDir(args.InPath, func(path string, d fs.DirEntry, err error) error {
		if strings.HasSuffix(path, ".log") {
			paths = append(paths, path)
		}
		return nil
	})
	res, err := collectOptLog(paths, 0, args.Picker, args.PairPicker)
	if err != nil {
		return err
	}
	fmt.Print(res)
	return nil
}

func collectOptLog(paths []string, minScore float64, picker, pairSel string) (string, *errs.Error) {
	res := make([]*OptGroup, 0)
	detailDir := ""
	for _, path := range paths {
		var name, pair, dirt, tfStr string
		inUnion := false
		var items []*OptInfo
		var long, short, both, union, longMain, shortMain *OptInfo
		var pickerMap = make(map[string]*OptInfo)
		fdata, err_ := os.ReadFile(path)
		if err_ != nil {
			return "", errs.New(errs.CodeIOReadFail, err_)
		}
		if detailDir == "" {
			detailDir = filepath.Join(filepath.Dir(path), "detail")
			_ = utils.EnsureDir(detailDir, 0755)
		}
		saveGroup := func() {
			if name == "" {
				return
			}
			var oneSide = long
			if short != nil && (oneSide == nil || short.Score > oneSide.Score) {
				oneSide = short
			}
			var twoSide = both
			if union != nil && (twoSide == nil || union.Score > twoSide.Score) {
				twoSide = union
			}
			sideMain := 0
			if longMain != nil && (twoSide == nil || longMain.Score > twoSide.Score) {
				twoSide = longMain
				sideMain = 1
			}
			if shortMain != nil && (twoSide == nil || shortMain.Score > twoSide.Score) {
				twoSide = shortMain
				sideMain = 2
			}
			var pols []*OptInfo
			var bestScore float64
			var useOne = true
			if twoSide == nil {
				useOne = true
			} else if oneSide == nil {
				useOne = false
			} else {
				scoreRate := twoSide.Score / oneSide.Score
				odNumRate := float64(twoSide.OrderNum) / float64(oneSide.OrderNum)
				if scoreRate > 1.25 || scoreRate > 1.1 && odNumRate < 1.5 {
					useOne = false
				}
			}
			if !useOne {
				bestScore = twoSide.Score
				if twoSide.Dirt == "long" {
					long.Params = twoSide.Params
				} else if twoSide.Dirt == "short" {
					short.Params = twoSide.Params
				}
				if twoSide.Dirt == "" {
					pols = []*OptInfo{twoSide}
				} else if sideMain == 1 {
					pols = []*OptInfo{long, longMain}
				} else if sideMain == 2 {
					pols = []*OptInfo{short, shortMain}
				} else {
					pols = []*OptInfo{long, short}
				}
			} else {
				bestScore = oneSide.Score
				pols = []*OptInfo{oneSide}
			}
			res = append(res, &OptGroup{
				Items: pols,
				Score: bestScore,
				Name:  name,
				Pair:  pair,
				TFStr: tfStr,
			})
			long, short, both, union, longMain, shortMain = nil, nil, nil, nil, nil, nil
			name, pair, dirt, tfStr = "", "", "", ""
			inUnion = false
			items = nil
			pickerMap = make(map[string]*OptInfo)
		}
		lines := strings.Split(string(fdata), "\n")
		outs := make([]string, 0, len(lines))
		outs = append(outs, lines[:2]...)
		for _, line := range lines[2:] {
			outs = append(outs, line)
			if strings.HasPrefix(line, "loss:") {
				opt := parseOptLine(line)
				items = append(items, opt)
			} else if strings.HasPrefix(line, "[") && strings.Contains(line, "loss:") {
				// 保存指定picker的结果
				start := strings.Index(line, "loss:")
				pkEnd := strings.IndexRune(line, ']')
				pickerMap[line[1:pkEnd]] = parseOptLine(line[start:])
			} else if line == "" {
				// end section, calc best
				var best *OptInfo
				if old, has := pickerMap[picker]; has {
					best = old
				} else {
					best = calcBestBy(items, picker)
				}
				if best != nil {
					needRun := best.BTResult == nil || best.ID == ""
					if inUnion {
						union = best
						union.Dirt = "union"
						inUnion = false
						if needRun {
							config.RunPolicy = []*config.RunPolicyConfig{
								long.ToPol(0, name, dirt, tfStr, pair),
								short.ToPol(1, name, dirt, tfStr, pair),
							}
						}
					} else if dirt == "long" {
						if long == nil {
							long = best
							if needRun {
								config.RunPolicy = []*config.RunPolicyConfig{
									long.ToPol(0, name, dirt, tfStr, pair),
								}
							}
						} else {
							shortMain = best
							if needRun {
								config.RunPolicy = []*config.RunPolicyConfig{
									short.ToPol(0, name, dirt, tfStr, pair),
									shortMain.ToPol(1, name, dirt, tfStr, pair),
								}
							}
						}
					} else if dirt == "short" {
						if short == nil {
							short = best
							if needRun {
								config.RunPolicy = []*config.RunPolicyConfig{
									short.ToPol(0, name, dirt, tfStr, pair)}
							}
						} else {
							longMain = best
							if needRun {
								config.RunPolicy = []*config.RunPolicyConfig{
									long.ToPol(0, name, dirt, tfStr, pair),
									longMain.ToPol(1, name, dirt, tfStr, pair)}
							}
						}
					} else {
						both = best
						if needRun {
							config.RunPolicy = []*config.RunPolicyConfig{both.ToPol(0, name, dirt, tfStr, pair)}
						}
					}
					var dumpPath string
					if best.ID != "" {
						dumpPath = filepath.Join(detailDir, best.ID+".json")
						btRes, err := ParseBtResult(dumpPath)
						if err != nil {
							log.Warn("parse BtResult fail", zap.String("err", err.Short()))
							needRun = true
						} else {
							best.BTResult = btRes
						}
					}
					if needRun {
						if best.BTResult == nil || len(best.PairGrps) == 0 {
							best.ID = utils.RandomStr(6)
							dumpPath = filepath.Join(detailDir, best.ID+".json")
							best.runGetBtResult(config.RunPolicy[len(config.RunPolicy)-1])
							best.dumpDetail(dumpPath)
						}
						l := fmt.Sprintf("[%s] %s", picker, best.ToLine())
						idx := len(outs) - 1
						outs = append(outs[:idx], []string{l, outs[idx]}...)
					}
				}
				items = nil
				if len(pickerMap) > 0 {
					pickerMap = make(map[string]*OptInfo)
				}
				continue
			}
			if strings.HasPrefix(line, "  ") || strings.HasPrefix(line, "# ") {
				// 跳过输出的配置、注释信息
				continue
			}
			if strings.HasPrefix(line, "==============") && strings.HasSuffix(line, "=============") {
				n, d, t, p := parseSectionTitle(strings.Split(line, " ")[1])
				if p != pair || n != name || t != tfStr {
					saveGroup()
				}
				name, dirt, tfStr, pair = n, d, t, p
				inUnion = false
			} else if strings.HasPrefix(line, "========== union") {
				inUnion = true
			}
		}
		if len(outs) > len(lines) {
			err := utils.WriteFile(path, []byte(strings.Join(outs, "\n")))
			if err != nil {
				log.Warn("write fail", zap.String("p", path), zap.Error(err))
			}
		}
		saveGroup()
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].Score > res[j].Score
	})
	var b strings.Builder
	b.WriteString("run_policy:\n")
	for _, gp := range res {
		if gp.Score < minScore {
			break
		}
		b.WriteString(fmt.Sprintf("\n  # score: %.2f\n", gp.Score))
		for _, p := range gp.Items {
			tfStr := strings.ReplaceAll(gp.TFStr, "|", ", ")
			b.WriteString(fmt.Sprintf("  - name: %s\n    run_timeframes: [ %s ]\n", gp.Name, tfStr))
			var pairStr string
			if pairSel != "" {
				pairs := selectPairs(p.BTResult, pairSel)
				if len(pairs) > 0 {
					pairStr = strings.Join(pairs, ", ")
				}
			}
			if pairStr == "" && gp.Pair != "" {
				pairStr = strings.ReplaceAll(gp.Pair, "|", ", ")
			}
			if pairStr != "" {
				b.WriteString(fmt.Sprintf("    pairs: [%s]\n", pairStr))
			}
			if p.Dirt != "" {
				b.WriteString(fmt.Sprintf("    dirt: %s\n", p.Dirt))
			}
			paramStr := utils.MapToStr(p.Params, true, 2)
			b.WriteString(fmt.Sprintf("    params: {%s}\n", paramStr))
		}
	}
	return b.String(), nil
}

/*
Parse the header and return: policy name, direction, tfStr, pairStr
解析标题，返回：策略名，方向，tfStr，pairStr
*/
func parseSectionTitle(title string) (string, string, string, string) {
	arr := strings.Split(title, "/")
	name, dirt := parsePolID(arr[0])
	return name, dirt, arr[1], arr[2]
}

func parsePolID(id string) (string, string) {
	arr := strings.Split(id, ":")
	last := arr[len(arr)-1]
	var name, dirt string
	name = strings.Join(arr[:len(arr)-1], ":")
	if last == "l" {
		dirt = "long"
	} else if last == "s" {
		dirt = "short"
	} else {
		name = strings.Join(arr, ":")
	}
	return name, dirt
}

func parseOptLine(line string) *OptInfo {
	if !strings.HasPrefix(line, "loss:") {
		return nil
	}
	paraStart := strings.IndexRune(line, '\t')
	paraEnd := strings.Index(line, "odNum:")
	if paraEnd < 0 {
		paraEnd = len(line)
	}
	res := &OptInfo{Params: make(map[string]float64), Ints: make(map[string]bool), BTResult: &BTResult{}}
	loss, _ := strconv.ParseFloat(strings.TrimSpace(line[5:paraStart]), 64)
	res.Score = -loss
	paraArr := strings.Split(strings.TrimSpace(line[paraStart:paraEnd]), ",")
	for _, str := range paraArr {
		arr := strings.Split(strings.TrimSpace(str), ":")
		res.Params[arr[0]], _ = strconv.ParseFloat(strings.TrimSpace(arr[1]), 64)
	}
	prefStr := strings.TrimSpace(line[paraEnd:])
	if len(prefStr) > 0 {
		prefArr := strings.Split(prefStr, ",")
		for _, str := range prefArr {
			arr := strings.Split(strings.TrimSpace(str), ":")
			key, val := arr[0], strings.TrimSpace(arr[1])
			if key == "odNum" {
				res.OrderNum, _ = strconv.Atoi(val)
			} else if key == "profit" {
				res.TotProfitPct, _ = strconv.ParseFloat(val[:len(val)-1], 64)
			} else if key == "drawDown" {
				res.ShowDrawDownPct, _ = strconv.ParseFloat(val[:len(val)-1], 64)
			} else if key == "sharpe" {
				res.SharpeRatio, _ = strconv.ParseFloat(val, 64)
			} else if key == "id" {
				res.ID = val
			}
		}
	}
	return res
}
