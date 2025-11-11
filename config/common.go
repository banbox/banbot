package config

import (
	"fmt"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/banbox/banexg/utils"
	"go.uber.org/zap"
	"math"
	"strconv"
	"strings"
	"sync"
)

func GetExchangeProxy(exgId string) (string, *errs.Error) {
	params, ok := Exchange.Items[exgId]
	if !ok {
		return "", errs.NewMsg(errs.CodeParamInvalid, "no exchange found: %v", exgId)
	}
	proxyUrl := utils.GetMapVal(params, "proxy", "")
	if proxyUrl == "" {
		proxyUrl = utils.GetSystemEnvProxy()
		if proxyUrl == "" {
			prx, err := utils.GetSystemProxy()
			if err != nil {
				log.Error("GetSystemProxy fail, skip", zap.Error(err))
			} else if prx != nil {
				proxyUrl = fmt.Sprintf("%s://%s:%s", prx.Protocol, prx.Host, prx.Port)
			}
		}
	} else if proxyUrl == "no" {
		proxyUrl = ""
	}
	return proxyUrl, nil
}

var (
	refineTfMap  = make(map[string]map[string]string)
	refineLock   sync.RWMutex
	preferTfSecs = []int{60, 180, 300, 600, 900, 3600, 7200, 14400, 28800, 43200,
		86400, 259200, 604800}
	preferTfs = map[int]string{
		60:     "1m",
		180:    "3m",
		300:    "5m",
		600:    "10m",
		900:    "15m",
		3600:   "1h",
		7200:   "2h",
		14400:  "4h",
		28800:  "8h",
		43200:  "12h",
		86400:  "1d",
		259200: "3d",
		604800: "1w",
	}
)

func ClearRefineMap() {
	refineLock.Lock()
	refineTfMap = make(map[string]map[string]string)
	refineLock.Unlock()
}

func GetStratRefineTF(stratName, timeframe string) (string, bool) {
	refineLock.RLock()
	outTf := ""
	if tfMap, ok := refineTfMap[stratName]; ok {
		subTf, ok2 := tfMap[timeframe]
		if ok2 {
			outTf = subTf
		}
	}
	refineLock.RUnlock()
	if outTf != "" {
		return outTf, true
	}
	return timeframe, false
}

func EnsureStratRefineTF(stratName, timeframe string) string {
	refineLock.RLock()
	outTf := ""
	if tfMap, ok := refineTfMap[stratName]; ok {
		subTf, ok2 := tfMap[timeframe]
		if ok2 {
			outTf = subTf
		}
	}
	refineLock.RUnlock()
	if outTf != "" {
		return outTf
	}
	refineLock.Lock()
	outTf, err := parseRefineTf(stratName, timeframe)
	refineLock.Unlock()
	if err != nil {
		panic(err)
	}
	return outTf
}

func parseRefineTf(stratName, timeframe string) (string, error) {
	outTf := ""
	tfMap, ok := refineTfMap[stratName]
	if !ok {
		tfMap = make(map[string]string)
		refineTfMap[stratName] = tfMap
	}
	refineRange, polName := "", ""
	for _, pol := range RunPolicy {
		polName = pol.ID()
		if polName == stratName {
			if pol.RefineTF != nil {
				refineRange = fmt.Sprintf("%v", pol.RefineTF)
			}
			break
		}
	}
	if refineRange == "" || timeframe == "1m" {
		outTf = timeframe
	} else {
		unit := refineRange[len(refineRange)-1]
		if unit >= 'A' {
			outTf = refineRange
		} else {
			start, end, err := parseIntRange(refineRange)
			if err != nil {
				tfMap[timeframe] = timeframe
				return timeframe, fmt.Errorf("invalid refine_ratio, use format like `5` or `3-6`: %s", refineRange)
			} else {
				if end <= 1 {
					outTf = timeframe
				} else {
					inTfSecs := utils.TFToSecs(timeframe)
					for rate := end; rate >= start; rate-- {
						modVal := inTfSecs % rate
						if modVal == 0 {
							subSecs := inTfSecs / rate
							if subTf, ok := preferTfs[subSecs]; ok {
								outTf = subTf
								break
							}
						}
					}
					if outTf == "" {
						minSubSecs := math.MaxInt
						for rate := end; rate >= start; rate-- {
							subSecs := int(math.Round(float64(inTfSecs) / float64(rate)))
							subSecs = int(utils.AlignTfSecs(int64(subSecs), 60))
							if subTf, ok := preferTfs[subSecs]; ok {
								outTf = subTf
								break
							}
							minSubSecs = min(minSubSecs, subSecs)
						}
						if inTfSecs <= 60 {
							outTf = timeframe
						} else if outTf == "" {
							for i := len(preferTfSecs) - 1; i >= 0; i-- {
								if preferTfSecs[i] <= minSubSecs {
									outTf = preferTfs[preferTfSecs[i]]
									break
								}
							}
							if outTf == "" {
								outTf = timeframe
							}
						}
					}
				}
			}
		}
	}
	tfMap[timeframe] = outTf
	return outTf, nil
}

func parseIntRange(str string) (int, int, error) {
	arr := strings.Split(str, "-")
	valArr := make([]int, 0, len(arr))
	for _, s := range arr {
		v, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil {
			return 0, 0, err
		}
		valArr = append(valArr, v)
	}
	if len(valArr) == 0 || len(valArr) > 2 {
		return 0, 0, fmt.Errorf("invalid int range: %v", str)
	}
	start, end := valArr[0], valArr[0]
	if len(valArr) > 1 {
		end = valArr[1]
	}
	if start > end {
		start, end = end, start
	}
	return start, end, nil
}
