package config

import (
	"context"
	"fmt"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/banbox/banexg/utils"
	"go.uber.org/zap"
	"math"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

func GetExchangeProxy(exgId string) (string, *errs.Error) {
	params, ok := Exchange.Items[exgId]
	if !ok {
		return "", errs.NewMsg(errs.CodeParamInvalid, "no exchange found: %v", exgId)
	}
	proxyUrl := ExchangeProxyOption(params)
	if proxyUrl == "no" {
		return "", nil
	}
	return proxyUrl, nil
}

// ExchangeProxyOption returns the value to pass as banexg's Proxy option.
// It is always explicit: a proxy URL, or "no" when none applies. banexg treats
// an empty Proxy as "probe the system proxy", and that probe logs an error on
// machines where it fails, so banbot never leaves the option empty.
func ExchangeProxyOption(params map[string]interface{}) string {
	proxyUrl := utils.GetMapVal(params, "proxy", "")
	if proxyUrl == "no" {
		return "no"
	}
	if proxyUrl != "" {
		return proxyUrl
	}
	if proxyUrl = detectSystemProxy(); proxyUrl != "" {
		return proxyUrl
	}
	return "no"
}

var (
	systemProxyOnce sync.Once
	systemProxyURL  string
)

// detectSystemProxy resolves the system proxy once per process.
func detectSystemProxy() string {
	systemProxyOnce.Do(func() {
		systemProxyURL = resolveSystemProxy()
	})
	return systemProxyURL
}

func resolveSystemProxy() string {
	if proxyUrl := utils.GetSystemEnvProxy(); proxyUrl != "" {
		return proxyUrl
	}
	if runtime.GOOS == "darwin" {
		return macSystemProxy()
	}
	prx, err := utils.GetSystemProxy()
	if err != nil {
		log.Debug("read system proxy fail, skip", zap.Error(err))
		return ""
	}
	if prx != nil {
		return fmt.Sprintf("%s://%s:%s", prx.Protocol, prx.Host, prx.Port)
	}
	return ""
}

// macSystemProxy reads `scutil --proxy`. Unlike banexg's osascript probe it
// needs no automation permission and does not depend on a GUI session.
func macSystemProxy() string {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "scutil", "--proxy").Output()
	if err != nil {
		log.Debug("scutil --proxy fail, skip", zap.Error(err))
		return ""
	}
	return parseScutilProxy(string(out))
}

// parseScutilProxy extracts the enabled HTTPS (preferred) or HTTP proxy from
// `scutil --proxy` output, whose lines look like "HTTPSProxy : 127.0.0.1".
func parseScutilProxy(out string) string {
	values := make(map[string]string)
	for _, line := range strings.Split(out, "\n") {
		key, val, ok := strings.Cut(strings.TrimSpace(line), " : ")
		if ok {
			values[strings.TrimSpace(key)] = strings.TrimSpace(val)
		}
	}
	for _, prefix := range []string{"HTTPS", "HTTP"} {
		host, port := values[prefix+"Proxy"], values[prefix+"Port"]
		if values[prefix+"Enable"] == "1" && host != "" && port != "" {
			return fmt.Sprintf("http://%s:%s", host, port)
		}
	}
	return ""
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

func lookupStratRefineTF(stratName, timeframe string) (string, bool) {
	refineLock.RLock()
	defer refineLock.RUnlock()
	if tfMap, ok := refineTfMap[stratName]; ok {
		if subTf, ok := tfMap[timeframe]; ok && subTf != "" {
			return subTf, true
		}
	}
	return "", false
}

func GetStratRefineTF(stratName, timeframe string) (string, bool) {
	if outTf, ok := lookupStratRefineTF(stratName, timeframe); ok {
		return outTf, true
	}
	return timeframe, false
}

func EnsureStratRefineTF(stratName, timeframe string) string {
	if outTf, ok := lookupStratRefineTF(stratName, timeframe); ok {
		return outTf
	}
	refineLock.Lock()
	outTf, err := parseRefineTf(stratName, timeframe)
	refineLock.Unlock()
	if err != nil {
		// invalid refine_ratio: fall back to the original timeframe (parseRefineTf caches it too)
		log.Error("invalid refine tf, use original timeframe", zap.String("strat", stratName),
			zap.String("tf", timeframe), zap.Error(err))
		return timeframe
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
