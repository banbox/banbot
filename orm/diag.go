package orm

import (
	"os"
	"strconv"
	"strings"
)

var (
	klineDiagEnabled = true
	klineDiagFilter  = "XRP/USDT:USDT"
)

func parseBoolEnv(name string) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(name)))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

func allowKlineDiag(sid int32, symbol, tf string) bool {
	if !klineDiagEnabled {
		return false
	}
	if klineDiagFilter == "" {
		return true
	}
	parts := strings.Split(klineDiagFilter, ",")
	for _, raw := range parts {
		p := strings.TrimSpace(raw)
		if p == "" {
			continue
		}
		switch {
		case strings.HasPrefix(p, "sid:"):
			v := strings.TrimSpace(p[4:])
			if n, err := strconv.Atoi(v); err == nil && int32(n) == sid {
				return true
			}
		case strings.HasPrefix(p, "tf:"):
			if strings.EqualFold(strings.TrimSpace(p[3:]), tf) {
				return true
			}
		case strings.HasPrefix(p, "symbol:"):
			if strings.EqualFold(strings.TrimSpace(p[7:]), symbol) {
				return true
			}
		default:
			if strings.Contains(strings.ToLower(symbol), strings.ToLower(p)) {
				return true
			}
		}
	}
	return false
}

func summarizeRangeBounds(ranges []MSRange) (int, int64, int64) {
	if len(ranges) == 0 {
		return 0, 0, 0
	}
	minStart := ranges[0].Start
	maxStop := ranges[0].Stop
	for _, r := range ranges[1:] {
		if r.Start < minStart {
			minStart = r.Start
		}
		if r.Stop > maxStop {
			maxStop = r.Stop
		}
	}
	return len(ranges), minStart, maxStop
}
