package btime

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/banbox/banbot/core"
	"github.com/banbox/bntp"
)

var (
	CurTimeMS    = int64(0)
	UTCLocale, _ = time.LoadLocation("UTC")
	LocShow      *time.Location // 用于显示的时区
)

func init() {
	time.Local = UTCLocale
}

/*
UTCTime
Get 10-digit second-level floating point number
获取10位秒级浮点数
*/
func UTCTime() float64 {
	return float64(bntp.UTCStamp()) / 1000
}

/*
UTCStamp
Get 13-digit millisecond timestamp
获取13位毫秒时间戳
*/
func UTCStamp() int64 {
	return bntp.UTCStamp()
}

/*
Time
Get the current 10-digit second-level timestamp
获取当前10位秒级时间戳
*/
func Time() float64 {
	if core.BackTestMode {
		if CurTimeMS == 0 {
			CurTimeMS = UTCStamp()
		}
		return float64(CurTimeMS) * 0.001
	} else {
		return UTCTime()
	}
}

/*
TimeMS
Get the current 13-digit millisecond timestamp
获取当前13位毫秒时间戳
*/
func TimeMS() int64 {
	if core.BackTestMode {
		if CurTimeMS == 0 {
			CurTimeMS = UTCStamp()
		}
		return CurTimeMS
	} else {
		return UTCStamp()
	}
}

func MSToTime(timeMSecs int64) *time.Time {
	seconds := timeMSecs / 1000
	nanos := (timeMSecs % 1000) * 1000000
	res := time.Unix(seconds, nanos).UTC()
	return &res
}

func Now() *time.Time {
	if core.BackTestMode {
		if CurTimeMS == 0 {
			CurTimeMS = UTCStamp()
		}
		return MSToTime(CurTimeMS)
	}
	res := bntp.Now().In(UTCLocale)
	return &res
}

/*
ParseTimeMS
Convert time string to 13-digit millisecond timestamp
Supported forms:
将时间字符串转为13位毫秒时间戳
支持的形式：
2006
200601
20060102
2006-01-02
10-digit timestamp
13-digit timestamp
2006-01-02 15:04
2006-01-02 15:04:05
*/
func ParseTimeMS(timeStr string) (int64, error) {
	timeStr = strings.TrimSpace(timeStr)
	digitNum := core.CountDigit(timeStr)
	arr := core.SplitDigits(timeStr)
	// 处理时间戳格式
	if len(arr) == 1 {
		if digitNum == 10 {
			// 10位时间戳(秒)
			secs, err := strconv.ParseInt(timeStr, 10, 64)
			if err != nil {
				return 0, err
			}
			return secs * 1000, nil
		} else if digitNum == 13 {
			// 13位时间戳(毫秒)
			msecs, err := strconv.ParseInt(timeStr, 10, 64)
			if err != nil {
				return 0, err
			}
			return msecs, nil
		}
	}

	// 处理年份格式
	if len(arr) == 1 && digitNum == 4 {
		// 仅年份: 2006
		return ParseTimeMSBy("2006", timeStr)
	}

	// 处理年月格式
	if digitNum == 6 {
		if len(arr) == 1 {
			// 紧凑年月: 200601
			return ParseTimeMSBy("200601", timeStr)
		} else if len(arr) == 2 {
			// 确保年在前
			if len(arr[0]) == 4 {
				return ParseTimeMSBy("200601", arr[0]+arr[1])
			} else if len(arr[1]) == 4 {
				return ParseTimeMSBy("200601", arr[1]+arr[0])
			}
		}
	}

	// 处理年月日格式
	if digitNum == 8 {
		if len(arr) == 1 {
			// 紧凑年月日: 20060102
			return ParseTimeMSBy("20060102", timeStr)
		} else if len(arr) == 3 {
			// 确保按年月日顺序连接
			if len(arr[0]) == 4 { // 年在前: 2006 01 02
				return ParseTimeMSBy("20060102", arr[0]+arr[1]+arr[2])
			}
		}
	}

	// 处理年月日时分格式
	if digitNum == 12 {
		if len(arr) == 1 {
			// 紧凑年月日时分: 200601021504
			return ParseTimeMSBy("200601021504", timeStr)
		} else if len(arr) == 5 {
			// 2006 01 02 15 04
			if len(arr[0]) == 4 { // 年在前
				return ParseTimeMSBy("200601021504", arr[0]+arr[1]+arr[2]+arr[3]+arr[4])
			}
		}
	}

	// 处理年月日时分秒格式
	if digitNum == 14 {
		if len(arr) == 1 {
			// 紧凑年月日时分秒: 20060102150405
			return ParseTimeMSBy("20060102150405", timeStr)
		} else if len(arr) == 6 {
			// 2006 01 02 15 04 05
			if len(arr[0]) == 4 { // 年在前
				return ParseTimeMSBy("20060102150405", arr[0]+arr[1]+arr[2]+arr[3]+arr[4]+arr[5])
			}
		}
	}

	// 特殊情况：年月日之间有分隔符
	if len(arr) == 3 && len(arr[0]) == 4 {
		// 日期部分: 2006-01-02 或 2006/01/02
		dateStr := arr[0] + arr[1] + arr[2]

		// 检查是否有时间部分
		timeParts := strings.Split(timeStr, " ")
		if len(timeParts) > 1 {
			timeArr := core.SplitDigits(timeParts[1])

			if len(timeArr) == 2 {
				// 时分: 15:04
				return ParseTimeMSBy("200601021504", dateStr+timeArr[0]+timeArr[1])
			} else if len(timeArr) == 3 {
				// 时分秒: 15:04:05
				return ParseTimeMSBy("20060102150405", dateStr+timeArr[0]+timeArr[1]+timeArr[2])
			}
		} else {
			// 只有日期部分
			return ParseTimeMSBy("20060102", dateStr)
		}
	}

	return 0, fmt.Errorf("unsupported date format: %s", timeStr)
}

func ParseTimeMSBy(layout, timeStr string) (int64, error) {
	t, err := time.Parse(layout, timeStr)
	if err != nil {
		return 0, err
	}
	return t.UnixMilli(), nil
}

/*
ToDateStr
Convert timestamp to time string
将时间戳转为时间字符串
format default: 2006-01-02 15:04:05
*/
func ToDateStr(timestamp int64, format string) string {
	t := ToTime(timestamp)
	if format == "" {
		format = core.DefaultDateFmt
	}
	return t.Format(format)
}

func ToDateStrLoc(timestamp int64, format string) string {
	t := ToTime(timestamp).In(LocShow)
	if format == "" {
		format = core.DefaultDateFmt
	}
	return t.Format(format)
}

func ToTime(timestamp int64) time.Time {
	var t time.Time
	if timestamp >= 1e11 {
		// 13位毫秒时间戳(至少12位数)
		seconds := timestamp / 1000             // 秒
		nanoseconds := (timestamp % 1000) * 1e6 // 毫秒转为纳秒
		t = time.Unix(seconds, nanoseconds)
	} else {
		// 10位秒级时间戳
		t = time.Unix(timestamp, 0)
	}
	return t
}
