package btime

import (
	"fmt"
	"github.com/banbox/bntp"
	"testing"
	"time"
)

func TestNow(t *testing.T) {
	tm := bntp.Now()
	fmt.Printf("time: %v", tm.Unix())
}

func TestParseTimeMS(t *testing.T) {
	// 设置时区为 UTC 以便测试结果一致
	loc, _ := time.LoadLocation("UTC")
	time.Local = loc

	tests := []struct {
		name     string
		timeStr  string
		expected int64
		wantErr  bool
	}{
		// 年份测试
		{"Year only", "2023", time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(), false},

		// 年月测试
		{"Year month compact", "202301", time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(), false},
		{"Year month with dash", "2023-01", time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(), false},
		{"Year month with slash", "2023/01", time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(), false},

		// 年月日测试
		{"Year month day compact", "20230102", time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC).UnixMilli(), false},
		{"Year month day with dash", "2023-01-02", time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC).UnixMilli(), false},
		{"Year month day with slash", "2023/01/02", time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC).UnixMilli(), false},
		{"Year month day with dot", "2023.01.02", time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC).UnixMilli(), false},
		{"Year month day with space", "2023 01 02", time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC).UnixMilli(), false},

		// 年月日时分测试
		{"Year month day hour minute compact", "202301021504", time.Date(2023, 1, 2, 15, 4, 0, 0, time.UTC).UnixMilli(), false},
		{"Year month day hour minute with dash", "2023-01-02 15:04", time.Date(2023, 1, 2, 15, 4, 0, 0, time.UTC).UnixMilli(), false},
		{"Year month day hour minute with slash", "2023/01/02 15:04", time.Date(2023, 1, 2, 15, 4, 0, 0, time.UTC).UnixMilli(), false},
		{"Year month day hour minute with space", "2023 01 02 15 04", time.Date(2023, 1, 2, 15, 4, 0, 0, time.UTC).UnixMilli(), false},

		// 年月日时分秒测试
		{"Year month day hour minute second compact", "20230102150405", time.Date(2023, 1, 2, 15, 4, 5, 0, time.UTC).UnixMilli(), false},
		{"Year month day hour minute second with dash", "2023-01-02 15:04:05", time.Date(2023, 1, 2, 15, 4, 5, 0, time.UTC).UnixMilli(), false},
		{"Year month day hour minute second with slash", "2023/01/02 15:04:05", time.Date(2023, 1, 2, 15, 4, 5, 0, time.UTC).UnixMilli(), false},
		{"Year month day hour minute second with space", "2023 01 02 15 04 05", time.Date(2023, 1, 2, 15, 4, 5, 0, time.UTC).UnixMilli(), false},

		// 时间戳测试
		{"10-digit timestamp", "1672617600", 1672617600000, false}, // 2023-01-02 00:00:00 UTC
		{"13-digit timestamp", "1672617600000", 1672617600000, false},

		// 错误测试
		{"Invalid format", "abcdef", 0, true},
		{"Invalid timestamp", "12345abcde", 0, true},
		{"Empty string", "", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTimeMS(tt.timeStr)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseTimeMS() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.expected {
				t.Errorf("ParseTimeMS() = %v, want %v", got, tt.expected)
				// 打印实际和期望的时间便于调试
				t.Logf("Got: %v, Expected: %v",
					time.UnixMilli(got).Format("2006-01-02 15:04:05.000"),
					time.UnixMilli(tt.expected).Format("2006-01-02 15:04:05.000"))
			}
		})
	}
}

// 辅助函数：同时测试ParseTimeMS和原始ParseTimeMSBy，以便对比两个函数的结果
func TestCompareParseResults(t *testing.T) {
	// 设置一些测试用例
	tests := []struct {
		timeStr string
		layout  string
	}{
		{"2023", "2006"},
		{"202301", "200601"},
		{"20230102", "20060102"},
		{"2023-01-02", "2006-01-02"},
		{"2023/01/02", "2006/01/02"},
		{"2023-01-02 15:04", "2006-01-02 15:04"},
		{"2023/01/02 15:04", "2006/01/02 15:04"},
		{"2023-01-02 15:04:05", "2006-01-02 15:04:05"},
		{"2023/01/02 15:04:05", "2006/01/02 15:04:05"},
	}

	for _, tt := range tests {
		t.Run(tt.timeStr, func(t *testing.T) {
			// 使用ParseTimeMS解析
			parsedMS, err := ParseTimeMS(tt.timeStr)
			if err != nil {
				t.Errorf("ParseTimeMS(%s) failed: %v", tt.timeStr, err)
				return
			}

			// 使用原始的ParseTimeMSBy解析
			parsedBy, err := ParseTimeMSBy(tt.layout, tt.timeStr)
			if err != nil {
				t.Errorf("ParseTimeMSBy(%s, %s) failed: %v", tt.layout, tt.timeStr, err)
				return
			}

			// 比较两个函数的结果
			if parsedMS != parsedBy {
				t.Errorf("Mismatch for %s: ParseTimeMS=%v, ParseTimeMSBy=%v",
					tt.timeStr, parsedMS, parsedBy)
				t.Logf("ParseTimeMS time: %v", time.UnixMilli(parsedMS).Format("2006-01-02 15:04:05.000"))
				t.Logf("ParseTimeMSBy time: %v", time.UnixMilli(parsedBy).Format("2006-01-02 15:04:05.000"))
			}
		})
	}
}
