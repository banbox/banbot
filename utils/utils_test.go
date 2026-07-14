package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestGcdInts(t *testing.T) {
	items := []struct {
		Nums []int
		Res  int
	}{
		{Nums: []int{10, 15, 30}, Res: 5},
		{Nums: []int{15, 30, 60}, Res: 15},
		{Nums: []int{9, 15, 60}, Res: 3},
	}
	for _, c := range items {
		res := GcdInts(c.Nums)
		if res != c.Res {
			t.Errorf("fail %v, exp: %d, res: %d", c.Nums, c.Res, res)
		}
	}
}

func TestCopyDir(t *testing.T) {
	root := t.TempDir()
	srcDir := filepath.Join(root, "source")
	tgtDir := filepath.Join(root, "target")
	nestedDir := filepath.Join(srcDir, "nested")
	if err := os.MkdirAll(nestedDir, 0o755); err != nil {
		t.Fatal(err)
	}
	fixtures := map[string]string{
		filepath.Join(srcDir, "strategy.go"):   "package main\n",
		filepath.Join(nestedDir, "config.yml"): "enabled: true\n",
	}
	for path, content := range fixtures {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	if err := CopyDir(srcDir, tgtDir); err != nil {
		t.Fatal(err)
	}
	for srcPath, want := range fixtures {
		relPath, err := filepath.Rel(srcDir, srcPath)
		if err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(filepath.Join(tgtDir, relPath))
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != want {
			t.Errorf("copied %s = %q, want %q", relPath, got, want)
		}
	}
}

func TestCopyDirExcluding(t *testing.T) {
	root := t.TempDir()
	srcDir := filepath.Join(root, "source")
	tgtDir := filepath.Join(root, "target")
	excludedDir := filepath.Join(srcDir, "backtest")
	if err := os.MkdirAll(filepath.Join(excludedDir, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "strategy.go"), []byte("package main\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(excludedDir, "nested", "report.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := CopyDirExcluding(srcDir, tgtDir, excludedDir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(tgtDir, "strategy.go")); err != nil {
		t.Fatalf("expected non-excluded source file to be copied: %v", err)
	}
	if _, err := os.Stat(filepath.Join(tgtDir, "backtest")); !os.IsNotExist(err) {
		t.Fatalf("expected excluded subtree to be absent, got %v", err)
	}
}

func TestHashToAlphaNum(t *testing.T) {
	tests := []struct {
		input  string
		length int
	}{
		{"banbot", 6},
		{"test", 6},
		{"mybot", 6},
		{"", 6},
	}
	for _, tt := range tests {
		result := HashToAlphaNum(tt.input, tt.length)
		if len(result) != tt.length {
			t.Errorf("HashToAlphaNum(%q, %d) length = %d; want %d", tt.input, tt.length, len(result), tt.length)
		}
		// Verify only alphanumeric
		for _, c := range result {
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'z')) {
				t.Errorf("HashToAlphaNum(%q, %d) contains invalid char %c", tt.input, tt.length, c)
			}
		}
		// Verify deterministic
		result2 := HashToAlphaNum(tt.input, tt.length)
		if result != result2 {
			t.Errorf("HashToAlphaNum(%q, %d) not deterministic: %s vs %s", tt.input, tt.length, result, result2)
		}
	}
	// Verify different inputs produce different outputs
	h1 := HashToAlphaNum("banbot", 6)
	h2 := HashToAlphaNum("test", 6)
	if h1 == h2 {
		t.Errorf("Different inputs should produce different hashes: %s == %s", h1, h2)
	}
}

func TestRoundSecsTF(t *testing.T) {
	tests := []struct {
		secs     int
		expected string
	}{
		// 秒级别边界测试
		{1, "1s"}, // 最小秒数
		{2, "2s"},
		{3, "3s"},
		{4, "5s"}, // 向上取整到5s
		{5, "5s"},
		{7, "5s"},  // 7秒向下取整到5s
		{8, "10s"}, // 8秒向上取整到10s
		{10, "10s"},
		{13, "15s"}, // 13秒向上取整到15s
		{15, "15s"},
		{17, "15s"}, // 17秒向下取整到15s
		{18, "20s"}, // 18秒向上取整到20s
		{20, "20s"},
		{22, "20s"}, // 22秒向下取整到20s
		{24, "30s"}, // 24秒向上取整到30s
		{30, "30s"},
		{45, "1m"}, // 45秒向上取整到1分钟
		{50, "1m"},

		// 分钟级别边界测试
		{60, "1m"},
		{120, "2m"},
		{180, "3m"},
		{240, "5m"},
		{250, "5m"}, // 向上取整到5分钟
		{300, "5m"},
		{600, "10m"},
		{900, "15m"},  // 15分钟
		{1200, "15m"}, // 20分钟
		{1800, "30m"}, // 30分钟

		// 小时级别边界测试
		{2520, "1h"},   // 42分钟，向上取整到45分钟
		{3600, "1h"},   // 1小时
		{5400, "2h"},   // 1.5小时，向上取整到2小时
		{7200, "2h"},   // 2小时
		{10800, "3h"},  // 3小时
		{14400, "4h"},  // 4小时
		{18000, "4h"},  // 5小时，向下取整到4小时
		{21600, "8h"},  // 6小时
		{43200, "12h"}, // 12小时

		// 天级别边界测试
		{68400, "1d"},  // 19小时
		{72000, "1d"},  // 20小时
		{86400, "1d"},  // 1天
		{129600, "2d"}, // 1.5天，向上取整到2天
		{172800, "2d"}, // 2天
		{432000, "5d"}, // 5天

		// 周级别边界测试
		{518400, "1w"},  // 6天
		{561600, "1w"},  // 6.5天，向上取整到1周
		{604800, "1w"},  // 1周
		{907200, "2w"},  // 1.5周，向上取整到2周
		{1209600, "2w"}, // 2周
		{1814400, "3w"}, // 3周
		{2419200, "1M"}, // 4周

		// 月级别边界测试
		{2505600, "1M"},   // 29天，向上取整到1月
		{2592000, "1M"},   // 1月（30天）
		{3888000, "2M"},   // 1.5月，向上取整到2月
		{5184000, "2M"},   // 2月
		{7776000, "3M"},   // 3月
		{15552000, "6M"},  // 6月
		{31104000, "12M"}, // 12月

		// 特殊边界情况
		{0, "0s"},           // 0秒
		{-3600, "-3600s"},   // 负数情况
		{999999999, "386M"}, // 超大数值
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d seconds", tt.secs), func(t *testing.T) {
			result := RoundSecsTF(tt.secs)
			if result != tt.expected {
				t.Errorf("RoundSecsTF(%d) = %s; want %s", tt.secs, result, tt.expected)
			}
		})
	}
}
