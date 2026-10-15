package config

import (
	"math"
	"testing"
)

func TestDataPageBytesConfiguration(t *testing.T) {
	for _, value := range []any{0, 1024, int64(math.MaxInt64)} {
		if err := validateAdvanced("data", map[string]any{"page_bytes": value}); err != nil {
			t.Fatalf("valid page bytes %v: %v", value, err)
		}
	}
	for _, value := range []any{-1, float64(1024), "1024", uint64(math.MaxUint64), nil} {
		if err := validateAdvanced("data", map[string]any{"page_bytes": value}); err == nil {
			t.Fatalf("invalid page bytes accepted: %v", value)
		}
	}
}
