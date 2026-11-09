package base

import (
	"os"
	"strings"
	"testing"
)

// banbot must not hardcode a specific exchange (see AGENTS.md).
func TestIndicatorsDoNotHardcodeExchange(t *testing.T) {
	src, err := os.ReadFile("indicators.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.ToLower(string(src)), `"binance"`) {
		t.Fatal("indicators.go hardcodes an exchange name")
	}
}
