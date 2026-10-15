// Package testutil provides shared test-only helpers.
package testutil

import (
	"os"
	"testing"
)

// RequireIntegration skips tests that need a real database, exchange, or network.
func RequireIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("BANBOT_TEST_INTEGRATION") != "1" {
		t.Skip("set BANBOT_TEST_INTEGRATION=1 to run external integration test")
	}
}
