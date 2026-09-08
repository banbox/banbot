package data

import (
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/legacygate"
)

func TestPublicTickRunnersShareLegacyGate(t *testing.T) {
	tests := []struct {
		name string
		run  func(*config.CmdArgs) error
	}{
		{name: "format", run: func(args *config.CmdArgs) error { return RunFormatTick(args) }},
		{name: "to-kline", run: func(args *config.CmdArgs) error { return Build1mWithTicks(args) }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			unlock := legacygate.Lock()
			released := false
			t.Cleanup(func() {
				if !released {
					unlock()
				}
			})

			done := make(chan error, 1)
			go func() { done <- test.run(&config.CmdArgs{}) }()
			select {
			case err := <-done:
				unlock()
				released = true
				t.Fatalf("tick runner bypassed the legacy gate: %v", err)
			case <-time.After(50 * time.Millisecond):
			}

			unlock()
			released = true
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("invalid tick invocation unexpectedly succeeded")
				}
			case <-time.After(time.Second):
				t.Fatal("tick runner did not enter after the legacy gate was released")
			}
		})
	}
}
