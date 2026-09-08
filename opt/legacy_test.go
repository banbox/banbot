package opt

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/legacygate"
	"github.com/spf13/cobra"
)

func TestWithLegacySessionSerializesConcurrentEntries(t *testing.T) {
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstDone := make(chan struct{})
	go func() {
		WithLegacySession(func(LegacySession) struct{} {
			close(firstEntered)
			<-releaseFirst
			return struct{}{}
		})
		close(firstDone)
	}()
	<-firstEntered

	secondEntered := make(chan struct{})
	secondDone := make(chan struct{})
	go func() {
		WithLegacySession(func(LegacySession) struct{} {
			close(secondEntered)
			return struct{}{}
		})
		close(secondDone)
	}()

	select {
	case <-secondEntered:
		t.Fatal("concurrent legacy sessions overlapped")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseFirst)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first legacy session did not finish")
	}
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("second legacy session did not start after release")
	}
}

func TestWithLegacySessionReleasesAfterPanic(t *testing.T) {
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("legacy session callback did not panic")
			}
		}()
		WithLegacySession(func(LegacySession) struct{} { panic("test panic") })
	}()

	done := make(chan struct{})
	go func() {
		WithLegacySession(func(LegacySession) struct{} {
			close(done)
			return struct{}{}
		})
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("legacy session remained locked after panic")
	}
}

func TestSessionAwareEntryDoesNotReacquireLegacyGate(t *testing.T) {
	done := make(chan struct{})
	go func() {
		WithLegacySession(func(session LegacySession) struct{} {
			if err := RunSimBTWithSession(&config.CmdArgs{}, session); err != nil {
				t.Errorf("session-aware simulation returned error: %v", err)
			}
			close(done)
			return struct{}{}
		})
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("session-aware nested entry reacquired the legacy gate")
	}
}

func TestLegacyToolDirectEntriesShareProcessGate(t *testing.T) {
	tests := []struct {
		name string
		run  func() error
	}{
		{name: "compare-orders", run: func() error { return CompareExgBTOrders([]string{"--invalid-flag"}) }},
		{name: "bt-factors", run: func() error { return BtFactors([]string{"--invalid-flag"}) }},
		{name: "bt-result", run: func() error { return BuildBtResult(&config.CmdArgs{}) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			unlock := legacygate.Lock()
			done := make(chan error, 1)
			go func() { done <- test.run() }()

			select {
			case err := <-done:
				unlock()
				t.Fatalf("legacy tool bypassed process gate: %v", err)
			case <-time.After(50 * time.Millisecond):
			}
			unlock()

			select {
			case err := <-done:
				if err == nil {
					t.Fatal("invalid tool invocation unexpectedly succeeded")
				}
			case <-time.After(time.Second):
				t.Fatal("legacy tool did not run after process gate release")
			}
		})
	}
}

func TestPublicToolCommandFactoriesShareProcessGate(t *testing.T) {
	tests := []struct {
		name string
		new  func() *cobra.Command
		args []string
	}{
		{name: "compare-orders", new: NewCompareExgBTOrdersCommand},
		{name: "bt-factors", new: NewBtFactorsCommand, args: []string{"--config", filepath.Join(t.TempDir(), "missing.yml")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command := test.new()
			if command.Annotations[legacygate.Annotation] != "1" {
				t.Fatal("public tool command is missing its legacy gate marker")
			}
			command.SetArgs(test.args)

			unlock := legacygate.Lock()
			done := make(chan error, 1)
			go func() { done <- command.Execute() }()
			select {
			case err := <-done:
				unlock()
				t.Fatalf("public tool command bypassed process gate: %v", err)
			case <-time.After(50 * time.Millisecond):
			}
			unlock()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("public tool command did not run after process gate release")
			}
		})
	}
}
