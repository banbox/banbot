package entry

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/rpc"
	"github.com/banbox/banexg/log"
)

func TestRuntimeLoggerDoesNotReplaceProcessLogger(t *testing.T) {
	before := log.L()
	notifications := rpc.NewSession(nil, nil)
	t.Cleanup(notifications.Close)
	session := &explicitEntrySession{logArgs: config.CmdArgs{LogLevel: "warn"}}
	t.Cleanup(session.close)
	session.configureRuntimeLogger(notifications)
	if log.L() != before {
		t.Fatal("runtime logger replaced the process logger")
	}
}

func TestRuntimeLogFilesAndLevelsRemainIndependent(t *testing.T) {
	dir := t.TempDir()
	first := &explicitEntrySession{logArgs: config.CmdArgs{LogLevel: "debug", Logfile: filepath.Join(dir, "a.log")}}
	second := &explicitEntrySession{logArgs: config.CmdArgs{LogLevel: "warn", Logfile: filepath.Join(dir, "b.log")}}
	t.Cleanup(first.close)
	t.Cleanup(second.close)
	a, err := first.configureRuntimeLogger(nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := second.configureRuntimeLogger(nil)
	if err != nil {
		t.Fatal(err)
	}
	a.Debug("a-before-close")
	b.Info("filtered")
	b.Warn("b-only")
	second.close()
	a.Debug("a-after-close")
	first.close()
	contentA, readErr := os.ReadFile(first.logArgs.Logfile)
	if readErr != nil {
		t.Fatal(readErr)
	}
	contentB, readErr := os.ReadFile(second.logArgs.Logfile)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !strings.Contains(string(contentA), "a-before-close") || !strings.Contains(string(contentA), "a-after-close") || strings.Contains(string(contentA), "b-only") {
		t.Fatalf("first logger mixed output: %s", contentA)
	}
	if !strings.Contains(string(contentB), "b-only") || strings.Contains(string(contentB), "filtered") || strings.Contains(string(contentB), "a-after-close") {
		t.Fatalf("second logger mixed output or level: %s", contentB)
	}
}
