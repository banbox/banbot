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

func TestRuntimeDefaultLogPaths(t *testing.T) {
	snapshot := config.NewSnapshotWithDirs(&config.Config{Name: "test-bot"}, t.TempDir(), "")
	trade := runtimeLogArgs(config.CmdArgs{}, snapshot, "trade")
	spider := runtimeLogArgs(config.CmdArgs{}, snapshot, "spider")
	if trade.Logfile == spider.Logfile || filepath.Dir(trade.Logfile) != filepath.Join(snapshot.DataDir, "logs") {
		t.Fatalf("default log paths are not isolated: %q, %q", trade.Logfile, spider.Logfile)
	}
	if !strings.Contains(filepath.Base(trade.Logfile), "test-bot-trade-") || !strings.Contains(filepath.Base(spider.Logfile), "test-bot-spider-") {
		t.Fatal("default paths do not identify bot and command")
	}
	explicit := runtimeLogArgs(config.CmdArgs{Logfile: "$/custom.log"}, snapshot, "trade")
	if explicit.Logfile != filepath.Join(snapshot.DataDir, "custom.log") {
		t.Fatalf("explicit path lost: %q", explicit.Logfile)
	}
	if runtimeLogArgs(config.CmdArgs{}, snapshot).Logfile != "" {
		t.Fatal("non-live command unexpectedly enabled file logging")
	}
	before, beforePath := log.L(), log.LogFilePath()
	logger, closeLogger, err := openEntryLogger(trade)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closeLogger)
	logger.Info("default-live-log")
	content, readErr := os.ReadFile(trade.Logfile)
	if readErr != nil || !strings.Contains(string(content), "default-live-log") {
		t.Fatalf("default file logging failed: %s, %v", content, readErr)
	}
	if log.L() != before || log.LogFilePath() != beforePath {
		t.Fatal("runtime log changed process globals")
	}
}

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
