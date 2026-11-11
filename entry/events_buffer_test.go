package entry

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/errs"
)

// The events file is buffered but stdout is not: a line written through the
// MultiWriter must be visible on the pipe at once, while the file only holds it
// after Sync (the deferred finish path), with bytes and order unchanged.
func TestBufferedEventFileKeepsStdoutImmediate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	events := newBufferedEventFile(file)
	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer pr.Close()
	writer := io.MultiWriter(events, pw)
	var want bytes.Buffer
	for _, line := range []string{"{\"Kind\":\"decision\",\"n\":1}\n", "{\"Kind\":\"nav\",\"n\":2}\n"} {
		if _, err := writer.Write([]byte(line)); err != nil {
			t.Fatal(err)
		}
		want.WriteString(line)
		got := make([]byte, len(line))
		if _, err := io.ReadFull(pr, got); err != nil || string(got) != line {
			t.Fatalf("stdout side not immediate: %q %v", got, err)
		}
	}
	if raw, _ := os.ReadFile(path); len(raw) != 0 {
		t.Fatalf("file side should still be buffered, has %q", raw)
	}
	if err := events.Sync(); err != nil {
		t.Fatal(err)
	}
	if raw, _ := os.ReadFile(path); !bytes.Equal(raw, want.Bytes()) {
		t.Fatalf("file after Sync = %q want %q", raw, want.Bytes())
	}
	// finish path: Sync + Close persists writes made after the previous flush
	if _, err := writer.Write([]byte("tail\n")); err != nil {
		t.Fatal(err)
	}
	_ = pw.Close()
	if err := events.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := events.Close(); err != nil {
		t.Fatal(err)
	}
	want.WriteString("tail\n")
	if raw, _ := os.ReadFile(path); !bytes.Equal(raw, want.Bytes()) {
		t.Fatalf("file after Close = %q", raw)
	}
}

// Error-return path: finishUnifiedFactorBacktest flushes buffered events before
// closing the file, so a failed run still leaves every event line on disk.
func TestFinishUnifiedFactorBacktestFlushesEventsOnError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	events := newBufferedEventFile(file)
	if _, err := events.Write([]byte("line-1\nline-2\n")); err != nil {
		t.Fatal(err)
	}
	failure := errs.NewMsg(core.ErrRunTime, "boom")
	got := finishUnifiedFactorBacktest(dir, events, func() error { return nil }, nil, failure)
	if got == nil {
		t.Fatal("primary error lost")
	}
	if raw, _ := os.ReadFile(path); string(raw) != "line-1\nline-2\n" {
		t.Fatalf("events after failed finish = %q", raw)
	}
}
