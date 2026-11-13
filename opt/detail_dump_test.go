package opt

import (
	"archive/zip"
	"bytes"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/banbox/banexg/errs"
)

func TestDumpDetailReportsWriteFailure(t *testing.T) {
	r := &BTResult{OutDir: filepath.Join(t.TempDir(), "missing", "dir")}
	err := r.dumpDetail("")
	if err == nil || err.Code != errs.CodeIOWriteFail {
		t.Fatalf("want IOWriteFail, got %v", err)
	}
}

func TestDumpDetailNaNInfAreSafeAndAtomic(t *testing.T) {
	dir := t.TempDir()
	r := &BTResult{OutDir: dir}
	r.TotProfitPct = math.NaN()
	r.SharpeRatio = math.Inf(1)
	r.MaxDrawDownPct = 12.5
	if err := r.dumpDetail(""); err != nil {
		t.Fatalf("dumpDetail: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "detail.json.tmp")); !os.IsNotExist(err) {
		t.Fatalf("temp file left behind: %v", err)
	}
	got, perr := ParseBtResult(filepath.Join(dir, "detail.json"))
	if perr != nil {
		t.Fatalf("parse: %v", perr)
	}
	if got.MaxDrawDownPct != 12.5 || got.TotProfitPct != 0 {
		t.Fatalf("unexpected values: dd=%v pct=%v", got.MaxDrawDownPct, got.TotProfitPct)
	}
}

func TestZipBacktestResultStrictKeepsLargeDetail(t *testing.T) {
	dir := t.TempDir()
	big := bytes.Repeat([]byte("a"), 3<<20)
	write := func(name string, data []byte) {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0644); err != nil {
			t.Fatal(err)
		}
	}
	write("detail.json", big)
	write("config.yml", []byte("x: 1\n"))
	write("out.log", []byte("log\n"))
	// the plain variant silently drops a detail.json over maxFileSize (the old shipping behaviour)
	plain, err := ZipBacktestResult(dir, false, false, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if names := zipNames(t, plain); strings.Contains(names, "detail.json") {
		t.Fatalf("plain zip unexpectedly holds detail.json: %s", names)
	}
	// strict: maxFileSize does not apply to detail.json
	data, err := ZipBacktestResultStrict(dir, false, false, 1<<20, 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	if names := zipNames(t, data); !strings.Contains(names, "detail.json") {
		t.Fatalf("strict zip lacks detail.json: %s", names)
	}
	// over its own limit: an error, and the directory is kept even with cleanIfSuccess
	if _, err = ZipBacktestResultStrict(dir, true, false, 1<<20, 1<<20); err == nil {
		t.Fatal("expected an error for an oversized detail.json")
	}
	if _, serr := os.Stat(filepath.Join(dir, "detail.json")); serr != nil {
		t.Fatalf("result dir must survive a failed pack: %v", serr)
	}
	// missing detail.json is an error too
	_ = os.Remove(filepath.Join(dir, "detail.json"))
	if _, err = ZipBacktestResultStrict(dir, true, false, 1<<20, 8<<20); err == nil {
		t.Fatal("expected an error for a missing detail.json")
	}
}

func zipNames(t *testing.T, data []byte) string {
	t.Helper()
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, f := range zr.File {
		rc, _ := f.Open()
		_, _ = io.Copy(io.Discard, rc)
		_ = rc.Close()
		names = append(names, f.Name)
	}
	return strings.Join(names, ",")
}
