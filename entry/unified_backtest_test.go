package entry

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg/errs"
)

func TestUnifiedBacktestCompletionIncludesJoinedCleanupFailures(t *testing.T) {
	for _, kind := range []string{"complete", "cleanup", "output", "primary-and-cleanup", "unresolved"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			file, err := os.Create(filepath.Join(dir, "events.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			var primary *errs.Error
			results := []runner.Result{{}}
			if kind == "primary-and-cleanup" {
				primary = errs.NewMsg(core.ErrRunTime, "primary replay failed")
			}
			if kind == "output" {
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "unresolved" {
				results[0].Unresolved = 1
			}
			calls := 0
			got := finishUnifiedFactorBacktest(dir, file, func() error {
				calls++
				if _, err := file.Stat(); err == nil {
					t.Error("storage cleanup ran before output closed")
				}
				if _, err := os.Stat(filepath.Join(dir, "run.json")); !os.IsNotExist(err) {
					t.Error("completion artifact published before storage cleanup")
				}
				if kind == "cleanup" || kind == "primary-and-cleanup" {
					return errors.New("storage join failed")
				}
				return nil
			}, results, primary)
			if calls != 1 {
				t.Fatalf("cleanup calls=%d", calls)
			}
			var artifact struct {
				Status string
				Errors []string
			}
			body, err := os.ReadFile(filepath.Join(dir, "run.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(body, &artifact); err != nil {
				t.Fatal(err)
			}
			want := "incomplete"
			if kind == "complete" {
				want = "complete"
			}
			if artifact.Status != want {
				t.Fatalf("status=%s want=%s", artifact.Status, want)
			}
			failed := kind == "cleanup" || kind == "output" || kind == "primary-and-cleanup"
			if (got != nil) != failed {
				t.Fatalf("result=%v failed=%v", got, failed)
			}
			if kind == "primary-and-cleanup" {
				for _, reason := range []string{"primary replay failed", "storage join failed"} {
					if !strings.Contains(got.Error(), reason) || !strings.Contains(strings.Join(artifact.Errors, ";"), reason) {
						t.Fatalf("lost failure %q: error=%v artifact=%s", reason, got, body)
					}
				}
			}
		})
	}
}

func TestUnifiedBacktestArtifactFailurePreservesPrimaryAndCleanup(t *testing.T) {
	dir := t.TempDir()
	file, err := os.Create(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "run.json"), 0700); err != nil {
		t.Fatal(err)
	}
	got := finishUnifiedFactorBacktest(dir, file, func() error {
		return errors.New("storage join failed")
	}, nil, errs.NewMsg(core.ErrRunTime, "primary replay failed"))
	if got == nil {
		t.Fatal("artifact failure swallowed")
	}
	for _, reason := range []string{"primary replay failed", "storage join failed", "run.json"} {
		if !strings.Contains(got.Error(), reason) {
			t.Fatalf("lost error %q: %v", reason, got)
		}
	}
}

// run.json carries skipped_pairs only when the pair refresh left a member out; otherwise the file is byte-identical to before.
func TestRunJSONSkippedPairsOnlyWhenSkipped(t *testing.T) {
	read := func(skipped []orm.SkippedPair) string {
		dir := t.TempDir()
		file, ferr := os.Create(filepath.Join(dir, "events.jsonl"))
		if ferr != nil {
			t.Fatal(ferr)
		}
		if got := finishUnifiedFactorBacktestSkipped(dir, file, func() error { return nil }, nil, nil, skipped); got != nil {
			t.Fatal(got)
		}
		b, err := os.ReadFile(filepath.Join(dir, "run.json"))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	if plain := read(nil); strings.Contains(plain, "skipped_pairs") || plain != "{\"Version\":1,\"Status\":\"complete\",\"Results\":null}\n" {
		t.Fatalf("no skip must not change the file: %q", plain)
	}
	if got := read([]orm.SkippedPair{{Symbol: "MATIC/USDT:USDT", Reason: "data_ends_before_window"}}); !strings.Contains(got, `"skipped_pairs":[{"symbol":"MATIC/USDT:USDT","reason":"data_ends_before_window"}]`) {
		t.Fatalf("%q", got)
	}
}

// BtFeatureLevel 11: pairs a statarb run skipped join skipped_pairs (leg_unavailable + pair) and all_pairs_unavailable marks a run
// without any usable pair; a run without such pairs writes neither key.
func TestRunJSONPairsUnavailable(t *testing.T) {
	read := func(results []runner.Result, skipped []orm.SkippedPair) string {
		dir := t.TempDir()
		file, ferr := os.Create(filepath.Join(dir, "events.jsonl"))
		if ferr != nil {
			t.Fatal(ferr)
		}
		if got := finishUnifiedFactorBacktestSkipped(dir, file, func() error { return nil }, results, nil, skipped); got != nil {
			t.Fatal(got)
		}
		b, err := os.ReadFile(filepath.Join(dir, "run.json"))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	pool := []orm.SkippedPair{{Symbol: "MATIC/USDT:USDT", Reason: "data_ends_before_window"}}
	ok := runner.Result{Pairs: []runner.PairResult{{Y: "AVAX/USDT:USDT", X: "GALA/USDT:USDT"}}}
	if got := read([]runner.Result{ok}, pool); strings.Contains(got, "leg_unavailable") || strings.Contains(got, "all_pairs_unavailable") {
		t.Fatalf("a run without skipped pairs must not change: %q", got)
	}
	part := runner.Result{Pairs: []runner.PairResult{{Y: "AVAX/USDT:USDT", X: "GALA/USDT:USDT"}, {Y: "DOGE/USDT:USDT", X: "MATIC/USDT:USDT", Unavailable: "leg_unavailable", UnavailableLeg: "MATIC/USDT:USDT"}}}
	got := read([]runner.Result{part}, pool)
	if !strings.Contains(got, `{"symbol":"MATIC/USDT:USDT","reason":"leg_unavailable","pair":"DOGE/USDT:USDT/MATIC/USDT:USDT"}`) || strings.Contains(got, "all_pairs_unavailable") {
		t.Fatalf("%q", got)
	}
	if !strings.Contains(got, `{"symbol":"MATIC/USDT:USDT","reason":"data_ends_before_window"}`) {
		t.Fatalf("pool-level entry lost: %q", got)
	}
	all := part
	all.AllPairsUnavailable = true
	if got = read([]runner.Result{all}, pool); !strings.Contains(got, `"all_pairs_unavailable":true`) {
		t.Fatalf("%q", got)
	}
	if len(pool) != 1 {
		t.Fatal("the caller's list must not be modified")
	}
}
