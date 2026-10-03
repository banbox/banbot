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
