# UpdatePairs Add Bypasses policy.Pairs Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Allow explicit `UpdatePairs.Add` pairs to be accepted even when not listed in `policy.Pairs`, while still respecting pairlist filters (unless `ForceAdd` is true).

**Architecture:** Adjust `UpdatePairs` allowed-set calculation so it is based on filters over a candidate list that includes both current `core.Pairs` and requested `Add` pairs, without applying `policy.Pairs` as a hard constraint for explicit Adds. Keep `ForceAdd` semantics and other paths unchanged.

**Tech Stack:** Go, existing strat tests in `strat/pair_update_test.go`.

### Task 1: Add failing test for explicit Add bypassing policy.Pairs

**Files:**
- Modify: `strat/pair_update_test.go`
- Test: `strat/pair_update_test.go`

**Step 1: Write the failing test**

```go
func TestUpdatePairs_AddIgnoresPolicyPairs(t *testing.T) {
	resetStratGlobals()
	setTestHooks()
	config.RunTimeframes = []string{"1s"}
	core.Pairs = []string{"ETH/USDT"}
	stg := &TradeStrat{
		Name:       "stg",
		WarmupNum:  50,
		MinTfScore: 0.1,
		Policy:     &config.RunPolicyConfig{RunTimeframes: []string{"1s"}, Pairs: []string{"ETH/USDT"}},
	}
	res, err := stg.UpdatePairs(PairUpdateReq{Add: []string{"BTC/USDT"}})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(res.Added) != 1 || res.Added[0] != "BTC/USDT" {
		t.Fatalf("expected BTC/USDT added, got %v", res.Added)
	}
	envKey := "BTC/USDT_1s"
	jobs := AccJobs[config.DefAcc][envKey]
	if jobs == nil || jobs[stg.Name] == nil {
		t.Fatalf("expected job created for %s", envKey)
	}
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./strat -run TestUpdatePairs_AddIgnoresPolicyPairs -v`
Expected: FAIL because `UpdatePairs` skips BTC due to policy.Pairs restriction.

### Task 2: Implement minimal change in UpdatePairs allowed-set logic

**Files:**
- Modify: `strat/pair_update.go`

**Step 1: Write minimal implementation**

Update the `allowedSet` calculation to:
- Build candidates from `core.Pairs` plus parsed `adds`.
- Apply filters to candidates without enforcing `policy.Pairs`.
- Keep `ForceAdd` logic unchanged.

**Step 2: Run test to verify it passes**

Run: `go test ./strat -run TestUpdatePairs_AddIgnoresPolicyPairs -v`
Expected: PASS

### Task 3: Regression check

**Files:**
- Test: `strat/pair_update_test.go`

**Step 1: Run strat tests**

Run: `go test ./strat -v`
Expected: PASS

### Task 4: Commit (optional)

```bash
git add strat/pair_update.go strat/pair_update_test.go docs/plans/2026-01-17-updatepairs-add-bypass-policy-pairs.md
git commit -m "fix: allow UpdatePairs Add to bypass policy pairs"
```
