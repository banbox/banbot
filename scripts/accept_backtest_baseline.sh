#!/usr/bin/env bash
# Compare a frozen historical Banbot source tree with the frozen candidate.
set -euo pipefail

run_root=${BANBOT_G001_ROOT:-/dev/shm/banbot-g001-20260913}
rounds=${BANBOT_G001_ROUNDS:-5}
cache_dir=${GOCACHE:-/dev/shm/banbot-refactor-cache}
tmp_dir=${GOTMPDIR:-/dev/shm/banbot-refactor-tmp}
compat_patch=${BANBOT_G001_CORE_COMPAT_PATCH:-$run_root/backtest_baseline_core_compat.generated.patch}
candidate_compat_patch=${BANBOT_G001_CANDIDATE_CORE_COMPAT_PATCH:-$compat_patch}
strategy_patch=${BANBOT_G001_STRATEGY_COMPAT_PATCH:-$run_root/backtest_baseline_strategy_compat.generated.patch}
normalizer="$(dirname "$0")/normalize_backtest_orders.py"
coverage_config=${BANBOT_G001_COVERAGE_CONFIG:-$run_root/coverage.yml}
replay_config=${BANBOT_G001_REPLAY_CONFIG:-}

for required in "$run_root/baseline" "$run_root/candidate" "$run_root/strategy" "$run_root/input"; do
	[[ -d $required ]] || { echo "missing frozen input: $required" >&2; exit 2; }
done
[[ -f $compat_patch ]] || { echo "missing compatibility patch: $compat_patch" >&2; exit 2; }
[[ $candidate_compat_patch == native || -f $candidate_compat_patch ]] || {
	echo "missing candidate compatibility patch: $candidate_compat_patch" >&2
	exit 2
}
[[ -f $strategy_patch ]] || { echo "missing strategy compatibility patch: $strategy_patch" >&2; exit 2; }
[[ -f $normalizer ]] || { echo "missing order normalizer: $normalizer" >&2; exit 2; }
[[ -f $coverage_config ]] || { echo "missing audited coverage config: $coverage_config" >&2; exit 2; }
[[ -n $replay_config && -f $replay_config ]] || {
	echo "BANBOT_G001_REPLAY_CONFIG must name an isolated or read-only database overlay" >&2
	exit 2
}
[[ $rounds =~ ^[0-9]+$ ]] && ((rounds >= 5)) || {
	echo "BANBOT_G001_ROUNDS must be an integer >= 5" >&2
	exit 2
}

result_dir=$(mktemp -d "$run_root/results.XXXXXX")
mkdir -p "$result_dir/bin" "$result_dir/raw" "$result_dir/normalized"
chmod 700 "$result_dir" "$result_dir/raw"
cat >"$result_dir/offline.yml" <<'EOF'
bt_strict: true
bt_no_kline_download: true
database:
  auto_create: false
pairs: ['BTC/USDT:USDT', 'ETH/USDT:USDT', 'BNB/USDT:USDT', 'SOL/USDT:USDT', 'XRP/USDT:USDT', 'ADA/USDT:USDT', 'DOGE/USDT:USDT', 'LINK/USDT:USDT', 'AVAX/USDT:USDT', 'DOT/USDT:USDT', 'NEAR/USDT:USDT', 'UNI/USDT:USDT', 'AAVE/USDT:USDT', 'SUI/USDT:USDT', 'TRX/USDT:USDT', 'ARB/USDT:USDT', '1000PEPE/USDT:USDT', 'FIL/USDT:USDT', 'LTC/USDT:USDT']
pairlists: []
pairmgr:
  force_filters: false
EOF

apply_compat_patch() {
	local patch_file=$1 target_dir=$2 label=$3
	local before after
	before=$(tree_digest "$target_dir")
	if patch -d "$target_dir" -p1 --batch -f --reverse --dry-run <"$patch_file" >/dev/null 2>&1; then
		patch -d "$target_dir" -p1 --batch -f --dry-run <"$patch_file" >/dev/null 2>&1 && {
			echo "$label compatibility patch is ambiguously both applied and unapplied: $target_dir" >&2
			return 2
		}
		:
	elif patch -d "$target_dir" -p1 --batch -f --dry-run <"$patch_file" >/dev/null 2>&1; then
		patch -d "$target_dir" -p1 --batch -f <"$patch_file"
		after=$(tree_digest "$target_dir")
		[[ $before != "$after" ]] || {
			echo "$label compatibility patch reported success without changing content: $target_dir" >&2
			return 2
		}
		patch -d "$target_dir" -p1 --batch -f --reverse --dry-run <"$patch_file" >/dev/null 2>&1 || {
			echo "$label compatibility patch did not produce the recorded content: $target_dir" >&2
			return 2
		}
	else
		echo "$label compatibility patch does not match frozen source: $target_dir" >&2
		return 2
	fi
}

tree_digest() {
	local target_dir=$1
	find "$target_dir" -type f -not -path '*/.git/*' -print0 | sort -z | xargs -0 sha256sum | sha256sum | awk '{print $1}'
}

build() {
	local label=$1 source_dir=$2 strategy_dir="$result_dir/strategy-$1"
	if [[ $label != candidate || $candidate_compat_patch != native ]]; then
		local source_patch=$compat_patch
		[[ $label == candidate ]] && source_patch=$candidate_compat_patch
		apply_compat_patch "$source_patch" "$source_dir" core
	fi
	cp -a "$run_root/strategy/." "$strategy_dir"
	apply_compat_patch "$strategy_patch" "$strategy_dir" strategy
	(
		cd "$strategy_dir"
		GOWORK=off go mod edit -replace "github.com/banbox/banbot=$source_dir"
		GOCACHE="$cache_dir" GOTMPDIR="$tmp_dir" GOWORK=off go mod tidy
		GOCACHE="$cache_dir" GOTMPDIR="$tmp_dir" GOWORK=off go build -o "$result_dir/bin/$label" .
	)
}

run_once() {
	local label=$1
	local iteration=$2
	local output="$result_dir/$label-$iteration"
	mkdir -p "$output"
	# The copied configs retain the existing database read endpoint. Do not pass
	# commands that download, write market data, or use a production account.
	BanStratDir="$result_dir/strategy-$label" \
		/usr/bin/time -f 'ELAPSED=%e USER=%U SYS=%S MAXRSS=%M' \
		"$result_dir/bin/$label" backtest \
		-datadir "$run_root/input" -config @adv.yml -config "$result_dir/offline.yml" \
		-config "$coverage_config" -config "$replay_config" \
		-timestart 20260101 -timeend 20260823 -bt-strict -net-off -out "$output" \
		>"$result_dir/raw/$label-$iteration.log" 2>&1
	python3 "$normalizer" "$output/orders.csv" >"$result_dir/normalized/$label-$iteration.orders.csv"
	jq -S -e '{barNum,orderNum,finBalance,totProfit,totProfitPct,winRatePct,maxDrawDownPct,maxDrawDownVal,totFee,totCost,startMS,endMS}
		| if ([.[] | select(. == null)] | length) == 0 then . else error("missing required backtest metric") end
		| if (.barNum | type == "number" and . > 0) then . else error("backtest processed no bars") end
		| if (.orderNum | type == "number" and . > 0) then . else error("backtest produced no orders") end' \
		"$output/detail.json" >"$result_dir/normalized/$label-$iteration.metrics.json"
}

timing_summary() {
	local label=$1 field=$2 values
	mapfile -t values < <(awk -v field="$field" '
		$1 == "ELAPSED=" || $1 ~ /^ELAPSED=/ {
			for (i = 1; i <= NF; i++) {
				if ($i ~ ("^" field "=")) {
					sub("^" field "=", "", $i)
					print $i
				}
			}
		}' "$result_dir"/raw/$label-[0-9]*.log | sort -n)
	((${#values[@]} == rounds)) || { echo "missing $field samples for $label" >&2; return 1; }
	printf '%s\n' "${values[@]}" | python3 -c '
import statistics
import sys
values = [float(line) for line in sys.stdin if line.strip()]
median = statistics.median(values)
mad = statistics.median(abs(value - median) for value in values)
print(f"{sys.argv[1]} median={median:g} mad={mad:g}")
' "$field"
}

build baseline "$run_root/baseline"
build candidate "$run_root/candidate"
run_once baseline cold
run_once candidate cold
for iteration in $(seq 1 "$rounds"); do
	for label in baseline candidate; do
		run_once "$label" "$iteration"
	done
done

reference=baseline-cold
for label in baseline candidate; do
	for iteration in cold $(seq 1 "$rounds"); do
		diff -u "$result_dir/normalized/$reference.orders.csv" "$result_dir/normalized/$label-$iteration.orders.csv"
		diff -u "$result_dir/normalized/$reference.metrics.json" "$result_dir/normalized/$label-$iteration.metrics.json"
	done
done

{
	printf 'result_dir=%s\n' "$result_dir"
	sha256sum "$compat_patch" "$strategy_patch"
	if [[ $candidate_compat_patch == native ]]; then
		printf 'candidate_core_compat=native tree_digest=%s\n' "$(tree_digest "$run_root/candidate")"
	elif [[ $candidate_compat_patch != "$compat_patch" ]]; then
		sha256sum "$candidate_compat_patch"
	fi
	sha256sum "$result_dir"/normalized/*
	for label in baseline candidate; do
		printf '\n[%s cold]\n' "$label"
		cat "$result_dir/normalized/$label-cold.metrics.json"
		grep '^ELAPSED=' "$result_dir/raw/$label-cold.log"
		printf '\n[%s warm]\n' "$label"
		timing_summary "$label" ELAPSED
		timing_summary "$label" USER
		timing_summary "$label" SYS
		timing_summary "$label" MAXRSS
	done
} >"$result_dir/summary.txt"

printf '%s\n' "$result_dir"
