"""Freeze dirty-source provenance and compare prebuilt Go benchmarks in AB/BA pairs.

Uses only Python's standard library. See architecture_performance.md for commands.
"""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import statistics
import subprocess
import time


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def json_argument(value):
    # JSON files avoid PowerShell 5's native argv quote stripping.
    return json.loads(Path(value[1:]).read_text(encoding="utf-8-sig") if value.startswith("@") else value)


def file_hash(path):
    result = hashlib.sha256()
    with open(path, "rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def workspace_hashes(root, excluded=()):
    names = subprocess.check_output(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=root
    ).decode().split("\0")
    excluded = {Path(p).resolve() for p in excluded}
    files = {}
    configs = {}
    for name in sorted(set(names) - {""}):
        path = root / name
        if not path.is_file() or path.resolve() in excluded:
            continue
        if path.suffix in {".yml", ".yaml"}:
            configs[name] = file_hash(path)
        else:
            # Include embedded SQL/HTML, assembly, go.work and benchmark scripts,
            # rather than claiming an exact tree from Go files alone.
            files[name] = file_hash(path)
    return files, configs


def freeze(root, inputs, go, environment, executables=(), benchmark_cwd=".", build_package=None):
    root = Path(root).resolve()
    benchmark_directory = (root / benchmark_cwd).resolve()
    relative_cwd = benchmark_directory.relative_to(root).as_posix()
    if not benchmark_directory.is_dir():
        raise ValueError("benchmark working directory must exist within source checkout")
    files, configs = workspace_hashes(root, executables)
    build = None
    if build_package is not None:
        if len(executables) != 1 or not (build_package == "." or build_package.startswith("./")):
            raise ValueError("managed build requires one --executable and one local --build-package")
        executable = Path(executables[0]).resolve()
        executable.parent.mkdir(parents=True, exist_ok=True)
        command = [go, "test", "-c", build_package, "-o", str(executable)]
        subprocess.check_call(command, cwd=root)
        after_files, after_configs = workspace_hashes(root, executables)
        if (files, configs) != (after_files, after_configs):
            raise ValueError("source or config changed during benchmark build; preserve the checkout and rebuild")
        build = {"command": command, "source_digest_before": digest(files), "source_digest_after": digest(after_files),
                 "config_digest_before": digest(configs), "config_digest_after": digest(after_configs),
                 "executable_sha256": file_hash(executable)}
    fixture = {str(Path(p).resolve()): file_hash(p) for p in inputs}
    # Compare content hashes, not checkout-specific absolute fixture paths.
    return {
        "root": str(root), "captured_at": time.time(),
        "head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
        "dirty": subprocess.check_output(["git", "status", "--porcelain=v1"], cwd=root, text=True),
        "source_files": files, "source_digest": digest(files),
        "config_files": configs, "config_digest": digest(configs),
        "inputs": fixture, "input_digest": digest(sorted(fixture.values())),
        "go_version": subprocess.check_output([go, "version"], cwd=root, text=True).strip(),
        "go_env": json.loads(subprocess.check_output([go, "env", "-json", "GOOS", "GOARCH", "CGO_ENABLED", "GOVERSION"], cwd=root)),
        "environment": environment,
        "benchmark_cwd": relative_cwd,
        "executable_sha256": {str(Path(p).resolve()): file_hash(p) for p in executables},
        "build": build,
        "host": {"node": platform.node(), "platform": platform.platform(), "cpus": os.cpu_count()},
        "benchmark_env": {k: v for k, v in os.environ.items() if k.startswith("BANBOT_") or k in {"GOMAXPROCS", "GOGC", "GOMEMLIMIT"}},
    }


def verify_runtime_files(manifest):
    for category, paths in (("input", manifest["inputs"]), ("config", manifest["config_files"])):
        for name, expected in paths.items():
            path = Path(name) if category == "input" else Path(manifest["root"]) / name
            if not path.is_file() or file_hash(path) != expected:
                raise ValueError(f"actual benchmark {category} changed after freeze: {path}")


def verify_build(manifest, executable, executable_hash):
    build = manifest.get("build")
    if not isinstance(build, dict):
        raise ValueError("prebuilt freeze has no managed build provenance; use --smoke or freeze --build-package")
    command = build.get("command", [])
    if (len(command) != 6 or command[1:3] != ["test", "-c"] or command[-2] != "-o"
            or str(Path(command[-1]).resolve()) != str(Path(executable).resolve())
            or build.get("executable_sha256") != executable_hash):
        raise ValueError("benchmark executable differs from managed build")
    for key in ("source_digest", "config_digest"):
        if build.get(key + "_before") != manifest[key] or build.get(key + "_after") != manifest[key]:
            raise ValueError(f"benchmark {key} changed during build or differs from frozen source")
    return command[3]


def confidence_interval(baseline, candidate):
    if len(baseline) != len(candidate) or len(baseline) < 10:
        raise ValueError("at least ten matched pairs are required")
    logs = [math.log(new / old) for old, new in zip(baseline, candidate)]
    mean = statistics.mean(logs)
    # t(9, .975)=2.262157. For n>=10 this conservative critical value
    # gives at least 95% coverage under the paired normal-log assumption.
    half = 2.262157 * statistics.stdev(logs) / math.sqrt(len(logs))
    return {"ratio": math.exp(mean), "low": math.exp(mean - half), "high": math.exp(mean + half), "pairs": len(logs)}


def parse_benchmarks(output):
    result = {}
    for line in output.splitlines():
        if not line.startswith("Benchmark"):
            continue
        parts = line.split()
        if len(parts) < 4 or not parts[1].isdigit():
            continue
        metrics = {}
        for i in range(2, len(parts) - 1, 2):
            try:
                metrics[parts[i + 1]] = float(parts[i])
            except ValueError:
                break
        if "ns/op" not in metrics:
            continue
        name = re.sub(r"-\d+$", "", parts[0])
        if name in result:
            raise ValueError("each command must emit each benchmark once; use -test.count=1")
        result[name] = metrics
    if not result:
        raise ValueError("command produced no Go benchmark ns/op measurements")
    return result


def sample_rss(process):
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        class Counters(ctypes.Structure):
            _fields_ = [("cb", wintypes.DWORD), ("faults", wintypes.DWORD)] + [
                (name, ctypes.c_size_t) for name in
                ("peak_ws", "ws", "peak_paged", "paged", "peak_nonpaged", "nonpaged", "pagefile", "peak_pagefile")
            ]

        counters = Counters()
        counters.cb = ctypes.sizeof(counters)
        getter = ctypes.windll.psapi.GetProcessMemoryInfo
        getter.argtypes = [wintypes.HANDLE, ctypes.c_void_p, wintypes.DWORD]
        return int(counters.peak_ws) if getter(wintypes.HANDLE(int(process._handle)), ctypes.byref(counters), counters.cb) else 0
    try:
        for line in Path(f"/proc/{process.pid}/status").read_text().splitlines():
            if line.startswith("VmHWM:"):
                return int(line.split()[1]) * 1024
    except (OSError, ValueError):
        pass
    return 0


def run(command, cwd, log, timeout):
    started = time.monotonic()
    last_progress = started
    peak = 0
    with log.open("wb") as output:
        process = subprocess.Popen(command, cwd=cwd, stdout=output, stderr=subprocess.STDOUT)
        while process.poll() is None:
            peak = max(peak, sample_rss(process))
            if time.monotonic() - started > timeout:
                process.kill()
                process.wait()
                raise TimeoutError(f"benchmark exceeded {timeout}s; log {log}")
            if time.monotonic() - last_progress >= 30:
                print(f"{log.name}: running {time.monotonic() - started:.0f}s; process peak RSS {peak / 1024 / 1024:.1f} MiB", flush=True)
                last_progress = time.monotonic()
            time.sleep(.01)
    if process.returncode:
        raise RuntimeError(f"benchmark exited {process.returncode}; log {log}")
    return {"wall_seconds": time.monotonic() - started, "command_process_peak_rss_bytes": peak,
            "benchmarks": parse_benchmarks(log.read_text(encoding="utf-8", errors="replace"))}


def compare(args):
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    # A rejected rerun must not leave an older successful verdict at this path.
    (output / "comparison.json").write_text(json.dumps({"acceptance_eligible": False, "passed": False, "status": "incomplete"}), encoding="utf-8")
    manifests = {name: json.loads(Path(getattr(args, name + "_manifest")).read_text(encoding="utf-8")) for name in ("baseline", "candidate")}
    commands = {name: json_argument(getattr(args, name + "_command")) for name in manifests}
    for name, command in commands.items():
        if not isinstance(command, list) or not command or not all(isinstance(p, str) for p in command):
            raise ValueError(f"{name} command must be a JSON string array")
        executable = Path(command[0])
        if not executable.is_absolute() or not executable.is_file():
            raise ValueError("use an absolute prebuilt benchmark executable path")
    if args.rounds < 10:
        raise ValueError("at least ten AB/BA rounds are required")
    baseline, candidate = manifests.values()
    if commands["baseline"][1:] != commands["candidate"][1:]:
        raise ValueError("baseline/candidate benchmark argv must match except executable path")
    for key in ("input_digest", "config_digest", "go_version", "go_env", "environment", "host", "benchmark_env", "benchmark_cwd"):
        if baseline[key] != candidate[key]:
            raise ValueError(f"provenance mismatch: {key}")
    current_env = {k: v for k, v in os.environ.items() if k.startswith("BANBOT_") or k in {"GOMAXPROCS", "GOGC", "GOMEMLIMIT"}}
    if current_env != candidate["benchmark_env"]:
        raise ValueError("current benchmark environment differs from frozen manifests")
    distinct = baseline["source_digest"] != candidate["source_digest"]
    if not distinct and not args.smoke:
        raise ValueError("identical source digests cannot prove old/new acceptance; use --smoke only to verify tooling")
    executable_hashes = {name: file_hash(command[0]) for name, command in commands.items()}
    for name, command in commands.items():
        expected = manifests[name].get("executable_sha256", {}).get(str(Path(command[0]).resolve()))
        if (expected is None and not args.smoke) or (expected is not None and expected != executable_hashes[name]):
            raise ValueError(f"{name} executable is not bound to frozen source manifest; freeze with --executable")
    distinct_binary = executable_hashes["baseline"] != executable_hashes["candidate"]
    if not distinct_binary and not args.smoke:
        raise ValueError("identical executables cannot prove old/new acceptance; use --smoke only to verify tooling")
    if not args.smoke:
        packages = [verify_build(manifests[name], command[0], executable_hashes[name]) for name, command in commands.items()]
        if len(set(packages)) != 1:
            raise ValueError("managed benchmark build packages differ")
    for manifest in manifests.values():
        verify_runtime_files(manifest)
    samples = []
    for round_no in range(args.rounds):
        pair = {}
        order = ("baseline", "candidate") if round_no % 2 == 0 else ("candidate", "baseline")
        for name in order:
            verify_runtime_files(manifests[name])
            if file_hash(commands[name][0]) != executable_hashes[name]:
                raise ValueError("benchmark executable changed during comparison")
            log = output / f"{round_no + 1:02d}-{name}.log"
            pair[name] = run(commands[name], str(Path(manifests[name]["root"]) / manifests[name]["benchmark_cwd"]), log, args.timeout)
            verify_runtime_files(manifests[name])
            if file_hash(commands[name][0]) != executable_hashes[name]:
                raise ValueError("benchmark executable changed during comparison")
            print(f"round {round_no + 1}/{args.rounds} {name}: {pair[name]['wall_seconds']:.3f}s", flush=True)
        if pair["baseline"]["benchmarks"].keys() != pair["candidate"]["benchmarks"].keys():
            raise ValueError("baseline/candidate benchmark sets differ")
        samples.append(pair)
        (output / "samples.json").write_text(json.dumps(samples, indent=2), encoding="utf-8")
    names = samples[0]["baseline"]["benchmarks"].keys()
    for manifest in manifests.values():
        verify_runtime_files(manifest)
    if any(sample["baseline"]["benchmarks"].keys() != names for sample in samples):
        raise ValueError("benchmark set changed between rounds")
    metrics = {}
    for name in names:
        ci = confidence_interval(
            [s["baseline"]["benchmarks"][name]["ns/op"] for s in samples],
            [s["candidate"]["benchmarks"][name]["ns/op"] for s in samples],
        )
        ci["verdict"] = "pass" if ci["high"] <= 1.05 else "fail" if ci["low"] > 1.05 else "inconclusive"
        metrics[name] = ci
    report = {"acceptance_eligible": distinct and distinct_binary and not args.smoke,
              "passed": distinct and distinct_binary and not args.smoke and all(m["verdict"] == "pass" for m in metrics.values()),
              "method": "paired log ratio, conservative Student t 95% CI; alternating AB/BA",
              "scope": "prebuilt executable benchmark ns/op; process RSS excludes subprocesses",
              "executable_sha256": executable_hashes, "manifests": manifests, "metrics": metrics}
    (output / "comparison.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in report.items() if k != "manifests"}, indent=2))
    return 0 if args.smoke or report["passed"] else 2


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    snapshot = sub.add_parser("freeze")
    snapshot.add_argument("--root", default=".")
    snapshot.add_argument("--go", default="go")
    snapshot.add_argument("--input", action="append", default=[])
    snapshot.add_argument("--executable", action="append", default=[], help="record binary digest; build provenance requires --build-package")
    snapshot.add_argument("--build-package", help="compile this local test package between source snapshots; required for acceptance provenance")
    snapshot.add_argument("--benchmark-cwd", default=".", help="package working directory relative to checkout root, e.g. factor/runner")
    snapshot.add_argument("--environment", default="{}", help="JSON: DB versions, cache mode, paging/warmup budget, fixture generator version")
    snapshot.add_argument("--output", required=True)
    comparison = sub.add_parser("compare")
    for name in ("baseline", "candidate"):
        comparison.add_argument(f"--{name}-manifest", required=True)
        comparison.add_argument(f"--{name}-command", required=True, help="JSON argv; prefer a prebuilt Go test binary")
    comparison.add_argument("--rounds", type=int, default=10)
    comparison.add_argument("--timeout", type=float, default=900)
    comparison.add_argument("--output", required=True)
    comparison.add_argument("--smoke", action="store_true", help="tool check only; never records acceptance")
    args = parser.parse_args()
    if args.action == "freeze":
        value = freeze(args.root, args.input, args.go, json_argument(args.environment), args.executable, args.benchmark_cwd, args.build_package)
        Path(args.output).write_text(json.dumps(value, indent=2), encoding="utf-8")
        print(value["source_digest"])
        return 0
    return compare(args)


if __name__ == "__main__":
    raise SystemExit(main())
