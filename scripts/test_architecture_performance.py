import hashlib
import json
import math
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from architecture_performance import compare, confidence_interval, freeze, parse_benchmarks


class ComparisonTest(unittest.TestCase):
    def comparison_fixture(self, root):
        manifests = {}
        commands = {}
        fixture = root / "fixture.gob"
        fixture.write_bytes(b"immutable input")
        config = root / "config.yaml"
        config.write_bytes(b"config_version: 2")
        for name in ("baseline", "candidate"):
            executable = root / (name + ".exe")
            executable.write_bytes(name.encode())
            commands[name] = json.dumps([str(executable)])
            manifests[name] = {
                "root": str(root), "source_digest": name,
                "input_digest": "fixture", "config_digest": "config",
                "go_version": "test", "go_env": {}, "environment": {}, "host": {},
                "benchmark_env": {k: v for k, v in os.environ.items() if k.startswith("BANBOT_") or k in {"GOMAXPROCS", "GOGC", "GOMEMLIMIT"}},
                "benchmark_cwd": ".", "executable_sha256": {str(executable): hashlib.sha256(name.encode()).hexdigest()},
                "inputs": {str(fixture): hashlib.sha256(fixture.read_bytes()).hexdigest()},
                "config_files": {config.name: hashlib.sha256(config.read_bytes()).hexdigest()},
                "build": {"command": ["go", "test", "-c", "./factor", "-o", str(executable)],
                          "source_digest_before": name, "source_digest_after": name,
                          "config_digest_before": "config", "config_digest_after": "config",
                          "executable_sha256": hashlib.sha256(name.encode()).hexdigest()},
            }
        args = SimpleNamespace(output=str(root / "output"), rounds=10, timeout=1, smoke=False)
        for name in manifests:
            setattr(args, name + "_manifest", str(root / (name + ".json")))
            setattr(args, name + "_command", commands[name])
        return args, manifests

    def test_runtime_inputs_argv_and_build_origin_fail_closed(self):
        for kind in ("argv", "input", "config", "missing-build", "changed-build-source", "wrong-build-binary"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                args, manifests = self.comparison_fixture(root)
                self.save_manifests(args, manifests)
                if kind == "argv":
                    args.candidate_command = json.dumps(json.loads(args.candidate_command) + ["-test.benchtime=1x"])
                elif kind == "input":
                    (root / "fixture.gob").write_bytes(b"different actual input")
                elif kind == "config":
                    (root / "config.yaml").write_bytes(b"config_version: 1")
                elif kind == "missing-build":
                    del manifests["candidate"]["build"]
                elif kind == "changed-build-source":
                    manifests["candidate"]["build"]["source_digest_after"] = "changed during build"
                else:
                    manifests["candidate"]["build"]["executable_sha256"] = "other binary"
                self.save_manifests(args, manifests)
                value = {"benchmarks": {"BenchmarkA": {"ns/op": 100}}, "wall_seconds": 0}
                with patch("architecture_performance.run", return_value=value) as execute, patch("builtins.print"), self.assertRaises(ValueError):
                    compare(args)
                execute.assert_not_called()

    def test_fixture_changes_between_pairs_are_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            args, manifests = self.comparison_fixture(root)
            self.save_manifests(args, manifests)
            def result(command, cwd, log, timeout):
                (root / "fixture.gob").write_bytes(b"changed while running")
                return {"benchmarks": {"BenchmarkA": {"ns/op": 100}}, "wall_seconds": 0}
            with patch("architecture_performance.run", side_effect=result) as execute, patch("builtins.print"), self.assertRaises(ValueError):
                compare(args)
            self.assertEqual(execute.call_count, 1)

    def test_final_command_mutation_cannot_publish_acceptance(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            args, manifests = self.comparison_fixture(root)
            self.save_manifests(args, manifests)
            calls = 0
            def result(command, cwd, log, timeout):
                nonlocal calls
                calls += 1
                if calls == 20:
                    (root / "config.yaml").write_bytes(b"changed on final command")
                return {"benchmarks": {"BenchmarkA": {"ns/op": 100}}, "wall_seconds": 0}
            with patch("architecture_performance.run", side_effect=result), patch("builtins.print"), self.assertRaises(ValueError):
                compare(args)
            report = json.loads((Path(args.output) / "comparison.json").read_text())
            self.assertFalse(report["passed"])
            self.assertFalse(report["acceptance_eligible"])

    def test_prebuilt_snapshot_remains_smoke_only(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            args, manifests = self.comparison_fixture(root)
            for manifest in manifests.values():
                del manifest["build"]
            args.smoke = True
            self.save_manifests(args, manifests)
            result = {"benchmarks": {"BenchmarkA": {"ns/op": 100}}, "wall_seconds": 0}
            with patch("architecture_performance.run", return_value=result), patch("builtins.print"):
                self.assertEqual(compare(args), 0)
            report = json.loads((Path(args.output) / "comparison.json").read_text())
            self.assertFalse(report["passed"])
            self.assertFalse(report["acceptance_eligible"])

    def test_managed_build_binds_binary_and_rejects_source_config_drift(self):
        for changed in (None, "main_test.go", "schema.sql", "config.yaml"):
            with self.subTest(changed=changed), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                for name in ("main_test.go", "schema.sql", "config.yaml"):
                    (root / name).write_bytes(name.encode())
                executable = root / "benchmark.exe"
                def inspect(command, cwd, **kwargs):
                    if command[0] == "git":
                        if command[1] == "ls-files":
                            return "\0".join(p.name for p in root.iterdir() if p.is_file()).encode()
                        return "head" if command[1] == "rev-parse" else "dirty"
                    return "go version test" if command[1] == "version" else b'{}'
                def compile_binary(command, cwd):
                    self.assertEqual(command, ["go", "test", "-c", "./factor", "-o", str(executable)])
                    self.assertEqual(Path(cwd), root)
                    executable.write_bytes(b"compiled benchmark")
                    if changed:
                        (root / changed).write_bytes(b"changed during build")
                with patch("architecture_performance.subprocess.check_output", side_effect=inspect), patch("architecture_performance.subprocess.check_call", side_effect=compile_binary), patch("architecture_performance.platform.node", return_value="test"), patch("architecture_performance.platform.platform", return_value="test"):
                    if changed:
                        with self.assertRaisesRegex(ValueError, "changed during benchmark build"):
                            freeze(root, [], "go", {}, [executable], ".", "./factor")
                    else:
                        manifest = freeze(root, [], "go", {}, [executable], ".", "./factor")
                        self.assertEqual(manifest["build"]["source_digest_before"], manifest["source_digest"])
                        self.assertEqual(manifest["build"]["source_digest_after"], manifest["source_digest"])
                        self.assertEqual(manifest["build"]["executable_sha256"], hashlib.sha256(executable.read_bytes()).hexdigest())
                        self.assertIn("schema.sql", manifest["source_files"])
                        self.assertNotIn("benchmark.exe", manifest["source_files"])

    def save_manifests(self, args, manifests):
        for name, value in manifests.items():
            Path(getattr(args, name + "_manifest")).write_text(json.dumps(value), encoding="utf-8")

    def test_matched_ratio_and_threshold_uncertainty(self):
        base = [100 + i for i in range(10)]
        same = confidence_interval(base, base)
        self.assertEqual(same["ratio"], 1)
        slower = confidence_interval(base, [v * 1.06 for v in base])
        self.assertAlmostEqual(slower["low"], 1.06)
        noisy = confidence_interval(base, [v * math.exp(.05 + (-1) ** i * .1) for i, v in enumerate(base)])
        self.assertLess(noisy["low"], 1.05)
        self.assertGreater(noisy["high"], 1.05)
        with self.assertRaises(ValueError):
            confidence_interval(base[:9], base[:9])

    def test_parser_preserves_metrics_rejects_duplicates(self):
        line = "BenchmarkArchitectureHotPath/TS-8 10 123.5 ns/op 32 asset-events/op 20 B/op 1 allocs/op"
        value = parse_benchmarks(line)["BenchmarkArchitectureHotPath/TS"]
        self.assertEqual(value["ns/op"], 123.5)
        self.assertEqual(value["asset-events/op"], 32)
        with self.assertRaises(ValueError):
            parse_benchmarks(line + "\n" + line)
        with self.assertRaises(ValueError):
            parse_benchmarks("PASS")

    def test_negative_identity_and_minimum_rounds(self):
        for kind in ("rounds", "source", "binary", "fixture", "cwd"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as folder:
                args, manifests = self.comparison_fixture(Path(folder))
                if kind == "rounds":
                    args.rounds = 9
                elif kind == "source":
                    manifests["candidate"]["source_digest"] = "baseline"
                elif kind == "binary":
                    manifests["candidate"]["executable_sha256"] = {}
                elif kind == "fixture":
                    manifests["candidate"]["input_digest"] = "different"
                else:
                    manifests["candidate"]["benchmark_cwd"] = "different-package"
                self.save_manifests(args, manifests)
                with patch("architecture_performance.run") as execute, self.assertRaises(ValueError):
                    compare(args)
                execute.assert_not_called()

    def test_comparison_requires_matched_benchmarks_and_alternates_pairs(self):
        with tempfile.TemporaryDirectory() as folder:
            args, manifests = self.comparison_fixture(Path(folder))
            self.save_manifests(args, manifests)
            def result(command, cwd, log, timeout):
                newer = Path(command[0]).stem == "candidate"
                return {"benchmarks": {"BenchmarkA": {"ns/op": 104 if newer else 100}}, "wall_seconds": 0}
            with patch("architecture_performance.run", side_effect=result) as execute, patch("builtins.print"):
                self.assertEqual(compare(args), 0)
                self.assertEqual([Path(c.args[0][0]).stem for c in execute.call_args_list[:4]], ["baseline", "candidate", "candidate", "baseline"])
            report = json.loads((Path(args.output) / "comparison.json").read_text())
            self.assertTrue(report["passed"])
            self.assertEqual(report["metrics"]["BenchmarkA"]["pairs"], 10)
            with patch("architecture_performance.run", side_effect=[
                {"benchmarks": {"BenchmarkA": {"ns/op": 100}}, "wall_seconds": 0},
                {"benchmarks": {"BenchmarkB": {"ns/op": 100}}, "wall_seconds": 0},
            ]), patch("builtins.print"), self.assertRaisesRegex(ValueError, "benchmark sets differ"):
                compare(args)


if __name__ == "__main__":
    unittest.main()
