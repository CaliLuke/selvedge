import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import gates

P = gates.MODULE


def fixture():
    report = {"version": 1, "tags": gates.TAGS,
              "packages": {P: ["a.go"]}, "source_hashes": {"a.go": "a" * 64},
              "coverage_files": ["a.go"], "metric_keys": ["file:a.go", "package:" + P],
              "metrics": {"file:a.go": {"lines": 700, "components": 2}, "package:" + P: {"files": 1, "lines": 700}}}
    policy = {"version": 1, "coverage": {P: {"minimum": 50, "reason": "fixture"}},
              "structural_limits": {"file": {"lines": 600, "components": 1}, "package": {"files": 25, "lines": 6000}},
              "structural_debt": {"file:a.go": {"ceilings": {"lines": 700, "components": 2},
                                               "kind": "debt", "reason": "reviewed fixture"}}}
    return report, policy


class StructuralTests(unittest.TestCase):
    def test_reviewed_debt_and_improvement_pass(self):
        report, policy = fixture()
        self.assertEqual([], gates.structural_failures(report, policy))
        report["metrics"]["file:a.go"]["lines"] = 650
        report["metrics"]["package:" + P]["lines"] = 650
        self.assertEqual([], gates.structural_failures(report, policy))

    def test_growth_and_disconnected_responsibility_fail(self):
        for metric, value in (("lines", 701), ("components", 3)):
            report, policy = fixture()
            report["metrics"]["file:a.go"][metric] = value
            report["metrics"]["package:" + P]["lines"] = report["metrics"]["file:a.go"]["lines"]
            self.assertTrue(gates.structural_failures(report, policy))

    def test_new_violation_is_not_hidden_by_exception_elsewhere(self):
        report, policy = fixture()
        report["packages"][P].append("new.go")
        report["source_hashes"]["new.go"] = "b" * 64
        report["metrics"]["file:new.go"] = {"lines": 701, "components": 1}
        report["metric_keys"].append("file:new.go")
        report["metrics"]["package:" + P] = {"files": 2, "lines": 1401}
        self.assertTrue(gates.structural_failures(report, policy))

    def test_missing_package_file_or_metric_fails_closed(self):
        report, policy = fixture()
        for key in ("packages", "metrics", "source_hashes"):
            broken = copy.deepcopy(report)
            broken[key] = {}
            with self.assertRaises(ValueError):
                gates.structural_failures(broken, policy)
        report["metrics"]["file:a.go"].pop("components")
        with self.assertRaises(ValueError):
            gates.structural_failures(report, policy)

    def test_missing_package_receiver_and_holder_measurements_fail(self):
        for kind in ("package", "receiver", "holder"):
            report, policy = fixture()
            identity = "package:" + P
            if kind != "package":
                identity = kind + ":" + P + ".Worker"
                report["metrics"][identity] = {"methods": 1} if kind == "receiver" else {"dependencies": 1}
                report["metric_keys"].append(identity)
            report["metrics"].pop(identity)
            with self.assertRaises(ValueError):
                gates.structural_failures(report, policy)
        report, policy = fixture()
        report["metrics"]["package:" + P]["files"] = 0
        with self.assertRaises(ValueError):
            gates.structural_failures(report, policy)

    def test_new_receiver_growth_fails(self):
        report, policy = fixture()
        policy["structural_limits"]["receiver"] = {"methods": 30, "lines": 600}
        report["metrics"]["receiver:" + P + ".Worker"] = {"methods": 31, "lines": 300}
        report["metric_keys"].append("receiver:" + P + ".Worker")
        self.assertTrue(gates.structural_failures(report, policy))


class CoverageTests(unittest.TestCase):
    def setup_files(self, directory):
        report, policy = fixture()
        storage = P + "/internal/storage"
        report["packages"][storage] = ["internal/storage/store.go"]
        report["source_hashes"]["internal/storage/store.go"] = "b" * 64
        report["metrics"]["file:internal/storage/store.go"] = {"lines": 10, "components": 0}
        report["metrics"]["package:" + storage] = {"files": 1, "lines": 10}
        report["metric_keys"].extend(["file:internal/storage/store.go", "package:" + storage])
        report["coverage_files"].append("internal/storage/store.go")
        policy["coverage"][storage] = {"minimum": 80, "reason": "fixture"}
        profiles, events = [], []
        for index in range(3):
            profile, event = Path(directory) / f"{index}.out", Path(directory) / f"{index}.jsonl"
            profile.write_text(f"mode: atomic\n{P}/a.go:1.1,2.1 1 1\n{P}/a.go:3.1,4.1 1 0\n{storage}/store.go:1.1,2.1 1 1\n")
            packages = [storage] if index == 1 else [P, storage]
            event.write_text("".join(json.dumps({"Package": p, "Action": "pass"}) + "\n" for p in packages))
            profiles.append(profile)
            events.append(event)
        expected = {("a.go", "1.1,2.1"): 1, ("a.go", "3.1,4.1"): 1, ("internal/storage/store.go", "1.1,2.1"): 1}
        return report, policy, profiles, events, expected

    def test_complete_coverage_passes_and_regression_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            report, policy, profiles, events, expected = self.setup_files(directory)
            with patch.object(gates, "instrumentation_blocks", return_value=expected):
                self.assertEqual([], gates.coverage_failures(report, policy, profiles, events))
                policy["coverage"][P]["minimum"] = 51
                self.assertTrue(gates.coverage_failures(report, policy, profiles, events))

    def test_truncated_valid_profile_cannot_inflate_coverage(self):
        with tempfile.TemporaryDirectory() as directory:
            report, policy, profiles, events, expected = self.setup_files(directory)
            for profile in profiles:
                profile.write_text(profile.read_text().replace(f"{P}/a.go:3.1,4.1 1 0\n", ""))
            with patch.object(gates, "instrumentation_blocks", return_value=expected), self.assertRaises(ValueError):
                gates.coverage_failures(report, policy, profiles, events)

    def test_incomplete_or_failed_test_run_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            report, policy, profiles, events, _ = self.setup_files(directory)
            for content in ("", json.dumps({"Package": P, "Action": "fail"}), json.dumps({"Package": P, "Action": "pass"})):
                events[0].write_text(content)
                with self.assertRaises(ValueError):
                    gates.coverage_failures(report, policy, profiles, events)

    def test_shared_package_blocks_merge_but_conflicting_counts_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profile"
            path.write_text(f"mode: atomic\n{P}/a.go:1.1,2.1 2 0\n{P}/a.go:1.1,2.1 2 3\n")
            self.assertEqual({("a.go", "1.1,2.1"): (2, True)}, gates.read_profile(path))
            path.write_text(path.read_text() + f"{P}/a.go:1.1,2.1 3 0\n")
            with self.assertRaises(ValueError):
                gates.read_profile(path)

    def test_missing_and_malformed_profiles_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profile"
            for content in ("", "mode: atomic\n", "mode: count\n", "mode: atomic\ntruncated"):
                path.write_text(content)
                with self.assertRaises(ValueError):
                    gates.read_profile(path)

    def test_instrumentation_uses_real_go_blocks_and_rejects_changed_source(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.go"
            path.write_text("package fixture\nfunc A(x bool) int { if x { return 1 }; return 2 }\n")
            report = {"source_hashes": {str(path): hashlib.sha256(path.read_bytes()).hexdigest()}}
            expected = gates.instrumentation_blocks(report)
            self.assertEqual(3, len(expected))
            path.write_text("package fixture\n")
            with self.assertRaises(ValueError):
                gates.instrumentation_blocks(report)

    def test_missing_tool_or_failed_analysis_is_not_clean(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.go"
            path.write_text("package fixture\n")
            report = {"source_hashes": {str(path): hashlib.sha256(path.read_bytes()).hexdigest()}}
            for failure in (FileNotFoundError("go"), subprocess.CalledProcessError(1, "go")):
                with patch.object(gates.subprocess, "run", side_effect=failure), self.assertRaises(type(failure)):
                    gates.instrumentation_blocks(report)
            with self.assertRaises(ValueError):
                gates.instrumented_values("partial output", "Pos", "uint32")


class RealLinterTests(unittest.TestCase):
    def assert_linter_rejects(self, linter, source):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "go.mod").write_text("module example.com/gatefixture\n\ngo 1.27.3\n")
            (root / "fixture.go").write_text(source)
            (root / ".golangci.yml").write_text("version: '2'\nlinters:\n  default: none\n  enable: [" + linter + "]\n  settings:\n    maintidx:\n      under: 20\n    dupl:\n      threshold: 150\nissues:\n  max-issues-per-linter: 0\n  max-same-issues: 0\n")
            output = root / "report.json"
            result = subprocess.run([os.environ.get("GOLANGCI_LINT", "golangci-lint"), "run", "--output.json.path=" + str(output), "./..."], cwd=root, capture_output=True, text=True, timeout=120)
            self.assertEqual(1, result.returncode, result.stdout + result.stderr)
            issues = json.loads(output.read_text())["Issues"]
            self.assertTrue(any(issue["FromLinter"] == linter for issue in issues))

    def test_new_low_maintainability_function_fails(self):
        body = "\n".join(f"if x > {n} {{ x += {n} }}" for n in range(160))
        self.assert_linter_rejects("maintidx", "package fixture\nfunc Bad(x int) int {\n" + body + "\nreturn x\n}\n")

    def test_duplicate_implementation_fails(self):
        body = "\n".join(f"if x > {n} {{ x += {n} }}" for n in range(30))
        self.assert_linter_rejects("dupl", "package fixture\nfunc A(x int) int {\n" + body + "\nreturn x\n}\nfunc B(x int) int {\n" + body + "\nreturn x\n}\n")


if __name__ == "__main__":
    unittest.main()
