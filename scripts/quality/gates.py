#!/usr/bin/env python3
"""Fail-closed structural and coverage policy checks. No baseline writes."""
import argparse
import json
from pathlib import Path
import re
import sys
import subprocess
import tempfile
import hashlib

TAGS = "integration,postgres,pglocal"
MODULE = "github.com/CaliLuke/selvedge"


def load(path):
    with open(path, encoding="utf-8") as stream:
        return json.load(stream)


def validate_inventory(report, policy):
    if report.get("version") != 1 or report.get("tags") != TAGS:
        raise ValueError("unsupported or incomplete source inventory")
    packages = report.get("packages")
    if not isinstance(packages, dict) or not packages:
        raise ValueError("empty package inventory")
    if set(packages) != set(policy["coverage"]):
        raise ValueError("package scope changed; review explicit coverage policy: "
                         f"missing={set(policy['coverage']) - set(packages)}, new={set(packages) - set(policy['coverage'])}")
    for package, files in packages.items():
        if not isinstance(files, list) or not files or len(files) != len(set(files)):
            raise ValueError(f"missing or duplicate sources for {package}")
    if not isinstance(report.get("metrics"), dict) or not report["metrics"]:
        raise ValueError("empty structural analysis")
    keys = report.get("metric_keys")
    if not isinstance(keys, list) or len(keys) != len(set(keys)) or set(keys) != set(report["metrics"]):
        raise ValueError("structural analysis is missing declared metric identities")
    expected_files = {name for files in packages.values() for name in files}
    actual_files = {key.removeprefix("file:") for key in report["metrics"] if key.startswith("file:")}
    if expected_files != actual_files:
        raise ValueError("structural analysis is missing source files")
    expected_packages = {"package:" + name for name in packages}
    actual_packages = {key for key in report["metrics"] if key.startswith("package:")}
    if expected_packages != actual_packages:
        raise ValueError("structural analysis is missing package measurements")
    for package, files in packages.items():
        metrics = report["metrics"]["package:" + package]
        if metrics.get("files") != len(files) or metrics.get("lines") != sum(report["metrics"]["file:" + name]["lines"] for name in files):
            raise ValueError(f"inconsistent package measurements: {package}")
    if not set(report.get("coverage_files", [])).issubset(expected_files):
        raise ValueError("coverage inventory contains unknown sources")
    if set(report.get("source_hashes", {})) != expected_files:
        raise ValueError("incomplete source fingerprints")
    return packages


def structural_failures(report, policy):
    validate_inventory(report, policy)
    failures = []
    for identity, metrics in report["metrics"].items():
        kind = identity.split(":", 1)[0]
        limits = policy["structural_limits"].get(kind)
        if limits is None or set(metrics) != set(limits):
            raise ValueError(f"unknown/incomplete metrics: {identity}")
        debt = policy["structural_debt"].get(identity, {})
        for name, current in metrics.items():
            if type(current) is not int or current < 0:
                raise ValueError(f"invalid measurement: {identity}.{name}")
            maximum = debt.get("ceilings", {}).get(name, limits[name])
            if current > maximum:
                failures.append(f"{identity}.{name}: {current} exceeds {maximum}")
    for identity, debt in policy["structural_debt"].items():
        if not debt.get("reason") or debt.get("kind") not in ("debt", "exception"):
            raise ValueError(f"unreviewed structural baseline: {identity}")
        if identity not in report["metrics"]:
            raise ValueError(f"stale baseline requires review/removal: {identity}")
        for name, maximum in debt["ceilings"].items():
            current = report["metrics"][identity][name]
            if current < maximum:
                print(f"IMPROVED {identity}.{name}: {maximum} -> {current}; review baseline tightening")
    return failures


def read_profile(path):
    lines = Path(path).read_text(encoding="utf-8").splitlines()
    if not lines or lines[0] != "mode: atomic":
        raise ValueError(f"missing/invalid atomic coverage profile: {path}")
    blocks = {}
    for line in lines[1:]:
        match = re.fullmatch(r"(.+):(\d+\.\d+,\d+\.\d+) (\d+) (\d+)", line)
        if not match or not match[1].startswith(MODULE + "/"):
            raise ValueError(f"malformed or foreign coverage block: {line}")
        filename = match[1].removeprefix(MODULE + "/")
        key = (filename, match[2])
        statements, covered = int(match[3]), int(match[4]) > 0
        if key in blocks and blocks[key][0] != statements:
            raise ValueError(f"conflicting statement count within profile: {key}")
        # -coverpkg instruments shared packages in multiple test binaries. Go
        # emits their blocks repeatedly; coverage is the union of those runs.
        blocks[key] = (statements, covered or blocks.get(key, (0, False))[1])
    if not blocks:
        raise ValueError(f"empty coverage profile: {path}")
    return blocks


def completed_packages(path):
    states = {}
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        event = json.loads(line)
        package = event.get("Package")
        if package and "Test" not in event and event.get("Action") in ("pass", "skip", "fail"):
            states[package] = event["Action"]
    if not states or "fail" in states.values():
        raise ValueError(f"failed/incomplete test run: {path}")
    return set(states)


def coverage_failures(report, policy, profiles, events):
    packages = validate_inventory(report, policy)
    if len(profiles) != 3 or len(events) != 3:
        raise ValueError("require fresh unit, PostgreSQL storage, and integration results")
    expected = [set(packages), {MODULE + "/internal/storage"}, set(packages)]
    for index, path in enumerate(events):
        completed = completed_packages(path)
        if expected[index] != completed:
            raise ValueError(f"missing completed packages in {path}: {expected[index] - completed}")
    blocks = {}
    for path in profiles:
        for key, (statements, covered) in read_profile(path).items():
            if key in blocks and blocks[key][0] != statements:
                raise ValueError(f"coverage sources changed between runs: {key}")
            blocks[key] = (statements, covered or blocks.get(key, (0, False))[1])
    generated = set(report.get("generated_files") or [])
    blocks = {key: value for key, value in blocks.items() if key[0] not in generated and value[0] > 0}
    expected_blocks = instrumentation_blocks(report)
    if {key: value[0] for key, value in blocks.items()} != expected_blocks:
        raise ValueError("incomplete coverage blocks or source mismatch")
    files = {key[0] for key in blocks}
    missing = set(report["coverage_files"]) - files
    if missing:
        raise ValueError(f"coverage profile is missing executable sources: {sorted(missing)}")
    owned = {name: package for package, names in packages.items() for name in names}
    totals = {package: [0, 0] for package in packages}
    file_totals = {name: [0, 0] for name in owned}
    for (filename, _), (count, covered) in blocks.items():
        if filename not in owned:
            raise ValueError(f"profile contains unknown source: {filename}")
        totals[owned[filename]][1] += count
        file_totals[filename][1] += count
        if covered:
            totals[owned[filename]][0] += count
            file_totals[filename][0] += count
    failures = []
    for package, (covered, total) in sorted(totals.items()):
        entry = policy["coverage"][package]
        floor = entry["minimum"]
        if not isinstance(floor, (int, float)) or not 0 <= floor <= 100 or not entry.get("reason"):
            raise ValueError(f"missing coverage policy: {package}")
        if total == 0:
            if not entry.get("no_statements"):
                raise ValueError(f"no instrumented statements: {package}")
            print(f"COVERAGE {package}: no executable statements (reviewed)")
            continue
        percent = 100 * covered / total
        print(f"COVERAGE {package}: {percent:.2f}% (minimum {floor}%)")
        if percent < floor:
            failures.append(f"{package}: coverage {percent:.2f}% below {floor}%")
    for filename, floor in policy.get("file_coverage", {}).items():
        if filename not in file_totals or not file_totals[filename][1]:
            raise ValueError(f"missing targeted file coverage: {filename}")
        covered, total = file_totals[filename]
        percent = 100 * covered / total
        print(f"COVERAGE {filename}: {percent:.2f}% (minimum {floor}%)")
        if percent < floor:
            failures.append(f"{filename}: coverage {percent:.2f}% below {floor}%")
    return failures


def instrumentation_blocks(report):
    """Use the active Go toolchain to reject syntactically valid truncated profiles."""
    expected = {}
    with tempfile.TemporaryDirectory(prefix="selvedge-cover-") as directory:
        output = Path(directory) / "instrumented.go"
        for filename, digest in report["source_hashes"].items():
            if hashlib.sha256(Path(filename).read_bytes()).hexdigest() != digest:
                raise ValueError(f"source changed during checks: {filename}")
            subprocess.run(["go", "tool", "cover", "-mode=atomic", "-var=QualityCover",
                            "-o=" + str(output), filename], check=True, capture_output=True, text=True)
            source = output.read_text(encoding="utf-8")
            positions = instrumented_values(source, "Pos", "uint32")
            statements = instrumented_values(source, "NumStmt", "uint16")
            if len(positions) != 3 * len(statements):
                raise ValueError(f"incomplete Go instrumentation: {filename}")
            for index, count in enumerate(statements):
                if count == 0:
                    continue
                start, end, columns = positions[3 * index:3 * index + 3]
                span = f"{start}.{columns & 0xffff},{end}.{columns >> 16}"
                expected[(filename, span)] = count
    return expected


def instrumented_values(source, name, integer_type):
    match = re.search(rf"\b{name}:\s*\[[^]]+\]{integer_type}\{{([^}}]*)\}}", source)
    if not match:
        raise ValueError(f"unsupported Go coverage instrumentation: missing {name}")
    values = re.sub(r"//[^\n]*", "", match[1])
    return [int(value.strip(), 0) for value in values.split(",") if value.strip()]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("structure", "coverage"))
    parser.add_argument("--inventory", required=True)
    parser.add_argument("--policy", required=True)
    parser.add_argument("--profile", action="append", default=[])
    parser.add_argument("--events", action="append", default=[])
    args = parser.parse_args()
    try:
        report, policy = load(args.inventory), load(args.policy)
        if policy.get("version") != 1:
            raise ValueError("unsupported policy version")
        if args.mode == "structure":
            failures = structural_failures(report, policy)
        else:
            failures = coverage_failures(report, policy, args.profile, args.events)
        if failures:
            raise ValueError("\n".join(failures))
    except (OSError, ValueError, KeyError, TypeError, subprocess.CalledProcessError) as error:
        print(f"FAIL {args.mode}: {error}", file=sys.stderr)
        return 1
    print(f"PASS {args.mode}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
