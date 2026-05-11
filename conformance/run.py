#!/usr/bin/env python3
"""Conformance kit entrypoint.

    python conformance/run.py --lang {python|typescript|php|rust} \
        [--fixture <path>] [--scenario <glob>] [--check-contract-version <contract.md>]

For each scenario in `conformance/scenarios/*.json` (sorted), spawns the
chosen language's conformance fixture, replays the scenario, and diffs
the result against the scenario's `expect*` steps. TAP-13-ish output;
exit 0 iff every non-skipped scenario passed.

`--lang rust` requires `--fixture <built-binary>` (the Rust fixture lives
in nexo-rs, not here). `--check-contract-version <contract.md>` asserts
that the contract doc's §13 top version equals `lib.SCENARIOS_TARGET`
(only the nexo-rs CI leg passes this — it's the one with the doc).
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import re
import subprocess
import sys
from pathlib import Path

# Make `conformance` importable when run as `python conformance/run.py`.
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from conformance import lib, mock_host  # noqa: E402
from conformance.lib import fixture_argv_for_lang, load_scenario  # noqa: E402

SCENARIOS_DIR = REPO_ROOT / "conformance" / "scenarios"


def _fixture_capabilities(fixture_argv: list[str]) -> list[str]:
    try:
        out = subprocess.run(
            fixture_argv + ["--print-capabilities"], capture_output=True, timeout=20
        )
    except Exception as e:  # noqa: BLE001
        raise SystemExit(f"failed to query fixture capabilities ({fixture_argv}): {e}")
    if out.returncode != 0:
        raise SystemExit(
            f"fixture --print-capabilities exited {out.returncode}; stderr:\n{out.stderr.decode('utf-8','replace')}"
        )
    try:
        caps = json.loads(out.stdout.decode("utf-8").strip())
    except json.JSONDecodeError as e:
        raise SystemExit(f"fixture --print-capabilities did not print a JSON array: {out.stdout!r} ({e})")
    if not isinstance(caps, list) or not all(isinstance(c, str) for c in caps):
        raise SystemExit(f"fixture --print-capabilities must be a JSON array of strings, got {caps!r}")
    return caps


def _version_tuple(v: str) -> tuple[int, ...]:
    return tuple(int(x) for x in re.findall(r"\d+", v))


def _check_contract_version(contract_md: Path) -> None:
    text = contract_md.read_text()
    # First `| `X.Y.Z` |` row of the §13 changelog table.
    m = re.search(r"\|\s*`(\d+\.\d+\.\d+)`\s*\|", text)
    if not m:
        raise SystemExit(f"--check-contract-version: could not find a `| `X.Y.Z` |` row in {contract_md}")
    doc_version = m.group(1)
    if doc_version != lib.SCENARIOS_TARGET:
        raise SystemExit(
            f"--check-contract-version: contract doc is at {doc_version} but the conformance kit targets "
            f"{lib.SCENARIOS_TARGET}. Add scenarios for the new surface and bump lib.SCENARIOS_TARGET, "
            f"then re-run."
        )
    print(f"# contract version check OK: doc {doc_version} == kit target {lib.SCENARIOS_TARGET}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="nexo plugin SDK conformance runner")
    ap.add_argument("--lang", required=True, choices=["python", "typescript", "php", "rust"])
    ap.add_argument("--fixture", default=None, help="path to the fixture (required for --lang rust)")
    ap.add_argument("--scenario", default="*", help="glob to filter scenario file names (default: all)")
    ap.add_argument("--check-contract-version", dest="contract_md", default=None,
                    help="path to docs/src/plugins/contract.md — assert §13 top version == kit target")
    args = ap.parse_args(argv)

    if args.contract_md:
        _check_contract_version(Path(args.contract_md))

    fixture_argv = fixture_argv_for_lang(args.lang, REPO_ROOT, args.fixture)
    caps = set(_fixture_capabilities(fixture_argv))
    target = _version_tuple(lib.SCENARIOS_TARGET)

    scenario_files = sorted(SCENARIOS_DIR.glob("*.json"))
    if not scenario_files:
        print("# no scenarios found", file=sys.stderr)
        return 1
    selected = [p for p in scenario_files if fnmatch.fnmatch(p.name, args.scenario) or fnmatch.fnmatch(p.stem, args.scenario)]
    if not selected:
        print(f"# no scenarios matched {args.scenario!r}", file=sys.stderr)
        return 1

    print(f"TAP version 13")
    print(f"1..{len(selected)}")
    n = 0
    failures = 0
    for path in selected:
        n += 1
        try:
            scn = load_scenario(path)
        except (ValueError, KeyError, json.JSONDecodeError) as e:
            failures += 1
            print(f"not ok {n} - {path.name} (load error)")
            print(f"  ---\n  error: {e}\n  ...")
            continue
        # Skip if the scenario targets a contract version newer than the kit,
        # or requires a capability the fixture doesn't declare.
        if _version_tuple(scn.contract_version) > target:
            print(f"ok {n} - {scn.name} # SKIP scenario targets contract {scn.contract_version} > kit {lib.SCENARIOS_TARGET}")
            continue
        if not set(scn.requires).issubset(caps):
            missing = sorted(set(scn.requires) - caps)
            print(f"ok {n} - {scn.name} # SKIP fixture lacks {missing}")
            continue
        res = mock_host.run_scenario(fixture_argv, scn)
        if res.passed:
            print(f"ok {n} - {scn.name}")
        else:
            failures += 1
            print(f"not ok {n} - {scn.name}")
            print("  ---")
            print(f"  file: {path.name}")
            for line in (res.failure or "").splitlines():
                print(f"  failure: {line}" if line.strip() else "  failure:")
            if res.stderr_tail:
                print("  stderr: |")
                for line in res.stderr_tail.splitlines():
                    print(f"    {line}")
            print("  ...")
    print(f"# {n - failures}/{n} scenarios passed for --lang {args.lang}")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
