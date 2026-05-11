"""The mock host: spawn an SDK conformance fixture, replay a scenario's
steps, return what happened. Pure I/O + structural matching — `run.py`
turns the result into TAP output.

A scenario "passes" iff every step succeeded (sends went out, every
`expect*` matched, the exit code was right, no leftover frames). The
result carries a failure string + the fixture's stderr tail for
diagnostics.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from . import lib
from .lib import Proc, Scenario, match_value, substitute_captures


@dataclass
class Result:
    passed: bool
    failure: str | None
    stderr_tail: str


def _default_timeout(step: dict[str, Any], fallback: float = 5.0) -> float:
    ms = step.get("timeout_ms")
    return (ms / 1000.0) if isinstance(ms, (int, float)) else fallback


def run_scenario(fixture_argv: list[str], scenario: Scenario) -> Result:
    # The fixture gets the scenario's nexo-plugin.toml body injected into
    # its config under "manifest" (so a fixture reads cfg["manifest"]).
    config = dict(scenario.fixture_config)
    config["manifest"] = scenario.manifest
    config_json = json.dumps(config)
    proc = Proc(fixture_argv + ["--config", config_json])
    captures: dict[str, Any] = {}
    try:
        for idx, step in enumerate(scenario.steps):
            where = f"step {idx}"
            try:
                _run_step(proc, step, captures, where)
            except (AssertionError, TimeoutError, KeyError) as e:
                return Result(False, f"{where}: {e}", proc.stderr_tail())
        # No expect_exit at the end → make sure nothing extra is buffered.
        if "expect_exit" not in scenario.steps[-1] if scenario.steps else True:
            try:
                proc.drain_no_extra_frames()
            except AssertionError as e:
                return Result(False, f"end-of-scenario: {e}", proc.stderr_tail())
        return Result(True, None, "")
    finally:
        proc.kill()


def _run_step(proc: Proc, step: dict[str, Any], captures: dict[str, Any], where: str) -> None:
    if "send" in step:
        proc.send_frame(substitute_captures(step["send"], captures))
        return
    if "send_raw" in step:
        proc.send_raw(str(step["send_raw"]))
        return
    if "send_raw_repeat" in step:
        spec = step["send_raw_repeat"]
        char = str(spec.get("char", "x"))
        count = int(spec.get("count", lib.MAX_FRAME_BYTES + 1000))
        proc.send_raw(char * count)
        return
    if "sleep_ms" in step:
        import time

        time.sleep(int(step["sleep_ms"]) / 1000.0)
        return
    if "expect_no_frame" in step:
        within = int(step["expect_no_frame"].get("within_ms", 300)) / 1000.0
        proc.expect_no_frame(within)
        return
    if "expect_stderr_contains" in step:
        needle = str(step["expect_stderr_contains"])
        # Give the fixture a beat to flush the log.
        import time

        deadline = time.monotonic() + _default_timeout(step, 3.0)
        while time.monotonic() < deadline:
            if needle in proc.stderr():
                return
            time.sleep(0.05)
        raise AssertionError(f"stderr did not contain {needle!r}; got:\n{proc.stderr_tail()}")
    if "expect_exit" in step:
        proc.wait_exit(int(step["expect_exit"]), _default_timeout(step, 5.0))
        return
    if "expect" in step:
        frame = proc.read_frame(_default_timeout(step))
        ok, why = match_value(step["expect"], frame, captures)
        if not ok:
            raise AssertionError(f"{why}\n  pattern: {json.dumps(step['expect'])}\n  actual:  {json.dumps(frame)}")
        return
    if "expect_request" in step:
        frame = proc.read_frame(_default_timeout(step))
        ok, why = match_value(step["expect_request"], frame, captures)
        if not ok:
            raise AssertionError(
                f"{why}\n  request pattern: {json.dumps(step['expect_request'])}\n  actual: {json.dumps(frame)}"
            )
        if "reply" in step:
            proc.send_frame(substitute_captures(step["reply"], captures))
        return
    if "expect_any_order" in step:
        patterns = list(step["expect_any_order"])
        n = len(patterns)
        received: list[dict[str, Any]] = []
        for _ in range(n):
            received.append(proc.read_frame(_default_timeout(step)))
        # Greedy assignment: for each received frame find a still-unmatched
        # pattern it satisfies. Try a few orderings cheaply (n is tiny).
        if not _match_unordered(patterns, received, captures):
            raise AssertionError(
                f"could not match all of {n} frames to patterns in any order\n"
                f"  patterns: {json.dumps(patterns)}\n  actual:   {json.dumps(received)}"
            )
        return
    raise AssertionError(f"unknown step shape: {sorted(step)}")


def _match_unordered(patterns: list[Any], frames: list[dict[str, Any]], captures: dict[str, Any]) -> bool:
    """Backtracking match: every frame matched to a distinct pattern.
    Captures bound on a successful full assignment only (we snapshot +
    restore on backtrack)."""
    used = [False] * len(patterns)

    def _try(fi: int, snap: dict[str, Any]) -> bool:
        if fi == len(frames):
            return True
        for pi, pat in enumerate(patterns):
            if used[pi]:
                continue
            local = dict(captures)
            ok, _ = match_value(pat, frames[fi], local)
            if ok:
                used[pi] = True
                saved = dict(captures)
                captures.clear()
                captures.update(local)
                if _try(fi + 1, snap):
                    return True
                captures.clear()
                captures.update(saved)
                used[pi] = False
        return False

    return _try(0, dict(captures))
