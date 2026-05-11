"""Shared building blocks for the conformance kit.

- Frame I/O over a spawned fixture's stdio (buffered reads, like the
  per-SDK test harnesses).
- A small structural matcher engine (`$IGNORE` / `$ANY_*` / `$capture` /
  `$optional` / `$one_of` / `$ABSENT` / `$extra_keys`).
- The scenario loader + a sanity validator.

`mock_host.py` and `run.py` import from here. Python 3.10+ stdlib only —
no third-party deps (the kit must run on every CI runner).
"""

from __future__ import annotations

import json
import os
import select
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# The contract version the scenarios are written against. Bumped (with
# new scenarios) when the contract gains a surface. The `nexo-rs` CI leg
# cross-checks this against the live `docs/src/plugins/contract.md` §13.
SCENARIOS_TARGET = "1.10.0"

# Matches `MAX_FRAME_BYTES` in the SDKs (1 MiB). Used by scenario 17.
MAX_FRAME_BYTES = 1 << 20


# ── frame I/O ────────────────────────────────────────────────────────


class Proc:
    """A spawned fixture: a Popen + a stdout frame buffer + captured
    stderr. Reads off stdout are line-buffered so a single OS read that
    grabs several frames is handled."""

    def __init__(self, argv: list[str]) -> None:
        self.argv = argv
        self.popen = subprocess.Popen(
            argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        self._buf = b""
        self._stderr = b""

    # outbound

    def send_frame(self, frame: dict[str, Any]) -> None:
        self.send_raw(json.dumps(frame))

    def send_raw(self, line: str) -> None:
        assert self.popen.stdin is not None
        self.popen.stdin.write((line + "\n").encode("utf-8"))
        self.popen.stdin.flush()

    # inbound

    def _pump_stderr(self) -> None:
        # Non-blocking drain of whatever stderr has produced so far.
        assert self.popen.stderr is not None
        try:
            r, _, _ = select.select([self.popen.stderr], [], [], 0)
        except (ValueError, OSError):
            return
        if r:
            try:
                chunk = os.read(self.popen.stderr.fileno(), 65536)
            except OSError:
                return
            if chunk:
                self._stderr += chunk

    def read_frame(self, timeout: float = 5.0) -> dict[str, Any]:
        """Read the next JSON-RPC line off stdout, or raise TimeoutError."""
        deadline = time.monotonic() + timeout
        assert self.popen.stdout is not None
        while True:
            self._pump_stderr()
            if b"\n" in self._buf:
                line, _, self._buf = self._buf.partition(b"\n")
                text = line.decode("utf-8", "replace").strip()
                if text == "":
                    continue
                try:
                    obj = json.loads(text)
                except json.JSONDecodeError as e:
                    raise AssertionError(
                        f"fixture wrote a non-JSON line to stdout: {text!r} ({e})"
                    )
                if not isinstance(obj, dict):
                    raise AssertionError(f"fixture wrote a non-object frame: {text!r}")
                return obj
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"no frame from fixture within {timeout}s (buf={self._buf!r})"
                )
            r, _, _ = select.select([self.popen.stdout], [], [], min(0.1, remaining))
            if r:
                chunk = os.read(self.popen.stdout.fileno(), 65536)
                if not chunk:
                    raise AssertionError(
                        f"fixture closed stdout (buf={self._buf!r}, exit={self.popen.poll()})"
                    )
                self._buf += chunk

    def expect_no_frame(self, within: float) -> None:
        """Assert no frame arrives within `within` seconds."""
        try:
            extra = self.read_frame(timeout=within)
        except TimeoutError:
            return
        raise AssertionError(f"expected no frame, but fixture sent: {extra!r}")

    def drain_no_extra_frames(self, settle: float = 0.3) -> None:
        """At end-of-scenario: assert stdout has no leftover frame."""
        self.expect_no_frame(settle)

    # lifecycle

    def wait_exit(self, code: int, timeout: float = 5.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self._pump_stderr()
            rc = self.popen.poll()
            if rc is not None:
                if rc != code:
                    raise AssertionError(
                        f"fixture exited {rc}, expected {code}; stderr tail:\n{self.stderr_tail()}"
                    )
                return
            time.sleep(0.02)
        raise AssertionError(f"fixture did not exit within {timeout}s")

    def stderr(self) -> str:
        self._pump_stderr()
        return self._stderr.decode("utf-8", "replace")

    def stderr_tail(self, n: int = 40) -> str:
        return "\n".join(self.stderr().splitlines()[-n:])

    def kill(self) -> None:
        if self.popen.poll() is None:
            self.popen.kill()
            try:
                self.popen.wait(timeout=2)
            except subprocess.TimeoutExpired:
                pass
        for s in (self.popen.stdin, self.popen.stdout, self.popen.stderr):
            try:
                if s is not None:
                    s.close()
            except OSError:
                pass


# ── matcher engine ──────────────────────────────────────────────────


_TYPE_MATCHERS = {
    "$ANY_STRING": lambda v: isinstance(v, str),
    "$ANY_INT": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "$ANY_NUMBER": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "$ANY_BOOL": lambda v: isinstance(v, bool),
    "$ANY_OBJECT": lambda v: isinstance(v, dict),
    "$ANY_ARRAY": lambda v: isinstance(v, list),
}

_MISSING = object()


def match_value(pattern: Any, actual: Any, captures: dict[str, Any], path: str = "$") -> tuple[bool, str]:
    """Structurally match `pattern` against `actual`. Returns (ok, reason).
    Binds `$capture:<name>` entries into `captures` (re-capture asserts
    equality with the prior binding)."""
    # String directives.
    if isinstance(pattern, str):
        if pattern == "$IGNORE":
            return True, ""
        if pattern in _TYPE_MATCHERS:
            ok = _TYPE_MATCHERS[pattern](actual)
            return ok, "" if ok else f"{path}: expected {pattern}, got {actual!r}"
        if pattern.startswith("$capture:"):
            name = pattern[len("$capture:"):]
            if name in captures:
                if captures[name] != actual:
                    return False, f"{path}: capture {name!r} was {captures[name]!r}, now {actual!r}"
                return True, ""
            captures[name] = actual
            return True, ""
        if pattern == "$ABSENT":
            # Only meaningful as an object value; reaching here as a top-level
            # pattern means the actual *was* present → mismatch.
            return False, f"{path}: expected absent, got {actual!r}"
        # Plain string.
        return (pattern == actual), ("" if pattern == actual else f"{path}: expected {pattern!r}, got {actual!r}")

    # Object-level directives.
    if isinstance(pattern, dict):
        if "$optional" in pattern and len(pattern) == 1:
            # As a *value*, $optional is handled by the parent object loop;
            # reaching here means it was used at a non-value position.
            return match_value(pattern["$optional"], actual, captures, path)
        if "$one_of" in pattern and len(pattern) == 1:
            for sub in pattern["$one_of"]:
                ok, _ = match_value(sub, actual, captures, path)
                if ok:
                    return True, ""
            return False, f"{path}: no $one_of branch matched {actual!r}"
        if not isinstance(actual, dict):
            return False, f"{path}: expected object, got {actual!r}"
        allow_extra = pattern.get("$extra_keys") == "allow"
        pat_keys = {k for k in pattern if k != "$extra_keys"}
        for k in pat_keys:
            pv = pattern[k]
            av = actual.get(k, _MISSING)
            # $ABSENT as a value: the key must not be present.
            if pv == "$ABSENT":
                if av is not _MISSING:
                    return False, f"{path}.{k}: expected absent, got {av!r}"
                continue
            # {"$optional": p} as a value: key may be absent; if present, match p.
            if isinstance(pv, dict) and "$optional" in pv and len(pv) == 1:
                if av is _MISSING:
                    continue
                ok, why = match_value(pv["$optional"], av, captures, f"{path}.{k}")
                if not ok:
                    return False, why
                continue
            if av is _MISSING:
                return False, f"{path}.{k}: missing"
            ok, why = match_value(pv, av, captures, f"{path}.{k}")
            if not ok:
                return False, why
        if not allow_extra:
            extra = set(actual) - pat_keys
            if extra:
                return False, f"{path}: unexpected keys {sorted(extra)}"
        return True, ""

    # Arrays.
    if isinstance(pattern, list):
        if not isinstance(actual, list):
            return False, f"{path}: expected array, got {actual!r}"
        if len(pattern) != len(actual):
            return False, f"{path}: array length {len(actual)}, expected {len(pattern)}"
        for i, (pv, av) in enumerate(zip(pattern, actual)):
            ok, why = match_value(pv, av, captures, f"{path}[{i}]")
            if not ok:
                return False, why
        return True, ""

    # Scalars. Keep bool distinct from int (JSON `true` != `1`).
    if isinstance(pattern, bool) or isinstance(actual, bool):
        ok = isinstance(pattern, bool) and isinstance(actual, bool) and pattern == actual
        return ok, "" if ok else f"{path}: expected {pattern!r}, got {actual!r}"
    if pattern == actual:
        return True, ""
    return False, f"{path}: expected {pattern!r}, got {actual!r}"


def substitute_captures(value: Any, captures: dict[str, Any]) -> Any:
    """Recursively replace `"$capture:<name>"` strings with the bound
    value (for outgoing send/reply frames)."""
    if isinstance(value, str) and value.startswith("$capture:"):
        name = value[len("$capture:"):]
        if name not in captures:
            raise KeyError(f"$capture:{name} referenced before it was bound")
        return captures[name]
    if isinstance(value, dict):
        return {k: substitute_captures(v, captures) for k, v in value.items()}
    if isinstance(value, list):
        return [substitute_captures(v, captures) for v in value]
    return value


# ── scenario loading + validation ───────────────────────────────────


@dataclass
class Scenario:
    name: str
    doc: str
    contract_version: str
    requires: list[str]
    manifest: str
    fixture_config: dict[str, Any]
    steps: list[dict[str, Any]]
    source: Path

    @property
    def declared_tools(self) -> list[str]:
        v = self.fixture_config.get("declare_tools") or []
        return list(v)


_STEP_KEYS = {
    "send", "send_raw", "send_raw_repeat", "expect", "expect_any_order",
    "expect_request", "expect_stderr_contains", "expect_exit", "expect_no_frame",
    "sleep_ms",
}


def _manifest_extends_tools(manifest_toml: str) -> list[str]:
    """Cheap extract of `[plugin.extends].tools` from the inline manifest
    string (no TOML lib needed — the scenario manifests are simple)."""
    import re

    m = re.search(r"\[plugin\.extends\][^\[]*?tools\s*=\s*\[([^\]]*)\]", manifest_toml, re.S)
    if not m:
        return []
    inner = m.group(1)
    return [s.strip().strip('"').strip("'") for s in inner.split(",") if s.strip()]


def load_scenario(path: Path) -> Scenario:
    data = json.loads(path.read_text())
    scn = Scenario(
        name=data["name"],
        doc=data.get("doc", ""),
        contract_version=data.get("contract_version", SCENARIOS_TARGET),
        requires=list(data.get("requires", [])),
        manifest=data["manifest"],
        fixture_config=data.get("fixture_config", {}),
        steps=data["steps"],
        source=path,
    )
    validate_scenario(scn)
    return scn


def validate_scenario(scn: Scenario) -> None:
    """Loud config errors for an inconsistent scenario — caught at load,
    never at run."""
    where = f"{scn.source.name} ({scn.name!r})"
    # declared tools must be a subset of the manifest's extends.tools
    decl = set(scn.declared_tools)
    manifest_tools = set(_manifest_extends_tools(scn.manifest))
    if decl and not decl.issubset(manifest_tools):
        raise ValueError(
            f"{where}: declare_tools {sorted(decl)} not ⊆ manifest [plugin.extends].tools {sorted(manifest_tools)}"
        )
    if decl and not manifest_tools:
        raise ValueError(f"{where}: declare_tools set but manifest has no [plugin.extends].tools")
    # step shapes + capture ordering + expect_exit-is-last
    bound: set[str] = set()

    def _scan_captures_in_pattern(pat: Any, into: set[str]) -> None:
        if isinstance(pat, str) and pat.startswith("$capture:"):
            into.add(pat[len("$capture:"):])
        elif isinstance(pat, dict):
            for v in pat.values():
                _scan_captures_in_pattern(v, into)
        elif isinstance(pat, list):
            for v in pat:
                _scan_captures_in_pattern(v, into)

    def _refs_in_outgoing(frame: Any, into: set[str]) -> None:
        if isinstance(frame, str) and frame.startswith("$capture:"):
            into.add(frame[len("$capture:"):])
        elif isinstance(frame, dict):
            for v in frame.values():
                _refs_in_outgoing(v, into)
        elif isinstance(frame, list):
            for v in frame:
                _refs_in_outgoing(v, into)

    for i, step in enumerate(scn.steps):
        keys = set(step) & _STEP_KEYS
        if len(keys) != 1 and not (keys == {"expect_request"} or {"expect_request"} <= set(step)):
            # expect_request carries a sibling "reply"; that's fine.
            if not ({"expect_request", "reply"} == set(step)):
                raise ValueError(f"{where}: step {i} must have exactly one of {sorted(_STEP_KEYS)}, got {sorted(step)}")
        if "expect_exit" in step and i != len(scn.steps) - 1:
            raise ValueError(f"{where}: expect_exit must be the last step")
        # bind captures from expect* before they're referenced in send/reply
        if "expect" in step:
            _scan_captures_in_pattern(step["expect"], bound)
        if "expect_any_order" in step:
            for p in step["expect_any_order"]:
                _scan_captures_in_pattern(p, bound)
        if "expect_request" in step:
            _scan_captures_in_pattern(step["expect_request"], bound)
            if "reply" in step:
                refs: set[str] = set()
                _refs_in_outgoing(step["reply"], refs)
                missing = refs - bound
                if missing:
                    raise ValueError(f"{where}: step {i} reply references unbound captures {sorted(missing)}")
        if "send" in step:
            refs = set()
            _refs_in_outgoing(step["send"], refs)
            missing = refs - bound
            if missing:
                raise ValueError(f"{where}: step {i} send references unbound captures {sorted(missing)}")


# ── argv resolution for the four languages ──────────────────────────


def fixture_argv_for_lang(lang: str, repo_root: Path, explicit_fixture: str | None) -> list[str]:
    if lang == "rust":
        if not explicit_fixture:
            raise SystemExit("--lang rust requires --fixture <path-to-built-binary>")
        return [explicit_fixture]
    if explicit_fixture:
        # Allow overriding the scripting fixture path too (rare).
        if lang == "python":
            return [sys.executable, explicit_fixture]
        if lang == "typescript":
            return ["node", explicit_fixture]
        if lang == "php":
            return ["php", explicit_fixture]
    fx = repo_root / "conformance" / "fixtures"
    if lang == "python":
        return [sys.executable, str(fx / "python_fixture.py")]
    if lang == "typescript":
        return ["node", str(fx / "ts_fixture.mjs")]
    if lang == "php":
        return ["php", str(fx / "php_fixture.php")]
    raise SystemExit(f"unknown --lang {lang!r} (expected python|typescript|php|rust)")
