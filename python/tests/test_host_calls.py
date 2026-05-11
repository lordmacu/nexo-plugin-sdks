"""Tests for the child→host call surface (memory.recall / llm.complete).

Each subprocess test spawns a tiny driver that imports
``nexo_plugin_sdk.PluginAdapter`` and, in its ``on_event`` handler,
makes a host call (``broker.memory_recall`` / ``broker.llm_complete`` /
``broker.llm_complete_stream``). The test then acts as the *host*: it
feeds the child a ``broker.event``, reads the child's request frame off
the child's stdout, writes a canned response to the child's stdin, and
asserts what the handler did (it publishes a marker derived from the
host's reply). Mirrors the spawn-driver style of the other test files.
"""

import json
import os
import select
import subprocess
import sys
import time
import unittest
from pathlib import Path

from nexo_plugin_sdk.host import LlmCompleteResult, MemoryEntry

SDK_ROOT = Path(__file__).resolve().parent.parent

# One driver covers the single-call cases; the event payload's `op`
# field selects which host call the handler makes.
DRIVER_HOST_CALL = r"""
import asyncio, sys
from nexo_plugin_sdk import PluginAdapter, Event
from nexo_plugin_sdk.errors import RpcServerError, RpcTimeoutError, RpcTransportError

MANIFEST = '''
[plugin]
id = "host_call_plugin"
version = "0.1.0"
name = "HostCall"
description = "fixture"
min_nexo_version = ">=0.1.0"
'''

async def on_event(topic, event, broker):
    op = event.payload.get("op")
    try:
        if op == "recall":
            entries = await broker.memory_recall(agent_id="agent_x", query="prefs", limit=3)
            await broker.publish("plugin.inbound.out", Event.new("plugin.inbound.out", "host_call_plugin",
                {"got": [e.content for e in entries], "ids": [e.id for e in entries]}))
        elif op == "complete":
            r = await broker.llm_complete(provider="minimax", model="m", messages=[{"role": "user", "content": "hi"}])
            await broker.publish("plugin.inbound.out", Event.new("plugin.inbound.out", "host_call_plugin",
                {"content": r.content, "finish_reason": r.finish_reason, "ct": r.usage.completion_tokens}))
        elif op == "stream":
            stream = broker.llm_complete_stream(provider="minimax", model="m", messages=[{"role": "user", "content": "hi"}])
            chunks = []
            async for c in stream:
                chunks.append(c)
            final = await stream.final_result()
            await broker.publish("plugin.inbound.out", Event.new("plugin.inbound.out", "host_call_plugin",
                {"chunks": chunks, "joined": "".join(chunks), "content_is_none": final.content is None,
                 "finish_reason": final.finish_reason}))
        elif op == "timeout":
            try:
                await broker.memory_recall(agent_id="a", query="q", timeout=0.4)
            except RpcTimeoutError as e:
                await broker.publish("plugin.inbound.out", Event.new("plugin.inbound.out", "host_call_plugin",
                    {"timed_out": True, "seconds": e.seconds}))
        elif op == "error":
            try:
                await broker.memory_recall(agent_id="a", query="q")
            except RpcServerError as e:
                await broker.publish("plugin.inbound.out", Event.new("plugin.inbound.out", "host_call_plugin",
                    {"rpc_error_code": e.code, "rpc_error_msg": e.message}))
        elif op == "inflight":
            # Never replied to — the adapter abandons it on shutdown.
            try:
                await broker.memory_recall(agent_id="a", query="q")
            except RpcTransportError as e:
                sys.stderr.write(f"HANDLER_GOT_TRANSPORT: {e}\n"); sys.stderr.flush()
    except Exception as e:
        sys.stderr.write(f"HANDLER_RAISED {type(e).__name__}: {e}\n"); sys.stderr.flush()

async def main():
    await PluginAdapter(manifest_toml=MANIFEST, on_event=on_event).run()

asyncio.run(main())
"""

# Two-event driver for the out-of-order multiplexing test: each event
# issues its own memory.recall with a distinct query echoed back.
DRIVER_MULTIPLEX = r"""
import asyncio
from nexo_plugin_sdk import PluginAdapter, Event

MANIFEST = '''
[plugin]
id = "mux_plugin"
version = "0.1.0"
name = "Mux"
description = "fixture"
min_nexo_version = ">=0.1.0"
'''

async def on_event(topic, event, broker):
    q = event.payload["q"]
    entries = await broker.memory_recall(agent_id="a", query=q)
    await broker.publish("plugin.inbound.out", Event.new("plugin.inbound.out", "mux_plugin",
        {"q": q, "got": [e.content for e in entries]}))

async def main():
    await PluginAdapter(manifest_toml=MANIFEST, on_event=on_event).run()

asyncio.run(main())
"""


def _kill_if_alive(proc) -> None:
    if proc.poll() is None:
        proc.kill()
        proc.wait()


def _spawn(self, driver: str) -> "subprocess.Popen[bytes]":
    env = dict(os.environ)
    env["PYTHONPATH"] = str(SDK_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    p = subprocess.Popen(
        [sys.executable, "-c", driver],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env,
    )
    p._framebuf = b""  # type: ignore[attr-defined]
    self.addCleanup(_kill_if_alive, p)
    return p


def _write_frame(proc, frame: dict) -> None:
    proc.stdin.write((json.dumps(frame) + "\n").encode("utf-8"))
    proc.stdin.flush()


def _read_frame(proc, timeout: float = 5.0) -> dict:
    """Read the next JSON-RPC line off the child's stdout (buffered, so a
    single OS read that grabs several frames is handled), or fail."""
    deadline = time.monotonic() + timeout
    while True:
        if b"\n" in proc._framebuf:
            line, _, proc._framebuf = proc._framebuf.partition(b"\n")
            return json.loads(line.decode("utf-8"))
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AssertionError(f"no frame from child within {timeout}s (buf={proc._framebuf!r})")
        r, _, _ = select.select([proc.stdout], [], [], min(0.1, remaining))
        if r:
            chunk = os.read(proc.stdout.fileno(), 65536)
            if not chunk:
                raise AssertionError(f"child stdout closed (buf={proc._framebuf!r})")
            proc._framebuf += chunk


def _expect_exit_0(proc) -> None:
    rc = proc.wait(timeout=5)
    if rc != 0:
        raise AssertionError(f"child exited {rc}; stderr={proc.stderr.read().decode('utf-8', 'replace')!r}")


def _event_frame(topic: str, payload: dict) -> dict:
    return {"jsonrpc": "2.0", "method": "broker.event",
            "params": {"topic": topic, "event": {"topic": topic, "source": "host", "payload": payload}}}


def _shutdown_frame(req_id: int = 99) -> dict:
    return {"jsonrpc": "2.0", "id": req_id, "method": "shutdown"}


class HostTypeTests(unittest.TestCase):
    def test_memory_entry_from_json(self):
        e = MemoryEntry.from_json({"id": "x1", "agent_id": "a", "content": "c", "tags": ["t"],
                                   "concept_tags": [], "created_at": "2026-01-01T00:00:00Z", "memory_type": None})
        self.assertEqual((e.id, e.agent_id, e.content, e.tags, e.memory_type), ("x1", "a", "c", ["t"], None))

    def test_llm_result_from_json(self):
        r = LlmCompleteResult.from_json({"content": "hi", "finish_reason": "stop",
                                         "usage": {"prompt_tokens": 5, "completion_tokens": 2}})
        self.assertEqual((r.content, r.finish_reason, r.usage.prompt_tokens, r.usage.completion_tokens), ("hi", "stop", 5, 2))
        r2 = LlmCompleteResult.from_json({"finish_reason": "length"})  # streamed final reply — no content
        self.assertIsNone(r2.content)
        self.assertEqual(r2.finish_reason, "length")


class MemoryRecallTests(unittest.TestCase):
    def test_recall_happy_path(self):
        proc = _spawn(self, DRIVER_HOST_CALL)
        _write_frame(proc, _event_frame("plugin.in", {"op": "recall"}))
        req = _read_frame(proc)
        self.assertEqual(req["method"], "memory.recall")
        self.assertEqual(req["params"], {"agent_id": "agent_x", "query": "prefs", "limit": 3})
        _write_frame(proc, {"jsonrpc": "2.0", "id": req["id"], "result": {"entries": [
            {"id": "m1", "agent_id": "agent_x", "content": "concise", "tags": ["pref"], "concept_tags": [],
             "created_at": "2026-01-01T00:00:00Z", "memory_type": None}]}})
        out = _read_frame(proc)
        self.assertEqual(out["method"], "broker.publish")
        self.assertEqual(out["params"]["event"]["payload"], {"got": ["concise"], "ids": ["m1"]})
        _write_frame(proc, _shutdown_frame())
        self.assertTrue(_read_frame(proc)["result"]["ok"])
        _expect_exit_0(proc)

    def test_server_error_surfaces_as_rpc_server_error(self):
        proc = _spawn(self, DRIVER_HOST_CALL)
        _write_frame(proc, _event_frame("plugin.in", {"op": "error"}))
        req = _read_frame(proc)
        self.assertEqual(req["method"], "memory.recall")
        _write_frame(proc, {"jsonrpc": "2.0", "id": req["id"], "error": {"code": -32603, "message": "memory not configured"}})
        out = _read_frame(proc)
        self.assertEqual(out["params"]["event"]["payload"], {"rpc_error_code": -32603, "rpc_error_msg": "memory not configured"})
        _write_frame(proc, _shutdown_frame())
        self.assertTrue(_read_frame(proc)["result"]["ok"])
        _expect_exit_0(proc)

    def test_timeout_surfaces_as_rpc_timeout_error(self):
        proc = _spawn(self, DRIVER_HOST_CALL)
        _write_frame(proc, _event_frame("plugin.in", {"op": "timeout"}))
        req = _read_frame(proc)
        self.assertEqual(req["method"], "memory.recall")
        # Deliberately never reply — the 0.4s per-call timeout fires.
        out = _read_frame(proc, timeout=5.0)
        self.assertTrue(out["params"]["event"]["payload"]["timed_out"])
        self.assertAlmostEqual(out["params"]["event"]["payload"]["seconds"], 0.4, places=2)
        # Dispatch still alive — a late reply for the timed-out id is dropped; shutdown still works.
        _write_frame(proc, {"jsonrpc": "2.0", "id": req["id"], "result": {"entries": []}})
        _write_frame(proc, _shutdown_frame())
        self.assertTrue(_read_frame(proc)["result"]["ok"])
        _expect_exit_0(proc)

    def test_late_response_for_unknown_id_dropped(self):
        proc = _spawn(self, DRIVER_HOST_CALL)
        _write_frame(proc, {"jsonrpc": "2.0", "id": 9999, "result": {"entries": []}})  # never issued — must not crash
        _write_frame(proc, _shutdown_frame())
        self.assertTrue(_read_frame(proc)["result"]["ok"])
        rc = proc.wait(timeout=5)
        self.assertEqual(rc, 0)
        self.assertIn("unknown/expired request id", proc.stderr.read().decode("utf-8"))


class LlmCompleteTests(unittest.TestCase):
    def test_complete_non_stream_happy_path(self):
        proc = _spawn(self, DRIVER_HOST_CALL)
        _write_frame(proc, _event_frame("plugin.in", {"op": "complete"}))
        req = _read_frame(proc)
        self.assertEqual(req["method"], "llm.complete")
        self.assertEqual(req["params"]["provider"], "minimax")
        self.assertNotIn("stream", req["params"])
        _write_frame(proc, {"jsonrpc": "2.0", "id": req["id"], "result": {
            "content": "one line.", "finish_reason": "stop", "usage": {"prompt_tokens": 3, "completion_tokens": 4}}})
        out = _read_frame(proc)
        self.assertEqual(out["params"]["event"]["payload"], {"content": "one line.", "finish_reason": "stop", "ct": 4})
        _write_frame(proc, _shutdown_frame())
        self.assertTrue(_read_frame(proc)["result"]["ok"])
        _expect_exit_0(proc)

    def test_complete_streaming_happy_path(self):
        proc = _spawn(self, DRIVER_HOST_CALL)
        _write_frame(proc, _event_frame("plugin.in", {"op": "stream"}))
        req = _read_frame(proc)
        self.assertEqual(req["method"], "llm.complete")
        self.assertTrue(req["params"]["stream"])
        rid = req["id"]
        for c in ("hel", "lo ", "world"):
            _write_frame(proc, {"jsonrpc": "2.0", "method": "llm.complete.delta", "params": {"request_id": rid, "chunk": c}})
        _write_frame(proc, {"jsonrpc": "2.0", "id": rid, "result": {"finish_reason": "stop", "usage": {"prompt_tokens": 1, "completion_tokens": 3}}})
        out = _read_frame(proc)
        p = out["params"]["event"]["payload"]
        self.assertEqual(p["chunks"], ["hel", "lo ", "world"])
        self.assertEqual(p["joined"], "hello world")
        self.assertTrue(p["content_is_none"])
        self.assertEqual(p["finish_reason"], "stop")
        _write_frame(proc, _shutdown_frame())
        self.assertTrue(_read_frame(proc)["result"]["ok"])
        _expect_exit_0(proc)


class MultiplexingTests(unittest.TestCase):
    def test_two_in_flight_calls_resolve_out_of_order(self):
        proc = _spawn(self, DRIVER_MULTIPLEX)
        _write_frame(proc, _event_frame("plugin.in", {"q": "alpha"}))
        _write_frame(proc, _event_frame("plugin.in", {"q": "beta"}))
        req1 = _read_frame(proc)
        req2 = _read_frame(proc)
        qs = {req1["params"]["query"]: req1["id"], req2["params"]["query"]: req2["id"]}
        self.assertEqual(set(qs), {"alpha", "beta"})
        # Reply to BETA first, then ALPHA.
        _write_frame(proc, {"jsonrpc": "2.0", "id": qs["beta"], "result": {"entries": [{"id": "b", "agent_id": "a", "content": "B-content"}]}})
        _write_frame(proc, {"jsonrpc": "2.0", "id": qs["alpha"], "result": {"entries": [{"id": "a", "agent_id": "a", "content": "A-content"}]}})
        out_a = _read_frame(proc)
        out_b = _read_frame(proc)
        by_q = {o["params"]["event"]["payload"]["q"]: o["params"]["event"]["payload"]["got"] for o in (out_a, out_b)}
        self.assertEqual(by_q, {"alpha": ["A-content"], "beta": ["B-content"]})
        _write_frame(proc, _shutdown_frame())
        self.assertTrue(_read_frame(proc)["result"]["ok"])
        _expect_exit_0(proc)


class ShutdownWhileInFlightTests(unittest.TestCase):
    def test_shutdown_while_host_call_in_flight_does_not_hang(self):
        proc = _spawn(self, DRIVER_HOST_CALL)
        _write_frame(proc, _event_frame("plugin.in", {"op": "inflight"}))
        req = _read_frame(proc)
        self.assertEqual(req["method"], "memory.recall")
        # Don't reply — shut down. The adapter must abandon the in-flight
        # call (handler gets RpcTransportError) and reply {ok:true}
        # promptly, not hang waiting for a response that won't come.
        _write_frame(proc, _shutdown_frame())
        self.assertTrue(_read_frame(proc, timeout=5.0)["result"]["ok"])
        rc = proc.wait(timeout=5)
        self.assertEqual(rc, 0)
        self.assertIn("HANDLER_GOT_TRANSPORT", proc.stderr.read().decode("utf-8"))


if __name__ == "__main__":
    unittest.main()
