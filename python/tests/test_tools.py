"""Tests for host→child tool dispatch (contract §4.1.1 + §5.t).

Plain unit tests for the tool data types / error band, plus subprocess
tests that spawn a tiny driver registering ``declare_tools`` + an
``on_tool_with_context`` handler; the test plays the *host* — feeds an
``initialize`` (asserts the advertised ``tools`` array), then
``tool.invoke`` frames, asserting the ``result`` / ``error`` reply (and,
for the with-context case, serving the inner ``memory.recall`` the tool
makes mid-invocation). Mirrors the spawn-driver style of
``test_host_calls.py``.
"""

import json
import os
import select
import subprocess
import sys
import time
import unittest
from pathlib import Path

from nexo_plugin_sdk import (
    ManifestError,
    ToolArgumentInvalid,
    ToolDef,
    ToolDenied,
    ToolExecutionFailed,
    ToolNotFound,
    ToolUnavailable,
    text_result,
)
from nexo_plugin_sdk.adapter import _manifest_extends_tools

SDK_ROOT = Path(__file__).resolve().parent.parent

_TOOL_NAMES = [
    "tool_plugin_echo",
    "tool_plugin_recall",
    "tool_plugin_slow",
    "tool_plugin_fail",
    "tool_plugin_argbad",
    "tool_plugin_busy",
    "tool_plugin_nope",
    "tool_plugin_denied",
    "tool_plugin_boom",
    "tool_plugin_badjson",
]

DRIVER_TOOLS = (
    r"""
import asyncio
from nexo_plugin_sdk import (PluginAdapter, Event, ToolDef, text_result,
    ToolNotFound, ToolArgumentInvalid, ToolExecutionFailed, ToolUnavailable, ToolDenied)

MANIFEST = '''
[plugin]
id = "tool_plugin"
version = "0.1.0"
name = "ToolPlugin"
description = "fixture"
min_nexo_version = ">=0.1.0"

[plugin.extends]
tools = %s
'''

async def on_event(topic, event, broker):
    await broker.publish("plugin.inbound.out", Event.new("plugin.inbound.out", "tool_plugin",
        {"event_seen": event.payload.get("k")}))

async def on_tool(inv, ctx):
    name = inv.tool_name
    if name == "tool_plugin_echo":
        return {"echoed": inv.args, "agent": inv.agent_id, "plugin": inv.plugin_id}
    if name == "tool_plugin_recall":
        entries = await ctx.broker.memory_recall(agent_id="a", query=inv.args["q"])
        return {"recalled": [e.content for e in entries], "plugin_id": ctx.plugin_id}
    if name == "tool_plugin_slow":
        await asyncio.sleep(0.35)
        return text_result("slow done")
    if name == "tool_plugin_fail":
        raise ToolExecutionFailed("downstream 500")
    if name == "tool_plugin_argbad":
        raise ToolArgumentInvalid("bad url", details={"field": "url"})
    if name == "tool_plugin_busy":
        raise ToolUnavailable("rate limited", retry_after_ms=5000)
    if name == "tool_plugin_nope":
        raise ToolNotFound(name)
    if name == "tool_plugin_denied":
        raise ToolDenied("tenant blocked")
    if name == "tool_plugin_boom":
        raise ValueError("uncaught generic")
    if name == "tool_plugin_badjson":
        return {"x": object()}
    raise ToolNotFound(name)

TOOLS = [ToolDef(name=n, description="t", input_schema={"type": "object"}) for n in %s]

async def main():
    await PluginAdapter(manifest_toml=MANIFEST, on_event=on_event, tools=TOOLS,
        on_tool_with_context=on_tool).run()

asyncio.run(main())
"""
    % (json.dumps(_TOOL_NAMES), json.dumps(_TOOL_NAMES))
)

DRIVER_NO_TOOL = r"""
import asyncio
from nexo_plugin_sdk import PluginAdapter

MANIFEST = '''
[plugin]
id = "notool_plugin"
version = "0.1.0"
name = "NoTool"
description = "fixture"
min_nexo_version = ">=0.1.0"
'''

async def main():
    await PluginAdapter(manifest_toml=MANIFEST).run()

asyncio.run(main())
"""

DRIVER_BAD_MANIFEST = r"""
import asyncio
from nexo_plugin_sdk import PluginAdapter, ToolDef

MANIFEST = '''
[plugin]
id = "bad_plugin"
version = "0.1.0"
name = "Bad"
description = "fixture"
min_nexo_version = ">=0.1.0"

[plugin.extends]
tools = ["bad_plugin_known"]
'''

async def main():
    await PluginAdapter(manifest_toml=MANIFEST,
        tools=[ToolDef(name="bad_plugin_unknown", description="x")],
        on_tool=lambda inv: {}).run()

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


def _initialize_frame(req_id: int = 1) -> dict:
    return {"jsonrpc": "2.0", "id": req_id, "method": "initialize", "params": {}}


def _tool_invoke_frame(req_id: int, tool_name: str, *, args=None, agent_id=None, plugin_id="tool_plugin") -> dict:
    params: dict = {"plugin_id": plugin_id, "tool_name": tool_name}
    if args is not None:
        params["args"] = args
    if agent_id is not None:
        params["agent_id"] = agent_id
    return {"jsonrpc": "2.0", "id": req_id, "method": "tool.invoke", "params": params}


def _event_frame(topic: str, payload: dict) -> dict:
    return {"jsonrpc": "2.0", "method": "broker.event",
            "params": {"topic": topic, "event": {"topic": topic, "source": "host", "payload": payload}}}


def _shutdown_frame(req_id: int = 99) -> dict:
    return {"jsonrpc": "2.0", "id": req_id, "method": "shutdown"}


class ToolTypeTests(unittest.TestCase):
    def test_tool_def_to_json(self):
        d = ToolDef(name="p_x", description="desc", input_schema={"type": "object"}).to_json()
        self.assertEqual(d, {"name": "p_x", "description": "desc", "input_schema": {"type": "object"}})

    def test_tool_def_default_schema_is_empty_object(self):
        self.assertEqual(ToolDef(name="p_x", description="d").to_json()["input_schema"], {})

    def test_error_codes(self):
        self.assertEqual(ToolNotFound("p_x").code, -33401)
        self.assertEqual(ToolArgumentInvalid("bad").code, -33402)
        self.assertEqual(ToolExecutionFailed("oops").code, -33403)
        self.assertEqual(ToolUnavailable("busy").code, -33404)
        self.assertEqual(ToolDenied("no").code, -33405)

    def test_error_messages_have_grep_prefixes(self):
        self.assertEqual(str(ToolNotFound("p_x")), "tool not found: p_x")
        self.assertEqual(str(ToolArgumentInvalid("u")), "invalid argument: u")
        self.assertEqual(str(ToolExecutionFailed("e")), "execution failed: e")
        self.assertEqual(str(ToolUnavailable("b")), "unavailable: b")
        self.assertEqual(str(ToolDenied("d")), "denied: d")

    def test_error_data(self):
        self.assertIsNone(ToolNotFound("p_x").error_data())
        self.assertIsNone(ToolArgumentInvalid("u").error_data())
        self.assertEqual(ToolArgumentInvalid("u", details={"f": 1}).error_data(), {"details": {"f": 1}})
        self.assertIsNone(ToolUnavailable("b").error_data())
        self.assertEqual(ToolUnavailable("b", retry_after_ms=500).error_data(), {"retry_after_ms": 500})

    def test_text_result_shape(self):
        self.assertEqual(text_result("hi"), {"content": [{"type": "text", "text": "hi"}], "is_error": False})
        self.assertTrue(text_result("bad", is_error=True)["is_error"])

    def test_manifest_extends_tools_parsing(self):
        self.assertIsNone(_manifest_extends_tools({"plugin": {"id": "p"}}))
        self.assertEqual(_manifest_extends_tools({"plugin": {"extends": {"tools": ["a", "b"]}}}), ["a", "b"])
        with self.assertRaises(ManifestError):
            _manifest_extends_tools({"plugin": {"extends": {"tools": "nope"}}})

    def test_constructor_rejects_drift(self):
        from nexo_plugin_sdk import PluginAdapter
        manifest = (
            '[plugin]\nid = "p"\nversion = "0.1.0"\nname = "P"\ndescription = "d"\n'
            'min_nexo_version = ">=0.1.0"\n[plugin.extends]\ntools = ["p_known"]\n'
        )
        with self.assertRaises(ManifestError):
            PluginAdapter(manifest_toml=manifest, tools=[ToolDef(name="p_unknown", description="x")],
                          on_tool=lambda inv: {})
        # the well-named one is fine
        a = PluginAdapter(manifest_toml=manifest, tools=[ToolDef(name="p_known", description="x")],
                          on_tool=lambda inv: {})
        self.assertEqual(a.manifest["plugin"]["id"], "p")


class ToolHandshakeTests(unittest.TestCase):
    def test_initialize_advertises_declared_tools(self):
        proc = _spawn(self, DRIVER_TOOLS)
        _write_frame(proc, _initialize_frame())
        reply = _read_frame(proc)
        tools = reply["result"]["tools"]
        self.assertEqual([t["name"] for t in tools], _TOOL_NAMES)
        self.assertTrue(all(t["description"] == "t" and t["input_schema"] == {"type": "object"} for t in tools))
        _write_frame(proc, _shutdown_frame())
        self.assertTrue(_read_frame(proc)["result"]["ok"])
        _expect_exit_0(proc)

    def test_initialize_omits_tools_when_none_declared(self):
        proc = _spawn(self, DRIVER_NO_TOOL)
        _write_frame(proc, _initialize_frame())
        reply = _read_frame(proc)
        self.assertNotIn("tools", reply["result"])
        _write_frame(proc, _shutdown_frame())
        self.assertTrue(_read_frame(proc)["result"]["ok"])
        _expect_exit_0(proc)


class ToolInvokeTests(unittest.TestCase):
    def _spawn_tools(self):
        return _spawn(self, DRIVER_TOOLS)

    def test_happy_path(self):
        proc = self._spawn_tools()
        _write_frame(proc, _tool_invoke_frame(10, "tool_plugin_echo", args={"a": 1}, agent_id="shopper"))
        reply = _read_frame(proc)
        self.assertEqual(reply["id"], 10)
        self.assertEqual(reply["result"], {"echoed": {"a": 1}, "agent": "shopper", "plugin": "tool_plugin"})
        _write_frame(proc, _shutdown_frame())
        self.assertTrue(_read_frame(proc)["result"]["ok"])
        _expect_exit_0(proc)

    def test_args_omitted_decodes_to_none(self):
        proc = self._spawn_tools()
        _write_frame(proc, _tool_invoke_frame(11, "tool_plugin_echo"))
        reply = _read_frame(proc)
        self.assertEqual(reply["result"], {"echoed": None, "agent": None, "plugin": "tool_plugin"})
        _write_frame(proc, _shutdown_frame())
        _read_frame(proc)
        _expect_exit_0(proc)

    def _assert_error(self, tool_name, code, msg_substr, *, data=None, args=None):
        proc = self._spawn_tools()
        _write_frame(proc, _tool_invoke_frame(20, tool_name, args=args))
        reply = _read_frame(proc)
        self.assertEqual(reply["id"], 20)
        self.assertEqual(reply["error"]["code"], code)
        self.assertIn(msg_substr, reply["error"]["message"])
        if data is not None:
            self.assertEqual(reply["error"]["data"], data)
        else:
            self.assertNotIn("data", reply["error"])
        _write_frame(proc, _shutdown_frame())
        self.assertTrue(_read_frame(proc)["result"]["ok"])
        _expect_exit_0(proc)

    def test_tool_not_found(self):
        self._assert_error("tool_plugin_nope", -33401, "tool not found: tool_plugin_nope")

    def test_argument_invalid_carries_details(self):
        self._assert_error("tool_plugin_argbad", -33402, "invalid argument", data={"details": {"field": "url"}})

    def test_execution_failed(self):
        self._assert_error("tool_plugin_fail", -33403, "execution failed: downstream 500")

    def test_uncaught_exception_maps_to_execution_failed(self):
        self._assert_error("tool_plugin_boom", -33403, "execution failed")

    def test_unavailable_carries_retry_after(self):
        self._assert_error("tool_plugin_busy", -33404, "unavailable", data={"retry_after_ms": 5000})

    def test_denied(self):
        self._assert_error("tool_plugin_denied", -33405, "denied: tenant blocked")

    def test_non_serializable_result_maps_to_execution_failed(self):
        self._assert_error("tool_plugin_badjson", -33403, "not JSON-serializable")

    def test_no_handler_replies_method_not_found(self):
        proc = _spawn(self, DRIVER_NO_TOOL)
        _write_frame(proc, _tool_invoke_frame(30, "anything"))
        reply = _read_frame(proc)
        self.assertEqual(reply["error"]["code"], -32601)
        self.assertIn("tool.invoke", reply["error"]["message"])
        _write_frame(proc, _shutdown_frame())
        self.assertTrue(_read_frame(proc)["result"]["ok"])
        _expect_exit_0(proc)

    def test_with_context_handler_calls_host_mid_invocation(self):
        proc = self._spawn_tools()
        _write_frame(proc, _tool_invoke_frame(40, "tool_plugin_recall", args={"q": "prefs"}))
        inner = _read_frame(proc)
        self.assertEqual(inner["method"], "memory.recall")
        self.assertEqual(inner["params"], {"agent_id": "a", "query": "prefs", "limit": 10})
        _write_frame(proc, {"jsonrpc": "2.0", "id": inner["id"], "result": {"entries": [
            {"id": "m1", "agent_id": "a", "content": "concise"}]}})
        reply = _read_frame(proc)
        self.assertEqual(reply["id"], 40)
        self.assertEqual(reply["result"], {"recalled": ["concise"], "plugin_id": "tool_plugin"})
        _write_frame(proc, _shutdown_frame())
        self.assertTrue(_read_frame(proc)["result"]["ok"])
        _expect_exit_0(proc)

    def test_two_tool_invokes_resolve_out_of_order(self):
        proc = self._spawn_tools()
        _write_frame(proc, _tool_invoke_frame(1, "tool_plugin_slow"))   # slow
        _write_frame(proc, _tool_invoke_frame(2, "tool_plugin_echo", args="quick"))  # fast
        first = _read_frame(proc)
        second = _read_frame(proc)
        self.assertEqual(first["id"], 2)
        self.assertEqual(first["result"], {"echoed": "quick", "agent": None, "plugin": "tool_plugin"})
        self.assertEqual(second["id"], 1)
        self.assertEqual(second["result"], text_result("slow done"))
        _write_frame(proc, _shutdown_frame())
        self.assertTrue(_read_frame(proc)["result"]["ok"])
        _expect_exit_0(proc)

    def test_tool_invoke_alongside_broker_event(self):
        proc = self._spawn_tools()
        _write_frame(proc, _event_frame("plugin.in", {"k": "v1"}))
        _write_frame(proc, _tool_invoke_frame(50, "tool_plugin_echo", args="x"))
        frames = [_read_frame(proc), _read_frame(proc)]
        pub = next(f for f in frames if f.get("method") == "broker.publish")
        tool = next(f for f in frames if "result" in f)
        self.assertEqual(pub["params"]["event"]["payload"], {"event_seen": "v1"})
        self.assertEqual(tool["id"], 50)
        self.assertEqual(tool["result"], {"echoed": "x", "agent": None, "plugin": "tool_plugin"})
        _write_frame(proc, _shutdown_frame())
        self.assertTrue(_read_frame(proc)["result"]["ok"])
        _expect_exit_0(proc)

    def test_shutdown_waits_for_in_flight_tool(self):
        proc = self._spawn_tools()
        _write_frame(proc, _tool_invoke_frame(1, "tool_plugin_slow"))
        _write_frame(proc, _shutdown_frame(99))
        first = _read_frame(proc, timeout=5.0)
        self.assertEqual(first["id"], 1)             # tool result lands FIRST
        self.assertEqual(first["result"], text_result("slow done"))
        second = _read_frame(proc, timeout=5.0)
        self.assertEqual(second["id"], 99)           # then the shutdown ack
        self.assertTrue(second["result"]["ok"])
        _expect_exit_0(proc)


class ToolManifestCrossCheckTests(unittest.TestCase):
    def test_declared_tool_not_in_manifest_fails_fast(self):
        env = dict(os.environ)
        env["PYTHONPATH"] = str(SDK_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
        r = subprocess.run([sys.executable, "-c", DRIVER_BAD_MANIFEST],
                           input=b"", capture_output=True, env=env, timeout=10)
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("[plugin.extends].tools", r.stderr.decode("utf-8", "replace"))


if __name__ == "__main__":
    unittest.main()
