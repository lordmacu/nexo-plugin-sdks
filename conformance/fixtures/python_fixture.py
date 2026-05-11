#!/usr/bin/env python3
"""Python conformance fixture — the `fixture_config` interpreter on top
of `nexo_plugin_sdk.PluginAdapter`. The mock-host spawns this with
`--config <json>` (the scenario's fixture_config + the manifest injected
under "manifest"), or `--print-capabilities`.

Wire output here must be byte-identical to the TS / PHP / Rust fixtures
for the same config — that's what the conformance kit checks.
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

# Import the SDK from the sibling `python/` package without needing it
# installed (CI installs it; local runs may not).
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "python"))

from nexo_plugin_sdk import (  # noqa: E402
    Event,
    PluginAdapter,
    ToolArgumentInvalid,
    ToolDef,
    ToolDenied,
    ToolExecutionFailed,
    ToolInvocation,
    ToolNotFound,
    ToolUnavailable,
    read_manifest,
    text_result,
)
from nexo_plugin_sdk.errors import RpcServerError, RpcTimeoutError  # noqa: E402

DEFAULT_CAPS = ["core", "memory", "llm", "tools"]


def _parse_args(argv: list[str]) -> tuple[str, dict]:
    if "--print-capabilities" in argv:
        return ("caps", {})
    if "--config" in argv:
        i = argv.index("--config")
        return ("run", json.loads(argv[i + 1]))
    raise SystemExit("python_fixture: expected --print-capabilities or --config <json>")


# ── tool behaviors ──────────────────────────────────────────────────


async def _apply_tool_behavior(behavior: str, inv: ToolInvocation, ctx) -> object:
    if behavior == "echo":
        return {"echoed": inv.args, "agent": inv.agent_id, "plugin": inv.plugin_id}
    if behavior.startswith("text:"):
        return text_result(behavior[len("text:"):])
    if behavior == "recall_then_return":
        assert ctx is not None, "recall_then_return needs use_tool_context: true"
        q = (inv.args or {}).get("q")
        entries = await ctx.broker.memory_recall(agent_id=inv.agent_id or "", query=q)
        return {"recalled": [e.content for e in entries], "plugin_id": ctx.plugin_id}
    if behavior.startswith("slow:"):
        await asyncio.sleep(int(behavior[len("slow:"):]) / 1000.0)
        return text_result("done")
    if behavior == "raise:33401" or behavior == "not_found":
        raise ToolNotFound(inv.tool_name)
    if behavior == "raise:33402":
        raise ToolArgumentInvalid("conformance bad arg", details={"field": "q"})
    if behavior == "raise:33403":
        raise ToolExecutionFailed("conformance exec fail")
    if behavior == "raise:33404":
        raise ToolUnavailable("conformance unavailable", retry_after_ms=5000)
    if behavior == "raise:33405":
        raise ToolDenied("conformance denied")
    if behavior == "raise_generic":
        raise ValueError("conformance generic error")
    if behavior == "return_unserializable":
        return {"handle": object()}
    raise ToolNotFound(inv.tool_name)


def _make_tool_handler(cfg: dict, use_ctx: bool):
    per_tool = cfg.get("tool_handlers") or {}
    default = cfg.get("tool_handler")

    def _behavior_for(name: str) -> str | None:
        return per_tool.get(name, default)

    if use_ctx:
        async def handler(inv: ToolInvocation, ctx):
            b = _behavior_for(inv.tool_name)
            if b is None:
                raise ToolNotFound(inv.tool_name)
            return await _apply_tool_behavior(b, inv, ctx)
        return handler

    async def handler(inv: ToolInvocation):  # type: ignore[no-redef]
        b = _behavior_for(inv.tool_name)
        if b is None:
            raise ToolNotFound(inv.tool_name)
        return await _apply_tool_behavior(b, inv, None)
    return handler


# ── event behaviors ─────────────────────────────────────────────────


def _make_event_handler(cfg: dict, plugin_id: str):
    eh = cfg.get("event_handler", "never")
    if eh == "never":
        return None

    async def handler(topic: str, event: Event, broker) -> None:
        payload = event.payload or {}
        if eh == "publish_marker":
            await broker.publish(
                "plugin.inbound.conf",
                Event.new("plugin.inbound.conf", plugin_id, {"event_seen": payload.get("k")}),
            )
        elif eh.startswith("slow_publish:"):
            await asyncio.sleep(int(eh[len("slow_publish:"):]) / 1000.0)
            await broker.publish(
                "plugin.inbound.conf",
                Event.new("plugin.inbound.conf", plugin_id, {"event_seen": payload.get("k")}),
            )
        elif eh == "host_call":
            op = payload.get("op")
            if op == "recall":
                entries = await broker.memory_recall(
                    agent_id="conf", query=payload.get("q", "q"), timeout=payload.get("timeout")
                )
                await broker.publish(
                    "plugin.inbound.conf",
                    Event.new("plugin.inbound.conf", plugin_id, {"recalled": [e.content for e in entries]}),
                )
            elif op == "recall_error":
                try:
                    await broker.memory_recall(agent_id="conf", query="q")
                except RpcServerError as e:
                    await broker.publish(
                        "plugin.inbound.conf",
                        Event.new("plugin.inbound.conf", plugin_id, {"rpc_error": {"code": e.code}}),
                    )
            elif op == "recall_timeout":
                try:
                    await broker.memory_recall(agent_id="conf", query="q", timeout=payload.get("timeout", 0.4))
                except RpcTimeoutError:
                    await broker.publish(
                        "plugin.inbound.conf",
                        Event.new("plugin.inbound.conf", plugin_id, {"timed_out": True}),
                    )
            elif op == "complete":
                r = await broker.llm_complete(
                    provider=payload.get("provider", "minimax"),
                    model=payload.get("model", "m"),
                    messages=[{"role": "user", "content": "hi"}],
                )
                await broker.publish(
                    "plugin.inbound.conf",
                    Event.new("plugin.inbound.conf", plugin_id,
                              {"content": r.content, "finish_reason": r.finish_reason, "ct": r.usage.completion_tokens}),
                )
            elif op == "complete_error":
                try:
                    await broker.llm_complete(provider="minimax", model="m", messages=[{"role": "user", "content": "hi"}])
                except RpcServerError as e:
                    await broker.publish(
                        "plugin.inbound.conf",
                        Event.new("plugin.inbound.conf", plugin_id, {"rpc_error": {"code": e.code}}),
                    )
            elif op == "stream":
                stream = broker.llm_complete_stream(provider="minimax", model="m", messages=[{"role": "user", "content": "hi"}])
                chunks = []
                async for c in stream:
                    chunks.append(c)
                final = await stream.final_result()
                await broker.publish(
                    "plugin.inbound.conf",
                    Event.new("plugin.inbound.conf", plugin_id,
                              {"joined": "".join(chunks), "content_is_none": final.content is None, "finish_reason": final.finish_reason}),
                )
        # else: unknown event_handler → no-op (a scenario bug, but don't crash)

    return handler


# ── main ────────────────────────────────────────────────────────────


async def _run(cfg: dict) -> None:
    manifest_toml = cfg["manifest"]
    use_ctx = cfg.get("use_tool_context", True)
    declare = list(cfg.get("declare_tools") or [])
    tools = [ToolDef(n, f"conformance tool {n}", {"type": "object"}) for n in declare]
    has_tool_handler = ("tool_handler" in cfg) or ("tool_handlers" in cfg)

    plugin_id = str(read_manifest(manifest_toml)["plugin"]["id"])
    kwargs = dict(
        manifest_toml=manifest_toml,
        server_version=cfg.get("server_version", "conformance-fixture-0"),
        on_shutdown=_noop_shutdown,
        tools=tools,
        handle_process_signals=False,
    )
    if has_tool_handler:
        if use_ctx:
            kwargs["on_tool_with_context"] = _make_tool_handler(cfg, True)
        else:
            kwargs["on_tool"] = _make_tool_handler(cfg, False)
    eh = _make_event_handler(cfg, plugin_id)
    if eh is not None:
        kwargs["on_event"] = eh
    await PluginAdapter(**kwargs).run()  # type: ignore[arg-type]


async def _noop_shutdown() -> None:
    return None


def main() -> int:
    mode, cfg = _parse_args(sys.argv[1:])
    if mode == "caps":
        caps = cfg.get("capabilities") if isinstance(cfg, dict) else None
        print(json.dumps(caps if caps else DEFAULT_CAPS))
        return 0
    caps_override = cfg.get("capabilities")  # accepted but only relevant in caps mode
    _ = caps_override
    asyncio.run(_run(cfg))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
