# Changelog — `nexoai` (Python plugin SDK)

All notable changes to the Python plugin SDK (PyPI distribution name
`nexoai`; importable module `nexo_plugin_sdk`). The format roughly follows
[Keep a Changelog](https://keepachangelog.com/); versions are tagged
`python-vX.Y.Z` in the [nexo-plugin-sdks](https://github.com/lordmacu/nexo-plugin-sdks)
mono-repo.

## 0.4.0 — 2026-05-11

Tool surface (nexo-rs sub-phase 31.10) — subprocess plugins written in
Python can now contribute agent tools, reaching parity with the Rust SDK
on `tool.invoke` (contract §4.1.1 + §5.t).

### Added
- `PluginAdapter(..., tools=[ToolDef(name, description, input_schema)])`
  — the catalog is advertised in the `initialize` reply's `tools` array
  (contract §4.1.1). Also settable via `.declare_tools([...])`.
- `PluginAdapter(..., on_tool=fn(inv))` / `on_tool_with_context=fn(inv, ctx)`
  (also `.on_tool(...)` / `.on_tool_with_context(...)`). The handler may
  be sync or async; the with-context variant wins when both are set.
  `ctx.broker` is the same broker handle `on_event` receives, so a tool
  body can `memory_recall` / `llm_complete` mid-invocation. `tool.invoke`
  runs on a detached task and is awaited by the shutdown drain.
- `ToolDef`, `ToolInvocation` (`plugin_id` / `tool_name` / `args` /
  `agent_id`), `ToolContext` (`broker` / `plugin_id`).
- `ToolInvocationError` band → JSON-RPC `-33401..-33405`: `ToolNotFound`
  / `ToolArgumentInvalid` (`.details`) / `ToolExecutionFailed` /
  `ToolUnavailable` (`.retry_after_ms`) / `ToolDenied`. An uncaught
  generic exception in a handler maps to `-33403`; a `tool.invoke` with
  no handler registered replies `-32601`.
- `text_result(text, is_error=False)` — the conventional
  `{content:[{type:"text",text}], is_error}` result shape.

### Changed
- The `initialize` reply now carries `tools: [...]` when a non-empty
  catalog was declared (omitted otherwise — additive).

### Notes
- Declaring a tool whose `name` is not in the manifest's
  `[plugin.extends].tools` raises `ManifestError` at construction —
  mirrors the host's hard-failure on the same drift.

## 0.3.0 — 2026-05-11

Child→host call surface (nexo-rs sub-phase 31.9) — the broker handle
passed into `on_event` can now call back into the host.

### Added
- `broker.memory_recall(agent_id=..., query=..., limit=10, timeout=None)`
  → `list[MemoryEntry]` — recall the agent's long-term memory
  (contract §5.2 `memory.recall`).
- `broker.llm_complete(provider=..., model=..., messages=..., max_tokens=4096,
  temperature=0.7, system_prompt=None, timeout=None)` → `LlmCompleteResult`
  — a (non-streaming) LLM completion (§5.3 `llm.complete`).
- `broker.llm_complete_stream(...)` → `LlmStream` — `async for chunk in
  stream` yields text chunks; `await stream.final_result()` gives the
  final `LlmCompleteResult` (`content` is `None` — the chunks were the
  content). (§5.3 `llm.complete.delta` streaming.)
- New types: `MemoryEntry`, `Message`, `TokenCount`, `LlmCompleteResult`,
  `LlmStream`; constant `DEFAULT_RPC_TIMEOUT` (30 s).
- `RpcError` hierarchy: `RpcServerError{code, message}` (host JSON-RPC
  error — `-32601` / `-32602` / `-32603`), `RpcTimeoutError{seconds}`,
  `RpcTransportError`, `RpcDecodeError`.

### Internal
- The dispatch loop gained child→host request multiplexing: an
  SDK-assigned request-id space, a pending-request registry, and routing
  of incoming responses + `llm.complete.delta` notifications to the
  awaiting caller — N concurrent `broker.event` handlers can each await
  their own host call over the one stdio pipe without blocking the
  reader. On shutdown/EOF, outstanding host calls are abandoned with
  `RpcTransportError` so handlers don't hang.

## 0.2.0 — 2026-05-11

Robustness parity with the TypeScript and PHP SDKs (nexo-rs sub-phase 31.4.c).

### Added
- `stdout_guard` module — a line-buffering `sys.stdout` proxy that forwards
  complete JSON lines and diverts everything else (a stray `print`) to stderr
  tagged `[stdout-guard]`. On by default via `PluginAdapter(enable_stdout_guard=True)`;
  also exported standalone (`install_stdout_guard` / `uninstall_stdout_guard` /
  `is_stdout_guard_installed` / `STDOUT_GUARD_MARKER`).
- `wire` module — `MAX_FRAME_BYTES`, `JSONRPC_VERSION`, `serialize_frame`,
  `build_response`, `build_error_response`, `build_notification`.
- `PluginAdapter` kwargs `max_frame_bytes` (default 1 MiB — oversized inbound
  frame → `WireError` log, dispatch continues) and `handle_process_signals`
  (default True — SIGTERM/SIGINT → graceful drain → exit 0, via
  `loop.add_signal_handler` with a `signal.signal` fallback).
- `manifest` now validates `plugin.id` against `^[a-z][a-z0-9_]{0,31}$`.
- `EventHandler` / `ShutdownHandler` type aliases exported.
- `py.typed` marker (PEP 561).
- `pyproject.toml` publish metadata: classifiers, keywords, repository URLs.

### Changed
- Stdin reader is now fully async (`loop.connect_read_pipe` + `asyncio.StreamReader`)
  instead of a threadpool `sys.stdin.readline` — no worker thread, clean
  signal cancellation. `get_event_loop` → `get_running_loop`.
- `BrokerSender` takes an injected line-writer (the captured original stdout),
  so blessed frames bypass the stdout guard.
- `PluginAdapter.run()` raises `PluginError` if invoked twice.

## 0.1.0 — 2026-05-03

Initial release (nexo-rs Phase 31.4). `PluginAdapter` async dispatch loop,
`BrokerSender.publish`, `Event` dataclass, `read_manifest` TOML reader, the
three exception types. Stdlib only (`tomllib`, `tomli` fallback on < 3.11).
