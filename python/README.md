# nexoai — nexo plugin SDK (Python)

Child-side SDK for nexo subprocess plugins written in Python 3.10+.
Mirrors the Rust counterpart in
[`crates/microapp-sdk/`](https://github.com/lordmacu/nexo-rs/tree/main/crates/microapp-sdk), the TypeScript
counterpart in [`typescript/`](../typescript) and
the PHP counterpart in [`php/`](../php) — same wire
format ([`nexo-plugin-contract.md`](https://github.com/lordmacu/nexo-rs/blob/main/nexo-plugin-contract.md)),
different language.

```bash
pip install nexoai
```

> **Distribution name vs import name** — the PyPI package is `nexoai`
> (the `nexo-plugin-sdk` name was already taken); the importable module is
> `nexo_plugin_sdk` (`from nexo_plugin_sdk import PluginAdapter`).

The reference plugin template lives at
[the Python plugin template](https://github.com/lordmacu/nexo-rs/tree/main/extensions/template-plugin-python) (or run `nexo plugin new --lang python`);
copy that directory to start a new plugin.

## Public API

```python
from nexo_plugin_sdk import (
    PluginAdapter,            # async dispatch loop
    BrokerSender,             # write-only handle to publish events back
    Event,                    # dataclass mirror of the host's broker event
    EventHandler,             # type alias: Callable[[str, Event, BrokerSender], Awaitable[None]]
    ShutdownHandler,          # type alias: Callable[[], Awaitable[None]]
    PluginError,              # base exception
    ManifestError,            # raised when nexo-plugin.toml is malformed
    WireError,                # raised on malformed / oversized JSON-RPC frames
    read_manifest,            # standalone manifest TOML parser + validator
    install_stdout_guard,     # defensive guard installable independently
    uninstall_stdout_guard,
    is_stdout_guard_installed,
    STDOUT_GUARD_MARKER,      # sentinel prefixed onto diverted stdout lines
    MAX_FRAME_BYTES,          # default inbound frame cap (1 MiB)
    JSONRPC_VERSION,
    serialize_frame, build_response, build_error_response, build_notification,
)
```

## Minimal example

```python
import asyncio
from nexo_plugin_sdk import PluginAdapter, Event

MANIFEST = open("nexo-plugin.toml").read()

async def on_event(topic: str, event: Event, broker) -> None:
    out = Event.new(
        "plugin.inbound.my_kind",
        "my_plugin",
        {"echoed": event.payload},
    )
    await broker.publish("plugin.inbound.my_kind", out)

async def main() -> None:
    adapter = PluginAdapter(manifest_toml=MANIFEST, on_event=on_event)
    await adapter.run()

if __name__ == "__main__":
    asyncio.run(main())
```

## Host calls

The `broker` handle passed into `on_event` can call back into the host
— read the agent's long-term memory, or run an LLM completion via the
agent's configured providers:

```python
async def on_event(topic, event, broker):
    entries = await broker.memory_recall(agent_id="my_agent", query="user prefers concise answers", limit=5)
    # entries: list[MemoryEntry]  (id, agent_id, content, tags, concept_tags, created_at, memory_type)

    result = await broker.llm_complete(
        provider="minimax", model="minimax-m2.5",
        messages=[{"role": "user", "content": "summarize: ..."}],
        system_prompt="You answer concisely.",
    )
    # result.content, result.finish_reason, result.usage.{prompt_tokens, completion_tokens}

    stream = broker.llm_complete_stream(provider="minimax", model="minimax-m2.5",
                                        messages=[{"role": "user", "content": "..."}])
    async for chunk in stream:
        ...                                # str chunks, in order
    final = await stream.final_result()    # LlmCompleteResult; final.content is None (chunks were the content)
```

Failures raise an `RpcError`: `RpcServerError` (`.code` — `-32603` =
backend/not-configured, `-32602` = bad params, `-32601` = not wired
host-side), `RpcTimeoutError` (`.seconds`; default 30 s, override per
call with `timeout=`), `RpcTransportError`, `RpcDecodeError`. Concurrent
`broker.event` handlers can each have a host call in flight at once.

## Robustness defaults

The `PluginAdapter` constructor defaults are picked to make the most
common plugin-author mistakes recoverable rather than fatal — matching
the TypeScript and PHP SDKs:

| Default | What it gives you |
|---------|-------------------|
| `enable_stdout_guard=True` | A stray `print("hi")` from your handler (or a chatty transitive dep) is diverted to stderr tagged `[stdout-guard]` rather than corrupting the JSON-RPC frame stream the host parses. The SDK's own replies and `broker.publish` frames write through the captured *original* stdout, so they bypass the guard. |
| `max_frame_bytes=1<<20` | Inbound JSON-RPC frames larger than 1 MiB are rejected with a `WireError` log; dispatch continues. An adversarial host cannot OOM the plugin via a single huge line. |
| `handle_process_signals=True` | SIGTERM / SIGINT trigger a graceful shutdown — in-flight handler tasks are awaited (no mid-publish cancellation), then the process exits 0 instead of the default `-15`. Uses `loop.add_signal_handler`, falling back to `signal.signal` where that is unavailable (Windows ProactorEventLoop / non-main-thread). |
| In-flight drain on `shutdown` | Handlers spawned for `broker.event` are awaited via `asyncio.gather(...)` before the SDK replies `{ok: true}` to a host `shutdown` request. Same idiom as the TypeScript SDK's `Promise.allSettled([...inflight])` and the PHP SDK's `Scheduler::drain()`. |

The stdin reader is fully async (`loop.connect_read_pipe` + an
`asyncio.StreamReader`) — no threadpool worker — so signal-driven
cancellation is clean.

### Stdout guard limitation

The guard replaces `sys.stdout` with a line-buffering proxy, so it only
intercepts the text-stream API (`print`, `sys.stdout.write`). A C
extension or subprocess that writes to file descriptor 1 directly
bypasses it. Plugin authors who need stdout output should use
`print()` / `sys.stdout.write()`; those are guarded.

## What the daemon expects

| Method | Direction | Reply |
|--------|-----------|-------|
| `initialize` | host → child | `{ manifest, server_version }` automatically — the SDK reads + validates your manifest TOML at construction time (incl. the `^[a-z][a-z0-9_]{0,31}$` `plugin.id` slug regex the host enforces). |
| `broker.event` (notification) | host → child | No JSON reply. Your `on_event` handler runs in a detached task so the dispatch loop continues reading stdin while the handler awaits broker round-trips. |
| `shutdown` | host → child | `{ ok: true }` after draining in-flight handler tasks + invoking your `on_shutdown` (if set). |
| `memory.recall` / `llm.complete` (+ `llm.complete.delta`) | child → host | Issued by `broker.memory_recall` / `broker.llm_complete` / `broker.llm_complete_stream` — the SDK assigns the request id, awaits the matching reply, and multiplexes concurrent calls. |

Full spec: [`nexo-plugin-contract.md`](https://github.com/lordmacu/nexo-rs/blob/main/nexo-plugin-contract.md).

## Tests

```bash
cd python
PYTHONPATH=. python3 -m unittest discover -v tests/
```

31 tests covering: the handshake (initialize reply, unknown method
`-32601`, unknown notification ignored), manifest validation (missing
id, invalid TOML, id-regex violation), dispatch (handler invocation,
non-blocking reader, in-flight drain, oversized frame rejected with
continued dispatch), the stdout guard (idempotent install, install /
uninstall round-trip, divert vs passthrough, chunked print-style
writes, partial-line flush, attr delegation, handler-print diverted
while the blessed frame stays clean), the `broker.publish` back
channel, host calls (`memory.recall` / `llm.complete` happy paths,
streaming, `-32603`, per-call timeout, out-of-order multiplexing,
shutdown-while-in-flight, unknown-response-id dropped), and lifecycle
(double `run()` rejected, SIGTERM exits 0, SIGTERM drains an in-flight
handler before exiting).

## Phase tracking

- 31.4 (shipped) — child-side SDK + 6 tests.
- 31.4.c (shipped, this package) — robustness parity with the
  TypeScript/PHP SDKs: default-on stdout guard, 1 MiB inbound frame
  cap, SIGTERM/SIGINT graceful drain, async stdin reader, `plugin.id`
  regex validation, 21 tests, `pyproject.toml` publish-ready.
- 31.4.b (deferred) — per-target Python tarballs (`pyXY-<triple>`
  targets) for plugins that need native extensions.
- 31.8 (shipped) — extracted to the
  [`nexo-plugin-sdks`](https://github.com/lordmacu/nexo-plugin-sdks)
  mono-repo and published to PyPI as `nexoai` (the `nexo-plugin-sdk`
  name was taken). Tagged `python-vX.Y.Z`.
- 31.9 (shipped, 0.3.0) — child→host call surface: `broker.memory_recall`
  / `broker.llm_complete` / `broker.llm_complete_stream`, the
  `RpcError` hierarchy, request multiplexing. Parity with the Rust
  child SDK. 31 tests.
