# nexo-plugin-sdk (TypeScript)

Child-side SDK for nexo subprocess plugins written
in TypeScript or plain JavaScript. Mirrors the Rust counterpart
in [`crates/microapp-sdk/`](https://github.com/lordmacu/nexo-rs/tree/main/crates/microapp-sdk) and the
Python counterpart in [`python/`](../python).
Same wire format ([`nexo-plugin-contract.md`](https://github.com/lordmacu/nexo-rs/blob/main/nexo-plugin-contract.md)),
different language.

The reference plugin template lives at
[the TypeScript plugin template](https://github.com/lordmacu/nexo-rs/tree/main/extensions/template-plugin-typescript) (or run `nexo plugin new --lang typescript`);
copy that directory to start a new plugin.

## Public API

```typescript
import {
  PluginAdapter,        // async dispatch loop
  BrokerSender,         // write-only handle to publish events back
  Event,                // value object mirror of the host's broker event
  PluginError,          // base exception
  ManifestError,        // raised when nexo-plugin.toml is malformed
  WireError,            // raised on malformed JSON-RPC frames or oversized lines
  installStdoutGuard,   // defensive guard installable independently
  parseManifest,        // standalone manifest TOML parser
  STDOUT_GUARD_MARKER,  // sentinel that prefixes diverted stderr lines
} from "nexo-plugin-sdk";
```

## Minimal example

```typescript
import { readFileSync } from "node:fs";
import { PluginAdapter, Event } from "nexo-plugin-sdk";

const MANIFEST = readFileSync("nexo-plugin.toml", "utf-8");

const adapter = new PluginAdapter({
  manifestToml: MANIFEST,
  onEvent: async (topic, event, broker) => {
    const out = Event.new(
      "plugin.inbound.my_kind",
      "my_plugin",
      { echoed: event.payload },
    );
    await broker.publish("plugin.inbound.my_kind", out);
  },
});

await adapter.run();
```

## Host calls

The `broker` handle passed into `onEvent` can call back into the host —
read the agent's long-term memory, or run an LLM completion via the
agent's configured providers:

```typescript
onEvent: async (topic, event, broker) => {
  const entries = await broker.memoryRecall({ agentId: "my_agent", query: "user prefers concise answers", limit: 5 });
  // entries: MemoryEntry[]  ({ id, agent_id, content, tags, concept_tags, created_at, memory_type })

  const result = await broker.llmComplete({
    provider: "minimax", model: "minimax-m2.5",
    messages: [{ role: "user", content: "summarize: ..." }],
    systemPrompt: "You answer concisely.",
  });
  // result.content, result.finish_reason, result.usage.{prompt_tokens, completion_tokens}

  const stream = broker.llmCompleteStream({ provider: "minimax", model: "minimax-m2.5", messages: [{ role: "user", content: "..." }] });
  for await (const chunk of stream) { /* str chunks, in order */ }
  const final = await stream.result;   // LlmCompleteResult; final.content is null (chunks were the content)
}
```

Failures throw an `RpcError`: `RpcServerError` (`.code` — `-32603` =
backend/not-configured, `-32602` = bad params, `-32601` = not wired
host-side), `RpcTimeoutError` (`.seconds`; default 30 s, override per
call with `timeoutMs`), `RpcTransportError`, `RpcDecodeError`.
Concurrent `onEvent` handlers can each have a host call in flight.

## Robustness defaults

The constructor defaults are picked to make the most common
plugin-author mistakes recoverable rather than fatal:

| Default | What it gives you |
|---------|-------------------|
| `enableStdoutGuard: true` | Stray `console.log("hi")` from your handler (or a chatty transitive dep) is diverted to stderr tagged with `[stdout-guard]` rather than corrupting the JSON-RPC frame stream the host parses. |
| `maxFrameBytes: 1 << 20` | Inbound JSON-RPC frames larger than 1 MiB are rejected with a `WireError` log; dispatch continues. Adversarial host cannot OOM the plugin via a single huge line. |
| `handleProcessSignals: true` | Ctrl-C / SIGTERM trigger a graceful shutdown — in-flight handler tasks are awaited (no mid-publish cancellation), then the process exits 0. |
| In-flight task drain on `shutdown` | Handlers spawned for `broker.event` are awaited via `Promise.allSettled([...inflight])` before the SDK replies `{ok: true}` to a host's `shutdown` request. Same idiom as the Python SDK's `_drain_inflight`. |

## What the daemon expects

| Method | Direction | Reply |
|--------|-----------|-------|
| `initialize` | host → child | `{ manifest, server_version }` automatically — the SDK reads + caches your manifest TOML at construction time. |
| `broker.event` (notification) | host → child | No JSON reply. Your `onEvent` handler runs in a detached task so the dispatch loop continues reading stdin while the handler awaits broker round-trips. |
| `shutdown` | host → child | `{ ok: true }` after draining in-flight tasks + invoking your `onShutdown` (if set). |
| `memory.recall` / `llm.complete` (+ `llm.complete.delta`) | child → host | Issued by `broker.memoryRecall` / `broker.llmComplete` / `broker.llmCompleteStream` — the SDK assigns the request id, awaits the matching reply, and multiplexes concurrent calls. |

Full spec: [`nexo-plugin-contract.md`](https://github.com/lordmacu/nexo-rs/blob/main/nexo-plugin-contract.md).

## Tests

```bash
cd typescript
npm install
npm run build
npm test
```

22 tests covering:
- Handshake: initialize reply, unknown method `-32601`, unknown
  notification silently ignored.
- Manifest validation: missing id, invalid TOML, id regex violation.
- Dispatch: handler invocation, non-blocking reader, in-flight drain
  on shutdown.
- Host calls: `memory.recall` / `llm.complete` happy paths, streaming,
  `-32603` → `RpcServerError`, per-call timeout → `RpcTimeoutError`,
  out-of-order multiplexing, shutdown-while-in-flight, unknown-response-id
  dropped, data-type parsers.
- Stdout guard: idempotent install, console.log diverted to stderr.
- Wire: oversized frame rejected with continued dispatch.
- Lifecycle: double `run()` rejects with PluginError.

## Phase tracking

- 31.5 (shipped) — child-side SDK + 13 tests + default-on stdout guard.
- 31.9 (shipped, 0.2.0) — child→host call surface:
  `broker.memoryRecall` / `broker.llmComplete` / `broker.llmCompleteStream`,
  the `RpcError` hierarchy, request multiplexing. Parity with the Rust
  child SDK. 22 tests.
- 31.5.b (deferred) — per-target TypeScript tarballs
  (`<id>-<version>-node20-x86_64-linux.tar.gz` etc.) for
  plugins that need native node addons.
- 31.5.c (deferred) — PHP SDK + template.
- npm publish deferred — once the API stabilizes after 31.5.c
  this package ships to npm as `nexo-plugin-sdk`. Until then
  plugin authors vendor it via `pack-tarball-typescript.sh`.
