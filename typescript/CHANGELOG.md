# Changelog — `nexo-plugin-sdk` (TypeScript)

Versions are tagged `ts-vX.Y.Z` in the
[nexo-plugin-sdks](https://github.com/lordmacu/nexo-plugin-sdks) mono-repo.

## 0.3.0 — 2026-05-11

Tool surface (nexo-rs sub-phase 31.10) — subprocess plugins written in
TypeScript can now contribute agent tools, reaching parity with the Rust
SDK on `tool.invoke` (contract §4.1.1 + §5.t).

### Added
- `PluginAdapterOptions.tools: ToolDef[]` (`{ name, description,
  inputSchema }`) — advertised in the `initialize` reply's `tools` array
  (serialized with the wire key `input_schema`, contract §4.1.1).
- `PluginAdapterOptions.onTool: (inv) => unknown | Promise<unknown>` /
  `onToolWithContext: (inv, ctx) => ...`. The with-context variant wins
  when both are set. `ctx.broker` is the same broker handle `onEvent`
  receives, so a tool body can `memoryRecall` / `llmComplete`
  mid-invocation. `tool.invoke` runs as a detached task awaited by the
  shutdown drain.
- `ToolDef`, `ToolInvocation` (`pluginId` / `toolName` / `args` /
  `agentId`), `ToolContext` (`broker` / `pluginId`), `toolDefToJson`.
- `ToolError` hierarchy → JSON-RPC `-33401..-33405`: `ToolNotFoundError`
  / `ToolArgumentInvalidError` (`.details`) / `ToolExecutionFailedError`
  / `ToolUnavailableError` (`.retryAfterMs`) / `ToolDeniedError`. An
  uncaught throw in a handler maps to `-33403`; a `tool.invoke` with no
  handler registered replies `-32601`.
- `textResult(text, isError = false)` — the conventional
  `{content:[{type:"text",text}], is_error}` result shape.

### Changed
- The `initialize` reply now carries `tools: [...]` when a non-empty
  catalog was declared (omitted otherwise — additive).

### Notes
- Declaring a tool whose `name` is not in the manifest's
  `[plugin.extends].tools` throws `ManifestError` from the constructor —
  mirrors the host's hard-failure on the same drift.

## 0.2.0 — 2026-05-11

Child→host call surface (nexo-rs sub-phase 31.9) — the broker handle
passed into `onEvent` can now call back into the host.

### Added
- `broker.memoryRecall({ agentId, query, limit?, timeoutMs? })`
  → `MemoryEntry[]` (contract §5.2 `memory.recall`).
- `broker.llmComplete({ provider, model, messages, maxTokens?, temperature?,
  systemPrompt?, timeoutMs? })` → `LlmCompleteResult` (§5.3 `llm.complete`).
- `broker.llmCompleteStream({ ... })` → `LlmStream` — `for await (const
  chunk of stream)` yields text chunks; `await stream.result` gives the
  final `LlmCompleteResult` (`content` is `null`). (§5.3 streaming.)
- Types `MemoryEntry`, `Message`, `TokenCount`, `LlmCompleteResult`,
  `LlmCompleteOptions`, `MemoryRecallOptions`, class `LlmStream`,
  parsers `parseMemoryEntry` / `parseLlmCompleteResult`, constant
  `DEFAULT_RPC_TIMEOUT_MS` (30 000).
- `RpcError` hierarchy: `RpcServerError` (`.code`), `RpcTimeoutError`
  (`.seconds`), `RpcTransportError`, `RpcDecodeError`.

### Internal
- The dispatch loop gained child→host request multiplexing: an
  SDK-assigned request-id space, a pending-request registry, and routing
  of incoming responses + `llm.complete.delta` notifications to the
  awaiting caller. On shutdown/EOF, outstanding host calls are abandoned
  with `RpcTransportError` so handlers don't hang.

## 0.1.0 — 2026-05-04

Initial release (nexo-rs Phase 31.5). ESM package, strict tsconfig. Public API:
`PluginAdapter`, `BrokerSender`, `Event`, `parseManifest`, `installStdoutGuard`
/ `uninstallStdoutGuard` / `isStdoutGuardInstalled` / `STDOUT_GUARD_MARKER`,
the JSON-RPC frame helpers (`buildResponse` / `buildErrorResponse` /
`serializeFrame` / `MAX_FRAME_BYTES` / `JSONRPC_VERSION`), the three exception
classes. Single runtime dep `smol-toml`. Robustness defaults on by default:
`enableStdoutGuard` (diverts non-JSON `process.stdout.write` to stderr),
`maxFrameBytes` (1 MiB), `handleProcessSignals` (SIGTERM/SIGINT graceful
shutdown), in-flight task drain on `shutdown`. `run()` throws on a second call.
