# Changelog — `nexo-plugin-sdk` (TypeScript)

Versions are tagged `ts-vX.Y.Z` in the
[nexo-plugin-sdks](https://github.com/lordmacu/nexo-plugin-sdks) mono-repo.

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
