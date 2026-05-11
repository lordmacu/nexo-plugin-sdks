# Changelog — `nexo/plugin-sdk` (PHP)

Versions are tagged `php-vX.Y.Z` in the
[nexo-plugin-sdks](https://github.com/lordmacu/nexo-plugin-sdks) mono-repo and
mirrored (as `vX.Y.Z`) to
[lordmacu/nexo-plugin-sdk-php](https://github.com/lordmacu/nexo-plugin-sdk-php),
which Packagist tracks.

## 0.2.0 — 2026-05-11

Child→host call surface (nexo-rs sub-phase 31.9) — the broker handle
passed into `onEvent` can now call back into the host.

### Added
- `$broker->memoryRecall(['agentId'=>..., 'query'=>..., 'limit'?=>10, 'timeoutSec'?=>30.0])`
  → `MemoryEntry[]` (contract §5.2 `memory.recall`).
- `$broker->llmComplete(['provider'=>..., 'model'=>..., 'messages'=>[...], 'maxTokens'?=>4096,
  'temperature'?=>0.7, 'systemPrompt'?=>..., 'timeoutSec'?=>30.0])` → `LlmCompleteResult`
  (§5.3 `llm.complete`).
- `$broker->llmCompleteStream($opts, fn(string $chunk) => ...)` →
  `LlmCompleteResult` — `$onChunk` is invoked once per text chunk, in
  order; the return value is the final result (`content` is `null`).
  (§5.3 `llm.complete.delta` streaming.)
- Classes `MemoryEntry`, `Message`, `TokenCount`, `LlmCompleteResult`,
  `PendingRegistry` (and the `RpcError` hierarchy: `RpcServerError`
  [`getCode()` + `$serverMessage`], `RpcTimeoutError` [`$seconds`],
  `RpcTransportError`, `RpcDecodeError`) — loaded via a `files` autoload
  entry (`src/Host.php`). `Wire::DEFAULT_RPC_TIMEOUT` (30.0 s).

### Internal
- `PluginAdapter` + `BrokerSender` gained child→host request
  multiplexing: an SDK-assigned request-id space (`PendingRegistry`),
  Fiber-suspend in the calling handler until the response lands, and
  routing of incoming responses + `llm.complete.delta` notifications to
  the awaiting call. N concurrent `onEvent` handlers can each have a call
  in flight. On shutdown/EOF, outstanding host calls are abandoned with
  `RpcTransportError` so handler Fibers terminate (otherwise `drain()`
  would spin forever).

## 0.1.0 — 2026-05-04

Initial release (nexo-rs Phase 31.5.c). PSR-4 `Nexo\Plugin\Sdk\`, PHP ≥ 8.1
(Fibers). Public API: `PluginAdapter`, `BrokerSender`, `Event`, `Manifest`,
`Wire`, `Scheduler`, `StdoutGuard`, the three exception classes. Single runtime
dep `yosymfony/toml`. Robustness defaults on by default: `enableStdoutGuard`
(`ob_start` diverts non-JSON `echo`/`print`/`printf`/`var_dump` to stderr;
direct `fwrite(STDOUT, ...)` bypasses — used deliberately for blessed frames),
`maxFrameBytes` (1 MiB), `handleProcessSignals` (`pcntl_async_signals`),
in-flight Fiber drain on `shutdown` via `Scheduler::drain()`. `run()` throws on
a second call.
