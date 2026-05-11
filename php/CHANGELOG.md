# Changelog — `nexo/plugin-sdk` (PHP)

Versions are tagged `php-vX.Y.Z` in the
[nexo-plugin-sdks](https://github.com/lordmacu/nexo-plugin-sdks) mono-repo and
mirrored (as `vX.Y.Z`) to
[lordmacu/nexo-plugin-sdk-php](https://github.com/lordmacu/nexo-plugin-sdk-php),
which Packagist tracks.

## 0.3.0 — 2026-05-11

Tool surface (nexo-rs sub-phase 31.10) — subprocess plugins written in
PHP can now contribute agent tools, reaching parity with the Rust SDK on
`tool.invoke` (contract §4.1.1 + §5.t).

### Added
- `PluginAdapter(['tools' => [new ToolDef($name, $description, $inputSchema)], ...])`
  — advertised in the `initialize` reply's `tools` array (serialized
  with the wire key `input_schema`, contract §4.1.1).
- `'onTool' => fn(ToolInvocation $inv)` / `'onToolWithContext' => fn(ToolInvocation $inv, ToolContext $ctx)`.
  The with-context variant wins when both are set. `$ctx->broker` is the
  same broker handle `onEvent` receives, so a tool body can
  `memoryRecall` / `llmComplete` mid-invocation. The handler runs in a
  Fiber tracked by the scheduler's drain set, so `shutdown` waits for an
  in-flight tool.
- `ToolDef`, `ToolInvocation` (`pluginId` / `toolName` / `args` /
  `agentId`), `ToolContext` (`broker` / `pluginId`), `Tool::text(...)` —
  in `src/Tool.php`, loaded via the `files` autoload entry alongside
  `src/Host.php`.
- `ToolError` hierarchy → JSON-RPC `-33401..-33405`: `ToolNotFound` /
  `ToolArgumentInvalid` (`$details`) / `ToolExecutionFailed` /
  `ToolUnavailable` (`$retryAfterMs`) / `ToolDenied`. Like
  `RpcServerError`, the code is carried via `parent::__construct($msg,
  $code)` — read it with `getCode()`. An uncaught `\Throwable` in a
  handler maps to `-33403`; a `tool.invoke` with no handler registered
  replies `-32601`.

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
