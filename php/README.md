# nexo/plugin-sdk (PHP)

Child-side SDK for nexo subprocess plugins
written in PHP 8.1+. Mirrors the Rust counterpart in
[`crates/microapp-sdk/`](https://github.com/lordmacu/nexo-rs/tree/main/crates/microapp-sdk), the
Python counterpart in [`python/`](../python),
and the TypeScript counterpart in
[`typescript/`](../typescript). Same wire
format ([`nexo-plugin-contract.md`](https://github.com/lordmacu/nexo-rs/blob/main/nexo-plugin-contract.md)),
different language.

The reference plugin template lives at
[the PHP plugin template](https://github.com/lordmacu/nexo-rs/tree/main/extensions/template-plugin-php) (or run `nexo plugin new --lang php`);
copy that directory to start a new plugin.

## Public API

```php
use Nexo\Plugin\Sdk\PluginAdapter;     // async dispatch loop
use Nexo\Plugin\Sdk\BrokerSender;      // write-only broker handle
use Nexo\Plugin\Sdk\Event;             // value object
use Nexo\Plugin\Sdk\Manifest;          // standalone TOML parser
use Nexo\Plugin\Sdk\StdoutGuard;       // defensive guard installable independently
use Nexo\Plugin\Sdk\Wire;              // JSON-RPC frame helpers + MAX_FRAME_BYTES
use Nexo\Plugin\Sdk\PluginError;       // base exception
use Nexo\Plugin\Sdk\ManifestError;     // raised when nexo-plugin.toml is malformed
use Nexo\Plugin\Sdk\WireError;         // raised on malformed JSON-RPC or oversized frames
```

## Minimal example

```php
<?php declare(strict_types=1);
require __DIR__ . '/../vendor/autoload.php';

use Nexo\Plugin\Sdk\BrokerSender;
use Nexo\Plugin\Sdk\Event;
use Nexo\Plugin\Sdk\PluginAdapter;

$adapter = new PluginAdapter([
    'manifestToml' => file_get_contents(__DIR__ . '/../../nexo-plugin.toml'),
    'onEvent' => function (string $topic, Event $event, BrokerSender $broker): void {
        $out = Event::new(
            'plugin.inbound.my_kind',
            'my_plugin',
            ['echoed' => $event->payload],
        );
        $broker->publish('plugin.inbound.my_kind', $out);
    },
]);
$adapter->run();
```

## Host calls

The `$broker` handle passed into `onEvent` can call back into the host —
read the agent's long-term memory, or run an LLM completion via the
agent's configured providers:

```php
'onEvent' => function (string $topic, Event $event, BrokerSender $broker): void {
    $entries = $broker->memoryRecall(['agentId' => 'my_agent', 'query' => 'user prefers concise answers', 'limit' => 5]);
    // $entries: MemoryEntry[]  (id, agentId, content, tags, conceptTags, createdAt, memoryType)

    $r = $broker->llmComplete([
        'provider' => 'minimax', 'model' => 'minimax-m2.5',
        'messages' => [['role' => 'user', 'content' => 'summarize: ...']],
        'systemPrompt' => 'You answer concisely.',
    ]);
    // $r->content, $r->finishReason, $r->usage->{promptTokens, completionTokens}

    $final = $broker->llmCompleteStream(
        ['provider' => 'minimax', 'model' => 'minimax-m2.5', 'messages' => [['role' => 'user', 'content' => '...']]],
        function (string $chunk): void { /* called once per chunk, in order */ },
    );
    // $final is the LlmCompleteResult; $final->content is null (the chunks were the content)
},
```

Failures throw an `RpcError`: `RpcServerError` (`getCode()` — `-32603` =
backend/not-configured, `-32602` = bad params, `-32601` = not wired
host-side; `$serverMessage` = the host's raw message), `RpcTimeoutError`
(`$seconds`; default 30 s, override per call with `'timeoutSec' => ...`),
`RpcTransportError`, `RpcDecodeError`. Host calls run inside the handler
Fiber and `Fiber::suspend()` until the reply lands, so N concurrent
`onEvent` handlers can each have a call in flight.

## Robustness defaults

The constructor defaults are picked to make the most common
plugin-author mistakes recoverable rather than fatal:

| Default | What it gives you |
|---------|-------------------|
| `enableStdoutGuard: true` | `ob_start` callback diverts every non-JSON `echo` / `print` / `printf` / `var_dump` line to stderr tagged with `[stdout-guard]` rather than corrupting the JSON-RPC frame stream the host parses. |
| `maxFrameBytes: 1048576` | Inbound JSON-RPC frames larger than 1 MiB are rejected with a `WireError` log; dispatch continues. Adversarial host cannot OOM the plugin via a single huge line. |
| `handleProcessSignals: true` | Ctrl-C / SIGTERM trigger a graceful shutdown via `pcntl_async_signals` — in-flight Fibers are drained (no mid-publish cancellation), then the process exits 0. |
| In-flight Fiber drain on `shutdown` | Handlers spawned for `broker.event` run in Fibers tracked by the scheduler; `Scheduler::drain()` resumes them all to completion before the SDK replies `{ok: true}` to a host's `shutdown` request. Same idiom as the Python SDK's `_drain_inflight` and the TypeScript SDK's `Promise.allSettled([...inflight])`. |

### Stdout guard limitation

`fwrite(STDOUT, $x)` direct writes BYPASS the guard (PHP
`ob_start` only intercepts the buffered output API: `echo`,
`print`, `printf`, `var_dump`). The SDK's own
`BrokerSender::publish()` uses direct `fwrite` deliberately so
blessed JSON frames always reach the host even when the guard
is active. **Plugin authors who need stdout output should use
`echo` / `print` / `printf` — those are guarded.** Calling
`fwrite(STDOUT, ...)` directly from author code is undefined
behavior.

## What the daemon expects

| Method | Direction | Reply |
|--------|-----------|-------|
| `initialize` | host → child | `{ manifest, server_version }` automatically — the SDK reads + caches your manifest TOML at construction time. |
| `broker.event` (notification) | host → child | No JSON reply. Your `onEvent` handler runs in a Fiber so the dispatch loop continues reading stdin while the handler awaits broker round-trips. Author code can call `Fiber::suspend()` at await points to yield control. |
| `shutdown` | host → child | `{ ok: true }` after draining in-flight Fibers + invoking your `onShutdown` (if set). |
| `memory.recall` / `llm.complete` (+ `llm.complete.delta`) | child → host | Issued by `$broker->memoryRecall` / `llmComplete` / `llmCompleteStream` — the SDK assigns the request id, Fiber-suspends until the reply lands, and multiplexes concurrent calls. |

Full spec: [`nexo-plugin-contract.md`](https://github.com/lordmacu/nexo-rs/blob/main/nexo-plugin-contract.md).

## Tests

```bash
cd php
composer install
php tests/run-all.php
```

23 test cases across 8 files covering:
- Handshake: initialize reply, unknown method `-32601`, unknown
  notification silently ignored.
- Manifest validation: missing id, invalid TOML, id regex violation.
- Dispatch: handler invocation, non-blocking reader, in-flight Fiber
  drain on shutdown.
- Host calls: `memory.recall` / `llm.complete` happy paths, streaming,
  `-32603` → `RpcServerError`, per-call timeout → `RpcTimeoutError`,
  out-of-order multiplexing, shutdown-while-in-flight, unknown-response-id
  dropped, `fromJson` round-trips.
- Stdout guard: idempotent install, echo diverted to stderr.
- Wire: oversized frame rejected with continued dispatch.
- Lifecycle: double `run()` rejects with PluginError.
- Event: `fromJson` validation + round-trip.

## Phase tracking

- 31.5.c (shipped) — child-side SDK + 14 tests + default-on stdout
  guard + Fiber-based scheduler.
- 31.8 (shipped) — extracted to the
  [`nexo-plugin-sdks`](https://github.com/lordmacu/nexo-plugin-sdks)
  mono-repo (`php/` subdir, mirrored to
  [`nexo-plugin-sdk-php`](https://github.com/lordmacu/nexo-plugin-sdk-php)
  for Packagist) and published as `nexo/plugin-sdk`.
- 31.9 (shipped, 0.2.0) — child→host call surface: `$broker->memoryRecall`
  / `$broker->llmComplete` / `$broker->llmCompleteStream`, the `RpcError`
  hierarchy, Fiber-driven request multiplexing. Parity with the Rust
  child SDK. 23 tests.
- 31.5.c.b (deferred) — per-target PHP tarballs
  (`<id>-<version>-php83-x86_64-linux.tar.gz` etc.) for plugins that need
  native PHP extensions.
