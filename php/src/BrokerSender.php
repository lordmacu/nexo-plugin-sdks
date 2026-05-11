<?php

declare(strict_types=1);

/**
 * Child-side handle to the daemon — broker publishes + host calls.
 *
 * Plugin authors reach this as the 3rd arg to `onEvent`:
 *
 *  - `$broker->publish($topic, $event)` — push an event onto the broker
 *    (topics must be on the manifest's `[[plugin.channels.register]]`
 *    allowlist).
 *  - `$broker->memoryRecall(['agentId'=>..., 'query'=>..., 'limit'?=>10, 'timeoutSec'?=>30.0])`
 *    → `MemoryEntry[]` (contract §5.2 `memory.recall`).
 *  - `$broker->llmComplete(['provider'=>..., 'model'=>..., 'messages'=>[...], 'maxTokens'?=>4096,
 *    'temperature'?=>0.7, 'systemPrompt'?=>..., 'timeoutSec'?=>30.0])` → `LlmCompleteResult`
 *    (§5.3 `llm.complete`).
 *  - `$broker->llmCompleteStream($opts, fn(string $chunk) => ...)` → `LlmCompleteResult`
 *    — `$onChunk` is called once per text chunk, in order; the return
 *    value is the final result (its `content` is `null`). (§5.3
 *    `llm.complete.delta`.)
 *
 * Concurrency: PHP is single-threaded and Fibers don't preempt mid-call,
 * so sequential `fwrite(STDOUT, ...)` calls preserve FIFO ordering
 * without locking; the direct fwrite bypasses `ob_start` (the
 * StdoutGuard). Host calls run inside the handler's Fiber and
 * `Fiber::suspend()` until the response lands in the shared
 * `PendingRegistry` — the dispatch loop's scheduler resumes them each
 * tick, so N concurrent handlers can each have a call in flight at once.
 * Mirrors the Rust SDK's `BrokerSender` in
 * `crates/microapp-sdk/src/plugin.rs`.
 */

namespace Nexo\Plugin\Sdk;

final class BrokerSender
{
    public function __construct(private readonly PendingRegistry $pending)
    {
    }

    // ── broker publish ──────────────────────────────────────────────

    public function publish(string $topic, Event $event): void
    {
        $this->writeFrame([
            'jsonrpc' => Wire::JSONRPC_VERSION,
            'method' => 'broker.publish',
            'params' => ['topic' => $topic, 'event' => $event->toJson()],
        ]);
    }

    // ── child→host requests ─────────────────────────────────────────

    /**
     * Recall up to `limit` (default 10, host caps 1000) memory entries
     * for `agentId` matching `query` (§5.2).
     *
     * @param array{agentId: string, query: string, limit?: int, timeoutSec?: float} $opts
     * @return MemoryEntry[]
     */
    public function memoryRecall(array $opts): array
    {
        $r = $this->request('memory.recall', [
            'agent_id' => (string) ($opts['agentId'] ?? ''),
            'query' => (string) ($opts['query'] ?? ''),
            'limit' => (int) ($opts['limit'] ?? 10),
        ], (float) ($opts['timeoutSec'] ?? Wire::DEFAULT_RPC_TIMEOUT));
        $entries = $r['entries'] ?? null;
        if (!is_array($entries)) {
            throw new RpcDecodeError('memory.recall result missing `entries` array');
        }
        return array_map(
            static fn ($e) => MemoryEntry::fromJson(is_array($e) ? $e : []),
            array_values($entries),
        );
    }

    /**
     * Run a (non-streaming) LLM completion via the agent's configured
     * provider / model (§5.3).
     *
     * @param array{provider: string, model: string, messages: list<Message|array>, maxTokens?: int, temperature?: float, systemPrompt?: string, timeoutSec?: float} $opts
     */
    public function llmComplete(array $opts): LlmCompleteResult
    {
        $r = $this->request(
            'llm.complete',
            $this->llmParams($opts, false),
            (float) ($opts['timeoutSec'] ?? Wire::DEFAULT_RPC_TIMEOUT),
        );
        return LlmCompleteResult::fromJson($r);
    }

    /**
     * Start a streaming LLM completion. `$onChunk` is invoked once per
     * text chunk, in order; the return value is the final
     * `LlmCompleteResult` (whose `content` is `null`). (§5.3 streaming.)
     *
     * @param array{provider: string, model: string, messages: list<Message|array>, maxTokens?: int, temperature?: float, systemPrompt?: string, timeoutSec?: float} $opts
     * @param callable(string): void $onChunk
     */
    public function llmCompleteStream(array $opts, callable $onChunk): LlmCompleteResult
    {
        $this->requireFiber();
        $id = $this->pending->allocId();
        $this->pending->registerStream($id);
        $this->writeFrame([
            'jsonrpc' => Wire::JSONRPC_VERSION,
            'id' => $id,
            'method' => 'llm.complete',
            'params' => $this->llmParams($opts, true),
        ]);
        $timeoutSec = (float) ($opts['timeoutSec'] ?? Wire::DEFAULT_RPC_TIMEOUT);
        $deadline = microtime(true) + $timeoutSec;
        $cursor = 0;
        while (true) {
            $entry = $this->pending->peek($id);
            if ($entry === null) {
                throw new RpcTransportError('llm.complete stream: pending entry vanished');
            }
            $chunks = is_array($entry['chunks'] ?? null) ? $entry['chunks'] : [];
            while ($cursor < count($chunks)) {
                $onChunk((string) $chunks[$cursor]);
                $cursor++;
            }
            if ($entry['done']) {
                break;
            }
            if (microtime(true) >= $deadline) {
                $this->pending->forget($id);
                throw new RpcTimeoutError($timeoutSec);
            }
            \Fiber::suspend();
        }
        // Flush any chunks that landed in the same frame as `done`.
        $entry = $this->pending->peek($id);
        $chunks = is_array($entry['chunks'] ?? null) ? $entry['chunks'] : [];
        while ($cursor < count($chunks)) {
            $onChunk((string) $chunks[$cursor]);
            $cursor++;
        }
        $this->pending->forget($id);
        $this->throwIfError($entry['error'] ?? null);
        return LlmCompleteResult::fromJson(is_array($entry['result'] ?? null) ? $entry['result'] : []);
    }

    // ── internals ───────────────────────────────────────────────────

    /**
     * @param array<string, mixed> $opts
     * @return array<string, mixed>
     */
    private function llmParams(array $opts, bool $stream): array
    {
        $messages = array_map(
            static fn ($m) => $m instanceof Message ? $m->toJson() : (is_array($m) ? $m : []),
            is_array($opts['messages'] ?? null) ? $opts['messages'] : [],
        );
        $p = [
            'provider' => (string) ($opts['provider'] ?? ''),
            'model' => (string) ($opts['model'] ?? ''),
            'messages' => array_values($messages),
            'max_tokens' => (int) ($opts['maxTokens'] ?? 4096),
            'temperature' => (float) ($opts['temperature'] ?? 0.7),
        ];
        if (isset($opts['systemPrompt'])) {
            $p['system_prompt'] = (string) $opts['systemPrompt'];
        }
        if ($stream) {
            $p['stream'] = true;
        }
        return $p;
    }

    /**
     * Send a child→host request and Fiber-suspend until the matching
     * reply lands in the shared registry.
     *
     * @param array<string, mixed> $params
     * @return array<string, mixed>
     */
    private function request(string $method, array $params, float $timeoutSec): array
    {
        $this->requireFiber();
        $id = $this->pending->allocId();
        $this->pending->registerSingle($id);
        $this->writeFrame(['jsonrpc' => Wire::JSONRPC_VERSION, 'id' => $id, 'method' => $method, 'params' => $params]);
        $deadline = microtime(true) + $timeoutSec;
        while (true) {
            $entry = $this->pending->peek($id);
            if ($entry === null) {
                throw new RpcTransportError("$method: pending entry vanished");
            }
            if ($entry['done']) {
                break;
            }
            if (microtime(true) >= $deadline) {
                $this->pending->forget($id);
                throw new RpcTimeoutError($timeoutSec);
            }
            \Fiber::suspend();
        }
        $entry = $this->pending->peek($id);
        $this->pending->forget($id);
        $this->throwIfError($entry['error'] ?? null);
        return is_array($entry['result'] ?? null) ? $entry['result'] : [];
    }

    private function requireFiber(): void
    {
        if (\Fiber::getCurrent() === null) {
            throw new RpcTransportError(
                'host calls must run inside an onEvent handler (no current Fiber)',
            );
        }
    }

    /** @param array<string, mixed>|null $error */
    private function throwIfError(?array $error): void
    {
        if ($error === null) {
            return;
        }
        if (($error['kind'] ?? 'server') === 'transport') {
            throw new RpcTransportError((string) ($error['message'] ?? 'transport error'));
        }
        throw new RpcServerError((int) ($error['code'] ?? -32603), (string) ($error['message'] ?? ''));
    }

    /** @param array<string, mixed> $frame */
    private function writeFrame(array $frame): void
    {
        fwrite(STDOUT, Wire::serializeFrame($frame));
        fflush(STDOUT);
    }
}
