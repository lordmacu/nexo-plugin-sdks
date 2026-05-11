<?php

declare(strict_types=1);

/**
 * Child→host call surface — data types, the RpcError hierarchy, and the
 * pending-request registry (shared between PluginAdapter's dispatch loop
 * and BrokerSender). Loaded via the `files` autoload entry so all the
 * classes here resolve without one-file-per-class PSR-4 mapping.
 *
 * Wire shapes: `nexo-plugin-contract.md` §5.2 (`memory.recall`) and §5.3
 * (`llm.complete` + `llm.complete.delta` streaming). Mirrors the Rust
 * child SDK's `crates/microapp-sdk/src/plugin.rs`.
 */

namespace Nexo\Plugin\Sdk;

/** A child→host JSON-RPC request failed. Base for the concrete failure
 * kinds below; mirrors the Rust SDK's `RpcError` enum. */
class RpcError extends PluginError
{
}

/** The host returned a JSON-RPC error response. The JSON-RPC error code
 * (common: `-32601` not wired host-side, `-32602` invalid params,
 * `-32603` backend / not-configured) is `getCode()`; `$serverMessage` is
 * the host's raw message (`getMessage()` is the formatted "rpc error
 * <code>: <message>"). */
final class RpcServerError extends RpcError
{
    public function __construct(int $code, public readonly string $serverMessage)
    {
        parent::__construct("rpc error {$code}: {$serverMessage}", $code);
    }
}

/** No reply within the request timeout (default 30 s). */
final class RpcTimeoutError extends RpcError
{
    public function __construct(public readonly float $seconds)
    {
        parent::__construct("no host reply within {$seconds}s");
    }
}

/** The request could not be sent, or the pending reply was abandoned
 * (host crashed mid-call, or the adapter is shutting down). */
final class RpcTransportError extends RpcError
{
}

/** The host's response did not match the expected result shape. */
final class RpcDecodeError extends RpcError
{
}

/** One long-term-memory entry returned by `memory.recall` (§5.2). */
final class MemoryEntry
{
    /**
     * @param string[] $tags
     * @param string[] $conceptTags
     */
    public function __construct(
        public readonly string $id,
        public readonly string $agentId,
        public readonly string $content,
        public readonly array $tags = [],
        public readonly array $conceptTags = [],
        public readonly string $createdAt = '',
        public readonly ?string $memoryType = null,
    ) {
    }

    /** @param array<string, mixed> $d */
    public static function fromJson(array $d): self
    {
        return new self(
            (string) ($d['id'] ?? ''),
            (string) ($d['agent_id'] ?? ''),
            (string) ($d['content'] ?? ''),
            is_array($d['tags'] ?? null) ? array_values(array_map('strval', $d['tags'])) : [],
            is_array($d['concept_tags'] ?? null) ? array_values(array_map('strval', $d['concept_tags'])) : [],
            (string) ($d['created_at'] ?? ''),
            isset($d['memory_type']) && is_string($d['memory_type']) ? $d['memory_type'] : null,
        );
    }
}

/** A chat message for `llm.complete` (§5.3). `$role` ∈ {system, user,
 * assistant, tool}. */
final class Message
{
    public function __construct(public readonly string $role, public readonly string $content)
    {
    }

    /** @return array<string, string> */
    public function toJson(): array
    {
        return ['role' => $this->role, 'content' => $this->content];
    }
}

/** Token usage from an `llm.complete` reply. */
final class TokenCount
{
    public function __construct(public readonly int $promptTokens = 0, public readonly int $completionTokens = 0)
    {
    }

    /** @param array<string, mixed>|null $d */
    public static function fromJson(?array $d): self
    {
        $d = $d ?? [];
        return new self((int) ($d['prompt_tokens'] ?? 0), (int) ($d['completion_tokens'] ?? 0));
    }
}

/** Result of `llm.complete` (§5.3). `$content` is `null` when the
 * completion was streamed (the chunks were the content). `$finishReason`
 * ∈ {stop, length, tool_use, "other:<reason>"}. */
final class LlmCompleteResult
{
    public function __construct(
        public readonly ?string $content,
        public readonly string $finishReason = '',
        public readonly TokenCount $usage = new TokenCount(),
    ) {
    }

    /** @param array<string, mixed> $d */
    public static function fromJson(array $d): self
    {
        return new self(
            isset($d['content']) && is_string($d['content']) ? $d['content'] : null,
            (string) ($d['finish_reason'] ?? ''),
            TokenCount::fromJson(is_array($d['usage'] ?? null) ? $d['usage'] : null),
        );
    }
}

/**
 * Shared registry of outstanding child→host requests, keyed by an
 * SDK-assigned id. `BrokerSender` registers entries + writes the request
 * frames; the `PluginAdapter` dispatch loop fills them when responses /
 * `llm.complete.delta` chunks arrive. Handler Fibers poll their own
 * entry via `Fiber::suspend()` until it's `done`. This is an object so
 * both sides share one instance (PHP arrays are by-value).
 *
 * Entry shape (array):
 *   - kind:   'single' | 'stream'
 *   - done:   bool
 *   - result: array|null   (the response `result` payload)
 *   - error:  array|null   ['kind'=>'server','code'=>int,'message'=>string] | ['kind'=>'transport','message'=>string]
 *   - chunks: string[]     (stream only — appended-only)
 */
final class PendingRegistry
{
    /** @var array<int, array<string, mixed>> */
    private array $entries = [];
    private int $nextId = 0;

    public function allocId(): int
    {
        return ++$this->nextId;
    }

    public function registerSingle(int $id): void
    {
        $this->entries[$id] = ['kind' => 'single', 'done' => false, 'result' => null, 'error' => null];
    }

    public function registerStream(int $id): void
    {
        $this->entries[$id] = ['kind' => 'stream', 'done' => false, 'result' => null, 'error' => null, 'chunks' => []];
    }

    public function has(int $id): bool
    {
        return isset($this->entries[$id]);
    }

    /** @return array<string, mixed>|null */
    public function peek(int $id): ?array
    {
        return $this->entries[$id] ?? null;
    }

    public function forget(int $id): void
    {
        unset($this->entries[$id]);
    }

    /** Route a host *response* frame (id present, no method) to its entry. */
    public function resolveResponse(int $id, mixed $result, ?array $error): void
    {
        if (!isset($this->entries[$id])) {
            return;
        }
        $this->entries[$id]['done'] = true;
        if ($error !== null) {
            $code = is_int($error['code'] ?? null) ? $error['code'] : -32603;
            $this->entries[$id]['error'] = ['kind' => 'server', 'code' => $code, 'message' => (string) ($error['message'] ?? '')];
        } else {
            $this->entries[$id]['result'] = is_array($result) ? $result : [];
        }
    }

    public function appendChunk(int $id, string $chunk): bool
    {
        if (!isset($this->entries[$id]) || ($this->entries[$id]['kind'] ?? null) !== 'stream') {
            return false;
        }
        $this->entries[$id]['chunks'][] = $chunk;
        return true;
    }

    /** Mark every outstanding request as failed (host gone — shutdown/EOF). */
    public function abandonAll(): void
    {
        foreach (array_keys($this->entries) as $id) {
            $this->entries[$id]['done'] = true;
            $this->entries[$id]['error'] = ['kind' => 'transport', 'message' => 'adapter shutting down'];
        }
    }
}
