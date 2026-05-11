<?php

declare(strict_types=1);

/**
 * Host→child tool dispatch — data types, the ToolError hierarchy, and a
 * small result helper. Loaded via the `files` autoload entry (alongside
 * Host.php) so the classes here resolve without one-file-per-class
 * PSR-4 mapping.
 *
 * Plugins that declare `[plugin.extends].tools = [...]` in their
 * `nexo-plugin.toml` advertise a tool catalog at handshake (the
 * `initialize` reply's `result.tools`, contract §4.1.1) and receive
 * one `tool.invoke` request per agent-loop tool call (contract §5.t).
 * Authors register the catalog via the `tools` PluginAdapter option and
 * a dispatch handler via `onTool` (or `onToolWithContext` when the
 * handler needs broker access mid-invocation). Mirrors the Rust child
 * SDK's `crates/microapp-sdk/src/plugin.rs` (`ToolDef` /
 * `ToolInvocation` / `ToolInvocationError` / `ToolContext`).
 */

namespace Nexo\Plugin\Sdk;

/** A tool the plugin exposes to the agent loop. Advertised in the
 * `initialize` reply's `tools` array (serialized with the wire key
 * `input_schema`). `$name` must satisfy the per-plugin namespace rule
 * (`<plugin_id>_*` / `ext_<plugin_id>_*`) AND appear in the manifest's
 * `[plugin.extends].tools` list — the SDK fails fast at construction if
 * it does not (mirrors the host's hard-failure on the same drift).
 * `$inputSchema` is an arbitrary JSON Schema object the daemon caches
 * for arg validation before each `tool.invoke`. */
final class ToolDef
{
    /** @param array<string, mixed> $inputSchema */
    public function __construct(
        public readonly string $name,
        public readonly string $description,
        public readonly array $inputSchema = [],
    ) {
    }

    /** @return array<string, mixed> */
    public function toJson(): array
    {
        return ['name' => $this->name, 'description' => $this->description, 'input_schema' => $this->inputSchema];
    }
}

/** A decoded `tool.invoke` request (contract §5.t). `$args` is whatever
 * JSON the daemon's LLM produced for the call — typically an array, but
 * a scalar passes through verbatim; `null` when omitted. `$agentId` is
 * the agent that issued the call, `null` when absent. */
final class ToolInvocation
{
    public function __construct(
        public readonly string $pluginId,
        public readonly string $toolName,
        public readonly mixed $args = null,
        public readonly ?string $agentId = null,
    ) {
    }
}

/** Host resources an `onToolWithContext` handler can reach: the same
 * broker handle `onEvent` receives (so a tool body can
 * `$ctx->broker->memoryRecall(...)` / `llmComplete(...)`
 * mid-invocation), plus the plugin id. Grows by property additions only
 * — handlers ignoring a future property are unaffected. */
final class ToolContext
{
    public function __construct(
        public readonly BrokerSender $broker,
        public readonly string $pluginId,
    ) {
    }
}

/** Base for the typed failures a `tool.invoke` handler can throw. Each
 * subclass maps onto a `-33401..-33405` JSON-RPC error code (read via
 * `getCode()`); the dispatch loop turns a thrown subclass into the
 * matching `error` reply, attaching `data` for the variants that carry
 * one (`details` / `retryAfterMs`). An *uncaught* non-`ToolError`
 * thrown by a handler is mapped to `ToolExecutionFailed` (`-33403`).
 * Message prefixes mirror the Rust SDK's `thiserror` strings.
 *
 * Like `RpcServerError`, this carries the code through
 * `parent::__construct($message, $code)` (no own `$code` property — it
 * collides with `Exception::$code`); read it via `getCode()`. */
class ToolError extends PluginError
{
    /** Extra `data` object for the JSON-RPC error reply, or `null` when
     * the variant carries none.
     * @return array<string, mixed>|null */
    public function errorData(): ?array
    {
        return null;
    }
}

/** `-33401` — the plugin doesn't actually implement the named tool
 * (drift between the manifest / advertised catalog and the runtime
 * handler). */
final class ToolNotFound extends ToolError
{
    public function __construct(public readonly string $toolName)
    {
        parent::__construct("tool not found: {$toolName}", -33401);
    }
}

/** `-33402` — arguments failed plugin-side validation (semantic checks
 * beyond the JSON-Schema the host already ran). `$details`, when set,
 * is surfaced as `error.data.details`. */
final class ToolArgumentInvalid extends ToolError
{
    public function __construct(string $message, public mixed $details = null)
    {
        parent::__construct("invalid argument: {$message}", -33402);
    }

    public function errorData(): ?array
    {
        return $this->details === null ? null : ['details' => $this->details];
    }
}

/** `-33403` — the tool ran but failed (network blip, downstream 5xx, a
 * hung dependency). Also the catch-all the dispatch loop maps an
 * uncaught generic throw onto. */
final class ToolExecutionFailed extends ToolError
{
    public function __construct(string $message)
    {
        parent::__construct("execution failed: {$message}", -33403);
    }
}

/** `-33404` — the tool exists but cannot run right now (resource
 * exhausted, rate-limited, dependency offline). `$retryAfterMs`, when
 * set, is surfaced as `error.data.retry_after_ms`. */
final class ToolUnavailable extends ToolError
{
    public function __construct(string $message, public ?int $retryAfterMs = null)
    {
        parent::__construct("unavailable: {$message}", -33404);
    }

    public function errorData(): ?array
    {
        return $this->retryAfterMs === null ? null : ['retry_after_ms' => $this->retryAfterMs];
    }
}

/** `-33405` — the tool exists but the caller is not authorised (the
 * plugin's per-tenant authorization rejected the call). */
final class ToolDenied extends ToolError
{
    public function __construct(string $message)
    {
        parent::__construct("denied: {$message}", -33405);
    }
}

/** Small helpers for tool authors. */
final class Tool
{
    /** Build the conventional `ToolResponse`-shaped result for a
     * plain-text outcome: `['content' => [['type' => 'text', 'text' =>
     * $text]], 'is_error' => $isError]`. Returning any other JSON value
     * from a handler is fine — the daemon doesn't validate the shape
     * beyond the JSON-RPC envelope.
     * @return array{content: list<array{type: string, text: string}>, is_error: bool} */
    public static function text(string $text, bool $isError = false): array
    {
        return ['content' => [['type' => 'text', 'text' => $text]], 'is_error' => $isError];
    }
}
