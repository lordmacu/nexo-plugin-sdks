/**
 * Host→child tool dispatch — data types, errors, the tool context.
 *
 * Plugins that declare `[plugin.extends].tools = [...]` in their
 * `nexo-plugin.toml` advertise a tool catalog at handshake (the
 * `initialize` reply's `result.tools`, contract §4.1.1) and receive
 * one `tool.invoke` request per agent-loop tool call (contract §5.t).
 * Authors register the catalog via `PluginAdapterOptions.tools` and a
 * dispatch handler via `onTool` (or `onToolWithContext` when the
 * handler needs broker access mid-invocation). Mirrors the Rust SDK's
 * `crates/microapp-sdk/src/plugin.rs` (`ToolDef` / `ToolInvocation` /
 * `ToolInvocationError` / `ToolContext`).
 */

import { PluginError } from "./errors.js";
import type { BrokerSender } from "./broker.js";

/** A tool the plugin exposes to the agent loop. Advertised in the
 * `initialize` reply's `tools` array (serialized with the wire key
 * `input_schema`). `name` must satisfy the per-plugin namespace rule
 * (`<plugin_id>_*` / `ext_<plugin_id>_*`) AND appear in the manifest's
 * `[plugin.extends].tools` list — the SDK fails fast at `run()` if it
 * does not (mirrors the host's hard-failure on the same drift).
 * `inputSchema` is an arbitrary JSON Schema object the daemon caches
 * for arg validation before each `tool.invoke`. */
export interface ToolDef {
  name: string;
  description: string;
  /** JSON Schema (object). Defaults to `{}` (accepts anything). */
  inputSchema?: Record<string, unknown>;
}

/** A decoded `tool.invoke` request (contract §5.t). `args` is whatever
 * JSON the daemon's LLM produced for the call — typically an object,
 * but a scalar / array passes through verbatim; `null` when omitted.
 * `agentId` is the agent that issued the call, `undefined` when
 * absent. */
export interface ToolInvocation {
  pluginId: string;
  toolName: string;
  args: unknown;
  agentId?: string;
}

/** Host resources an `onToolWithContext` handler can reach: the same
 * broker handle `onEvent` receives (so a tool body can
 * `await ctx.broker.memoryRecall(...)` / `llmComplete(...)`
 * mid-invocation), plus the plugin id. Grows by field additions only —
 * handlers ignoring a future field are unaffected. */
export interface ToolContext {
  broker: BrokerSender;
  pluginId: string;
}

/** A `tool.invoke` handler — sync or async. */
export type ToolHandler = (inv: ToolInvocation) => unknown | Promise<unknown>;
/** A `tool.invoke` handler with the broker / plugin-id context. */
export type ToolHandlerWithContext = (
  inv: ToolInvocation,
  ctx: ToolContext,
) => unknown | Promise<unknown>;

/** Wire shape of one `ToolDef` in the `initialize` reply. */
export function toolDefToJson(t: ToolDef): {
  name: string;
  description: string;
  input_schema: Record<string, unknown>;
} {
  return { name: t.name, description: t.description, input_schema: t.inputSchema ?? {} };
}

/** Base for the typed failures a `tool.invoke` handler can throw. Each
 * subclass maps onto a `-33401..-33405` JSON-RPC error code (see
 * `code`); the dispatch loop turns a thrown subclass into the matching
 * `error` reply, attaching `data` for the variants that carry one
 * (`details` / `retryAfterMs`). An *uncaught* non-`ToolError` thrown by
 * a handler is mapped to `ToolExecutionFailedError` (`-33403`). Message
 * prefixes mirror the Rust SDK's `thiserror` strings. */
export class ToolError extends PluginError {
  override readonly name: string = "ToolError";
  /** JSON-RPC error code; overridden per subclass. */
  readonly code: number = -33403;

  constructor(message: string) {
    super(message);
    Object.setPrototypeOf(this, new.target.prototype);
  }

  /** Extra `data` object for the JSON-RPC error reply, or `undefined`
   * when the variant carries none. */
  errorData(): Record<string, unknown> | undefined {
    return undefined;
  }
}

/** `-33401` — the plugin doesn't actually implement the named tool
 * (drift between the manifest / advertised catalog and the runtime
 * handler). */
export class ToolNotFoundError extends ToolError {
  override readonly name: string = "ToolNotFoundError";
  override readonly code = -33401;
  readonly toolName: string;

  constructor(toolName: string) {
    super(`tool not found: ${toolName}`);
    Object.setPrototypeOf(this, ToolNotFoundError.prototype);
    this.toolName = toolName;
  }
}

/** `-33402` — arguments failed plugin-side validation (semantic checks
 * beyond the JSON-Schema the host already ran). `details`, when given,
 * is surfaced as `error.data.details`. */
export class ToolArgumentInvalidError extends ToolError {
  override readonly name: string = "ToolArgumentInvalidError";
  override readonly code = -33402;
  readonly details?: unknown;

  constructor(message: string, details?: unknown) {
    super(`invalid argument: ${message}`);
    Object.setPrototypeOf(this, ToolArgumentInvalidError.prototype);
    this.details = details;
  }

  override errorData(): Record<string, unknown> | undefined {
    return this.details === undefined ? undefined : { details: this.details };
  }
}

/** `-33403` — the tool ran but failed (network blip, downstream 5xx,
 * a hung dependency). Also the catch-all the dispatch loop maps an
 * uncaught generic throw onto. */
export class ToolExecutionFailedError extends ToolError {
  override readonly name: string = "ToolExecutionFailedError";
  override readonly code = -33403;

  constructor(message: string) {
    super(`execution failed: ${message}`);
    Object.setPrototypeOf(this, ToolExecutionFailedError.prototype);
  }
}

/** `-33404` — the tool exists but cannot run right now (resource
 * exhausted, rate-limited, dependency offline). `retryAfterMs`, when
 * given, is surfaced as `error.data.retry_after_ms`. */
export class ToolUnavailableError extends ToolError {
  override readonly name: string = "ToolUnavailableError";
  override readonly code = -33404;
  readonly retryAfterMs?: number;

  constructor(message: string, retryAfterMs?: number) {
    super(`unavailable: ${message}`);
    Object.setPrototypeOf(this, ToolUnavailableError.prototype);
    this.retryAfterMs = retryAfterMs;
  }

  override errorData(): Record<string, unknown> | undefined {
    return this.retryAfterMs === undefined ? undefined : { retry_after_ms: this.retryAfterMs };
  }
}

/** `-33405` — the tool exists but the caller is not authorised (the
 * plugin's per-tenant authorization rejected the call). */
export class ToolDeniedError extends ToolError {
  override readonly name: string = "ToolDeniedError";
  override readonly code = -33405;

  constructor(message: string) {
    super(`denied: ${message}`);
    Object.setPrototypeOf(this, ToolDeniedError.prototype);
  }
}

/** Build the conventional `ToolResponse`-shaped result a tool handler
 * returns for a plain-text outcome:
 * `{ content: [{ type: "text", text }], is_error }`. Returning any
 * other JSON value from a handler is fine — the daemon doesn't
 * validate the shape beyond the JSON-RPC envelope. */
export function textResult(text: string, isError = false): {
  content: { type: "text"; text: string }[];
  is_error: boolean;
} {
  return { content: [{ type: "text", text }], is_error: isError };
}
