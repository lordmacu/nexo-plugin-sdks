/**
 * Child-side dispatch loop.
 *
 * Mirrors the Rust counterpart in
 * `crates/microapp-sdk/src/plugin.rs::PluginAdapter`. Reads JSON-RPC
 * 2.0 newline-delimited frames from stdin, dispatches:
 *
 * - `method == "initialize"` (request) → reply with manifest +
 *   server_version.
 * - `method == "broker.event"` (notification) → spawn a detached task
 *   running `onEvent` so the reader keeps polling stdin while the
 *   handler awaits its own broker / host round-trips.
 * - `method == "shutdown"` (request) → abandon in-flight host calls,
 *   drain in-flight handler tasks, reply `{ok: true}`, exit.
 * - `method == "llm.complete.delta"` (notification) → route the chunk
 *   to the awaiting `LlmStream`.
 * - a frame with an `id` and no `method` → a *response* to a child→host
 *   request we issued (`memory.recall` / `llm.complete`) → resolve the
 *   awaiting promise; unknown id → dropped with a warn.
 * - anything else with an `id` → reply `-32601 method not found`.
 * - anything else without an `id` → silently ignored (JSON-RPC §4.1).
 */

import { Buffer } from "node:buffer";
import * as readline from "node:readline";

import { BrokerSender } from "./broker.js";
import {
  ManifestError,
  PluginError,
  RpcServerError,
  RpcTransportError,
  WireError,
} from "./errors.js";
import { Event } from "./events.js";
import { parseLlmCompleteResult, type Pending } from "./host.js";
import { parseManifest, type ParsedManifest } from "./manifest.js";
import { installStdoutGuard, isStdoutGuardInstalled } from "./stdout-guard.js";
import {
  ToolError,
  ToolExecutionFailedError,
  toolDefToJson,
  type ToolContext,
  type ToolDef,
  type ToolHandler,
  type ToolHandlerWithContext,
  type ToolInvocation,
} from "./tools.js";
import {
  buildErrorResponse,
  buildResponse,
  JSONRPC_VERSION,
  MAX_FRAME_BYTES,
  serializeFrame,
  type JsonRpcErrorResponse,
  type JsonRpcId,
} from "./wire.js";

export type EventHandler = (
  topic: string,
  event: Event,
  broker: BrokerSender,
) => Promise<void>;

export type ShutdownHandler = () => Promise<void>;

/** Return the manifest's `[plugin.extends].tools` list, or `undefined`
 * when the section / field is absent. Throws `ManifestError` when
 * present but not a list of strings. */
function manifestExtendsTools(raw: Record<string, unknown>): string[] | undefined {
  const plugin = raw["plugin"];
  if (typeof plugin !== "object" || plugin === null) return undefined;
  const extends_ = (plugin as Record<string, unknown>)["extends"];
  if (typeof extends_ !== "object" || extends_ === null) return undefined;
  if (!("tools" in extends_)) return undefined;
  const tools = (extends_ as Record<string, unknown>)["tools"];
  if (!Array.isArray(tools) || !tools.every((t) => typeof t === "string")) {
    throw new ManifestError("[plugin.extends].tools must be a list of strings", "plugin.extends.tools");
  }
  return tools as string[];
}

export interface PluginAdapterOptions {
  /** Body of nexo-plugin.toml. Parsed once at construction. */
  manifestToml: string;
  /** Returned in the initialize reply. Default `"0.1.0"`. */
  serverVersion?: string;
  /** Invoked for every broker.event notification. Detached task — the
   * reader does not block while the handler awaits broker / host
   * round-trips. */
  onEvent?: EventHandler;
  /** Awaited before `{ok: true}` reply to shutdown. */
  onShutdown?: ShutdownHandler;
  /** Tool catalog advertised in the `initialize` reply (contract
   * §4.1.1). Every `name` must appear in the manifest's
   * `[plugin.extends].tools` — a name that doesn't is a hard error at
   * `run()` (mirrors the host's drift check). */
  tools?: ToolDef[];
  /** `tool.invoke` dispatch handler — `fn(inv)`, sync or async.
   * Mutually exclusive with `onToolWithContext`; if both are set, the
   * with-context one wins. */
  onTool?: ToolHandler;
  /** `tool.invoke` dispatch handler — `fn(inv, ctx)`, sync or async,
   * where `ctx.broker` lets the tool body call `memoryRecall` /
   * `llmComplete` mid-invocation. Wins over `onTool` when both set. */
  onToolWithContext?: ToolHandlerWithContext;
  /** Default true — patches `process.stdout.write` to divert non-JSON
   * lines to stderr. Critical for plugin authors who accidentally
   * `console.log`. Set false only if you have another guard layer. */
  enableStdoutGuard?: boolean;
  /** Default `MAX_FRAME_BYTES` (1 MiB). Reject inbound frames larger
   * than this with a WireError; dispatch continues. */
  maxFrameBytes?: number;
  /** Default true — listen for SIGTERM + SIGINT and trigger graceful
   * shutdown (drain in-flight, exit 0). */
  handleProcessSignals?: boolean;
}

interface JsonRpcFrameLike {
  jsonrpc?: unknown;
  id?: unknown;
  method?: unknown;
  params?: unknown;
  result?: unknown;
  error?: { code?: unknown; message?: unknown } | undefined;
}

export class PluginAdapter {
  private readonly parsed: ParsedManifest;
  private readonly serverVersion: string;
  private readonly onEvent?: EventHandler;
  private readonly onShutdown?: ShutdownHandler;
  private readonly maxFrameBytes: number;
  private readonly handleProcessSignals: boolean;
  private readonly inflight = new Set<Promise<void>>();
  private readonly broker: BrokerSender;
  private readonly declaredTools: ToolDef[];
  private readonly onTool?: ToolHandler;
  private readonly onToolWithContext?: ToolHandlerWithContext;
  /** Outstanding child→host requests, keyed by SDK-assigned id; shared
   * with the broker handle so the dispatch loop can route responses +
   * `llm.complete.delta` chunks back to awaiting callers. */
  private readonly pending = new Map<number, Pending>();

  private started = false;
  private stopped = false;
  private nextId = 0;
  private rl: readline.Interface | null = null;
  private signalCleanup: (() => void) | null = null;

  constructor(opts: PluginAdapterOptions) {
    this.parsed = parseManifest(opts.manifestToml);
    this.serverVersion = opts.serverVersion ?? "0.1.0";
    this.onEvent = opts.onEvent;
    this.onShutdown = opts.onShutdown;
    this.maxFrameBytes = opts.maxFrameBytes ?? MAX_FRAME_BYTES;
    this.handleProcessSignals = opts.handleProcessSignals ?? true;

    // ── tool dispatch (contract §4.1.1 + §5.t) ──────────────────────
    this.declaredTools = [...(opts.tools ?? [])];
    // with-context wins when both are set — mirrors the Rust SDK.
    this.onToolWithContext = opts.onToolWithContext;
    this.onTool = opts.onToolWithContext !== undefined ? undefined : opts.onTool;
    if (this.declaredTools.length > 0) {
      const allowed = manifestExtendsTools(this.parsed.raw);
      if (allowed === undefined) {
        throw new ManifestError(
          "tools= used but the manifest has no [plugin.extends].tools list; declared: " +
            this.declaredTools.map((t) => t.name).join(", "),
          "plugin.extends.tools",
        );
      }
      const offenders = this.declaredTools.filter((t) => !allowed.includes(t.name)).map((t) => t.name);
      if (offenders.length > 0) {
        throw new ManifestError(
          `declared tool(s) not in [plugin.extends].tools [${allowed.join(", ")}]: ${offenders.join(", ")}`,
          "plugin.extends.tools",
        );
      }
    }

    if (opts.enableStdoutGuard !== false) {
      installStdoutGuard();
    }

    this.broker = new BrokerSender(
      (line) => {
        // Direct stdout write through the original handle — the guard
        // would no-op on JSON lines anyway, but skipping it saves one
        // parse round-trip per frame. The guard remains installed for
        // everything OTHER than the SDK's blessed path.
        process.stdout.write(line);
      },
      this.pending,
      () => ++this.nextId,
    );
  }

  get manifest(): Readonly<Record<string, unknown>> {
    return this.parsed.raw;
  }

  /** Single-shot. Throws PluginError if called twice. */
  async run(): Promise<void> {
    if (this.started) {
      throw new PluginError("PluginAdapter.run() already invoked");
    }
    this.started = true;

    if (this.handleProcessSignals) {
      const onSig = (): void => {
        // Closing the readline interface breaks the for-await loop
        // below; the surrounding code handles drain + exit.
        this.rl?.close();
      };
      process.on("SIGTERM", onSig);
      process.on("SIGINT", onSig);
      this.signalCleanup = (): void => {
        process.removeListener("SIGTERM", onSig);
        process.removeListener("SIGINT", onSig);
      };
    }

    this.rl = readline.createInterface({
      input: process.stdin,
      terminal: false,
      crlfDelay: Infinity,
    });

    try {
      for await (const rawLine of this.rl) {
        if (this.stopped) {
          break;
        }
        await this.handleLine(rawLine);
      }
    } finally {
      this.rl?.close();
      this.rl = null;
      this.signalCleanup?.();
      this.signalCleanup = null;
      // Abandon any outstanding host calls (host is gone), then drain
      // handler tasks — most relevant for SIGTERM-initiated exits.
      this.abandonPending();
      await this.drainInflight();
    }
  }

  private async handleLine(line: string): Promise<void> {
    if (line.length === 0) {
      return;
    }
    const byteLen = Buffer.byteLength(line, "utf-8");
    if (byteLen > this.maxFrameBytes) {
      process.stderr.write(
        `plugin: inbound frame ${byteLen} bytes exceeds maxFrameBytes ${this.maxFrameBytes}\n`,
      );
      return;
    }

    let msg: JsonRpcFrameLike;
    try {
      msg = JSON.parse(line);
    } catch (e) {
      process.stderr.write(`plugin: malformed jsonrpc line: ${e instanceof Error ? e.message : String(e)}\n`);
      return;
    }
    if (typeof msg !== "object" || msg === null) {
      process.stderr.write(`plugin: jsonrpc frame must be an object\n`);
      return;
    }

    const method = msg.method;
    const id = msg.id;

    if (typeof method !== "string") {
      // A response (id + result/error) to a request we issued, or
      // garbage. Never reply to it.
      if (typeof id === "number" && this.pending.has(id)) {
        this.routeResponse(id, msg);
      } else if (id !== undefined && id !== null) {
        process.stderr.write(`plugin: response for unknown/expired request id ${JSON.stringify(id)}, dropped\n`);
      }
      return;
    }

    if (method === "initialize") {
      this.replyInitialize(id);
    } else if (method === "broker.event") {
      this.dispatchEvent(msg.params);
    } else if (method === "tool.invoke") {
      this.handleToolInvoke(id, msg.params);
    } else if (method === "llm.complete.delta") {
      this.routeDelta(msg.params);
    } else if (method === "shutdown") {
      await this.replyShutdown(id);
      this.stopped = true;
      this.rl?.close();
    } else if (id !== undefined) {
      // Unknown request — JSON-RPC requires a reply.
      this.writeFrame(buildErrorResponse(id as never, -32601, "method not found"));
    }
    // Unknown notification (no id) — silently ignore per JSON-RPC §4.1.
  }

  // ── child→host response routing ─────────────────────────────────

  private routeResponse(id: number, msg: JsonRpcFrameLike): void {
    const p = this.pending.get(id);
    if (!p) return;
    this.pending.delete(id);
    const err = msg.error;
    const code = err && typeof err.code === "number" ? err.code : -32603;
    const emsg = err ? String(err.message ?? "") : "";
    if (p.kind === "single") {
      if (p.timer) clearTimeout(p.timer);
      if (err) {
        p.reject(new RpcServerError(code, emsg));
      } else {
        const r = msg.result;
        p.resolve(typeof r === "object" && r !== null ? (r as Record<string, unknown>) : {});
      }
      return;
    }
    // stream — this is the final reply (finish_reason + usage, no content).
    p.chunks.close();
    if (err) {
      p.rejectFinal(new RpcServerError(code, emsg));
    } else {
      p.resolveFinal(parseLlmCompleteResult(msg.result));
    }
  }

  private routeDelta(params: unknown): void {
    const p = (typeof params === "object" && params !== null ? params : {}) as {
      request_id?: unknown;
      chunk?: unknown;
    };
    const pending = typeof p.request_id === "number" ? this.pending.get(p.request_id) : undefined;
    if (pending && pending.kind === "stream" && typeof p.chunk === "string") {
      pending.chunks.push(p.chunk);
    } else {
      process.stderr.write(
        `plugin: llm.complete.delta for unknown/non-stream request ${JSON.stringify(p.request_id)}, dropped\n`,
      );
    }
  }

  private abandonPending(): void {
    for (const [id, p] of [...this.pending.entries()]) {
      this.pending.delete(id);
      if (p.kind === "single") {
        if (p.timer) clearTimeout(p.timer);
        p.reject(new RpcTransportError("adapter shutting down"));
      } else {
        p.chunks.close();
        p.rejectFinal(new RpcTransportError("adapter shutting down"));
      }
    }
  }

  // ── lifecycle ───────────────────────────────────────────────────

  private replyInitialize(id: unknown): void {
    if (id === undefined) {
      return; // spec-violating notification; nothing to reply to.
    }
    const result: Record<string, unknown> = {
      manifest: this.parsed.raw,
      server_version: this.serverVersion,
    };
    if (this.declaredTools.length > 0) {
      result.tools = this.declaredTools.map(toolDefToJson);
    }
    this.writeFrame(buildResponse(id as never, result));
  }

  // ── tool.invoke dispatch (contract §5.t) ────────────────────────

  private handleToolInvoke(id: unknown, params: unknown): void {
    if (id === undefined || id === null) {
      process.stderr.write("plugin: tool.invoke frame without an id, dropped\n");
      return;
    }
    // No-handler wins over param shape — mirrors the Rust SDK so the
    // host's RemoteToolHandler surfaces a clear "not implemented".
    if (this.onTool === undefined && this.onToolWithContext === undefined) {
      this.writeFrame(buildErrorResponse(id as never, -32601, "method not found: tool.invoke"));
      return;
    }
    if (typeof params !== "object" || params === null) {
      this.writeFrame(buildErrorResponse(id as never, -32602, "tool.invoke params must be an object"));
      return;
    }
    const p = params as { plugin_id?: unknown; tool_name?: unknown; args?: unknown; agent_id?: unknown };
    if (typeof p.tool_name !== "string" || p.tool_name.length === 0) {
      this.writeFrame(buildErrorResponse(id as never, -32602, "tool.invoke params missing string `tool_name`"));
      return;
    }
    const inv: ToolInvocation = {
      pluginId: typeof p.plugin_id === "string" ? p.plugin_id : this.parsed.plugin.id,
      toolName: p.tool_name,
      args: p.args ?? null,
      agentId: typeof p.agent_id === "string" ? p.agent_id : undefined,
    };
    const task: Promise<void> = Promise.resolve()
      .then(() => this.invokeToolHandler(inv))
      .then((result) => {
        this.sendToolResult(id, result);
      })
      .catch((e) => {
        if (e instanceof ToolError) {
          this.sendToolError(id, e);
        } else {
          this.sendToolError(
            id,
            new ToolExecutionFailedError(e instanceof Error ? e.message : String(e)),
          );
        }
      });
    this.inflight.add(task);
    task.finally(() => {
      this.inflight.delete(task);
    });
  }

  private invokeToolHandler(inv: ToolInvocation): unknown | Promise<unknown> {
    if (this.onToolWithContext !== undefined) {
      const ctx: ToolContext = { broker: this.broker, pluginId: this.parsed.plugin.id };
      return this.onToolWithContext(inv, ctx);
    }
    // onTool is defined — checked in handleToolInvoke before this runs.
    return (this.onTool as ToolHandler)(inv);
  }

  private sendToolResult(id: unknown, result: unknown): void {
    let line: string;
    try {
      line = serializeFrame(buildResponse(id as never, result));
    } catch (e) {
      this.sendToolError(
        id,
        new ToolExecutionFailedError(
          `tool result not JSON-serializable: ${e instanceof Error ? e.message : String(e)}`,
        ),
      );
      return;
    }
    process.stdout.write(line);
  }

  private sendToolError(id: unknown, err: ToolError): void {
    const data = err.errorData();
    const errorObj: { code: number; message: string; data?: unknown } = {
      code: err.code,
      message: err.message,
    };
    if (data !== undefined) errorObj.data = data;
    const frame: JsonRpcErrorResponse = {
      jsonrpc: JSONRPC_VERSION,
      id: id as JsonRpcId,
      error: errorObj,
    };
    this.writeFrame(frame);
  }

  private async replyShutdown(id: unknown): Promise<void> {
    this.abandonPending();
    await this.drainInflight();
    if (this.onShutdown !== undefined) {
      try {
        await this.onShutdown();
      } catch (e) {
        process.stderr.write(`plugin: onShutdown raised: ${e instanceof Error ? e.message : String(e)}\n`);
      }
    }
    if (id !== undefined) {
      this.writeFrame(buildResponse(id as never, { ok: true }));
    }
  }

  private dispatchEvent(params: unknown): void {
    if (this.onEvent === undefined) {
      return;
    }
    let topic: string;
    let event: Event;
    try {
      if (typeof params !== "object" || params === null) {
        throw new WireError("broker.event params must be a JSON object");
      }
      const p = params as { topic?: unknown; event?: unknown };
      if (typeof p.topic !== "string") {
        throw new WireError("broker.event params missing string `topic`");
      }
      topic = p.topic;
      event = Event.fromJson(p.event);
    } catch (e) {
      process.stderr.write(`plugin: dispatch decode failed: ${e instanceof Error ? e.message : String(e)}\n`);
      return;
    }

    const handler = this.onEvent;
    const task: Promise<void> = Promise.resolve()
      .then(() => handler(topic, event, this.broker))
      .catch((e) => {
        process.stderr.write(`plugin: onEvent raised: ${e instanceof Error ? e.message : String(e)}\n`);
      });
    this.inflight.add(task);
    task.finally(() => {
      this.inflight.delete(task);
    });
  }

  private writeFrame(frame: Parameters<typeof serializeFrame>[0]): void {
    process.stdout.write(serializeFrame(frame));
  }

  private async drainInflight(): Promise<void> {
    if (this.inflight.size === 0) {
      return;
    }
    await Promise.allSettled([...this.inflight]);
  }
}

// Re-export ManifestError so importers can tell it apart from generic
// PluginError without pulling errors.js too.
export { ManifestError } from "./errors.js";

// Touch helpers/types referenced only for side effects / type-position
// so the linter doesn't flag the imports as unused.
void isStdoutGuardInstalled;
void JSONRPC_VERSION;
