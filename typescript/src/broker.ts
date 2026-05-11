/**
 * Child-side handle to the daemon — broker publishes + host calls.
 *
 * Plugin authors reach this as the 3rd arg to `onEvent`:
 *
 *  - `await broker.publish(topic, event)` — push an event onto the
 *    broker (topics must be on the manifest's
 *    `[[plugin.channels.register]]` allowlist).
 *  - `await broker.memoryRecall({ agentId, query, limit?, timeoutMs? })`
 *    — read the agent's long-term memory (`memory.recall` §5.2).
 *  - `await broker.llmComplete({ provider, model, messages, ... })` — a
 *    (non-streaming) LLM completion (`llm.complete` §5.3).
 *  - `broker.llmCompleteStream({ ... })` — streaming variant, an
 *    `LlmStream` (`llm.complete.delta` §5.3).
 *
 * Concurrency: a Promise-chain write lock serializes outbound frames so
 * concurrent handler tasks never interleave half-written JSON lines on
 * stdout; an SDK-assigned request-id space + a pending-request registry
 * (shared with the adapter) multiplex concurrent child→host calls.
 * Mirrors the Rust SDK's `BrokerSender` in
 * `crates/microapp-sdk/src/plugin.rs`.
 */

import { RpcDecodeError, RpcTimeoutError, RpcTransportError } from "./errors.js";
import { Event } from "./events.js";
import {
  AsyncChannel,
  DEFAULT_RPC_TIMEOUT_MS,
  LlmStream,
  parseLlmCompleteResult,
  parseMemoryEntry,
  type LlmCompleteOptions,
  type LlmCompleteResult,
  type MemoryEntry,
  type Pending,
} from "./host.js";
import { JSONRPC_VERSION, serializeFrame, type JsonRpcFrame } from "./wire.js";

export type LineWriter = (line: string) => void;

export interface MemoryRecallOptions {
  agentId: string;
  query: string;
  limit?: number;
  timeoutMs?: number;
}

export class BrokerSender {
  private writeChain: Promise<void> = Promise.resolve();
  private readonly write: LineWriter;
  private readonly pending: Map<number, Pending>;
  private readonly idAlloc: () => number;

  constructor(write: LineWriter, pending?: Map<number, Pending>, idAlloc?: () => number) {
    this.write = write;
    // A standalone BrokerSender (tests) gets its own registry — host
    // calls then have no responder, but `publish` still works.
    this.pending = pending ?? new Map();
    if (idAlloc) {
      this.idAlloc = idAlloc;
    } else {
      let n = 0;
      this.idAlloc = () => ++n;
    }
  }

  // ── broker publish ──────────────────────────────────────────────

  async publish(topic: string, event: Event): Promise<void> {
    return this.writeFrame({
      jsonrpc: JSONRPC_VERSION,
      method: "broker.publish",
      params: { topic, event: Event.toJson(event) },
    });
  }

  // ── child→host requests ─────────────────────────────────────────

  /** Recall up to `limit` (default 10, host caps 1000) memory entries
   * for `agentId` matching `query` (§5.2). */
  async memoryRecall(opts: MemoryRecallOptions): Promise<MemoryEntry[]> {
    const r = await this.request(
      "memory.recall",
      { agent_id: opts.agentId, query: opts.query, limit: opts.limit ?? 10 },
      opts.timeoutMs,
    );
    const entries = r.entries;
    if (!Array.isArray(entries)) {
      throw new RpcDecodeError("memory.recall result missing `entries` array");
    }
    return entries.map(parseMemoryEntry);
  }

  /** Run a (non-streaming) LLM completion via the agent's configured
   * provider / model (§5.3). */
  async llmComplete(opts: LlmCompleteOptions): Promise<LlmCompleteResult> {
    const r = await this.request("llm.complete", this.llmParams(opts, false), opts.timeoutMs);
    return parseLlmCompleteResult(r);
  }

  /** Start a streaming LLM completion. The returned `LlmStream` yields
   * text chunks; `await stream.result` gives the final
   * `LlmCompleteResult` (whose `content` is `null`). */
  llmCompleteStream(opts: LlmCompleteOptions): LlmStream {
    const id = this.idAlloc();
    const chunks = new AsyncChannel<string>();
    let resolveFinal!: (r: LlmCompleteResult) => void;
    let rejectFinal!: (e: Error) => void;
    const result = new Promise<LlmCompleteResult>((res, rej) => {
      resolveFinal = res;
      rejectFinal = rej;
    });
    this.pending.set(id, { kind: "stream", chunks, resolveFinal, rejectFinal });
    this.writeFrame({
      jsonrpc: JSONRPC_VERSION,
      id,
      method: "llm.complete",
      params: this.llmParams(opts, true),
    }).catch((e) => {
      this.pending.delete(id);
      chunks.close();
      rejectFinal(
        new RpcTransportError(
          `failed to send llm.complete stream: ${e instanceof Error ? e.message : String(e)}`,
        ),
      );
    });
    return new LlmStream(chunks, result);
  }

  // ── internals ───────────────────────────────────────────────────

  private llmParams(opts: LlmCompleteOptions, stream: boolean): Record<string, unknown> {
    const p: Record<string, unknown> = {
      provider: opts.provider,
      model: opts.model,
      messages: opts.messages,
      max_tokens: opts.maxTokens ?? 4096,
      temperature: opts.temperature ?? 0.7,
    };
    if (opts.systemPrompt !== undefined) p.system_prompt = opts.systemPrompt;
    if (stream) p.stream = true;
    return p;
  }

  /** Send a child→host request, await the matching reply. The
   * pending-registry entry is resolved/rejected by the adapter's
   * dispatch loop when the response (or a host error) lands. */
  private request(
    method: string,
    params: Record<string, unknown>,
    timeoutMs?: number,
  ): Promise<Record<string, unknown>> {
    const ms = timeoutMs ?? DEFAULT_RPC_TIMEOUT_MS;
    const id = this.idAlloc();
    return new Promise<Record<string, unknown>>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new RpcTimeoutError(ms / 1000));
      }, ms);
      this.pending.set(id, { kind: "single", resolve, reject, timer });
      this.writeFrame({ jsonrpc: JSONRPC_VERSION, id, method, params }).catch((e) => {
        clearTimeout(timer);
        this.pending.delete(id);
        reject(
          new RpcTransportError(
            `failed to send ${method}: ${e instanceof Error ? e.message : String(e)}`,
          ),
        );
      });
    });
  }

  /** Serialize + write one JSON-RPC frame, FIFO-ordered with every
   * other write on this handle. */
  private writeFrame(frame: JsonRpcFrame): Promise<void> {
    const next = this.writeChain.then(() => {
      this.write(serializeFrame(frame));
    });
    this.writeChain = next.catch(() => {
      /* reset the chain so one failed write doesn't poison the rest */
    });
    return next;
  }
}
