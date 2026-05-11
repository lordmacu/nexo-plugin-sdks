/**
 * Child→host call surface — data types, the streaming handle, and the
 * internal pending-request shapes.
 *
 * The plugin author reaches the host through the broker handle passed
 * into `onEvent` (3rd arg): `await broker.memoryRecall(...)` /
 * `await broker.llmComplete(...)` / `broker.llmCompleteStream(...)`.
 * Wire shapes: `nexo-plugin-contract.md` §5.2 (`memory.recall`) and
 * §5.3 (`llm.complete` + `llm.complete.delta` streaming). Mirrors the
 * Rust child SDK's `crates/microapp-sdk/src/plugin.rs`.
 */

/** ms to wait for a host reply before failing with `RpcTimeoutError`.
 * Matches the Rust SDK's `DEFAULT_RPC_TIMEOUT` (30 s). Overridable per
 * call via `timeoutMs`. */
export const DEFAULT_RPC_TIMEOUT_MS = 30_000;

/** One long-term-memory entry returned by `memory.recall` (§5.2).
 * Field names mirror the wire (snake_case), as elsewhere in this SDK. */
export interface MemoryEntry {
  id: string;
  agent_id: string;
  content: string;
  tags: string[];
  concept_tags: string[];
  created_at: string;
  memory_type: string | null;
}

/** A chat message for `llm.complete` (§5.3). */
export interface Message {
  role: "system" | "user" | "assistant" | "tool";
  content: string;
}

/** Token usage from an `llm.complete` reply. */
export interface TokenCount {
  prompt_tokens: number;
  completion_tokens: number;
}

/** Result of `llm.complete` (§5.3). `content` is `null` when the
 * completion was streamed (the chunks were the content).
 * `finish_reason` ∈ `{stop, length, tool_use, "other:<reason>"}`. */
export interface LlmCompleteResult {
  content: string | null;
  finish_reason: string;
  usage: TokenCount;
}

export interface LlmCompleteOptions {
  provider: string;
  model: string;
  messages: Message[];
  maxTokens?: number;
  temperature?: number;
  systemPrompt?: string;
  timeoutMs?: number;
}

function toTokenCount(v: unknown): TokenCount {
  const o = (typeof v === "object" && v !== null ? v : {}) as Record<string, unknown>;
  return {
    prompt_tokens: typeof o.prompt_tokens === "number" ? o.prompt_tokens : 0,
    completion_tokens: typeof o.completion_tokens === "number" ? o.completion_tokens : 0,
  };
}

/** Parse a wire `memory.recall` entry. */
export function parseMemoryEntry(v: unknown): MemoryEntry {
  const o = (typeof v === "object" && v !== null ? v : {}) as Record<string, unknown>;
  return {
    id: String(o.id ?? ""),
    agent_id: String(o.agent_id ?? ""),
    content: String(o.content ?? ""),
    tags: Array.isArray(o.tags) ? (o.tags as string[]) : [],
    concept_tags: Array.isArray(o.concept_tags) ? (o.concept_tags as string[]) : [],
    created_at: String(o.created_at ?? ""),
    memory_type: typeof o.memory_type === "string" ? o.memory_type : null,
  };
}

/** Parse a wire `llm.complete` result (or the streamed final reply,
 * which carries only `finish_reason` + `usage`). */
export function parseLlmCompleteResult(v: unknown): LlmCompleteResult {
  const o = (typeof v === "object" && v !== null ? v : {}) as Record<string, unknown>;
  return {
    content: typeof o.content === "string" ? o.content : null,
    finish_reason: String(o.finish_reason ?? ""),
    usage: toTokenCount(o.usage),
  };
}

/** A minimal async channel: producers `push` / `close`, consumers
 * drain it with `for await`. Backs `LlmStream`'s chunk delivery. */
export class AsyncChannel<T> implements AsyncIterableIterator<T> {
  private readonly buf: T[] = [];
  private readonly waiters: Array<(r: IteratorResult<T>) => void> = [];
  private closed = false;

  push(value: T): void {
    const w = this.waiters.shift();
    if (w) w({ value, done: false });
    else this.buf.push(value);
  }

  close(): void {
    this.closed = true;
    let w: ((r: IteratorResult<T>) => void) | undefined;
    while ((w = this.waiters.shift())) w({ value: undefined as unknown as T, done: true });
  }

  next(): Promise<IteratorResult<T>> {
    if (this.buf.length > 0) return Promise.resolve({ value: this.buf.shift() as T, done: false });
    if (this.closed) return Promise.resolve({ value: undefined as unknown as T, done: true });
    return new Promise((resolve) => this.waiters.push(resolve));
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<T> {
    return this;
  }
}

/** Streaming `llm.complete` handle — `for await (const chunk of stream)`
 * yields text chunks; `await stream.result` resolves with the final
 * `LlmCompleteResult` (`content` is `null`). */
export class LlmStream implements AsyncIterable<string> {
  constructor(
    private readonly chunks: AsyncChannel<string>,
    /** Resolves after every delta has been delivered and the host's
     * final reply lands. Rejects with `RpcServerError` on a mid-stream
     * host error, `RpcTransportError` if the adapter shut down first. */
    readonly result: Promise<LlmCompleteResult>,
  ) {}

  [Symbol.asyncIterator](): AsyncIterator<string> {
    return this.chunks;
  }
}

// ── internal: pending-request registry entries ──────────────────────

export interface SinglePending {
  kind: "single";
  resolve: (result: Record<string, unknown>) => void;
  reject: (err: Error) => void;
  timer: ReturnType<typeof setTimeout> | null;
}

export interface StreamPending {
  kind: "stream";
  chunks: AsyncChannel<string>;
  resolveFinal: (r: LlmCompleteResult) => void;
  rejectFinal: (e: Error) => void;
}

export type Pending = SinglePending | StreamPending;
