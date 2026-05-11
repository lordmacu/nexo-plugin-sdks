// Tests for the child→host call surface (memory.recall / llm.complete).
// The fixture makes the host call in onEvent; the test plays the host —
// feeds broker.event, reads the child's request off stdout, writes a
// canned reply to stdin, asserts the published-back marker.
import { test } from "node:test";
import { strict as assert } from "node:assert";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

import { parseMemoryEntry, parseLlmCompleteResult } from "../dist/index.js";

const HERE = dirname(fileURLToPath(import.meta.url));
const FIXTURE = join(HERE, "fixtures", "host-call-plugin.mjs");

class FrameReader {
  constructor(stream) {
    this.buf = "";
    this.queue = [];
    this.waiters = [];
    stream.setEncoding("utf-8");
    stream.on("data", (chunk) => {
      this.buf += chunk;
      let i;
      while ((i = this.buf.indexOf("\n")) !== -1) {
        const line = this.buf.slice(0, i);
        this.buf = this.buf.slice(i + 1);
        if (line.trim() === "") continue;
        const frame = JSON.parse(line);
        const w = this.waiters.shift();
        if (w) w(frame);
        else this.queue.push(frame);
      }
    });
  }
  read(timeoutMs = 5000) {
    if (this.queue.length) return Promise.resolve(this.queue.shift());
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error(`frame read timeout (buf=${JSON.stringify(this.buf)})`)), timeoutMs);
      this.waiters.push((f) => {
        clearTimeout(t);
        resolve(f);
      });
    });
  }
}

function awaitExit(proc, timeoutMs = 5000) {
  return new Promise((resolve, reject) => {
    if (proc.exitCode !== null) return resolve(proc.exitCode);
    const t = setTimeout(() => reject(new Error("exit timeout")), timeoutMs);
    proc.once("exit", (code) => {
      clearTimeout(t);
      resolve(code);
    });
  });
}

function spawnFixture() {
  const proc = spawn(process.execPath, [FIXTURE], { stdio: ["pipe", "pipe", "pipe"] });
  let stderr = "";
  proc.stderr.setEncoding("utf-8");
  proc.stderr.on("data", (c) => (stderr += c));
  const rdr = new FrameReader(proc.stdout);
  const write = (frame) => proc.stdin.write(JSON.stringify(frame) + "\n");
  const kill = () => {
    if (proc.exitCode === null && proc.signalCode === null) proc.kill();
  };
  return { proc, rdr, write, kill, getStderr: () => stderr };
}

const eventFrame = (topic, payload) => ({
  jsonrpc: "2.0",
  method: "broker.event",
  params: { topic, event: { topic, source: "host", payload } },
});
const shutdownFrame = (id = 99) => ({ jsonrpc: "2.0", id, method: "shutdown" });

test("data type parsers", () => {
  const e = parseMemoryEntry({ id: "x1", agent_id: "a", content: "c", tags: ["t"], concept_tags: [], created_at: "2026-01-01T00:00:00Z", memory_type: null });
  assert.deepEqual([e.id, e.agent_id, e.content, e.tags, e.memory_type], ["x1", "a", "c", ["t"], null]);
  const r = parseLlmCompleteResult({ content: "hi", finish_reason: "stop", usage: { prompt_tokens: 5, completion_tokens: 2 } });
  assert.deepEqual([r.content, r.finish_reason, r.usage.prompt_tokens, r.usage.completion_tokens], ["hi", "stop", 5, 2]);
  const r2 = parseLlmCompleteResult({ finish_reason: "length" });
  assert.equal(r2.content, null);
  assert.equal(r2.finish_reason, "length");
});

test("memory.recall happy path", async () => {
  const { proc, rdr, write, kill, getStderr } = spawnFixture();
  try {
    write(eventFrame("plugin.in", { op: "recall" }));
    const req = await rdr.read();
    assert.equal(req.method, "memory.recall");
    assert.deepEqual(req.params, { agent_id: "agent_x", query: "prefs", limit: 3 });
    write({ jsonrpc: "2.0", id: req.id, result: { entries: [
      { id: "m1", agent_id: "agent_x", content: "concise", tags: ["pref"], concept_tags: [], created_at: "2026-01-01T00:00:00Z", memory_type: null }] } });
    const out = await rdr.read();
    assert.equal(out.method, "broker.publish");
    assert.deepEqual(out.params.event.payload, { got: ["concise"], ids: ["m1"] });
    write(shutdownFrame());
    assert.equal((await rdr.read()).result.ok, true);
    assert.equal(await awaitExit(proc), 0, `stderr=${getStderr()}`);
  } finally {
    kill();
  }
});

test("memory.recall host error surfaces as RpcServerError", async () => {
  const { proc, rdr, write, kill, getStderr } = spawnFixture();
  try {
    write(eventFrame("plugin.in", { op: "error" }));
    const req = await rdr.read();
    assert.equal(req.method, "memory.recall");
    write({ jsonrpc: "2.0", id: req.id, error: { code: -32603, message: "memory not configured" } });
    const out = await rdr.read();
    assert.deepEqual(out.params.event.payload, { rpc_error_code: -32603, rpc_error_msg: "rpc error -32603: memory not configured" });
    write(shutdownFrame());
    assert.equal((await rdr.read()).result.ok, true);
    assert.equal(await awaitExit(proc), 0, `stderr=${getStderr()}`);
  } finally {
    kill();
  }
});

test("per-call timeout surfaces as RpcTimeoutError; dispatch survives", async () => {
  const { proc, rdr, write, kill, getStderr } = spawnFixture();
  try {
    write(eventFrame("plugin.in", { op: "timeout" }));
    const req = await rdr.read();
    assert.equal(req.method, "memory.recall");
    // Never reply — the 400 ms per-call timeout fires.
    const out = await rdr.read(5000);
    assert.equal(out.params.event.payload.timed_out, true);
    assert.equal(out.params.event.payload.seconds, 0.4);
    // A late reply for the timed-out id is dropped; shutdown still works.
    write({ jsonrpc: "2.0", id: req.id, result: { entries: [] } });
    write(shutdownFrame());
    assert.equal((await rdr.read()).result.ok, true);
    assert.equal(await awaitExit(proc), 0, `stderr=${getStderr()}`);
  } finally {
    kill();
  }
});

test("late response for unknown request id dropped", async () => {
  const { proc, rdr, write, kill } = spawnFixture();
  try {
    write({ jsonrpc: "2.0", id: 9999, result: { entries: [] } }); // never issued — must not crash
    write(shutdownFrame());
    assert.equal((await rdr.read()).result.ok, true);
    assert.equal(await awaitExit(proc), 0);
  } finally {
    kill();
  }
});

test("llm.complete non-streaming happy path", async () => {
  const { proc, rdr, write, kill, getStderr } = spawnFixture();
  try {
    write(eventFrame("plugin.in", { op: "complete" }));
    const req = await rdr.read();
    assert.equal(req.method, "llm.complete");
    assert.equal(req.params.provider, "minimax");
    assert.equal(req.params.stream, undefined);
    write({ jsonrpc: "2.0", id: req.id, result: { content: "one line.", finish_reason: "stop", usage: { prompt_tokens: 3, completion_tokens: 4 } } });
    const out = await rdr.read();
    assert.deepEqual(out.params.event.payload, { content: "one line.", finish_reason: "stop", ct: 4 });
    write(shutdownFrame());
    assert.equal((await rdr.read()).result.ok, true);
    assert.equal(await awaitExit(proc), 0, `stderr=${getStderr()}`);
  } finally {
    kill();
  }
});

test("llm.complete streaming happy path", async () => {
  const { proc, rdr, write, kill, getStderr } = spawnFixture();
  try {
    write(eventFrame("plugin.in", { op: "stream" }));
    const req = await rdr.read();
    assert.equal(req.method, "llm.complete");
    assert.equal(req.params.stream, true);
    const rid = req.id;
    for (const c of ["hel", "lo ", "world"]) write({ jsonrpc: "2.0", method: "llm.complete.delta", params: { request_id: rid, chunk: c } });
    write({ jsonrpc: "2.0", id: rid, result: { finish_reason: "stop", usage: { prompt_tokens: 1, completion_tokens: 3 } } });
    const out = await rdr.read();
    const p = out.params.event.payload;
    assert.deepEqual(p.chunks, ["hel", "lo ", "world"]);
    assert.equal(p.joined, "hello world");
    assert.equal(p.content_is_null, true);
    assert.equal(p.finish_reason, "stop");
    write(shutdownFrame());
    assert.equal((await rdr.read()).result.ok, true);
    assert.equal(await awaitExit(proc), 0, `stderr=${getStderr()}`);
  } finally {
    kill();
  }
});

test("two in-flight memory.recall calls resolve out of order", async () => {
  const { proc, rdr, write, kill, getStderr } = spawnFixture();
  try {
    write(eventFrame("plugin.in", { q: "alpha" }));
    write(eventFrame("plugin.in", { q: "beta" }));
    const req1 = await rdr.read();
    const req2 = await rdr.read();
    const qs = { [req1.params.query]: req1.id, [req2.params.query]: req2.id };
    assert.deepEqual(Object.keys(qs).sort(), ["alpha", "beta"]);
    // Reply to BETA first, then ALPHA.
    write({ jsonrpc: "2.0", id: qs.beta, result: { entries: [{ id: "b", agent_id: "a", content: "B-content" }] } });
    write({ jsonrpc: "2.0", id: qs.alpha, result: { entries: [{ id: "a", agent_id: "a", content: "A-content" }] } });
    const o1 = await rdr.read();
    const o2 = await rdr.read();
    const byQ = {};
    for (const o of [o1, o2]) byQ[o.params.event.payload.q] = o.params.event.payload.got;
    assert.deepEqual(byQ, { alpha: ["A-content"], beta: ["B-content"] });
    write(shutdownFrame());
    assert.equal((await rdr.read()).result.ok, true);
    assert.equal(await awaitExit(proc), 0, `stderr=${getStderr()}`);
  } finally {
    kill();
  }
});

test("shutdown while a host call is in flight does not hang", async () => {
  const { proc, rdr, write, kill, getStderr } = spawnFixture();
  try {
    write(eventFrame("plugin.in", { op: "inflight" }));
    const req = await rdr.read();
    assert.equal(req.method, "memory.recall");
    // Don't reply — shut down. The adapter must abandon the in-flight
    // call (handler gets RpcTransportError) and reply {ok:true} promptly.
    write(shutdownFrame());
    assert.equal((await rdr.read(5000)).result.ok, true);
    assert.equal(await awaitExit(proc), 0);
    assert.match(getStderr(), /HANDLER_GOT_TRANSPORT/);
  } finally {
    kill();
  }
});
