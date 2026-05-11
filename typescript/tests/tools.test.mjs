// Tests for host→child tool dispatch (contract §4.1.1 + §5.t).
// Unit tests for the tool data types / error band, plus subprocess
// tests: spawn a fixture declaring a tool catalog + an onToolWithContext
// handler; play the host — feed `initialize` (assert the advertised
// `tools`), then `tool.invoke` frames, asserting the result/error reply
// (and serving the inner `memory.recall` the with-context tool makes).
import { test } from "node:test";
import { strict as assert } from "node:assert";
import { spawn } from "node:child_process";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

import {
  ToolError,
  ToolNotFoundError,
  ToolArgumentInvalidError,
  ToolExecutionFailedError,
  ToolUnavailableError,
  ToolDeniedError,
  toolDefToJson,
  textResult,
} from "../dist/index.js";

const HERE = dirname(fileURLToPath(import.meta.url));
const TOOL_FIXTURE = join(HERE, "fixtures", "tool-plugin.mjs");
const NOTOOL_FIXTURE = join(HERE, "fixtures", "notool-plugin.mjs");
const BAD_FIXTURE = join(HERE, "fixtures", "bad-manifest-plugin.mjs");

const TOOL_NAMES = [
  "tool_plugin_echo", "tool_plugin_recall", "tool_plugin_slow", "tool_plugin_fail",
  "tool_plugin_argbad", "tool_plugin_busy", "tool_plugin_nope", "tool_plugin_denied",
  "tool_plugin_boom", "tool_plugin_badjson",
];

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

function spawnFixture(fixture = TOOL_FIXTURE) {
  const proc = spawn(process.execPath, [fixture], { stdio: ["pipe", "pipe", "pipe"] });
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

const initializeFrame = (id = 1) => ({ jsonrpc: "2.0", id, method: "initialize", params: {} });
const eventFrame = (topic, payload) => ({
  jsonrpc: "2.0", method: "broker.event", params: { topic, event: { topic, source: "host", payload } },
});
const shutdownFrame = (id = 99) => ({ jsonrpc: "2.0", id, method: "shutdown" });
const toolInvokeFrame = (id, toolName, { args, agentId, pluginId = "tool_plugin" } = {}) => {
  const params = { plugin_id: pluginId, tool_name: toolName };
  if (args !== undefined) params.args = args;
  if (agentId !== undefined) params.agent_id = agentId;
  return { jsonrpc: "2.0", id, method: "tool.invoke", params };
};

// ── unit tests ────────────────────────────────────────────────────

test("tool error codes + grep-prefixed messages", () => {
  assert.equal(new ToolNotFoundError("p_x").code, -33401);
  assert.equal(new ToolArgumentInvalidError("u").code, -33402);
  assert.equal(new ToolExecutionFailedError("e").code, -33403);
  assert.equal(new ToolUnavailableError("b").code, -33404);
  assert.equal(new ToolDeniedError("d").code, -33405);
  assert.equal(new ToolNotFoundError("p_x").message, "tool not found: p_x");
  assert.equal(new ToolArgumentInvalidError("u").message, "invalid argument: u");
  assert.equal(new ToolExecutionFailedError("e").message, "execution failed: e");
  assert.equal(new ToolUnavailableError("b").message, "unavailable: b");
  assert.equal(new ToolDeniedError("d").message, "denied: d");
  assert.ok(new ToolNotFoundError("p_x") instanceof ToolError);
});

test("tool error data only present when set", () => {
  assert.equal(new ToolNotFoundError("p_x").errorData(), undefined);
  assert.equal(new ToolArgumentInvalidError("u").errorData(), undefined);
  assert.deepEqual(new ToolArgumentInvalidError("u", { f: 1 }).errorData(), { details: { f: 1 } });
  assert.equal(new ToolUnavailableError("b").errorData(), undefined);
  assert.deepEqual(new ToolUnavailableError("b", 500).errorData(), { retry_after_ms: 500 });
});

test("toolDefToJson + textResult shapes", () => {
  assert.deepEqual(toolDefToJson({ name: "p_x", description: "d", inputSchema: { type: "object" } }),
    { name: "p_x", description: "d", input_schema: { type: "object" } });
  assert.deepEqual(toolDefToJson({ name: "p_x", description: "d" }).input_schema, {});
  assert.deepEqual(textResult("hi"), { content: [{ type: "text", text: "hi" }], is_error: false });
  assert.equal(textResult("bad", true).is_error, true);
});

// ── handshake ─────────────────────────────────────────────────────

test("initialize advertises declared tools", async () => {
  const { proc, rdr, write, kill, getStderr } = spawnFixture();
  try {
    write(initializeFrame());
    const reply = await rdr.read();
    assert.deepEqual(reply.result.tools.map((t) => t.name), TOOL_NAMES);
    assert.ok(reply.result.tools.every((t) => t.description === "t" && t.input_schema.type === "object"));
    write(shutdownFrame());
    assert.equal((await rdr.read()).result.ok, true);
    assert.equal(await awaitExit(proc), 0, `stderr=${getStderr()}`);
  } finally {
    kill();
  }
});

test("initialize omits tools when none declared", async () => {
  const { proc, rdr, write, kill, getStderr } = spawnFixture(NOTOOL_FIXTURE);
  try {
    write(initializeFrame());
    const reply = await rdr.read();
    assert.equal(reply.result.tools, undefined);
    write(shutdownFrame());
    assert.equal((await rdr.read()).result.ok, true);
    assert.equal(await awaitExit(proc), 0, `stderr=${getStderr()}`);
  } finally {
    kill();
  }
});

// ── tool.invoke ───────────────────────────────────────────────────

test("tool.invoke happy path", async () => {
  const { proc, rdr, write, kill, getStderr } = spawnFixture();
  try {
    write(toolInvokeFrame(10, "tool_plugin_echo", { args: { a: 1 }, agentId: "shopper" }));
    const reply = await rdr.read();
    assert.equal(reply.id, 10);
    assert.deepEqual(reply.result, { echoed: { a: 1 }, agent: "shopper", plugin: "tool_plugin" });
    write(shutdownFrame());
    assert.equal((await rdr.read()).result.ok, true);
    assert.equal(await awaitExit(proc), 0, `stderr=${getStderr()}`);
  } finally {
    kill();
  }
});

test("tool.invoke with args omitted decodes to null", async () => {
  const { proc, rdr, write, kill } = spawnFixture();
  try {
    write(toolInvokeFrame(11, "tool_plugin_echo"));
    const reply = await rdr.read();
    assert.deepEqual(reply.result, { echoed: null, agent: null, plugin: "tool_plugin" });
    write(shutdownFrame());
    await rdr.read();
    assert.equal(await awaitExit(proc), 0);
  } finally {
    kill();
  }
});

async function assertToolError(t, toolName, code, msgSubstr, { data } = {}) {
  const { proc, rdr, write, kill, getStderr } = spawnFixture();
  try {
    write(toolInvokeFrame(20, toolName));
    const reply = await rdr.read();
    assert.equal(reply.id, 20);
    assert.equal(reply.error.code, code);
    assert.match(reply.error.message, new RegExp(msgSubstr));
    if (data !== undefined) assert.deepEqual(reply.error.data, data);
    else assert.equal(reply.error.data, undefined);
    write(shutdownFrame());
    assert.equal((await rdr.read()).result.ok, true);
    assert.equal(await awaitExit(proc), 0, `stderr=${getStderr()}`);
  } finally {
    kill();
  }
}

test("tool.invoke -33401 ToolNotFound", (t) => assertToolError(t, "tool_plugin_nope", -33401, "tool not found: tool_plugin_nope"));
test("tool.invoke -33402 ToolArgumentInvalid carries details", (t) =>
  assertToolError(t, "tool_plugin_argbad", -33402, "invalid argument", { data: { details: { field: "url" } } }));
test("tool.invoke -33403 ToolExecutionFailed", (t) => assertToolError(t, "tool_plugin_fail", -33403, "execution failed: downstream 500"));
test("tool.invoke uncaught throw maps to -33403", (t) => assertToolError(t, "tool_plugin_boom", -33403, "execution failed"));
test("tool.invoke -33404 ToolUnavailable carries retry_after_ms", (t) =>
  assertToolError(t, "tool_plugin_busy", -33404, "unavailable", { data: { retry_after_ms: 5000 } }));
test("tool.invoke -33405 ToolDenied", (t) => assertToolError(t, "tool_plugin_denied", -33405, "denied: tenant blocked"));
test("tool.invoke non-serializable result maps to -33403", (t) => assertToolError(t, "tool_plugin_badjson", -33403, "not JSON-serializable"));

test("tool.invoke with no handler replies -32601", async () => {
  const { proc, rdr, write, kill } = spawnFixture(NOTOOL_FIXTURE);
  try {
    write(toolInvokeFrame(30, "anything"));
    const reply = await rdr.read();
    assert.equal(reply.error.code, -32601);
    assert.match(reply.error.message, /tool\.invoke/);
    write(shutdownFrame());
    assert.equal((await rdr.read()).result.ok, true);
    assert.equal(await awaitExit(proc), 0);
  } finally {
    kill();
  }
});

test("onToolWithContext handler calls the host mid-invocation", async () => {
  const { proc, rdr, write, kill, getStderr } = spawnFixture();
  try {
    write(toolInvokeFrame(40, "tool_plugin_recall", { args: { q: "prefs" } }));
    const inner = await rdr.read();
    assert.equal(inner.method, "memory.recall");
    assert.deepEqual(inner.params, { agent_id: "a", query: "prefs", limit: 10 });
    write({ jsonrpc: "2.0", id: inner.id, result: { entries: [{ id: "m1", agent_id: "a", content: "concise" }] } });
    const reply = await rdr.read();
    assert.equal(reply.id, 40);
    assert.deepEqual(reply.result, { recalled: ["concise"], plugin_id: "tool_plugin" });
    write(shutdownFrame());
    assert.equal((await rdr.read()).result.ok, true);
    assert.equal(await awaitExit(proc), 0, `stderr=${getStderr()}`);
  } finally {
    kill();
  }
});

test("two tool.invokes resolve out of order", async () => {
  const { proc, rdr, write, kill, getStderr } = spawnFixture();
  try {
    write(toolInvokeFrame(1, "tool_plugin_slow"));
    write(toolInvokeFrame(2, "tool_plugin_echo", { args: "quick" }));
    const first = await rdr.read();
    const second = await rdr.read();
    assert.equal(first.id, 2);
    assert.deepEqual(first.result, { echoed: "quick", agent: null, plugin: "tool_plugin" });
    assert.equal(second.id, 1);
    assert.deepEqual(second.result, textResult("slow done"));
    write(shutdownFrame());
    assert.equal((await rdr.read()).result.ok, true);
    assert.equal(await awaitExit(proc), 0, `stderr=${getStderr()}`);
  } finally {
    kill();
  }
});

test("tool.invoke alongside a broker.event handler", async () => {
  const { proc, rdr, write, kill, getStderr } = spawnFixture();
  try {
    write(eventFrame("plugin.in", { k: "v1" }));
    write(toolInvokeFrame(50, "tool_plugin_echo", { args: "x" }));
    const frames = [await rdr.read(), await rdr.read()];
    const pub = frames.find((f) => f.method === "broker.publish");
    const tool = frames.find((f) => "result" in f);
    assert.deepEqual(pub.params.event.payload, { event_seen: "v1" });
    assert.equal(tool.id, 50);
    assert.deepEqual(tool.result, { echoed: "x", agent: null, plugin: "tool_plugin" });
    write(shutdownFrame());
    assert.equal((await rdr.read()).result.ok, true);
    assert.equal(await awaitExit(proc), 0, `stderr=${getStderr()}`);
  } finally {
    kill();
  }
});

test("shutdown waits for an in-flight tool", async () => {
  const { proc, rdr, write, kill, getStderr } = spawnFixture();
  try {
    write(toolInvokeFrame(1, "tool_plugin_slow"));
    write(shutdownFrame(99));
    const first = await rdr.read(5000);
    assert.equal(first.id, 1); // tool result lands FIRST
    assert.deepEqual(first.result, textResult("slow done"));
    const second = await rdr.read(5000);
    assert.equal(second.id, 99); // then the shutdown ack
    assert.equal(second.result.ok, true);
    assert.equal(await awaitExit(proc), 0, `stderr=${getStderr()}`);
  } finally {
    kill();
  }
});

test("declared tool not in [plugin.extends].tools fails fast", () => {
  let exitCode = 0;
  let stderr = "";
  try {
    execFileSync(process.execPath, [BAD_FIXTURE], { stdio: ["ignore", "ignore", "pipe"] });
  } catch (e) {
    exitCode = e.status ?? 1;
    stderr = String(e.stderr ?? "");
  }
  assert.notEqual(exitCode, 0);
  assert.match(stderr, /\[plugin\.extends\]\.tools/);
});
