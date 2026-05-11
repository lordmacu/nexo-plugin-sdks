#!/usr/bin/env node
// TypeScript conformance fixture — the `fixture_config` interpreter on
// top of `nexo-plugin-sdk`'s PluginAdapter. The mock-host spawns this
// with `--config <json>` (the scenario's fixture_config + the manifest
// injected under "manifest"), or `--print-capabilities`.
//
// Wire output here must be byte-identical to the Python / PHP / Rust
// fixtures for the same config — that's what the conformance kit checks.

import {
  PluginAdapter,
  Event,
  parseManifest,
  textResult,
  ToolNotFoundError,
  ToolArgumentInvalidError,
  ToolExecutionFailedError,
  ToolUnavailableError,
  ToolDeniedError,
  RpcServerError,
  RpcTimeoutError,
  RpcTransportError,
} from "../../typescript/dist/index.js";

const DEFAULT_CAPS = ["core", "memory", "llm", "tools"];

function parseArgs(argv) {
  if (argv.includes("--print-capabilities")) return { mode: "caps", cfg: {} };
  const i = argv.indexOf("--config");
  if (i === -1 || i + 1 >= argv.length) {
    process.stderr.write("ts_fixture: expected --print-capabilities or --config <json>\n");
    process.exit(2);
  }
  return { mode: "run", cfg: JSON.parse(argv[i + 1]) };
}

// ── tool behaviors ──────────────────────────────────────────────────

async function applyToolBehavior(behavior, inv, ctx) {
  if (behavior === "echo") {
    return { echoed: inv.args, agent: inv.agentId ?? null, plugin: inv.pluginId };
  }
  if (behavior.startsWith("text:")) return textResult(behavior.slice("text:".length));
  if (behavior === "recall_then_return") {
    if (!ctx) throw new ToolExecutionFailedError("recall_then_return needs use_tool_context: true");
    const q = (inv.args && typeof inv.args === "object") ? inv.args.q : undefined;
    const entries = await ctx.broker.memoryRecall({ agentId: inv.agentId ?? "", query: q });
    return { recalled: entries.map((e) => e.content), plugin_id: ctx.pluginId };
  }
  if (behavior.startsWith("slow:")) {
    await new Promise((r) => setTimeout(r, Number(behavior.slice("slow:".length))));
    return textResult("done");
  }
  if (behavior === "raise:33401" || behavior === "not_found") throw new ToolNotFoundError(inv.toolName);
  if (behavior === "raise:33402") throw new ToolArgumentInvalidError("conformance bad arg", { field: "q" });
  if (behavior === "raise:33403") throw new ToolExecutionFailedError("conformance exec fail");
  if (behavior === "raise:33404") throw new ToolUnavailableError("conformance unavailable", 5000);
  if (behavior === "raise:33405") throw new ToolDeniedError("conformance denied");
  if (behavior === "raise_generic") throw new Error("conformance generic error");
  if (behavior === "return_unserializable") return { handle: 1n }; // BigInt — JSON.stringify throws
  throw new ToolNotFoundError(inv.toolName);
}

function makeToolHandler(cfg, useCtx) {
  const perTool = cfg.tool_handlers || {};
  const def = cfg.tool_handler;
  const behaviorFor = (name) => (name in perTool ? perTool[name] : def);
  if (useCtx) {
    return async (inv, ctx) => {
      const b = behaviorFor(inv.toolName);
      if (b == null) throw new ToolNotFoundError(inv.toolName);
      return applyToolBehavior(b, inv, ctx);
    };
  }
  return async (inv) => {
    const b = behaviorFor(inv.toolName);
    if (b == null) throw new ToolNotFoundError(inv.toolName);
    return applyToolBehavior(b, inv, null);
  };
}

// ── event behaviors ─────────────────────────────────────────────────

function makeEventHandler(cfg, pluginId) {
  const eh = cfg.event_handler || "never";
  if (eh === "never") return undefined;
  const pub = (broker, payload) =>
    broker.publish("plugin.inbound.conf", Event.new("plugin.inbound.conf", pluginId, payload));

  return async (topic, event, broker) => {
    const payload = event.payload || {};
    if (eh === "publish_marker") {
      await pub(broker, { event_seen: payload.k });
      return;
    }
    if (eh.startsWith("slow_publish:")) {
      await new Promise((r) => setTimeout(r, Number(eh.slice("slow_publish:".length))));
      await pub(broker, { event_seen: payload.k });
      return;
    }
    if (eh === "host_call") {
      const op = payload.op;
      const tmo = (s) => (typeof s === "number" ? s * 1000 : undefined);
      if (op === "recall") {
        const entries = await broker.memoryRecall({ agentId: "conf", query: payload.q ?? "q", timeoutMs: tmo(payload.timeout) });
        await pub(broker, { recalled: entries.map((e) => e.content) });
      } else if (op === "recall_error") {
        try {
          await broker.memoryRecall({ agentId: "conf", query: "q" });
        } catch (e) {
          if (e instanceof RpcServerError) await pub(broker, { rpc_error: { code: e.code } });
          else throw e;
        }
      } else if (op === "recall_timeout") {
        try {
          await broker.memoryRecall({ agentId: "conf", query: "q", timeoutMs: tmo(payload.timeout ?? 0.4) });
        } catch (e) {
          if (e instanceof RpcTimeoutError) await pub(broker, { timed_out: true });
          else throw e;
        }
      } else if (op === "complete") {
        const r = await broker.llmComplete({ provider: payload.provider ?? "minimax", model: payload.model ?? "m", messages: [{ role: "user", content: "hi" }] });
        await pub(broker, { content: r.content, finish_reason: r.finish_reason, ct: r.usage.completion_tokens });
      } else if (op === "complete_error") {
        try {
          await broker.llmComplete({ provider: "minimax", model: "m", messages: [{ role: "user", content: "hi" }] });
        } catch (e) {
          if (e instanceof RpcServerError) await pub(broker, { rpc_error: { code: e.code } });
          else throw e;
        }
      } else if (op === "stream") {
        const stream = broker.llmCompleteStream({ provider: "minimax", model: "m", messages: [{ role: "user", content: "hi" }] });
        const chunks = [];
        for await (const c of stream) chunks.push(c);
        const final = await stream.result;
        await pub(broker, { joined: chunks.join(""), content_is_none: final.content === null, finish_reason: final.finish_reason });
      } else if (op === "inflight") {
        try {
          await broker.memoryRecall({ agentId: "conf", query: "q" });
        } catch (e) {
          if (e instanceof RpcTransportError) process.stderr.write("INFLIGHT_GOT_TRANSPORT\n");
          else throw e;
        }
      }
    }
  };
}

// ── main ────────────────────────────────────────────────────────────

async function run(cfg) {
  const manifestToml = cfg.manifest;
  const useCtx = cfg.use_tool_context !== false;
  const declare = Array.isArray(cfg.declare_tools) ? cfg.declare_tools : [];
  const tools = declare.map((n) => ({ name: n, description: `conformance tool ${n}`, inputSchema: { type: "object" } }));
  const hasToolHandler = ("tool_handler" in cfg) || ("tool_handlers" in cfg);
  const pluginId = parseManifest(manifestToml).plugin.id;

  const opts = {
    manifestToml,
    serverVersion: cfg.server_version ?? "conformance-fixture-0",
    onShutdown: async () => {},
    tools,
    handleProcessSignals: false,
  };
  if (hasToolHandler) {
    if (useCtx) opts.onToolWithContext = makeToolHandler(cfg, true);
    else opts.onTool = makeToolHandler(cfg, false);
  }
  const eh = makeEventHandler(cfg, pluginId);
  if (eh) opts.onEvent = eh;
  await new PluginAdapter(opts).run();
}

const { mode, cfg } = parseArgs(process.argv.slice(2));
if (mode === "caps") {
  const caps = Array.isArray(cfg.capabilities) && cfg.capabilities.length ? cfg.capabilities : DEFAULT_CAPS;
  process.stdout.write(JSON.stringify(caps) + "\n");
  process.exit(0);
}
await run(cfg);
