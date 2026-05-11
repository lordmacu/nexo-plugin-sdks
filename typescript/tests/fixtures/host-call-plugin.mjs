// Host-call fixture: in onEvent, makes a child→host call selected by
// the event payload's `op` field, then publishes a marker derived from
// the host's reply (or the error it raised). Used by host-call.test.mjs.
import { PluginAdapter, Event, RpcServerError, RpcTimeoutError, RpcTransportError } from "../../dist/index.js";

const MANIFEST = `
[plugin]
id = "host_call_plugin"
version = "0.1.0"
name = "HostCall"
description = "fixture"
min_nexo_version = ">=0.1.0"
`;

const adapter = new PluginAdapter({
  manifestToml: MANIFEST,
  handleProcessSignals: false,
  onEvent: async (topic, event, broker) => {
    const op = event.payload?.op;
    const q = event.payload?.q;
    try {
      if (op === "recall" || q !== undefined) {
        const entries = await broker.memoryRecall({ agentId: "agent_x", query: q ?? "prefs", limit: q !== undefined ? 10 : 3 });
        await broker.publish("plugin.inbound.out", Event.new("plugin.inbound.out", "host_call_plugin",
          q !== undefined ? { q, got: entries.map((e) => e.content) }
                          : { got: entries.map((e) => e.content), ids: entries.map((e) => e.id) }));
      } else if (op === "complete") {
        const r = await broker.llmComplete({ provider: "minimax", model: "m", messages: [{ role: "user", content: "hi" }] });
        await broker.publish("plugin.inbound.out", Event.new("plugin.inbound.out", "host_call_plugin",
          { content: r.content, finish_reason: r.finish_reason, ct: r.usage.completion_tokens }));
      } else if (op === "stream") {
        const stream = broker.llmCompleteStream({ provider: "minimax", model: "m", messages: [{ role: "user", content: "hi" }] });
        const chunks = [];
        for await (const c of stream) chunks.push(c);
        const final = await stream.result;
        await broker.publish("plugin.inbound.out", Event.new("plugin.inbound.out", "host_call_plugin",
          { chunks, joined: chunks.join(""), content_is_null: final.content === null, finish_reason: final.finish_reason }));
      } else if (op === "timeout") {
        try {
          await broker.memoryRecall({ agentId: "a", query: "q", timeoutMs: 400 });
        } catch (e) {
          if (e instanceof RpcTimeoutError) {
            await broker.publish("plugin.inbound.out", Event.new("plugin.inbound.out", "host_call_plugin",
              { timed_out: true, seconds: e.seconds }));
          } else throw e;
        }
      } else if (op === "error") {
        try {
          await broker.memoryRecall({ agentId: "a", query: "q" });
        } catch (e) {
          if (e instanceof RpcServerError) {
            await broker.publish("plugin.inbound.out", Event.new("plugin.inbound.out", "host_call_plugin",
              { rpc_error_code: e.code, rpc_error_msg: e.message }));
          } else throw e;
        }
      } else if (op === "inflight") {
        try {
          await broker.memoryRecall({ agentId: "a", query: "q" });
        } catch (e) {
          if (e instanceof RpcTransportError) process.stderr.write(`HANDLER_GOT_TRANSPORT: ${e.message}\n`);
          else throw e;
        }
      }
    } catch (e) {
      process.stderr.write(`HANDLER_RAISED ${e && e.name}: ${e && e.message}\n`);
    }
  },
});
await adapter.run();
