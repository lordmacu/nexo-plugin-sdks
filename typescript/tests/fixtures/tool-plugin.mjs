// Tool fixture: declares a tool catalog and an onToolWithContext
// handler that dispatches by toolName — happy paths, the -33xxx error
// band, an uncaught throw, a non-serializable result, a slow tool, and
// a tool that calls back into the host mid-invocation. Used by
// tools.test.mjs.
import {
  PluginAdapter,
  Event,
  textResult,
  ToolNotFoundError,
  ToolArgumentInvalidError,
  ToolExecutionFailedError,
  ToolUnavailableError,
  ToolDeniedError,
} from "../../dist/index.js";

const NAMES = [
  "tool_plugin_echo",
  "tool_plugin_recall",
  "tool_plugin_slow",
  "tool_plugin_fail",
  "tool_plugin_argbad",
  "tool_plugin_busy",
  "tool_plugin_nope",
  "tool_plugin_denied",
  "tool_plugin_boom",
  "tool_plugin_badjson",
];

const MANIFEST = `
[plugin]
id = "tool_plugin"
version = "0.1.0"
name = "ToolPlugin"
description = "fixture"
min_nexo_version = ">=0.1.0"

[plugin.extends]
tools = [${NAMES.map((n) => `"${n}"`).join(", ")}]
`;

const adapter = new PluginAdapter({
  manifestToml: MANIFEST,
  handleProcessSignals: false,
  tools: NAMES.map((n) => ({ name: n, description: "t", inputSchema: { type: "object" } })),
  onEvent: async (topic, event, broker) => {
    void topic;
    await broker.publish(
      "plugin.inbound.out",
      Event.new("plugin.inbound.out", "tool_plugin", { event_seen: event.payload?.k }),
    );
  },
  onToolWithContext: async (inv, ctx) => {
    const name = inv.toolName;
    if (name === "tool_plugin_echo") return { echoed: inv.args, agent: inv.agentId ?? null, plugin: inv.pluginId };
    if (name === "tool_plugin_recall") {
      const entries = await ctx.broker.memoryRecall({ agentId: "a", query: inv.args.q });
      return { recalled: entries.map((e) => e.content), plugin_id: ctx.pluginId };
    }
    if (name === "tool_plugin_slow") {
      await new Promise((r) => setTimeout(r, 350));
      return textResult("slow done");
    }
    if (name === "tool_plugin_fail") throw new ToolExecutionFailedError("downstream 500");
    if (name === "tool_plugin_argbad") throw new ToolArgumentInvalidError("bad url", { field: "url" });
    if (name === "tool_plugin_busy") throw new ToolUnavailableError("rate limited", 5000);
    if (name === "tool_plugin_nope") throw new ToolNotFoundError(name);
    if (name === "tool_plugin_denied") throw new ToolDeniedError("tenant blocked");
    if (name === "tool_plugin_boom") throw new Error("uncaught generic");
    if (name === "tool_plugin_badjson") return { x: 1n }; // BigInt — JSON.stringify throws
    throw new ToolNotFoundError(name);
  },
});
await adapter.run();
