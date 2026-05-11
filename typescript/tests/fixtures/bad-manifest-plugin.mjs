// Drift fixture: declares a tool whose name is NOT in the manifest's
// [plugin.extends].tools, so the PluginAdapter constructor throws a
// ManifestError before the handshake — the process exits non-zero.
import { PluginAdapter } from "../../dist/index.js";

const MANIFEST = `
[plugin]
id = "bad_plugin"
version = "0.1.0"
name = "Bad"
description = "fixture"
min_nexo_version = ">=0.1.0"

[plugin.extends]
tools = ["bad_plugin_known"]
`;

const adapter = new PluginAdapter({
  manifestToml: MANIFEST,
  handleProcessSignals: false,
  tools: [{ name: "bad_plugin_unknown", description: "x" }],
  onTool: () => ({}),
});
await adapter.run();
