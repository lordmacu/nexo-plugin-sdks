// No-tool fixture: declares no tools and registers no tool handler, so
// the initialize reply omits `tools` and any tool.invoke gets -32601.
import { PluginAdapter } from "../../dist/index.js";

const MANIFEST = `
[plugin]
id = "notool_plugin"
version = "0.1.0"
name = "NoTool"
description = "fixture"
min_nexo_version = ">=0.1.0"
`;

const adapter = new PluginAdapter({ manifestToml: MANIFEST, handleProcessSignals: false });
await adapter.run();
