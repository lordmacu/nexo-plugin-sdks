# Changelog — `nexo-plugin-sdk` (TypeScript)

Versions are tagged `ts-vX.Y.Z` in the
[nexo-plugin-sdks](https://github.com/lordmacu/nexo-plugin-sdks) mono-repo.

## 0.1.0 — 2026-05-04

Initial release (nexo-rs Phase 31.5). ESM package, strict tsconfig. Public API:
`PluginAdapter`, `BrokerSender`, `Event`, `parseManifest`, `installStdoutGuard`
/ `uninstallStdoutGuard` / `isStdoutGuardInstalled` / `STDOUT_GUARD_MARKER`,
the JSON-RPC frame helpers (`buildResponse` / `buildErrorResponse` /
`serializeFrame` / `MAX_FRAME_BYTES` / `JSONRPC_VERSION`), the three exception
classes. Single runtime dep `smol-toml`. Robustness defaults on by default:
`enableStdoutGuard` (diverts non-JSON `process.stdout.write` to stderr),
`maxFrameBytes` (1 MiB), `handleProcessSignals` (SIGTERM/SIGINT graceful
shutdown), in-flight task drain on `shutdown`. `run()` throws on a second call.
