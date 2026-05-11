# Changelog — `nexo/plugin-sdk` (PHP)

Versions are tagged `php-vX.Y.Z` in the
[nexo-plugin-sdks](https://github.com/lordmacu/nexo-plugin-sdks) mono-repo and
mirrored (as `vX.Y.Z`) to
[lordmacu/nexo-plugin-sdk-php](https://github.com/lordmacu/nexo-plugin-sdk-php),
which Packagist tracks.

## 0.1.0 — 2026-05-04

Initial release (nexo-rs Phase 31.5.c). PSR-4 `Nexo\Plugin\Sdk\`, PHP ≥ 8.1
(Fibers). Public API: `PluginAdapter`, `BrokerSender`, `Event`, `Manifest`,
`Wire`, `Scheduler`, `StdoutGuard`, the three exception classes. Single runtime
dep `yosymfony/toml`. Robustness defaults on by default: `enableStdoutGuard`
(`ob_start` diverts non-JSON `echo`/`print`/`printf`/`var_dump` to stderr;
direct `fwrite(STDOUT, ...)` bypasses — used deliberately for blessed frames),
`maxFrameBytes` (1 MiB), `handleProcessSignals` (`pcntl_async_signals`),
in-flight Fiber drain on `shutdown` via `Scheduler::drain()`. `run()` throws on
a second call.
