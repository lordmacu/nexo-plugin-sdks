# nexo-plugin-sdks

Child-side SDKs for [nexo-rs](https://github.com/lordmacu/nexo-rs) subprocess
plugins — three renderings of one wire contract, one repo.

A nexo plugin is an executable the daemon spawns and talks to over stdio in
**JSON-RPC 2.0**. The contract is language-agnostic
([`nexo-plugin-contract.md`](https://github.com/lordmacu/nexo-rs/blob/main/nexo-plugin-contract.md)
in the daemon repo is the canonical source of truth); these packages let you
write a plugin in your language of choice without cloning the agent framework.

| Language | Directory | Package | Install |
|----------|-----------|---------|---------|
| Python ≥ 3.10 | [`python/`](./python) | [`nexoai`](https://pypi.org/project/nexoai/) (PyPI) — import name `nexo_plugin_sdk` | `pip install nexoai` |
| TypeScript / JS (Node ≥ 18) | [`typescript/`](./typescript) | [`nexo-plugin-sdk`](https://www.npmjs.com/package/nexo-plugin-sdk) (npm) | `npm install nexo-plugin-sdk` |
| PHP ≥ 8.1 | [`php/`](./php) | [`nexo/plugin-sdk`](https://packagist.org/packages/nexo/plugin-sdk) (Packagist) | `composer require nexo/plugin-sdk` |

The Rust child-side SDK lives in the daemon repo as
[`nexo-microapp-sdk`](https://github.com/lordmacu/nexo-rs/tree/main/crates/microapp-sdk)
(feature `plugin`) — it has heavy path-dep coupling to the daemon crates, so it
stays there.

## Scaffolding a plugin

Don't copy these dirs by hand — use the daemon's scaffolder:

```bash
nexo plugin new my-plugin --lang python   # or: typescript, php, rust
```

The generated project depends on the published SDK package.

## Conformance

[`conformance/`](./conformance) is a cross-language conformance kit: one
Python mock-host, a set of declarative scenarios (`conformance/scenarios/*.json`)
whose `expect*` steps are the golden, and one config-driven fixture per
SDK. An SDK is **conformant** iff `python conformance/run.py --lang <lang>`
passes for it — the kit drives the fixture through every contract
exchange (`initialize` / `shutdown` / `broker.event` / `broker.publish` /
`memory.recall` / `llm.complete` (+ streaming) / `tool.invoke` + the
catalog) and diffs its frames structurally.

```bash
python conformance/run.py --lang python      # also: typescript, php
```

CI ([`.github/workflows/conformance.yml`](.github/workflows/conformance.yml))
runs the `{python, typescript, php}` matrix on every push/PR. The Rust
SDK (which lives in [`nexo-rs`](https://github.com/lordmacu/nexo-rs))
runs the same kit in its own CI — a shallow clone of this repo + `--lang
rust --fixture <built-binary>`. The kit checks **frame structure, not
message text**, and does **not** replace the per-SDK test suites. See
[`conformance/README.md`](./conformance/README.md) for the scenario
format, the matcher vocabulary, and how to add scenarios / SDKs /
contract surfaces.

## Repo layout

Each `<lang>/` subdir is a self-contained package with its own `README.md`,
`CHANGELOG.md`, tests, and build config. CI runs each language's test suite on
its own matrix; releases are tagged per language: `python-vX.Y.Z`,
`ts-vX.Y.Z`, `php-vX.Y.Z`.

PyPI and npm publish directly from this repo's workflows (the registries don't
care about repo layout). Packagist needs `composer.json` at a repo root, so
`php/` is mirrored read-only to
[`lordmacu/nexo-plugin-sdk-php`](https://github.com/lordmacu/nexo-plugin-sdk-php)
by [`.github/workflows/php-split.yml`](.github/workflows/php-split.yml) and
Packagist tracks the mirror — don't touch the mirror, it's auto-generated.

## License

Dual-licensed under [MIT](./LICENSE-MIT) or [Apache-2.0](./LICENSE-APACHE), at
your option — same as nexo-rs.
