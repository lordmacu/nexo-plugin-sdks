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
| Python ≥ 3.10 | [`python/`](./python) | [`nexo-plugin-sdk`](https://pypi.org/project/nexo-plugin-sdk/) (PyPI) | `pip install nexo-plugin-sdk` |
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
