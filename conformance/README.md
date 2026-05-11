# Conformance kit — one host, one scenario set, every SDK

An SDK is **conformant** iff `python conformance/run.py --lang <lang>`
passes for it. The kit drives each SDK's "conformance fixture" through a
fixed set of declarative scenarios that exercise the
[`nexo-plugin-contract.md`](https://github.com/lordmacu/nexo-rs/blob/main/docs/src/plugins/contract.md)
wire surface (v`1.10.0`), and diffs the fixture's frames against golden
expectations.

```
conformance/
├── lib.py            # frame I/O over a spawned fixture + the matcher engine + scenario loader/validator
├── mock_host.py      # the host: spawn a fixture, replay a scenario, return a pass/fail transcript
├── run.py            # entrypoint: iterate scenarios × the chosen lang, diff vs golden, TAP-ish output
├── test_lib.py       # unit tests for the matcher engine + the validator (`python conformance/test_lib.py`)
├── scenarios/        # NN_*.json — one declarative exchange spec each (the `expect*` steps ARE the golden)
├── fixtures/         # one config-driven fixture per scripting SDK (rust's lives in nexo-rs)
└── README.md         # this file
```

## Running it

```bash
# from the repo root:
python conformance/run.py --lang python
python conformance/run.py --lang typescript     # needs: cd typescript && npm ci && npm run build
python conformance/run.py --lang php            # needs: cd php && composer install
python conformance/run.py --lang python --scenario '11_*'   # a subset
```

`--lang rust` is **not** run here — the Rust fixture lives in
[`nexo-rs`](https://github.com/lordmacu/nexo-rs)
(`crates/conformance-fixture` / `examples/`) because it needs the
`nexo-microapp-sdk` path dep; `nexo-rs` CI shallow-clones this repo and
runs `python conformance/run.py --lang rust --fixture <built-binary>
--check-contract-version docs/src/plugins/contract.md`.

CI (`.github/workflows/conformance.yml`) runs the matrix
`{python, typescript, php}` on every push/PR.

## What the kit checks — and what it doesn't

- **It checks frame *structure*** — methods, ids, JSON-RPC `result` /
  `error` shapes, error `code`s, the presence/absence and shape of
  `error.data`, the `tools` catalog shape, exchange ordering, exit codes,
  and that stdout stays clean JSON-RPC.
- **It does NOT check human-readable message text.** `error.message`
  wording is a non-normative diagnostic; the per-SDK suites pin those.
  Don't add brittle message-text assertions here.
- It does **not** replace the per-SDK test suites
  (`python/tests/`, `typescript/tests/`, `php/tests/`). Those cover
  lang-specific robustness (the Python async reader, PHP Fiber
  scheduling, the stdout guard internals, signal handling) that's out of
  the wire-conformance scope. Some redundancy is fine — the kit is the
  cross-language gate; the per-SDK suites are the deep ones. Keep both.

## Scenario format

A scenario is one JSON object: `name`, optional `doc`,
`contract_version` (default `"1.10.0"` — the kit skips a scenario
targeting a version newer than `lib.SCENARIOS_TARGET`), optional
`requires` (capability tags the fixture must declare via
`--print-capabilities`), `manifest` (the inline `nexo-plugin.toml` the
fixture is given — must include `[plugin.extends].tools` when the
scenario declares tools), `fixture_config` (passed verbatim to the
fixture as `--config <json>` — vocabulary below), and `steps`.

Each step has exactly one of:

| Step | Meaning |
|------|---------|
| `{"send": <frame>}` | write `<frame>` (a JSON-RPC object; may contain `"$capture:<name>"` bound by an earlier `expect*`) to the fixture's stdin |
| `{"send_raw": "<str>"}` | write the literal string + `\n` (for malformed-frame cases) |
| `{"send_raw_repeat": {"char": "x", "count": 1100000}}` | write `char` repeated `count` times + `\n` (the >`MAX_FRAME_BYTES` case) |
| `{"expect": <pattern>}` | read the next stdout frame; it must structurally match `<pattern>` |
| `{"expect_any_order": [<pattern>, ...]}` | read N frames; each must match exactly one pattern, any order |
| `{"expect_request": <pattern>, "reply": <frame>}` | read the next frame (a child→host request the fixture made); match `<pattern>` (typically capturing `"id": "$capture:rid"`); then send `<frame>` with captures substituted |
| `{"expect_stderr_contains": "<substr>"}` | assert the fixture's stderr (so far) contains `<substr>` — non-consuming |
| `{"expect_no_frame": {"within_ms": 300}}` | assert no frame arrives in the window |
| `{"sleep_ms": 500}` | wait (rare — only where a timer must fire) |
| `{"expect_exit": 0}` | wait for the fixture to exit; the code must equal this. Must be the last step. |

Any step may add `"timeout_ms": <int>` to override the 5 s default.

### Matchers (in `expect*` patterns)

`$IGNORE` (any value) · `$ANY_STRING|ANY_INT|ANY_NUMBER|ANY_BOOL|ANY_OBJECT|ANY_ARRAY`
(type guards) · `"$capture:<name>"` (matches anything, binds it for later
`send`/`reply`; re-capture asserts equality) · `"$ABSENT"` as an object
*value* (the key must not be present) · `{"$optional": <pattern>}` as an
object *value* (key may be absent; if present, match) · `{"$one_of": [<pattern>, ...]}` ·
`"$extra_keys": "allow"` at an object level (don't fail on extra keys —
used for `initialize.result` whose `manifest` echo we don't pin).
Everything else matches by structural equality (object key sets must
match exactly unless `$extra_keys: allow`).

## Fixture contract

A conformance fixture is an executable that:

- `<fixture> --print-capabilities` → prints a JSON array of strings on
  stdout and exits 0. v1 fixtures print `["core","memory","llm","tools"]`.
- `<fixture> --config <json>` → builds the SDK's `PluginAdapter` from the
  `fixture_config` (see vocabulary) + the scenario's `manifest` (passed
  in the config under `"manifest"` — see below), registers exactly the
  handlers the config asks for, and runs the adapter on stdio with
  process-signal handling **disabled** (it runs under the mock-host, not
  under signals).

> The mock-host invokes `<fixture> --config <json>` where `<json>` is the
> scenario's `fixture_config` with the scenario's `manifest` injected as
> the `"manifest"` key. So a fixture reads `cfg["manifest"]` for the TOML
> body.

### `fixture_config` vocabulary (v1 — frozen; additive only)

| Key | Values | Effect |
|-----|--------|--------|
| `manifest` | TOML string | the `nexo-plugin.toml` body (injected by the mock-host from the scenario) |
| `server_version` | string | overrides the reported `server_version` (default `"conformance-fixture-0"`) |
| `declare_tools` | `string[]` | advertise `ToolDef(name, "conformance tool <name>", {"type":"object"})` for each — names must be in the manifest's `[plugin.extends].tools` |
| `tool_handler` | a handler-string (below) | the default tool behavior for every declared tool |
| `tool_handlers` | `{ "<name>": "<handler-string>" }` | per-tool override of `tool_handler` |
| `use_tool_context` | bool (default `true`) | register the tool handler via `on_tool_with_context` (`true`) or `on_tool` (`false`) |
| `event_handler` | `"publish_marker"` / `"host_call"` / `"slow_publish:<ms>"` / `"never"` (default) | the `on_event` behavior |
| `capabilities` | `string[]` | echoed by `--print-capabilities` (default `["core","memory","llm","tools"]`) |

Tool handler-strings: `"echo"` (return `{"echoed": args, "agent": agent_id, "plugin": plugin_id}`) ·
`"text:<s>"` (return `{"content":[{"type":"text","text":"<s>"}],"is_error":false}`) ·
`"recall_then_return"` (call `broker.memory_recall(agent_id=agent_id or "", query=args["q"])`, return `{"recalled":[contents], "plugin_id": ctx.plugin_id}` — needs `use_tool_context: true`) ·
`"slow:<ms>"` (yield ~`<ms>` ms — `asyncio.sleep` / `setTimeout` / a `Fiber::suspend()` loop — then return the `text:done` shape) ·
`"raise:<code>"` (raise the typed tool error for `<code>` ∈ `{33401,33402,33403,33404,33405}`; for `33402` attach `details:{"field":"q"}`; for `33404` attach `retry_after_ms:5000`) ·
`"raise_generic"` (raise a plain language exception → host should see `-33403`) ·
`"return_unserializable"` (return a value `json.dumps`/`JSON.stringify`/`json_encode` can't handle → host should see `-33403`) ·
`"not_found"` (raise the `ToolNotFound` error for the called `tool_name`).

Event handler-strings:
`"publish_marker"` (on `broker.event`, publish a `broker.publish` to `plugin.inbound.conf` with payload `{"event_seen": event.payload.get("k")}`) ·
`"slow_publish:<ms>"` (yield ~`<ms>` ms, then publish `{"event_seen": event.payload.get("k")}`) ·
`"never"` (no `on_event` handler registered) ·
`"host_call"` — dispatches on `event.payload["op"]` (the host-call exerciser, mirroring the per-SDK `DRIVER_HOST_CALL`):

| `op` | does | publishes (to `plugin.inbound.conf`) |
|------|------|--------------------------------------|
| `"recall"` (+ optional `q`, `timeout`) | `broker.memory_recall(agent_id="conf", query=q\|"q", timeout=timeout)` | `{"recalled":[contents]}` |
| `"recall_error"` | `broker.memory_recall(...)`; catches the server-error | `{"rpc_error":{"code":<code>}}` |
| `"recall_timeout"` (+ optional `timeout`, default 0.4) | `broker.memory_recall(..., timeout=timeout)`; catches the timeout | `{"timed_out":true}` |
| `"complete"` (+ optional `provider`, `model`) | `broker.llm_complete(provider, model, messages=[{role:"user",content:"hi"}])` | `{"content":<c>,"finish_reason":<r>,"ct":<completion_tokens>}` |
| `"complete_error"` | `broker.llm_complete(...)`; catches the server-error | `{"rpc_error":{"code":<code>}}` |
| `"stream"` | `broker.llm_complete_stream(...)`, consume chunks, await the final | `{"joined":<chunks joined>,"content_is_none":true,"finish_reason":<r>}` |
| `"inflight"` | `broker.memory_recall(...)`; the host never replies; on shutdown the SDK abandons it → handler catches the transport error | writes `INFLIGHT_GOT_TRANSPORT` to stderr (no publish) |

## Adding things

- **A scenario** — drop `conformance/scenarios/NN_<name>.json`. `run.py`
  sorts by file name. The `expect*` steps are the golden — no separate
  golden file. Run `python conformance/run.py --lang python --scenario 'NN_*'`
  while iterating. Keep each scenario to one concern.
- **A new SDK** — add `conformance/fixtures/<lang>_fixture.<ext>`
  implementing the fixture contract above (and the `fixture_config`
  vocabulary), then teach `run.py`'s `--lang` choices + `fixture_argv_for_lang`
  about it, and add a matrix leg to `conformance.yml`. The fixture's
  *wire output* must be byte-identical to the others' for the same
  config — that's the whole point.
- **A new contract surface** (a future phase: channels, `llm.chat`,
  hooks, memory-vector, the `[plugin.extends]` handshake) — bump
  `lib.SCENARIOS_TARGET`, extend the `fixture_config` vocabulary +
  the fixtures, add scenarios, and watch which SDKs go red. The kit
  grows with the contract.
