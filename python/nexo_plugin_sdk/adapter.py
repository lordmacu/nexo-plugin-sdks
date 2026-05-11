"""Child-side dispatch loop.

Mirrors the Rust counterpart in
``crates/microapp-sdk/src/plugin.rs::PluginAdapter``. Reads JSON-RPC
2.0 newline-delimited frames from stdin and dispatches:

  - ``method == "initialize"`` (request) → reply with manifest +
    server_version.
  - ``method == "broker.event"`` (notification) → spawn a detached
    task running ``on_event`` so the reader keeps polling stdin while
    the handler awaits its own broker / host interactions.
  - ``method == "shutdown"`` (request) → abandon in-flight host calls,
    drain in-flight handler tasks, reply ``{"ok": true}``, exit.
  - ``method == "llm.complete.delta"`` (notification) → route the chunk
    to the awaiting :class:`~nexo_plugin_sdk.host.LlmStream`.
  - a frame with an ``id`` and no ``method`` → a *response* to a
    child→host request we issued (``memory.recall`` / ``llm.complete``)
    → resolve the awaiting future; unknown id → dropped with a warn.
  - anything else with an ``id`` → reply ``-32601 method not found``.
  - anything else without an ``id`` → silently ignored (JSON-RPC §4.1).

Wire format: ``nexo-plugin-contract.md``.
"""

import asyncio
import itertools
import json
import signal
import sys
from typing import Any, Awaitable, Callable, Iterator, Union

from . import stdout_guard, wire
from .broker import BrokerSender
from .errors import PluginError, RpcServerError, RpcTransportError, WireError
from .events import Event
from .host import LlmCompleteResult, _STREAM_END, _StreamPending
from .manifest import read_manifest

EventHandler = Callable[[str, Event, BrokerSender], Awaitable[None]]
ShutdownHandler = Callable[[], Awaitable[None]]

_Pending = Union["asyncio.Future[Any]", _StreamPending]

# Signals that trigger a graceful shutdown (drain in-flight handlers,
# then exit 0). Mirrors the TS SDK's SIGTERM/SIGINT handling.
_SHUTDOWN_SIGNALS = (signal.SIGTERM, signal.SIGINT)


def _safe_remove_asyncio_signal(loop: asyncio.AbstractEventLoop, sig: int) -> None:
    try:
        loop.remove_signal_handler(sig)
    except (ValueError, RuntimeError):
        pass


def _safe_restore_signal(sig: int, previous: Any) -> None:
    # signal.signal returns SIG_DFL/SIG_IGN/a callable, or None when a
    # non-Python handler was installed (which we cannot restore).
    if previous is None:
        return
    try:
        signal.signal(sig, previous)
    except (ValueError, OSError, TypeError):
        pass


class PluginAdapter:
    """Wraps the JSON-RPC dispatch loop. Construct once with the
    manifest TOML body, then ``await adapter.run()`` from your async
    entrypoint.
    """

    def __init__(
        self,
        *,
        manifest_toml: str,
        server_version: str = "0.1.0",
        on_event: EventHandler | None = None,
        on_shutdown: ShutdownHandler | None = None,
        enable_stdout_guard: bool = True,
        max_frame_bytes: int = wire.MAX_FRAME_BYTES,
        handle_process_signals: bool = True,
    ) -> None:
        # Parse + validate the manifest first — a failed construction
        # must not leave a dangling stdout guard installed.
        self._manifest = read_manifest(manifest_toml)
        self._server_version = server_version
        self._on_event = on_event
        self._on_shutdown = on_shutdown
        self._enable_stdout_guard = enable_stdout_guard
        self._max_frame_bytes = max_frame_bytes
        self._handle_process_signals = handle_process_signals
        # Single-shot guard — run() may be called at most once.
        self._started = False

        if enable_stdout_guard:
            stdout_guard.install_stdout_guard()
        # Blessed frames (initialize/shutdown replies, broker.publish,
        # child→host requests) write through the captured *original*
        # stdout so they bypass the guard.
        self._stdout = stdout_guard.original_stdout() or sys.stdout

        self._write_lock = asyncio.Lock()
        # Pending child→host requests, keyed by SDK-assigned request id;
        # shared with the broker handle so the dispatch loop can route
        # responses + llm.complete.delta chunks back to awaiting callers.
        self._pending: dict[int, _Pending] = {}
        self._id_alloc: Iterator[int] = itertools.count(1)
        self._broker = BrokerSender(self._write_lock, self._stdout, self._pending, self._id_alloc)
        # In-flight handler tasks — shutdown awaits them before exiting.
        self._inflight: set[asyncio.Task[None]] = set()

    @property
    def manifest(self) -> dict[str, Any]:
        return self._manifest

    def _install_signal_handlers(
        self, loop: asyncio.AbstractEventLoop, stop_event: asyncio.Event
    ) -> list[Callable[[], None]]:
        cleanups: list[Callable[[], None]] = []
        if not self._handle_process_signals:
            return cleanups
        for sig in _SHUTDOWN_SIGNALS:
            try:
                loop.add_signal_handler(sig, stop_event.set)
                cleanups.append((lambda s: lambda: _safe_remove_asyncio_signal(loop, s))(sig))
            except (NotImplementedError, RuntimeError, ValueError):
                try:
                    previous = signal.signal(
                        sig, lambda *_a: loop.call_soon_threadsafe(stop_event.set)
                    )
                except (ValueError, OSError, RuntimeError):
                    sys.stderr.write(
                        f"plugin: signal {sig} not installable on this platform; "
                        "graceful shutdown on signal disabled\n"
                    )
                    sys.stderr.flush()
                    continue
                cleanups.append((lambda s, p: lambda: _safe_restore_signal(s, p))(sig, previous))
        return cleanups

    async def run(self) -> None:
        if self._started:
            raise PluginError("PluginAdapter.run() already invoked")
        self._started = True

        loop = asyncio.get_running_loop()
        reader = asyncio.StreamReader(limit=self._max_frame_bytes + 64 * 1024)
        protocol = asyncio.StreamReaderProtocol(reader)
        transport, _ = await loop.connect_read_pipe(lambda: protocol, sys.stdin)

        stop_event = asyncio.Event()
        signal_cleanups = self._install_signal_handlers(loop, stop_event)
        try:
            while not stop_event.is_set():
                read_task = asyncio.ensure_future(reader.readline())
                stop_task = asyncio.ensure_future(stop_event.wait())
                try:
                    await asyncio.wait({read_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
                finally:
                    if not read_task.done():
                        read_task.cancel()
                    if not stop_task.done():
                        stop_task.cancel()
                if stop_event.is_set():
                    break
                try:
                    line_bytes = read_task.result()
                except ValueError as e:
                    err = WireError(f"oversized inbound frame dropped: {e}")
                    sys.stderr.write(f"plugin: {err}\n")
                    sys.stderr.flush()
                    continue
                except asyncio.CancelledError:
                    break
                if not line_bytes:
                    break  # EOF — host closed stdin.
                if len(line_bytes) > self._max_frame_bytes:
                    err = WireError(
                        f"inbound frame {len(line_bytes)} bytes exceeds "
                        f"max_frame_bytes {self._max_frame_bytes}"
                    )
                    sys.stderr.write(f"plugin: {err}\n")
                    sys.stderr.flush()
                    continue
                line = line_bytes.decode("utf-8", "replace").strip()
                if not line:
                    continue
                stop = await self._handle_line(line)
                if stop:
                    break
        finally:
            for undo in signal_cleanups:
                undo()
            transport.close()
            self._abandon_pending()
            await self._drain_inflight()
            if self._enable_stdout_guard:
                stdout_guard.uninstall_stdout_guard()

    async def _handle_line(self, line: str) -> bool:
        """Process one JSON-RPC frame. Returns True if the loop should
        stop (a ``shutdown`` request was handled)."""
        try:
            msg = json.loads(line)
        except json.JSONDecodeError as e:
            sys.stderr.write(f"plugin: malformed jsonrpc line: {e}\n")
            sys.stderr.flush()
            return False
        if not isinstance(msg, dict):
            sys.stderr.write("plugin: jsonrpc frame must be an object\n")
            sys.stderr.flush()
            return False
        method = msg.get("method")
        req_id = msg.get("id")

        if method is None:
            # A response (id + result/error) to a request we issued, or
            # garbage. Never reply to it.
            if req_id is not None and req_id in self._pending:
                self._route_response(req_id, msg)
            elif req_id is not None:
                sys.stderr.write(
                    f"plugin: response for unknown/expired request id {req_id!r}, dropped\n"
                )
                sys.stderr.flush()
            return False

        if method == "initialize":
            await self._reply_initialize(req_id)
        elif method == "broker.event":
            params = msg.get("params") or {}
            task = asyncio.create_task(self._dispatch_event(params))
            self._inflight.add(task)
            task.add_done_callback(self._inflight.discard)
        elif method == "llm.complete.delta":
            self._route_delta(msg.get("params") or {})
        elif method == "shutdown":
            self._abandon_pending()
            await self._drain_inflight()
            await self._reply_shutdown(req_id)
            return True
        elif req_id is not None:
            await self._send_error(req_id, -32601, "method not found")
        # Unknown notification (no id) — silently ignore (JSON-RPC §4.1).
        return False

    # ── child→host response routing ───────────────────────────────

    def _route_response(self, req_id: Any, msg: dict[str, Any]) -> None:
        pending = self._pending.pop(req_id, None)
        if pending is None:
            return
        if isinstance(pending, _StreamPending):
            pending.chunks.put_nowait(_STREAM_END)
            if pending.final.done():
                return
            if "error" in msg:
                err = msg["error"] or {}
                pending.final.set_exception(
                    RpcServerError(int(err.get("code", -32603)), str(err.get("message", "")))
                )
            else:
                try:
                    pending.final.set_result(LlmCompleteResult.from_json(msg.get("result") or {}))
                except Exception as e:  # noqa: BLE001
                    pending.final.set_exception(RpcTransportError(f"bad stream final reply: {e}"))
            return
        # Single-shot future.
        if pending.done():
            return
        if "error" in msg:
            err = msg["error"] or {}
            pending.set_exception(
                RpcServerError(int(err.get("code", -32603)), str(err.get("message", "")))
            )
        else:
            pending.set_result(msg.get("result") or {})

    def _route_delta(self, params: dict[str, Any]) -> None:
        rid = params.get("request_id")
        chunk = params.get("chunk")
        pending = self._pending.get(rid)
        if isinstance(pending, _StreamPending) and isinstance(chunk, str):
            pending.chunks.put_nowait(chunk)
        else:
            sys.stderr.write(
                f"plugin: llm.complete.delta for unknown/non-stream request {rid!r}, dropped\n"
            )
            sys.stderr.flush()

    def _abandon_pending(self) -> None:
        """Reject every outstanding child→host request — the host is
        gone (shutdown / EOF), it will not answer them."""
        for req_id in list(self._pending.keys()):
            pending = self._pending.pop(req_id, None)
            if pending is None:
                continue
            if isinstance(pending, _StreamPending):
                pending.chunks.put_nowait(_STREAM_END)
                if not pending.final.done():
                    pending.final.set_exception(RpcTransportError("adapter shutting down"))
            elif not pending.done():
                pending.set_exception(RpcTransportError("adapter shutting down"))

    # ── lifecycle replies ─────────────────────────────────────────

    async def _drain_inflight(self) -> None:
        if not self._inflight:
            return
        await asyncio.gather(*list(self._inflight), return_exceptions=True)

    async def _reply_initialize(self, req_id: Any) -> None:
        await self._send_response(
            req_id, {"manifest": self._manifest, "server_version": self._server_version}
        )

    async def _reply_shutdown(self, req_id: Any) -> None:
        if self._on_shutdown is not None:
            try:
                await self._on_shutdown()
            except Exception as e:  # noqa: BLE001
                sys.stderr.write(f"plugin: on_shutdown raised: {e}\n")
                sys.stderr.flush()
        await self._send_response(req_id, {"ok": True})

    async def _dispatch_event(self, params: dict[str, Any]) -> None:
        if self._on_event is None:
            return
        try:
            topic = params.get("topic")
            raw_event = params.get("event") or {}
            if not isinstance(topic, str):
                raise WireError("broker.event params missing string `topic`")
            if not isinstance(raw_event, dict):
                raise WireError("broker.event params missing dict `event`")
            event = Event.from_json(raw_event)
            await self._on_event(topic, event, self._broker)
        except Exception as e:  # noqa: BLE001
            sys.stderr.write(f"plugin: on_event raised: {e}\n")
            sys.stderr.flush()

    async def _send_response(self, req_id: Any, result: dict[str, Any]) -> None:
        line = wire.serialize_frame(wire.build_response(req_id, result))
        async with self._write_lock:
            self._stdout.write(line)
            self._stdout.flush()

    async def _send_error(self, req_id: Any, code: int, message: str) -> None:
        line = wire.serialize_frame(wire.build_error_response(req_id, code, message))
        async with self._write_lock:
            self._stdout.write(line)
            self._stdout.flush()
