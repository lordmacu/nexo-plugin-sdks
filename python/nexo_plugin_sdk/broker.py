"""Child-side handle to the daemon — broker publishes + host calls.

Plugin authors reach this as the 3rd arg to ``on_event``:

  - ``await broker.publish(topic, event)`` — push an event onto the
    broker (topics must be on the manifest's
    ``[[plugin.channels.register]]`` allowlist; the host drops
    disallowed ones with a warn).
  - ``await broker.memory_recall(agent_id=..., query=...)`` — read the
    agent's long-term memory (``memory.recall`` §5.2).
  - ``await broker.llm_complete(provider=..., model=..., messages=...)``
    — an LLM completion via the agent's configured providers
    (``llm.complete`` §5.3).
  - ``broker.llm_complete_stream(...)`` — streaming variant, an
    :class:`~nexo_plugin_sdk.host.LlmStream`.

The handle keeps the wire-spec name ``BrokerSender`` for source
compatibility; it is conceptually the "host client". Mirrors the Rust
SDK's ``BrokerSender`` in ``crates/microapp-sdk/src/plugin.rs``.
"""

import asyncio
import itertools
import sys
from typing import Any, Iterator, TextIO, Union

from . import wire
from .errors import RpcDecodeError, RpcServerError, RpcTimeoutError, RpcTransportError
from .events import Event
from .host import (
    DEFAULT_RPC_TIMEOUT,
    LlmCompleteResult,
    LlmStream,
    Message,
    MemoryEntry,
    _StreamPending,
)

# A pending-registry entry: either a single-shot future (request that
# expects one response frame) or a streaming entry (request that gets
# llm.complete.delta notifications then a final response).
_Pending = Union["asyncio.Future[Any]", _StreamPending]


class BrokerSender:
    """Handle to the daemon: ``publish`` + child→host requests.

    Wraps an injected line-writer (the captured *original* stdout, so
    blessed JSON-RPC frames bypass the stdout guard) behind an async
    lock so concurrent handler tasks do not interleave half-written
    frames. Shares the adapter's pending-request registry + id
    allocator so the dispatch loop can route responses back here.
    """

    def __init__(
        self,
        write_lock: asyncio.Lock,
        writer: TextIO | None = None,
        pending: "dict[int, _Pending] | None" = None,
        id_alloc: "Iterator[int] | None" = None,
    ) -> None:
        self._lock = write_lock
        # Default to bare ``sys.stdout`` for callers (mainly tests)
        # that construct a BrokerSender without going through the
        # adapter. The adapter always injects the captured original.
        self._writer: TextIO = writer if writer is not None else sys.stdout
        # Shared with the adapter. A standalone BrokerSender (tests)
        # gets its own — host calls then have no responder, but
        # ``publish`` still works.
        self._pending: dict[int, _Pending] = pending if pending is not None else {}
        self._id_alloc: Iterator[int] = id_alloc if id_alloc is not None else itertools.count(1)

    # ── broker publish ────────────────────────────────────────────

    async def publish(self, topic: str, event: Event) -> None:
        await self._write(
            wire.build_notification(
                "broker.publish", {"topic": topic, "event": event.to_json()}
            )
        )

    # ── child→host requests ───────────────────────────────────────

    async def memory_recall(
        self,
        *,
        agent_id: str,
        query: str,
        limit: int = 10,
        timeout: float | None = None,
    ) -> list[MemoryEntry]:
        """Recall up to ``limit`` (default 10, host caps 1000) memory
        entries for ``agent_id`` matching ``query`` (§5.2)."""
        result = await self._request(
            "memory.recall",
            {"agent_id": agent_id, "query": query, "limit": limit},
            timeout,
        )
        try:
            entries = result["entries"]
            return [MemoryEntry.from_json(e) for e in entries]
        except (KeyError, TypeError) as e:
            raise RpcDecodeError(f"memory.recall result: {e}") from e

    async def llm_complete(
        self,
        *,
        provider: str,
        model: str,
        messages: "list[Message] | list[dict[str, Any]]",
        max_tokens: int = 4096,
        temperature: float = 0.7,
        system_prompt: str | None = None,
        timeout: float | None = None,
    ) -> LlmCompleteResult:
        """Run a (non-streaming) LLM completion via the agent's
        configured ``provider`` / ``model`` (§5.3)."""
        result = await self._request(
            "llm.complete",
            self._llm_params(
                provider, model, messages, max_tokens, temperature, system_prompt, stream=False
            ),
            timeout,
        )
        try:
            return LlmCompleteResult.from_json(result)
        except (KeyError, TypeError) as e:
            raise RpcDecodeError(f"llm.complete result: {e}") from e

    def llm_complete_stream(
        self,
        *,
        provider: str,
        model: str,
        messages: "list[Message] | list[dict[str, Any]]",
        max_tokens: int = 4096,
        temperature: float = 0.7,
        system_prompt: str | None = None,
    ) -> LlmStream:
        """Start a streaming LLM completion. The returned
        :class:`LlmStream` yields text chunks; call ``final_result()``
        for the final ``LlmCompleteResult`` (whose ``content`` is
        ``None`` — the chunks were the content)."""
        req_id = next(self._id_alloc)
        loop = asyncio.get_event_loop()
        entry = _StreamPending(chunks=asyncio.Queue(), final=loop.create_future())
        self._pending[req_id] = entry
        params = self._llm_params(
            provider, model, messages, max_tokens, temperature, system_prompt, stream=True
        )
        # Fire-and-forget the request frame; the LlmStream awaits the
        # deltas + final reply the dispatch loop routes into ``entry``.
        asyncio.ensure_future(self._send_stream_request(req_id, params, entry))
        return LlmStream(entry)

    # ── internals ─────────────────────────────────────────────────

    @staticmethod
    def _llm_params(
        provider: str,
        model: str,
        messages: "list[Message] | list[dict[str, Any]]",
        max_tokens: int,
        temperature: float,
        system_prompt: str | None,
        *,
        stream: bool,
    ) -> dict[str, Any]:
        msgs = [m.to_json() if isinstance(m, Message) else dict(m) for m in messages]
        p: dict[str, Any] = {
            "provider": provider,
            "model": model,
            "messages": msgs,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        if system_prompt is not None:
            p["system_prompt"] = system_prompt
        if stream:
            p["stream"] = True
        return p

    async def _request(
        self, method: str, params: dict[str, Any], timeout: float | None
    ) -> dict[str, Any]:
        """Send a child→host request, await the matching reply.

        Raises :class:`RpcServerError` (host JSON-RPC error),
        :class:`RpcTimeoutError`, :class:`RpcTransportError`.
        """
        timeout = DEFAULT_RPC_TIMEOUT if timeout is None else timeout
        req_id = next(self._id_alloc)
        fut: "asyncio.Future[Any]" = asyncio.get_event_loop().create_future()
        self._pending[req_id] = fut
        try:
            await self._write(
                {"jsonrpc": wire.JSONRPC_VERSION, "id": req_id, "method": method, "params": params}
            )
        except Exception as e:
            self._pending.pop(req_id, None)
            raise RpcTransportError(f"failed to send {method}: {e}") from e
        try:
            return await asyncio.wait_for(fut, timeout)
        except asyncio.TimeoutError:
            self._pending.pop(req_id, None)
            raise RpcTimeoutError(timeout)
        except asyncio.CancelledError:
            self._pending.pop(req_id, None)
            raise RpcTransportError(f"{method} cancelled before reply")

    async def _send_stream_request(
        self, req_id: int, params: dict[str, Any], entry: _StreamPending
    ) -> None:
        try:
            await self._write(
                {"jsonrpc": wire.JSONRPC_VERSION, "id": req_id, "method": "llm.complete", "params": params}
            )
        except Exception as e:
            self._pending.pop(req_id, None)
            from .host import _STREAM_END

            entry.chunks.put_nowait(_STREAM_END)
            if not entry.final.done():
                entry.final.set_exception(RpcTransportError(f"failed to send llm.complete stream: {e}"))

    async def _write(self, frame: dict[str, Any]) -> None:
        line = wire.serialize_frame(frame)
        async with self._lock:
            self._writer.write(line)
            self._writer.flush()
