"""Child→host call surface — data types + the streaming handle.

The plugin author reaches the host through the broker handle passed
into ``on_event`` (3rd arg): ``await broker.memory_recall(...)`` /
``await broker.llm_complete(...)`` / ``broker.llm_complete_stream(...)``.
Wire shapes: ``nexo-plugin-contract.md`` §5.2 (``memory.recall``) and
§5.3 (``llm.complete`` + ``llm.complete.delta`` streaming). Mirrors the
Rust SDK's ``crates/microapp-sdk/src/plugin.rs``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

#: Seconds to wait for a host reply to a child→host request before
#: raising ``RpcTimeoutError``. Matches the Rust SDK's
#: ``DEFAULT_RPC_TIMEOUT``. Overridable per call.
DEFAULT_RPC_TIMEOUT = 30.0


@dataclass
class MemoryEntry:
    """One long-term-memory entry returned by ``memory.recall`` (§5.2)."""

    id: str
    agent_id: str
    content: str
    tags: list[str] = field(default_factory=list)
    concept_tags: list[str] = field(default_factory=list)
    created_at: str = ""
    memory_type: str | None = None

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> "MemoryEntry":
        return cls(
            id=str(d["id"]),
            agent_id=str(d["agent_id"]),
            content=str(d["content"]),
            tags=list(d.get("tags") or []),
            concept_tags=list(d.get("concept_tags") or []),
            created_at=str(d.get("created_at") or ""),
            memory_type=d.get("memory_type"),
        )


@dataclass
class Message:
    """A chat message for ``llm.complete`` (§5.3). ``role`` ∈
    ``{system, user, assistant, tool}``."""

    role: str
    content: str

    def to_json(self) -> dict[str, Any]:
        return {"role": self.role, "content": self.content}


@dataclass
class TokenCount:
    """Token usage from an ``llm.complete`` reply."""

    prompt_tokens: int = 0
    completion_tokens: int = 0

    @classmethod
    def from_json(cls, d: dict[str, Any] | None) -> "TokenCount":
        d = d or {}
        return cls(
            prompt_tokens=int(d.get("prompt_tokens") or 0),
            completion_tokens=int(d.get("completion_tokens") or 0),
        )


@dataclass
class LlmCompleteResult:
    """Result of ``llm.complete`` (§5.3). ``content`` is ``None`` when
    the completion was streamed (the child reassembled it from
    ``llm.complete.delta`` chunks). ``finish_reason`` ∈
    ``{stop, length, tool_use, "other:<reason>"}``."""

    content: str | None
    finish_reason: str = ""
    usage: TokenCount = field(default_factory=TokenCount)

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> "LlmCompleteResult":
        return cls(
            content=d.get("content"),
            finish_reason=str(d.get("finish_reason") or ""),
            usage=TokenCount.from_json(d.get("usage")),
        )


# Internal sentinel marking the end of a streamed completion.
_STREAM_END = object()


@dataclass
class _StreamPending:
    """Pending-registry entry for a streaming ``llm.complete`` request:
    a queue the dispatch loop pushes ``llm.complete.delta`` chunks onto,
    plus a future resolved with the final ``LlmCompleteResult``."""

    chunks: "asyncio.Queue[Any]"
    final: "asyncio.Future[LlmCompleteResult]"


class LlmStream:
    """Async iterator over a streamed ``llm.complete``'s text chunks.

    Usage::

        stream = broker.llm_complete_stream(provider=..., model=..., messages=...)
        async for chunk in stream:
            ...                         # str chunks, in order
        result = await stream.final_result()   # LlmCompleteResult (content is None)

    Drain the iterator (or call ``final_result()``) — an abandoned
    stream's pending entry survives until the adapter shuts down.
    """

    def __init__(self, pending: _StreamPending) -> None:
        self._pending = pending

    def __aiter__(self) -> "LlmStream":
        return self

    async def __anext__(self) -> str:
        item = await self._pending.chunks.get()
        if item is _STREAM_END:
            raise StopAsyncIteration
        return item  # type: ignore[no-any-return]

    async def final_result(self) -> LlmCompleteResult:
        """Await the host's final reply (resolves after every delta has
        been delivered). Re-awaitable. Raises ``RpcServerError`` if the
        host returned an error mid-stream, ``RpcTransportError`` if the
        adapter shut down before the reply landed."""
        return await self._pending.final
