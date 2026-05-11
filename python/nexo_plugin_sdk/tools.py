"""Host→child tool dispatch — data types, errors, the tool context.

Plugins that declare ``[plugin.extends].tools = [...]`` in their
``nexo-plugin.toml`` advertise a tool catalog at handshake
(``initialize``-reply ``result.tools``, contract §4.1.1) and receive
one ``tool.invoke`` request per agent-loop tool call (contract §5.t).
Authors register the catalog via :meth:`PluginAdapter.declare_tools`
and a single dispatch handler via :meth:`PluginAdapter.on_tool` (or
:meth:`PluginAdapter.on_tool_with_context` when the handler needs
broker access mid-invocation). Mirrors the Rust SDK's
``crates/microapp-sdk/src/plugin.rs`` (``ToolDef`` / ``ToolInvocation``
/ ``ToolInvocationError`` / ``ToolContext``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .broker import BrokerSender


@dataclass(frozen=True)
class ToolDef:
    """A tool the plugin exposes to the agent loop.

    Advertised in the ``initialize`` reply's ``tools`` array. ``name``
    must satisfy the per-plugin namespace rule (``<plugin_id>_*`` or
    ``ext_<plugin_id>_*``) and MUST appear in the manifest's
    ``[plugin.extends].tools`` list — the SDK fails fast at
    :meth:`PluginAdapter.run` if it does not (mirrors the host's
    hard-failure on the same drift). ``input_schema`` is an arbitrary
    JSON Schema object describing the tool's arguments; the daemon
    caches it for arg validation before each ``tool.invoke``.
    """

    name: str
    description: str
    input_schema: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "input_schema": self.input_schema,
        }


@dataclass(frozen=True)
class ToolInvocation:
    """A decoded ``tool.invoke`` request (contract §5.t).

    ``args`` is whatever JSON the daemon's LLM produced for the call —
    typically an object, but a scalar / array is passed through
    verbatim; ``None`` when the request omitted ``args``. ``agent_id``
    is the agent that issued the call, ``None`` when absent.
    """

    plugin_id: str
    tool_name: str
    args: Any = None
    agent_id: str | None = None


@dataclass
class ToolContext:
    """Host resources a :meth:`PluginAdapter.on_tool_with_context`
    handler can reach: the same broker handle ``on_event`` receives
    (so a tool body can ``await ctx.broker.memory_recall(...)`` /
    ``llm_complete(...)`` mid-invocation), plus the plugin id (handy
    for handlers that dispatch by ``tool_name`` across plugins).
    Designed to grow by field additions only — handlers that don't
    read a future field are unaffected.
    """

    broker: "BrokerSender"
    plugin_id: str


class ToolInvocationError(Exception):
    """Base for the typed failures a ``tool.invoke`` handler can raise.

    Each subclass maps onto a ``-33401..-33405`` JSON-RPC error code
    (see :attr:`code`); the dispatch loop turns a raised subclass into
    the matching ``error`` reply, attaching ``data`` for the variants
    that carry one (``details`` / ``retry_after_ms``). An *uncaught*
    non-``ToolInvocationError`` raised by a handler is mapped to
    :class:`ToolExecutionFailed` (``-33403``). Message prefixes mirror
    the Rust SDK's ``thiserror`` strings so operator greps are
    uniform across SDKs.
    """

    #: JSON-RPC error code for this variant. Overridden per subclass.
    code: int = -33403

    def error_data(self) -> dict[str, Any] | None:
        """Extra ``data`` object for the JSON-RPC error reply, or
        ``None`` when the variant carries none."""
        return None


class ToolNotFound(ToolInvocationError):
    """``-33401`` — the plugin doesn't actually implement the named
    tool (drift between the manifest / advertised catalog and the
    runtime handler)."""

    code = -33401

    def __init__(self, tool_name: str) -> None:
        super().__init__(f"tool not found: {tool_name}")
        self.tool_name = tool_name


class ToolArgumentInvalid(ToolInvocationError):
    """``-33402`` — arguments failed plugin-side validation (semantic
    checks beyond the JSON-Schema the host already ran). ``details``,
    when given, is surfaced as ``error.data.details``."""

    code = -33402

    def __init__(self, message: str, *, details: Any = None) -> None:
        super().__init__(f"invalid argument: {message}")
        self.details = details

    def error_data(self) -> dict[str, Any] | None:
        return None if self.details is None else {"details": self.details}


class ToolExecutionFailed(ToolInvocationError):
    """``-33403`` — the tool ran but failed (network blip, downstream
    5xx, a hung dependency). Also the catch-all the dispatch loop maps
    an uncaught generic exception onto."""

    code = -33403

    def __init__(self, message: str) -> None:
        super().__init__(f"execution failed: {message}")


class ToolUnavailable(ToolInvocationError):
    """``-33404`` — the tool exists but cannot run right now (resource
    exhausted, rate-limited, dependency offline). ``retry_after_ms``,
    when given, is surfaced as ``error.data.retry_after_ms``."""

    code = -33404

    def __init__(self, message: str, *, retry_after_ms: int | None = None) -> None:
        super().__init__(f"unavailable: {message}")
        self.retry_after_ms = retry_after_ms

    def error_data(self) -> dict[str, Any] | None:
        return None if self.retry_after_ms is None else {"retry_after_ms": self.retry_after_ms}


class ToolDenied(ToolInvocationError):
    """``-33405`` — the tool exists but the caller is not authorised
    (the plugin's per-tenant authorization rejected the call)."""

    code = -33405

    def __init__(self, message: str) -> None:
        super().__init__(f"denied: {message}")


def text_result(text: str, *, is_error: bool = False) -> dict[str, Any]:
    """Build the conventional ``ToolResponse``-shaped result a tool
    handler returns for a plain-text outcome:
    ``{"content": [{"type": "text", "text": text}], "is_error": ...}``.
    Returning any other JSON value from a handler is fine — the daemon
    doesn't validate the shape beyond the JSON-RPC envelope."""
    return {"content": [{"type": "text", "text": text}], "is_error": is_error}
