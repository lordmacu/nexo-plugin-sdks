"""Python SDK for nexo subprocess plugins.

Child-side counterpart to the host's plugin protocol
(``nexo-plugin-contract.md``). Distributed on PyPI as ``nexoai``; the
importable module is ``nexo_plugin_sdk``. Public API mirrors the Rust
child SDK (``crates/microapp-sdk``, feature ``plugin``).
"""

from .adapter import (
    EventHandler,
    PluginAdapter,
    ShutdownHandler,
    ToolHandler,
    ToolHandlerWithContext,
)
from .broker import BrokerSender
from .errors import (
    ManifestError,
    PluginError,
    RpcDecodeError,
    RpcError,
    RpcServerError,
    RpcTimeoutError,
    RpcTransportError,
    WireError,
)
from .events import Event
from .host import (
    DEFAULT_RPC_TIMEOUT,
    LlmCompleteResult,
    LlmStream,
    Message,
    MemoryEntry,
    TokenCount,
)
from .manifest import read_manifest
from .tools import (
    ToolArgumentInvalid,
    ToolContext,
    ToolDef,
    ToolDenied,
    ToolExecutionFailed,
    ToolInvocation,
    ToolInvocationError,
    ToolNotFound,
    ToolUnavailable,
    text_result,
)
from .stdout_guard import (
    STDOUT_GUARD_MARKER,
    install_stdout_guard,
    is_stdout_guard_installed,
    uninstall_stdout_guard,
)
from .wire import (
    JSONRPC_VERSION,
    MAX_FRAME_BYTES,
    build_error_response,
    build_notification,
    build_response,
    serialize_frame,
)

__all__ = [
    # Core
    "PluginAdapter",
    "BrokerSender",
    "Event",
    "EventHandler",
    "ShutdownHandler",
    "ToolHandler",
    "ToolHandlerWithContext",
    # Tool dispatch surface (contract §4.1.1 + §5.t)
    "ToolDef",
    "ToolInvocation",
    "ToolContext",
    "ToolInvocationError",
    "ToolNotFound",
    "ToolArgumentInvalid",
    "ToolExecutionFailed",
    "ToolUnavailable",
    "ToolDenied",
    "text_result",
    # Host-call surface
    "MemoryEntry",
    "Message",
    "TokenCount",
    "LlmCompleteResult",
    "LlmStream",
    "DEFAULT_RPC_TIMEOUT",
    # Errors
    "PluginError",
    "ManifestError",
    "WireError",
    "RpcError",
    "RpcServerError",
    "RpcTimeoutError",
    "RpcTransportError",
    "RpcDecodeError",
    # Manifest
    "read_manifest",
    # Stdout guard
    "install_stdout_guard",
    "uninstall_stdout_guard",
    "is_stdout_guard_installed",
    "STDOUT_GUARD_MARKER",
    # Wire helpers
    "JSONRPC_VERSION",
    "MAX_FRAME_BYTES",
    "serialize_frame",
    "build_response",
    "build_error_response",
    "build_notification",
]

__version__ = "0.3.0"
