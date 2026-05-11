"""Exception types raised by the Python plugin SDK."""


class PluginError(Exception):
    """Base class for SDK errors."""


class ManifestError(PluginError):
    """`nexo-plugin.toml` failed to parse or required fields are missing."""


class WireError(PluginError):
    """Malformed JSON-RPC frame received from the host, or malformed
    payload sent by the plugin author's handler."""


class RpcError(PluginError):
    """A child→host JSON-RPC request failed. Base for the concrete
    failure kinds below; mirrors the Rust SDK's ``RpcError`` enum."""


class RpcServerError(RpcError):
    """The host returned a JSON-RPC error response. ``code`` is the
    JSON-RPC error code (common: ``-32601`` method not wired host-side,
    ``-32602`` invalid params, ``-32603`` backend / not-configured)."""

    def __init__(self, code: int, message: str) -> None:
        super().__init__(f"rpc error {code}: {message}")
        self.code = code
        self.message = message


class RpcTimeoutError(RpcError):
    """No reply within the request timeout (default 30 s)."""

    def __init__(self, seconds: float) -> None:
        super().__init__(f"no host reply within {seconds:g}s")
        self.seconds = seconds


class RpcTransportError(RpcError):
    """The request could not be sent, or the pending reply was abandoned
    (host crashed mid-call, or the adapter is shutting down)."""


class RpcDecodeError(RpcError):
    """The host's response did not match the expected result shape."""
