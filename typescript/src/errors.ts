/**
 * Phase 31.5 — exception types raised by the TypeScript plugin SDK.
 *
 * The `name` property is set explicitly on each subclass so
 * `error instanceof PluginError` and `error.name === "ManifestError"`
 * keep working after TypeScript transpilation. Without that,
 * subclasses inherit `Error`'s `name === "Error"` and instanceof
 * checks become unreliable across module boundaries.
 */

export class PluginError extends Error {
  override readonly name: string = "PluginError";

  constructor(message: string) {
    super(message);
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export class ManifestError extends PluginError {
  override readonly name: string = "ManifestError";
  /** When the failure points at a specific manifest field
   * (`plugin.id`, `plugin.version`, etc.) the SDK populates this
   * so callers can render targeted error messages.
   */
  readonly field?: string;

  constructor(message: string, field?: string) {
    super(message);
    Object.setPrototypeOf(this, ManifestError.prototype);
    this.field = field;
  }
}

export class WireError extends PluginError {
  override readonly name: string = "WireError";

  constructor(message: string) {
    super(message);
    Object.setPrototypeOf(this, WireError.prototype);
  }
}

/** A child→host JSON-RPC request failed. Base for the concrete failure
 * kinds below; mirrors the Rust SDK's `RpcError` enum. */
export class RpcError extends PluginError {
  override readonly name: string = "RpcError";

  constructor(message: string) {
    super(message);
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/** The host returned a JSON-RPC error response. `code` is the JSON-RPC
 * error code (common: `-32601` not wired host-side, `-32602` invalid
 * params, `-32603` backend / not-configured). */
export class RpcServerError extends RpcError {
  override readonly name: string = "RpcServerError";
  readonly code: number;

  constructor(code: number, message: string) {
    super(`rpc error ${code}: ${message}`);
    Object.setPrototypeOf(this, RpcServerError.prototype);
    this.code = code;
  }
}

/** No reply within the request timeout (default 30 s). */
export class RpcTimeoutError extends RpcError {
  override readonly name: string = "RpcTimeoutError";
  /** The timeout in seconds. */
  readonly seconds: number;

  constructor(seconds: number) {
    super(`no host reply within ${seconds}s`);
    Object.setPrototypeOf(this, RpcTimeoutError.prototype);
    this.seconds = seconds;
  }
}

/** The request could not be sent, or the pending reply was abandoned
 * (host crashed mid-call, or the adapter is shutting down). */
export class RpcTransportError extends RpcError {
  override readonly name: string = "RpcTransportError";

  constructor(message: string) {
    super(message);
    Object.setPrototypeOf(this, RpcTransportError.prototype);
  }
}

/** The host's response did not match the expected result shape. */
export class RpcDecodeError extends RpcError {
  override readonly name: string = "RpcDecodeError";

  constructor(message: string) {
    super(message);
    Object.setPrototypeOf(this, RpcDecodeError.prototype);
  }
}
