/**
 * Public API for the TypeScript plugin SDK (npm: `nexo-plugin-sdk`).
 *
 * Mirrors the Rust child SDK (`crates/microapp-sdk`, feature `plugin`).
 */

export { PluginAdapter } from "./adapter.js";
export type {
  EventHandler,
  ShutdownHandler,
  PluginAdapterOptions,
} from "./adapter.js";
export {
  ToolError,
  ToolNotFoundError,
  ToolArgumentInvalidError,
  ToolExecutionFailedError,
  ToolUnavailableError,
  ToolDeniedError,
  toolDefToJson,
  textResult,
} from "./tools.js";
export type {
  ToolDef,
  ToolInvocation,
  ToolContext,
  ToolHandler,
  ToolHandlerWithContext,
} from "./tools.js";
export { BrokerSender } from "./broker.js";
export type { LineWriter, MemoryRecallOptions } from "./broker.js";
export { Event } from "./events.js";
export {
  PluginError,
  ManifestError,
  WireError,
  RpcError,
  RpcServerError,
  RpcTimeoutError,
  RpcTransportError,
  RpcDecodeError,
} from "./errors.js";
export {
  DEFAULT_RPC_TIMEOUT_MS,
  LlmStream,
  parseMemoryEntry,
  parseLlmCompleteResult,
} from "./host.js";
export type {
  MemoryEntry,
  Message,
  TokenCount,
  LlmCompleteResult,
  LlmCompleteOptions,
} from "./host.js";
export { parseManifest } from "./manifest.js";
export type { ManifestPluginSection, ParsedManifest } from "./manifest.js";
export {
  installStdoutGuard,
  uninstallStdoutGuard,
  isStdoutGuardInstalled,
  STDOUT_GUARD_MARKER,
} from "./stdout-guard.js";
export {
  JSONRPC_VERSION,
  MAX_FRAME_BYTES,
  serializeFrame,
  buildResponse,
  buildErrorResponse,
} from "./wire.js";
export type {
  JsonRpcId,
  JsonRpcRequest,
  JsonRpcNotification,
  JsonRpcResponse,
  JsonRpcErrorResponse,
  JsonRpcFrame,
} from "./wire.js";

export const VERSION = "0.3.0";
