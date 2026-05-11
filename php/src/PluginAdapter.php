<?php

declare(strict_types=1);

/**
 * Child-side dispatch loop for PHP subprocess plugins.
 *
 * Mirrors the Rust counterpart in
 * `crates/microapp-sdk/src/plugin.rs::PluginAdapter` (and the Python /
 * TypeScript SDKs). Reads JSON-RPC 2.0 newline-delimited frames from
 * stdin via non-blocking polls + `stream_select`, dispatches:
 *
 * - `method == "initialize"` (request) → reply with manifest +
 *   server_version.
 * - `method == "broker.event"` (notification) → spawn a Fiber running
 *   `onEvent` so the reader keeps polling stdin while the handler awaits
 *   its own broker / host round-trips.
 * - `method == "shutdown"` (request) → abandon in-flight host calls,
 *   drain in-flight Fibers, reply `{ok: true}`, exit.
 * - `method == "llm.complete.delta"` (notification) → route the chunk to
 *   the awaiting `llmCompleteStream` call.
 * - a frame with an `id` and no `method` → a *response* to a child→host
 *   request we issued (`memory.recall` / `llm.complete`) → fill its
 *   pending entry; unknown id → dropped with a warn.
 * - anything else with an `id` → reply `-32601 method not found`.
 * - anything else without an `id` → silently ignored (JSON-RPC §4.1).
 */

namespace Nexo\Plugin\Sdk;

final class PluginAdapter
{
    private array $manifest;
    private string $serverVersion;
    /** @var (callable(string, Event, BrokerSender): void)|null */
    private $onEvent = null;
    /** @var (callable(): void)|null */
    private $onShutdown = null;
    private bool $enableStdoutGuard;
    private int $maxFrameBytes;
    private bool $handleProcessSignals;
    /** @var ToolDef[] */
    private array $declaredTools = [];
    /** @var (callable(ToolInvocation): mixed)|null */
    private $onTool = null;
    /** @var (callable(ToolInvocation, ToolContext): mixed)|null */
    private $onToolWithContext = null;

    private bool $started = false;
    private bool $stopped = false;
    private string $stdinBuffer = '';
    private Scheduler $scheduler;
    private PendingRegistry $pending;
    private BrokerSender $broker;

    /**
     * @param array{
     *   manifestToml: string,
     *   serverVersion?: string,
     *   onEvent?: callable(string, Event, BrokerSender): void,
     *   onShutdown?: callable(): void,
     *   enableStdoutGuard?: bool,
     *   maxFrameBytes?: int,
     *   handleProcessSignals?: bool,
     * } $opts
     */
    public function __construct(array $opts)
    {
        if (!isset($opts['manifestToml']) || !is_string($opts['manifestToml'])) {
            throw new PluginError("PluginAdapter requires 'manifestToml' string option");
        }
        $this->manifest = Manifest::parse($opts['manifestToml']);
        $this->serverVersion = $opts['serverVersion'] ?? '0.1.0';
        if (isset($opts['onEvent']) && is_callable($opts['onEvent'])) {
            $this->onEvent = $opts['onEvent'];
        }
        if (isset($opts['onShutdown']) && is_callable($opts['onShutdown'])) {
            $this->onShutdown = $opts['onShutdown'];
        }
        $this->enableStdoutGuard = $opts['enableStdoutGuard'] ?? true;
        $this->maxFrameBytes = $opts['maxFrameBytes'] ?? Wire::MAX_FRAME_BYTES;
        $this->handleProcessSignals = $opts['handleProcessSignals'] ?? true;

        // ── tool dispatch (contract §4.1.1 + §5.t) ──────────────────
        if (isset($opts['tools']) && is_array($opts['tools'])) {
            foreach ($opts['tools'] as $t) {
                if (!$t instanceof ToolDef) {
                    throw new PluginError("PluginAdapter 'tools' option must be an array of ToolDef");
                }
                $this->declaredTools[] = $t;
            }
        }
        $owc = (isset($opts['onToolWithContext']) && is_callable($opts['onToolWithContext'])) ? $opts['onToolWithContext'] : null;
        $ot = (isset($opts['onTool']) && is_callable($opts['onTool'])) ? $opts['onTool'] : null;
        // with-context wins when both are set — mirrors the Rust SDK.
        $this->onToolWithContext = $owc;
        $this->onTool = $owc !== null ? null : $ot;
        if ($this->declaredTools !== []) {
            $allowed = self::manifestExtendsTools($this->manifest);
            $names = array_map(static fn (ToolDef $t): string => $t->name, $this->declaredTools);
            if ($allowed === null) {
                throw new ManifestError(
                    "'tools' option used but the manifest has no [plugin.extends].tools list; declared: " . implode(', ', $names),
                );
            }
            $offenders = array_values(array_filter($names, static fn (string $n): bool => !in_array($n, $allowed, true)));
            if ($offenders !== []) {
                throw new ManifestError(
                    'declared tool(s) not in [plugin.extends].tools [' . implode(', ', $allowed) . ']: ' . implode(', ', $offenders),
                );
            }
        }

        if ($this->enableStdoutGuard) {
            StdoutGuard::install();
        }

        $this->scheduler = new Scheduler();
        $this->pending = new PendingRegistry();
        $this->broker = new BrokerSender($this->pending);
    }

    /** @return array<string, mixed> */
    public function manifest(): array
    {
        return $this->manifest;
    }

    /** Single-shot. Throws PluginError if called twice. */
    public function run(): void
    {
        if ($this->started) {
            throw new PluginError('PluginAdapter::run() already invoked');
        }
        $this->started = true;

        if ($this->handleProcessSignals && function_exists('pcntl_async_signals')) {
            pcntl_async_signals(true);
            $stopCb = function (): void {
                $this->stopped = true;
            };
            if (defined('SIGTERM')) {
                pcntl_signal(SIGTERM, $stopCb);
            }
            if (defined('SIGINT')) {
                pcntl_signal(SIGINT, $stopCb);
            }
        }

        stream_set_blocking(STDIN, false);

        try {
            while (!$this->stopped) {
                $line = $this->readLineNonBlocking();
                if ($line !== null && $line !== '') {
                    $this->handleLine($line);
                }
                if (feof(STDIN) && $this->stdinBuffer === '') {
                    break;
                }
                $this->scheduler->tick();
                usleep(1_000);
            }
        } finally {
            // Abandon outstanding host calls (host is gone) so handler
            // Fibers stuck waiting for a reply get RpcTransportError and
            // terminate — otherwise drain() would spin forever.
            $this->pending->abandonAll();
            $this->scheduler->drain();
        }
    }

    /**
     * Pull complete lines from stdin (non-blocking). Returns the next
     * complete line if one is available, '' if the buffer has data but
     * no newline yet, or null if no input is ready.
     */
    private function readLineNonBlocking(): ?string
    {
        $read = [STDIN];
        $write = null;
        $except = null;
        $count = @stream_select($read, $write, $except, 0, 0);
        if ($count === false || $count === 0) {
            return $this->popBufferedLine();
        }
        $chunk = fread(STDIN, 65536);
        if ($chunk === false || $chunk === '') {
            return $this->popBufferedLine();
        }
        $this->stdinBuffer .= $chunk;
        $idx = strpos($this->stdinBuffer, "\n");
        if ($idx === false) {
            return '';
        }
        $line = substr($this->stdinBuffer, 0, $idx);
        $this->stdinBuffer = substr($this->stdinBuffer, $idx + 1);
        return $line;
    }

    private function popBufferedLine(): ?string
    {
        $idx = strpos($this->stdinBuffer, "\n");
        if ($idx === false) {
            return null;
        }
        $line = substr($this->stdinBuffer, 0, $idx);
        $this->stdinBuffer = substr($this->stdinBuffer, $idx + 1);
        return $line;
    }

    private function handleLine(string $line): void
    {
        $byteLen = strlen($line);
        if ($byteLen > $this->maxFrameBytes) {
            fwrite(STDERR, "plugin: inbound frame $byteLen bytes exceeds maxFrameBytes {$this->maxFrameBytes}\n");
            return;
        }
        try {
            $msg = json_decode($line, true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            fwrite(STDERR, 'plugin: malformed jsonrpc line: ' . $e->getMessage() . "\n");
            return;
        }
        if (!is_array($msg)) {
            fwrite(STDERR, "plugin: jsonrpc frame must be an object\n");
            return;
        }
        $method = $msg['method'] ?? null;
        $id = $msg['id'] ?? null;

        if (!is_string($method)) {
            // A response (id + result/error) to a request we issued, or
            // garbage. Never reply to it.
            if (is_int($id) && $this->pending->has($id)) {
                $err = (isset($msg['error']) && is_array($msg['error'])) ? $msg['error'] : null;
                $this->pending->resolveResponse($id, $msg['result'] ?? null, $err);
            } elseif ($id !== null) {
                fwrite(STDERR, 'plugin: response for unknown/expired request id ' . json_encode($id) . ", dropped\n");
            }
            return;
        }

        if ($method === 'initialize') {
            $this->replyInitialize($id);
        } elseif ($method === 'broker.event') {
            $this->dispatchEvent($msg['params'] ?? null);
        } elseif ($method === 'tool.invoke') {
            $this->handleToolInvoke($id, $msg['params'] ?? null);
        } elseif ($method === 'llm.complete.delta') {
            $this->routeDelta($msg['params'] ?? null);
        } elseif ($method === 'shutdown') {
            $this->replyShutdown($id);
            $this->stopped = true;
        } elseif ($id !== null) {
            $this->writeFrame(Wire::buildErrorResponse($id, -32601, 'method not found'));
        }
        // Unknown notification (no id) — silently ignore per JSON-RPC §4.1.
    }

    private function routeDelta(mixed $params): void
    {
        $p = is_array($params) ? $params : [];
        $rid = $p['request_id'] ?? null;
        $chunk = $p['chunk'] ?? null;
        if (is_int($rid) && is_string($chunk) && $this->pending->appendChunk($rid, $chunk)) {
            return;
        }
        fwrite(STDERR, 'plugin: llm.complete.delta for unknown/non-stream request ' . json_encode($rid) . ", dropped\n");
    }

    private function replyInitialize(int|string|null $id): void
    {
        if ($id === null) {
            return;
        }
        $result = [
            'manifest' => $this->manifest,
            'server_version' => $this->serverVersion,
        ];
        if ($this->declaredTools !== []) {
            $result['tools'] = array_map(static fn (ToolDef $t): array => $t->toJson(), $this->declaredTools);
        }
        $this->writeFrame(Wire::buildResponse($id, $result));
    }

    /**
     * Return the manifest's `[plugin.extends].tools` list, or `null`
     * when the section / field is absent.
     *
     * @param array<string, mixed> $manifest
     * @return list<string>|null
     * @throws ManifestError when present but not a list of strings
     */
    private static function manifestExtendsTools(array $manifest): ?array
    {
        $plugin = $manifest['plugin'] ?? null;
        $extends = is_array($plugin) ? ($plugin['extends'] ?? null) : null;
        if (!is_array($extends) || !array_key_exists('tools', $extends)) {
            return null;
        }
        $tools = $extends['tools'];
        if (!is_array($tools)) {
            throw new ManifestError('[plugin.extends].tools must be a list of strings');
        }
        foreach ($tools as $t) {
            if (!is_string($t)) {
                throw new ManifestError('[plugin.extends].tools must be a list of strings');
            }
        }
        return array_values($tools);
    }

    /**
     * Handle a `tool.invoke` host→child request (contract §5.t). No
     * handler registered → `-32601` (mirrors the Rust SDK: no-handler
     * wins over param shape). The handler runs in a Fiber so a tool
     * body can call `$broker->memoryRecall(...)` / `llmComplete(...)`
     * mid-invocation; the Fiber is tracked in the scheduler's drain
     * set so `shutdown` waits for an in-flight tool.
     */
    private function handleToolInvoke(int|string|null $id, mixed $params): void
    {
        if ($id === null) {
            fwrite(STDERR, "plugin: tool.invoke frame without an id, dropped\n");
            return;
        }
        if ($this->onTool === null && $this->onToolWithContext === null) {
            $this->writeFrame(Wire::buildErrorResponse($id, -32601, 'method not found: tool.invoke'));
            return;
        }
        if (!is_array($params)) {
            $this->writeFrame(Wire::buildErrorResponse($id, -32602, 'tool.invoke params must be an object'));
            return;
        }
        $toolName = $params['tool_name'] ?? null;
        if (!is_string($toolName) || $toolName === '') {
            $this->writeFrame(Wire::buildErrorResponse($id, -32602, 'tool.invoke params missing string `tool_name`'));
            return;
        }
        $pluginId = $params['plugin_id'] ?? null;
        $agentId = $params['agent_id'] ?? null;
        $invocation = new ToolInvocation(
            is_string($pluginId) ? $pluginId : (string) ($this->manifest['plugin']['id'] ?? ''),
            $toolName,
            $params['args'] ?? null,
            is_string($agentId) ? $agentId : null,
        );
        $this->scheduler->spawn(function () use ($invocation, $id): void {
            try {
                if ($this->onToolWithContext !== null) {
                    $ctx = new ToolContext($this->broker, (string) ($this->manifest['plugin']['id'] ?? ''));
                    $result = ($this->onToolWithContext)($invocation, $ctx);
                } else {
                    // onTool is non-null — checked in handleToolInvoke.
                    $result = ($this->onTool)($invocation);
                }
            } catch (ToolError $e) {
                $this->writeToolError($id, $e);
                return;
            } catch (\Throwable $e) {
                $this->writeToolError($id, new ToolExecutionFailed($e->getMessage()));
                return;
            }
            $encoded = json_encode(
                ['jsonrpc' => Wire::JSONRPC_VERSION, 'id' => $id, 'result' => $result],
                JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE,
            );
            if ($encoded === false) {
                $this->writeToolError(
                    $id,
                    new ToolExecutionFailed('tool result not JSON-serializable: ' . json_last_error_msg()),
                );
                return;
            }
            fwrite(STDOUT, $encoded . "\n");
            fflush(STDOUT);
        });
    }

    private function writeToolError(int|string|null $id, ToolError $err): void
    {
        $error = ['code' => $err->getCode(), 'message' => $err->getMessage()];
        $data = $err->errorData();
        if ($data !== null) {
            $error['data'] = $data;
        }
        $this->writeFrame(['jsonrpc' => Wire::JSONRPC_VERSION, 'id' => $id, 'error' => $error]);
    }

    private function replyShutdown(int|string|null $id): void
    {
        $this->pending->abandonAll();
        $this->scheduler->drain();
        if ($this->onShutdown !== null) {
            try {
                ($this->onShutdown)();
            } catch (\Throwable $e) {
                fwrite(STDERR, 'plugin: onShutdown raised: ' . $e->getMessage() . "\n");
            }
        }
        if ($id !== null) {
            $this->writeFrame(Wire::buildResponse($id, ['ok' => true]));
        }
    }

    private function dispatchEvent(mixed $params): void
    {
        if ($this->onEvent === null) {
            return;
        }
        if (!is_array($params)) {
            fwrite(STDERR, "plugin: broker.event params must be a JSON object\n");
            return;
        }
        $topic = $params['topic'] ?? null;
        if (!is_string($topic)) {
            fwrite(STDERR, "plugin: broker.event params missing string `topic`\n");
            return;
        }
        $rawEvent = $params['event'] ?? [];
        if (!is_array($rawEvent)) {
            $rawEvent = [];
        }
        try {
            $event = Event::fromJson($rawEvent);
        } catch (WireError $e) {
            fwrite(STDERR, 'plugin: dispatch decode failed: ' . $e->getMessage() . "\n");
            return;
        }
        $handler = $this->onEvent;
        $broker = $this->broker;
        $this->scheduler->spawn(function () use ($handler, $topic, $event, $broker): void {
            try {
                $handler($topic, $event, $broker);
            } catch (\Throwable $e) {
                fwrite(STDERR, 'plugin: onEvent raised: ' . $e->getMessage() . "\n");
            }
        });
    }

    /** @param array<string, mixed> $frame */
    private function writeFrame(array $frame): void
    {
        fwrite(STDOUT, Wire::serializeFrame($frame));
        fflush(STDOUT);
    }
}
