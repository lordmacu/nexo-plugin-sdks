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
        $this->writeFrame(Wire::buildResponse($id, [
            'manifest' => $this->manifest,
            'server_version' => $this->serverVersion,
        ]));
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
