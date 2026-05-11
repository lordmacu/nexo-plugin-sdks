<?php

declare(strict_types=1);

/**
 * PHP conformance fixture — the `fixture_config` interpreter on top of
 * `nexo/plugin-sdk`'s PluginAdapter. The mock-host spawns this with
 * `--config <json>` (the scenario's fixture_config + the manifest
 * injected under "manifest"), or `--print-capabilities`.
 *
 * Wire output here must be byte-identical to the Python / TS / Rust
 * fixtures for the same config — that's what the conformance kit checks.
 *
 * Note: PHP is Fiber-cooperative. The `slow:<ms>` / `slow_publish:<ms>`
 * behaviors yield via `\Fiber::suspend()` in a loop, NOT `usleep()`,
 * so the dispatch reader isn't blocked.
 */

require __DIR__ . '/../../php/vendor/autoload.php';

use Nexo\Plugin\Sdk\BrokerSender;
use Nexo\Plugin\Sdk\Event;
use Nexo\Plugin\Sdk\Manifest;
use Nexo\Plugin\Sdk\PluginAdapter;
use Nexo\Plugin\Sdk\RpcServerError;
use Nexo\Plugin\Sdk\RpcTimeoutError;
use Nexo\Plugin\Sdk\RpcTransportError;
use Nexo\Plugin\Sdk\Tool;
use Nexo\Plugin\Sdk\ToolArgumentInvalid;
use Nexo\Plugin\Sdk\ToolContext;
use Nexo\Plugin\Sdk\ToolDef;
use Nexo\Plugin\Sdk\ToolDenied;
use Nexo\Plugin\Sdk\ToolExecutionFailed;
use Nexo\Plugin\Sdk\ToolInvocation;
use Nexo\Plugin\Sdk\ToolNotFound;
use Nexo\Plugin\Sdk\ToolUnavailable;

const DEFAULT_CAPS = ['core', 'memory', 'llm', 'tools'];

/** @return array{0:string,1:array<string,mixed>} */
function parse_args(array $argv): array
{
    if (in_array('--print-capabilities', $argv, true)) {
        return ['caps', []];
    }
    $i = array_search('--config', $argv, true);
    if ($i === false || !isset($argv[$i + 1])) {
        fwrite(STDERR, "php_fixture: expected --print-capabilities or --config <json>\n");
        exit(2);
    }
    $cfg = json_decode($argv[$i + 1], true);
    return ['run', is_array($cfg) ? $cfg : []];
}

function yield_for_ms(int $ms): void
{
    $deadline = microtime(true) + $ms / 1000.0;
    while (microtime(true) < $deadline) {
        \Fiber::suspend();
    }
}

// ── tool behaviors ──────────────────────────────────────────────────

/** @return mixed */
function apply_tool_behavior(string $behavior, ToolInvocation $inv, ?ToolContext $ctx)
{
    if ($behavior === 'echo') {
        return ['echoed' => $inv->args, 'agent' => $inv->agentId, 'plugin' => $inv->pluginId];
    }
    if (str_starts_with($behavior, 'text:')) {
        return Tool::text(substr($behavior, strlen('text:')));
    }
    if ($behavior === 'recall_then_return') {
        if ($ctx === null) {
            throw new ToolExecutionFailed('recall_then_return needs use_tool_context: true');
        }
        $q = is_array($inv->args) ? ($inv->args['q'] ?? null) : null;
        $entries = $ctx->broker->memoryRecall(['agentId' => $inv->agentId ?? '', 'query' => $q]);
        return ['recalled' => array_map(static fn ($e) => $e->content, $entries), 'plugin_id' => $ctx->pluginId];
    }
    if (str_starts_with($behavior, 'slow:')) {
        yield_for_ms((int) substr($behavior, strlen('slow:')));
        return Tool::text('done');
    }
    if ($behavior === 'raise:33401' || $behavior === 'not_found') {
        throw new ToolNotFound($inv->toolName);
    }
    if ($behavior === 'raise:33402') {
        throw new ToolArgumentInvalid('conformance bad arg', ['field' => 'q']);
    }
    if ($behavior === 'raise:33403') {
        throw new ToolExecutionFailed('conformance exec fail');
    }
    if ($behavior === 'raise:33404') {
        throw new ToolUnavailable('conformance unavailable', 5000);
    }
    if ($behavior === 'raise:33405') {
        throw new ToolDenied('conformance denied');
    }
    if ($behavior === 'raise_generic') {
        throw new \RuntimeException('conformance generic error');
    }
    if ($behavior === 'return_unserializable') {
        return ['handle' => fopen('php://memory', 'r')]; // a resource — json_encode returns false
    }
    throw new ToolNotFound($inv->toolName);
}

function make_tool_handler(array $cfg, bool $useCtx): callable
{
    $perTool = is_array($cfg['tool_handlers'] ?? null) ? $cfg['tool_handlers'] : [];
    $default = $cfg['tool_handler'] ?? null;
    $behaviorFor = static fn (string $name) => $perTool[$name] ?? $default;
    if ($useCtx) {
        return static function (ToolInvocation $inv, ToolContext $ctx) use ($behaviorFor) {
            $b = $behaviorFor($inv->toolName);
            if ($b === null) {
                throw new ToolNotFound($inv->toolName);
            }
            return apply_tool_behavior($b, $inv, $ctx);
        };
    }
    return static function (ToolInvocation $inv) use ($behaviorFor) {
        $b = $behaviorFor($inv->toolName);
        if ($b === null) {
            throw new ToolNotFound($inv->toolName);
        }
        return apply_tool_behavior($b, $inv, null);
    };
}

// ── event behaviors ─────────────────────────────────────────────────

function make_event_handler(array $cfg, string $pluginId): ?callable
{
    $eh = $cfg['event_handler'] ?? 'never';
    if ($eh === 'never') {
        return null;
    }
    $pub = static fn (BrokerSender $broker, array $payload) =>
        $broker->publish('plugin.inbound.conf', Event::new('plugin.inbound.conf', $pluginId, $payload));

    return static function (string $topic, Event $event, BrokerSender $broker) use ($eh, $pub): void {
        $payload = $event->payload ?? [];
        if ($eh === 'publish_marker') {
            $pub($broker, ['event_seen' => $payload['k'] ?? null]);
            return;
        }
        if (str_starts_with($eh, 'slow_publish:')) {
            yield_for_ms((int) substr($eh, strlen('slow_publish:')));
            $pub($broker, ['event_seen' => $payload['k'] ?? null]);
            return;
        }
        if ($eh === 'host_call') {
            $op = $payload['op'] ?? null;
            if ($op === 'recall') {
                $opts = ['agentId' => 'conf', 'query' => $payload['q'] ?? 'q'];
                if (isset($payload['timeout'])) {
                    $opts['timeoutSec'] = $payload['timeout'];
                }
                $entries = $broker->memoryRecall($opts);
                $pub($broker, ['recalled' => array_map(static fn ($e) => $e->content, $entries)]);
            } elseif ($op === 'recall_error') {
                try {
                    $broker->memoryRecall(['agentId' => 'conf', 'query' => 'q']);
                } catch (RpcServerError $e) {
                    $pub($broker, ['rpc_error' => ['code' => $e->getCode()]]);
                }
            } elseif ($op === 'recall_timeout') {
                try {
                    $broker->memoryRecall(['agentId' => 'conf', 'query' => 'q', 'timeoutSec' => $payload['timeout'] ?? 0.4]);
                } catch (RpcTimeoutError $e) {
                    $pub($broker, ['timed_out' => true]);
                }
            } elseif ($op === 'complete') {
                $r = $broker->llmComplete(['provider' => $payload['provider'] ?? 'minimax', 'model' => $payload['model'] ?? 'm', 'messages' => [['role' => 'user', 'content' => 'hi']]]);
                $pub($broker, ['content' => $r->content, 'finish_reason' => $r->finishReason, 'ct' => $r->usage->completionTokens]);
            } elseif ($op === 'complete_error') {
                try {
                    $broker->llmComplete(['provider' => 'minimax', 'model' => 'm', 'messages' => [['role' => 'user', 'content' => 'hi']]]);
                } catch (RpcServerError $e) {
                    $pub($broker, ['rpc_error' => ['code' => $e->getCode()]]);
                }
            } elseif ($op === 'stream') {
                $chunks = [];
                $final = $broker->llmCompleteStream(
                    ['provider' => 'minimax', 'model' => 'm', 'messages' => [['role' => 'user', 'content' => 'hi']]],
                    function (string $c) use (&$chunks): void { $chunks[] = $c; },
                );
                $pub($broker, ['joined' => implode('', $chunks), 'content_is_none' => $final->content === null, 'finish_reason' => $final->finishReason]);
            } elseif ($op === 'inflight') {
                try {
                    $broker->memoryRecall(['agentId' => 'conf', 'query' => 'q']);
                } catch (RpcTransportError $e) {
                    fwrite(STDERR, "INFLIGHT_GOT_TRANSPORT\n");
                }
            }
        }
    };
}

// ── main ────────────────────────────────────────────────────────────

[$mode, $cfg] = parse_args($argv);
if ($mode === 'caps') {
    $caps = (is_array($cfg['capabilities'] ?? null) && $cfg['capabilities']) ? $cfg['capabilities'] : DEFAULT_CAPS;
    fwrite(STDOUT, json_encode($caps) . "\n");
    exit(0);
}

$manifestToml = $cfg['manifest'];
$useCtx = ($cfg['use_tool_context'] ?? true) !== false;
$declare = is_array($cfg['declare_tools'] ?? null) ? $cfg['declare_tools'] : [];
$tools = array_map(static fn (string $n) => new ToolDef($n, "conformance tool {$n}", ['type' => 'object']), $declare);
$hasToolHandler = array_key_exists('tool_handler', $cfg) || array_key_exists('tool_handlers', $cfg);
$pluginId = (string) (Manifest::parse($manifestToml)['plugin']['id'] ?? '');

$opts = [
    'manifestToml' => $manifestToml,
    'serverVersion' => $cfg['server_version'] ?? 'conformance-fixture-0',
    'onShutdown' => static function (): void {},
    'tools' => $tools,
    'handleProcessSignals' => false,
];
if ($hasToolHandler) {
    if ($useCtx) {
        $opts['onToolWithContext'] = make_tool_handler($cfg, true);
    } else {
        $opts['onTool'] = make_tool_handler($cfg, false);
    }
}
$eh = make_event_handler($cfg, $pluginId);
if ($eh !== null) {
    $opts['onEvent'] = $eh;
}
(new PluginAdapter($opts))->run();
