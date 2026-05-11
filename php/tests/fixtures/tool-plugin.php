<?php

declare(strict_types=1);

/**
 * Tool fixture: declares a tool catalog and an onToolWithContext handler
 * that dispatches by toolName — happy paths, the -33xxx error band, an
 * uncaught throw, a non-serializable result, a slow tool, and a tool
 * that calls back into the host mid-invocation. Used by test_tools.php.
 */

require __DIR__ . '/../../vendor/autoload.php';

use Nexo\Plugin\Sdk\BrokerSender;
use Nexo\Plugin\Sdk\Event;
use Nexo\Plugin\Sdk\PluginAdapter;
use Nexo\Plugin\Sdk\Tool;
use Nexo\Plugin\Sdk\ToolArgumentInvalid;
use Nexo\Plugin\Sdk\ToolContext;
use Nexo\Plugin\Sdk\ToolDef;
use Nexo\Plugin\Sdk\ToolDenied;
use Nexo\Plugin\Sdk\ToolExecutionFailed;
use Nexo\Plugin\Sdk\ToolInvocation;
use Nexo\Plugin\Sdk\ToolNotFound;
use Nexo\Plugin\Sdk\ToolUnavailable;

$NAMES = [
    'tool_plugin_echo', 'tool_plugin_recall', 'tool_plugin_slow', 'tool_plugin_fail',
    'tool_plugin_argbad', 'tool_plugin_busy', 'tool_plugin_nope', 'tool_plugin_denied',
    'tool_plugin_boom', 'tool_plugin_badjson',
];

$MANIFEST = "[plugin]\n"
    . "id = \"tool_plugin\"\n"
    . "version = \"0.1.0\"\n"
    . "name = \"ToolPlugin\"\n"
    . "description = \"fixture\"\n"
    . "min_nexo_version = \">=0.1.0\"\n\n"
    . "[plugin.extends]\n"
    . 'tools = [' . implode(', ', array_map(static fn (string $n): string => "\"$n\"", $NAMES)) . "]\n";

$adapter = new PluginAdapter([
    'manifestToml' => $MANIFEST,
    'handleProcessSignals' => false,
    'tools' => array_map(static fn (string $n): ToolDef => new ToolDef($n, 't', ['type' => 'object']), $NAMES),
    'onEvent' => function (string $topic, Event $event, BrokerSender $broker): void {
        $broker->publish('plugin.inbound.out', Event::new('plugin.inbound.out', 'tool_plugin', ['event_seen' => $event->payload['k'] ?? null]));
    },
    'onToolWithContext' => function (ToolInvocation $inv, ToolContext $ctx): mixed {
        switch ($inv->toolName) {
            case 'tool_plugin_echo':
                return ['echoed' => $inv->args, 'agent' => $inv->agentId, 'plugin' => $inv->pluginId];
            case 'tool_plugin_recall':
                $entries = $ctx->broker->memoryRecall(['agentId' => 'a', 'query' => $inv->args['q']]);
                return ['recalled' => array_map(static fn ($e) => $e->content, $entries), 'plugin_id' => $ctx->pluginId];
            case 'tool_plugin_slow':
                $deadline = microtime(true) + 0.35;
                while (microtime(true) < $deadline) {
                    \Fiber::suspend();
                }
                return Tool::text('slow done');
            case 'tool_plugin_fail':
                throw new ToolExecutionFailed('downstream 500');
            case 'tool_plugin_argbad':
                throw new ToolArgumentInvalid('bad url', ['field' => 'url']);
            case 'tool_plugin_busy':
                throw new ToolUnavailable('rate limited', 5000);
            case 'tool_plugin_nope':
                throw new ToolNotFound($inv->toolName);
            case 'tool_plugin_denied':
                throw new ToolDenied('tenant blocked');
            case 'tool_plugin_boom':
                throw new \RuntimeException('uncaught generic');
            case 'tool_plugin_badjson':
                return ['handle' => fopen('php://memory', 'r')]; // resource — json_encode returns false
            default:
                throw new ToolNotFound($inv->toolName);
        }
    },
]);
$adapter->run();
