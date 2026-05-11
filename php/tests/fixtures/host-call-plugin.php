<?php

declare(strict_types=1);

/**
 * Host-call fixture: in onEvent, makes a child→host call selected by the
 * event payload's `op` (or `q`) field, then publishes a marker derived
 * from the host's reply (or the error it raised). Used by
 * test_host_calls.php.
 */

require __DIR__ . '/../../vendor/autoload.php';

use Nexo\Plugin\Sdk\BrokerSender;
use Nexo\Plugin\Sdk\Event;
use Nexo\Plugin\Sdk\PluginAdapter;
use Nexo\Plugin\Sdk\RpcServerError;
use Nexo\Plugin\Sdk\RpcTimeoutError;
use Nexo\Plugin\Sdk\RpcTransportError;

$MANIFEST = <<<'TOML'
[plugin]
id = "host_call_plugin"
version = "0.1.0"
name = "HostCall"
description = "fixture"
min_nexo_version = ">=0.1.0"
TOML;

$adapter = new PluginAdapter([
    'manifestToml' => $MANIFEST,
    'handleProcessSignals' => false,
    'onEvent' => function (string $topic, Event $event, BrokerSender $broker): void {
        $op = $event->payload['op'] ?? null;
        $q = $event->payload['q'] ?? null;
        try {
            if ($op === 'recall' || $q !== null) {
                $entries = $broker->memoryRecall([
                    'agentId' => 'agent_x',
                    'query' => $q ?? 'prefs',
                    'limit' => $q !== null ? 10 : 3,
                ]);
                $payload = $q !== null
                    ? ['q' => $q, 'got' => array_map(fn ($e) => $e->content, $entries)]
                    : ['got' => array_map(fn ($e) => $e->content, $entries), 'ids' => array_map(fn ($e) => $e->id, $entries)];
                $broker->publish('plugin.inbound.out', Event::new('plugin.inbound.out', 'host_call_plugin', $payload));
            } elseif ($op === 'complete') {
                $r = $broker->llmComplete(['provider' => 'minimax', 'model' => 'm', 'messages' => [['role' => 'user', 'content' => 'hi']]]);
                $broker->publish('plugin.inbound.out', Event::new('plugin.inbound.out', 'host_call_plugin',
                    ['content' => $r->content, 'finish_reason' => $r->finishReason, 'ct' => $r->usage->completionTokens]));
            } elseif ($op === 'stream') {
                $chunks = [];
                $final = $broker->llmCompleteStream(
                    ['provider' => 'minimax', 'model' => 'm', 'messages' => [['role' => 'user', 'content' => 'hi']]],
                    function (string $c) use (&$chunks): void { $chunks[] = $c; },
                );
                $broker->publish('plugin.inbound.out', Event::new('plugin.inbound.out', 'host_call_plugin',
                    ['chunks' => $chunks, 'joined' => implode('', $chunks), 'content_is_null' => $final->content === null, 'finish_reason' => $final->finishReason]));
            } elseif ($op === 'timeout') {
                try {
                    $broker->memoryRecall(['agentId' => 'a', 'query' => 'q', 'timeoutSec' => 0.4]);
                } catch (RpcTimeoutError $e) {
                    $broker->publish('plugin.inbound.out', Event::new('plugin.inbound.out', 'host_call_plugin',
                        ['timed_out' => true, 'seconds' => $e->seconds]));
                }
            } elseif ($op === 'error') {
                try {
                    $broker->memoryRecall(['agentId' => 'a', 'query' => 'q']);
                } catch (RpcServerError $e) {
                    $broker->publish('plugin.inbound.out', Event::new('plugin.inbound.out', 'host_call_plugin',
                        ['rpc_error_code' => $e->getCode(), 'rpc_error_msg' => $e->serverMessage]));
                }
            } elseif ($op === 'inflight') {
                try {
                    $broker->memoryRecall(['agentId' => 'a', 'query' => 'q']);
                } catch (RpcTransportError $e) {
                    fwrite(STDERR, "HANDLER_GOT_TRANSPORT: {$e->getMessage()}\n");
                }
            }
        } catch (\Throwable $e) {
            fwrite(STDERR, 'HANDLER_RAISED ' . get_class($e) . ': ' . $e->getMessage() . "\n");
        }
    },
]);
$adapter->run();
