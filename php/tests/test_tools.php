<?php

declare(strict_types=1);

/**
 * Tests for host→child tool dispatch (contract §4.1.1 + §5.t).
 *
 * Inline unit assertions for the tool data types / error band, plus
 * interactive subprocess tests: spawn a fixture declaring a tool catalog
 * + an onToolWithContext handler; play the *host* — feed `initialize`
 * (assert the advertised `tools`), then `tool.invoke` frames, asserting
 * the result/error reply (and serving the inner `memory.recall` the
 * with-context tool makes). Mirrors test_host_calls.php's harness.
 */

require __DIR__ . '/../vendor/autoload.php';

use Nexo\Plugin\Sdk\ManifestError;
use Nexo\Plugin\Sdk\Tool;
use Nexo\Plugin\Sdk\ToolArgumentInvalid;
use Nexo\Plugin\Sdk\ToolDef;
use Nexo\Plugin\Sdk\ToolDenied;
use Nexo\Plugin\Sdk\ToolError;
use Nexo\Plugin\Sdk\ToolExecutionFailed;
use Nexo\Plugin\Sdk\ToolNotFound;
use Nexo\Plugin\Sdk\ToolUnavailable;

const TOOL_FIXTURE = __DIR__ . '/fixtures/tool-plugin.php';
const NOTOOL_FIXTURE = __DIR__ . '/fixtures/notool-plugin.php';
const BAD_FIXTURE = __DIR__ . '/fixtures/bad-manifest-plugin.php';

const TOOL_NAMES = [
    'tool_plugin_echo', 'tool_plugin_recall', 'tool_plugin_slow', 'tool_plugin_fail',
    'tool_plugin_argbad', 'tool_plugin_busy', 'tool_plugin_nope', 'tool_plugin_denied',
    'tool_plugin_boom', 'tool_plugin_badjson',
];

function fail(string $msg): never
{
    fwrite(STDERR, "FAIL: $msg\n");
    exit(1);
}

function expect(bool $cond, string $msg): void
{
    if (!$cond) {
        fail($msg);
    }
}

/** @return array{proc: resource, in: resource, out: resource, err: resource, buf: string} */
function spawn_fixture(string $fixture = TOOL_FIXTURE): array
{
    $proc = proc_open(['php', $fixture], [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
    if (!is_resource($proc)) {
        fail('proc_open failed');
    }
    stream_set_blocking($pipes[1], false);
    stream_set_blocking($pipes[2], false);
    return ['proc' => $proc, 'in' => $pipes[0], 'out' => $pipes[1], 'err' => $pipes[2], 'buf' => ''];
}

function write_frame(array &$ctx, array $frame): void
{
    fwrite($ctx['in'], json_encode($frame) . "\n");
    fflush($ctx['in']);
}

function read_frame(array &$ctx, float $timeoutSec = 5.0): array
{
    $deadline = microtime(true) + $timeoutSec;
    while (true) {
        $nl = strpos($ctx['buf'], "\n");
        if ($nl !== false) {
            $line = substr($ctx['buf'], 0, $nl);
            $ctx['buf'] = substr($ctx['buf'], $nl + 1);
            if (trim($line) === '') {
                continue;
            }
            $f = json_decode($line, true);
            if (!is_array($f)) {
                fail('read_frame: non-object line: ' . $line);
            }
            return $f;
        }
        if (microtime(true) >= $deadline) {
            fail('read_frame: no frame within ' . $timeoutSec . 's (buf=' . $ctx['buf'] . ')');
        }
        $chunk = stream_get_contents($ctx['out']);
        if ($chunk !== false && $chunk !== '') {
            $ctx['buf'] .= $chunk;
        } else {
            usleep(5_000);
        }
    }
}

function expect_exit_0(array &$ctx, float $timeoutSec = 5.0): void
{
    $deadline = microtime(true) + $timeoutSec;
    $exitCode = null;
    while (microtime(true) < $deadline) {
        $status = proc_get_status($ctx['proc']);
        if (!$status['running']) {
            $exitCode = $status['exitcode']; // proc_get_status reaped it; proc_close would return -1
            break;
        }
        usleep(5_000);
    }
    $err = stream_get_contents($ctx['err']) ?: '';
    @fclose($ctx['in']);
    @fclose($ctx['out']);
    @fclose($ctx['err']);
    proc_close($ctx['proc']);
    if ($exitCode !== 0) {
        fail('child exited ' . var_export($exitCode, true) . '; stderr=' . $err);
    }
}

function kill_proc(array &$ctx): void
{
    $status = @proc_get_status($ctx['proc']);
    if (is_array($status) && $status['running']) {
        @proc_terminate($ctx['proc'], 9);
    }
    @fclose($ctx['in']);
    @fclose($ctx['out']);
    @fclose($ctx['err']);
    @proc_close($ctx['proc']);
}

function initialize_frame(int $id = 1): array
{
    return ['jsonrpc' => '2.0', 'id' => $id, 'method' => 'initialize', 'params' => []];
}
function tool_invoke_frame(int $id, string $toolName, ?array $args = null, ?string $agentId = null, string $pluginId = 'tool_plugin'): array
{
    $params = ['plugin_id' => $pluginId, 'tool_name' => $toolName];
    if ($args !== null) {
        $params['args'] = $args;
    }
    if ($agentId !== null) {
        $params['agent_id'] = $agentId;
    }
    return ['jsonrpc' => '2.0', 'id' => $id, 'method' => 'tool.invoke', 'params' => $params];
}
function event_frame(string $topic, array $payload): array
{
    return ['jsonrpc' => '2.0', 'method' => 'broker.event', 'params' => ['topic' => $topic, 'event' => ['topic' => $topic, 'source' => 'host', 'payload' => $payload]]];
}
function shutdown_frame(int $id = 99): array
{
    return ['jsonrpc' => '2.0', 'id' => $id, 'method' => 'shutdown'];
}

$n = 0;
$ok = function (string $name) use (&$n): void {
    $n++;
    fwrite(STDOUT, "ok $n - $name\n");
};

// ── unit assertions ───────────────────────────────────────────────
$echoSchema = (new ToolDef('p_x', 'd', ['type' => 'object']))->toJson();
expect($echoSchema === ['name' => 'p_x', 'description' => 'd', 'input_schema' => ['type' => 'object']], 'ToolDef::toJson');
expect((new ToolDef('p_x', 'd'))->toJson()['input_schema'] === [], 'ToolDef default schema');
expect((new ToolNotFound('p_x'))->getCode() === -33401, 'ToolNotFound code');
expect((new ToolArgumentInvalid('u'))->getCode() === -33402, 'ToolArgumentInvalid code');
expect((new ToolExecutionFailed('e'))->getCode() === -33403, 'ToolExecutionFailed code');
expect((new ToolUnavailable('b'))->getCode() === -33404, 'ToolUnavailable code');
expect((new ToolDenied('d'))->getCode() === -33405, 'ToolDenied code');
expect((new ToolNotFound('p_x'))->getMessage() === 'tool not found: p_x', 'ToolNotFound msg');
expect((new ToolArgumentInvalid('u'))->getMessage() === 'invalid argument: u', 'ToolArgumentInvalid msg');
expect((new ToolExecutionFailed('e'))->getMessage() === 'execution failed: e', 'ToolExecutionFailed msg');
expect((new ToolUnavailable('b'))->getMessage() === 'unavailable: b', 'ToolUnavailable msg');
expect((new ToolDenied('d'))->getMessage() === 'denied: d', 'ToolDenied msg');
expect((new ToolNotFound('p_x')) instanceof ToolError, 'ToolNotFound is ToolError');
expect((new ToolNotFound('p_x'))->errorData() === null, 'ToolNotFound no data');
expect((new ToolArgumentInvalid('u'))->errorData() === null, 'ToolArgumentInvalid no data when unset');
expect((new ToolArgumentInvalid('u', ['f' => 1]))->errorData() === ['details' => ['f' => 1]], 'ToolArgumentInvalid data');
expect((new ToolUnavailable('b'))->errorData() === null, 'ToolUnavailable no data when unset');
expect((new ToolUnavailable('b', 500))->errorData() === ['retry_after_ms' => 500], 'ToolUnavailable data');
expect(Tool::text('hi') === ['content' => [['type' => 'text', 'text' => 'hi']], 'is_error' => false], 'Tool::text');
expect(Tool::text('bad', true)['is_error'] === true, 'Tool::text is_error');
$ok('tool_types_and_error_band');

// ── handshake: initialize advertises tools ────────────────────────
$ctx = spawn_fixture();
write_frame($ctx, initialize_frame());
$reply = read_frame($ctx);
$names = array_map(static fn ($t) => $t['name'], $reply['result']['tools']);
expect($names === TOOL_NAMES, 'initialize tools names: ' . json_encode($names));
foreach ($reply['result']['tools'] as $t) {
    expect($t['description'] === 't' && ($t['input_schema']['type'] ?? null) === 'object', 'initialize tools shape: ' . json_encode($t));
}
write_frame($ctx, shutdown_frame());
expect((read_frame($ctx)['result']['ok'] ?? null) === true, 'shutdown ok');
expect_exit_0($ctx);
$ok('initialize_advertises_declared_tools');

// ── handshake: omits tools when none declared ─────────────────────
$ctx = spawn_fixture(NOTOOL_FIXTURE);
write_frame($ctx, initialize_frame());
$reply = read_frame($ctx);
expect(!array_key_exists('tools', $reply['result']), 'no tools key when undeclared');
write_frame($ctx, shutdown_frame());
expect((read_frame($ctx)['result']['ok'] ?? null) === true, 'shutdown ok');
expect_exit_0($ctx);
$ok('initialize_omits_tools_when_none_declared');

// ── tool.invoke happy path ────────────────────────────────────────
$ctx = spawn_fixture();
write_frame($ctx, tool_invoke_frame(10, 'tool_plugin_echo', ['a' => 1], 'shopper'));
$reply = read_frame($ctx);
expect($reply['id'] === 10, 'echo id');
expect($reply['result'] === ['echoed' => ['a' => 1], 'agent' => 'shopper', 'plugin' => 'tool_plugin'], 'echo result: ' . json_encode($reply['result']));
write_frame($ctx, shutdown_frame());
read_frame($ctx);
expect_exit_0($ctx);
$ok('tool_invoke_happy_path');

// ── args omitted → null ───────────────────────────────────────────
$ctx = spawn_fixture();
write_frame($ctx, tool_invoke_frame(11, 'tool_plugin_echo'));
$reply = read_frame($ctx);
expect($reply['result'] === ['echoed' => null, 'agent' => null, 'plugin' => 'tool_plugin'], 'args-omitted result: ' . json_encode($reply['result']));
write_frame($ctx, shutdown_frame());
read_frame($ctx);
expect_exit_0($ctx);
$ok('tool_invoke_args_omitted_is_null');

// ── error band ────────────────────────────────────────────────────
$assertToolError = function (string $tool, int $code, string $msgSub, ?array $data, callable $ok, string $okName): void {
    $ctx = spawn_fixture();
    write_frame($ctx, tool_invoke_frame(20, $tool));
    $reply = read_frame($ctx);
    expect($reply['id'] === 20, "$okName: id");
    expect(($reply['error']['code'] ?? null) === $code, "$okName: code, got " . json_encode($reply['error'] ?? null));
    expect(str_contains((string) ($reply['error']['message'] ?? ''), $msgSub), "$okName: msg, got " . ($reply['error']['message'] ?? ''));
    if ($data !== null) {
        expect(($reply['error']['data'] ?? null) === $data, "$okName: data, got " . json_encode($reply['error']['data'] ?? null));
    } else {
        expect(!array_key_exists('data', $reply['error']), "$okName: no data");
    }
    write_frame($ctx, shutdown_frame());
    expect((read_frame($ctx)['result']['ok'] ?? null) === true, "$okName: shutdown ok");
    expect_exit_0($ctx);
    $ok($okName);
};
$assertToolError('tool_plugin_nope', -33401, 'tool not found: tool_plugin_nope', null, $ok, 'tool_invoke_33401_not_found');
$assertToolError('tool_plugin_argbad', -33402, 'invalid argument', ['details' => ['field' => 'url']], $ok, 'tool_invoke_33402_argument_invalid_with_details');
$assertToolError('tool_plugin_fail', -33403, 'execution failed: downstream 500', null, $ok, 'tool_invoke_33403_execution_failed');
$assertToolError('tool_plugin_boom', -33403, 'execution failed', null, $ok, 'tool_invoke_uncaught_maps_to_33403');
$assertToolError('tool_plugin_busy', -33404, 'unavailable', ['retry_after_ms' => 5000], $ok, 'tool_invoke_33404_unavailable_with_retry_after');
$assertToolError('tool_plugin_denied', -33405, 'denied: tenant blocked', null, $ok, 'tool_invoke_33405_denied');
$assertToolError('tool_plugin_badjson', -33403, 'not JSON-serializable', null, $ok, 'tool_invoke_non_serializable_result_maps_to_33403');

// ── no handler → -32601 ───────────────────────────────────────────
$ctx = spawn_fixture(NOTOOL_FIXTURE);
write_frame($ctx, tool_invoke_frame(30, 'anything'));
$reply = read_frame($ctx);
expect(($reply['error']['code'] ?? null) === -32601, 'no-handler code');
expect(str_contains((string) ($reply['error']['message'] ?? ''), 'tool.invoke'), 'no-handler msg');
write_frame($ctx, shutdown_frame());
expect((read_frame($ctx)['result']['ok'] ?? null) === true, 'shutdown ok');
expect_exit_0($ctx);
$ok('tool_invoke_no_handler_replies_32601');

// ── onToolWithContext calls the host mid-invocation ───────────────
$ctx = spawn_fixture();
write_frame($ctx, tool_invoke_frame(40, 'tool_plugin_recall', ['q' => 'prefs']));
$inner = read_frame($ctx);
expect($inner['method'] === 'memory.recall', 'inner method');
expect($inner['params'] === ['agent_id' => 'a', 'query' => 'prefs', 'limit' => 10], 'inner params: ' . json_encode($inner['params']));
write_frame($ctx, ['jsonrpc' => '2.0', 'id' => $inner['id'], 'result' => ['entries' => [['id' => 'm1', 'agent_id' => 'a', 'content' => 'concise']]]]);
$reply = read_frame($ctx);
expect($reply['id'] === 40, 'recall reply id');
expect($reply['result'] === ['recalled' => ['concise'], 'plugin_id' => 'tool_plugin'], 'recall result: ' . json_encode($reply['result']));
write_frame($ctx, shutdown_frame());
expect((read_frame($ctx)['result']['ok'] ?? null) === true, 'shutdown ok');
expect_exit_0($ctx);
$ok('tool_with_context_calls_host_mid_invocation');

// ── two tool.invokes resolve out of order ─────────────────────────
$ctx = spawn_fixture();
write_frame($ctx, tool_invoke_frame(1, 'tool_plugin_slow'));
write_frame($ctx, tool_invoke_frame(2, 'tool_plugin_echo', ['a' => 'quick']));
$first = read_frame($ctx);
$second = read_frame($ctx);
expect($first['id'] === 2, 'fast reply first: ' . json_encode($first));
expect($first['result'] === ['echoed' => ['a' => 'quick'], 'agent' => null, 'plugin' => 'tool_plugin'], 'fast result');
expect($second['id'] === 1, 'slow reply second');
expect($second['result'] === Tool::text('slow done'), 'slow result: ' . json_encode($second['result']));
write_frame($ctx, shutdown_frame());
expect((read_frame($ctx)['result']['ok'] ?? null) === true, 'shutdown ok');
expect_exit_0($ctx);
$ok('two_tool_invokes_resolve_out_of_order');

// ── tool.invoke alongside a broker.event handler ──────────────────
$ctx = spawn_fixture();
write_frame($ctx, event_frame('plugin.in', ['k' => 'v1']));
write_frame($ctx, tool_invoke_frame(50, 'tool_plugin_echo', ['a' => 'x']));
$f1 = read_frame($ctx);
$f2 = read_frame($ctx);
$frames = [$f1, $f2];
$pub = null;
$tool = null;
foreach ($frames as $f) {
    if (($f['method'] ?? null) === 'broker.publish') {
        $pub = $f;
    }
    if (array_key_exists('result', $f)) {
        $tool = $f;
    }
}
expect($pub !== null && $pub['params']['event']['payload'] === ['event_seen' => 'v1'], 'event marker');
expect($tool !== null && $tool['id'] === 50 && $tool['result'] === ['echoed' => ['a' => 'x'], 'agent' => null, 'plugin' => 'tool_plugin'], 'tool reply');
write_frame($ctx, shutdown_frame());
expect((read_frame($ctx)['result']['ok'] ?? null) === true, 'shutdown ok');
expect_exit_0($ctx);
$ok('tool_invoke_alongside_broker_event');

// ── shutdown waits for an in-flight tool ──────────────────────────
$ctx = spawn_fixture();
write_frame($ctx, tool_invoke_frame(1, 'tool_plugin_slow'));
write_frame($ctx, shutdown_frame(99));
$first = read_frame($ctx, 5.0);
expect($first['id'] === 1, 'tool result lands first: ' . json_encode($first));
expect($first['result'] === Tool::text('slow done'), 'slow result');
$second = read_frame($ctx, 5.0);
expect($second['id'] === 99 && ($second['result']['ok'] ?? null) === true, 'shutdown ack second');
expect_exit_0($ctx);
$ok('shutdown_waits_for_in_flight_tool');

// ── manifest cross-check fail-fast ────────────────────────────────
$proc = proc_open(['php', BAD_FIXTURE], [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
expect(is_resource($proc), 'bad fixture proc_open');
fclose($pipes[0]);
stream_get_contents($pipes[1]);
$stderr = stream_get_contents($pipes[2]);
fclose($pipes[1]);
fclose($pipes[2]);
$code = proc_close($proc);
expect($code !== 0, 'bad fixture exit non-zero, got ' . $code);
expect(str_contains($stderr, '[plugin.extends].tools'), 'bad fixture stderr mentions [plugin.extends].tools, got: ' . $stderr);
$ok('manifest_cross_check_fails_fast');
