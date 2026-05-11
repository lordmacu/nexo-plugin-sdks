<?php

declare(strict_types=1);

/**
 * Tests for the child→host call surface (memory.recall / llm.complete).
 *
 * The fixture makes the host call in onEvent; this test plays the *host*
 * — it spawns the fixture, feeds it a broker.event, reads the child's
 * request frame off stdout, writes a canned reply to stdin, and asserts
 * the published-back marker. Interactive (interleaved reads/writes), so
 * it uses its own non-blocking harness rather than run_fixture().
 */

require __DIR__ . '/../vendor/autoload.php';

const FIXTURE = __DIR__ . '/fixtures/host-call-plugin.php';

function fail(string $msg): never
{
    fwrite(STDERR, "FAIL: $msg\n");
    exit(1);
}

/** @return array{proc: resource, in: resource, out: resource, err: resource, buf: string} */
function spawn_fixture(): array
{
    $proc = proc_open(
        ['php', FIXTURE],
        [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
        $pipes,
    );
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

/** Read the next JSON-RPC line off the child's stdout (buffered). */
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

function expect_exit_0(array &$ctx, float $timeoutSec = 5.0): string
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
    return $err;
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

function read_stderr(array &$ctx): string
{
    return stream_get_contents($ctx['err']) ?: '';
}

function event_frame(string $topic, array $payload): array
{
    return ['jsonrpc' => '2.0', 'method' => 'broker.event',
        'params' => ['topic' => $topic, 'event' => ['topic' => $topic, 'source' => 'host', 'payload' => $payload]]];
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

// ── data type fromJson round-trips ─────────────────────────────
$e = \Nexo\Plugin\Sdk\MemoryEntry::fromJson(['id' => 'x1', 'agent_id' => 'a', 'content' => 'c', 'tags' => ['t'], 'concept_tags' => [], 'created_at' => '2026-01-01T00:00:00Z', 'memory_type' => null]);
if ([$e->id, $e->agentId, $e->content, $e->tags, $e->memoryType] !== ['x1', 'a', 'c', ['t'], null]) {
    fail('MemoryEntry::fromJson: ' . print_r($e, true));
}
$r = \Nexo\Plugin\Sdk\LlmCompleteResult::fromJson(['content' => 'hi', 'finish_reason' => 'stop', 'usage' => ['prompt_tokens' => 5, 'completion_tokens' => 2]]);
if ([$r->content, $r->finishReason, $r->usage->promptTokens, $r->usage->completionTokens] !== ['hi', 'stop', 5, 2]) {
    fail('LlmCompleteResult::fromJson: ' . print_r($r, true));
}
$r2 = \Nexo\Plugin\Sdk\LlmCompleteResult::fromJson(['finish_reason' => 'length']);
if ($r2->content !== null || $r2->finishReason !== 'length') {
    fail('LlmCompleteResult::fromJson streamed: ' . print_r($r2, true));
}
$ok('data_type_fromJson_roundtrips');

// ── memory.recall happy path ───────────────────────────────────
$ctx = spawn_fixture();
write_frame($ctx, event_frame('plugin.in', ['op' => 'recall']));
$req = read_frame($ctx);
if (($req['method'] ?? null) !== 'memory.recall') {
    kill_proc($ctx);
    fail('memory_recall_happy: expected memory.recall, got ' . json_encode($req));
}
if ($req['params'] !== ['agent_id' => 'agent_x', 'query' => 'prefs', 'limit' => 3]) {
    kill_proc($ctx);
    fail('memory_recall_happy: wrong params ' . json_encode($req['params']));
}
write_frame($ctx, ['jsonrpc' => '2.0', 'id' => $req['id'], 'result' => ['entries' => [
    ['id' => 'm1', 'agent_id' => 'agent_x', 'content' => 'concise', 'tags' => ['pref'], 'concept_tags' => [], 'created_at' => '2026-01-01T00:00:00Z', 'memory_type' => null]]]]);
$out = read_frame($ctx);
if (($out['method'] ?? null) !== 'broker.publish' || ($out['params']['event']['payload'] ?? null) !== ['got' => ['concise'], 'ids' => ['m1']]) {
    kill_proc($ctx);
    fail('memory_recall_happy: wrong publish ' . json_encode($out));
}
write_frame($ctx, shutdown_frame());
if ((read_frame($ctx)['result']['ok'] ?? null) !== true) {
    kill_proc($ctx);
    fail('memory_recall_happy: shutdown reply not ok');
}
expect_exit_0($ctx);
$ok('memory_recall_happy_path');

// ── host error → RpcServerError ────────────────────────────────
$ctx = spawn_fixture();
write_frame($ctx, event_frame('plugin.in', ['op' => 'error']));
$req = read_frame($ctx);
write_frame($ctx, ['jsonrpc' => '2.0', 'id' => $req['id'], 'error' => ['code' => -32603, 'message' => 'memory not configured']]);
$out = read_frame($ctx);
if (($out['params']['event']['payload'] ?? null) !== ['rpc_error_code' => -32603, 'rpc_error_msg' => 'memory not configured']) {
    kill_proc($ctx);
    fail('server_error: wrong publish ' . json_encode($out));
}
write_frame($ctx, shutdown_frame());
read_frame($ctx);
expect_exit_0($ctx);
$ok('host_error_surfaces_as_RpcServerError');

// ── per-call timeout → RpcTimeoutError; dispatch survives ──────
$ctx = spawn_fixture();
write_frame($ctx, event_frame('plugin.in', ['op' => 'timeout']));
$req = read_frame($ctx);
if (($req['method'] ?? null) !== 'memory.recall') {
    kill_proc($ctx);
    fail('timeout: expected memory.recall');
}
// Never reply — the 0.4s per-call timeout fires.
$out = read_frame($ctx, 5.0);
if (($out['params']['event']['payload']['timed_out'] ?? null) !== true) {
    kill_proc($ctx);
    fail('timeout: expected timed_out marker, got ' . json_encode($out));
}
// A late reply for the timed-out id is dropped; shutdown still works.
write_frame($ctx, ['jsonrpc' => '2.0', 'id' => $req['id'], 'result' => ['entries' => []]]);
write_frame($ctx, shutdown_frame());
read_frame($ctx);
expect_exit_0($ctx);
$ok('per_call_timeout_surfaces_as_RpcTimeoutError');

// ── late response for unknown id dropped ───────────────────────
$ctx = spawn_fixture();
write_frame($ctx, ['jsonrpc' => '2.0', 'id' => 9999, 'result' => ['entries' => []]]); // never issued — must not crash
write_frame($ctx, shutdown_frame());
if ((read_frame($ctx)['result']['ok'] ?? null) !== true) {
    kill_proc($ctx);
    fail('late_response: shutdown reply not ok');
}
expect_exit_0($ctx);
$ok('late_response_for_unknown_id_dropped');

// ── llm.complete non-streaming happy path ──────────────────────
$ctx = spawn_fixture();
write_frame($ctx, event_frame('plugin.in', ['op' => 'complete']));
$req = read_frame($ctx);
if (($req['method'] ?? null) !== 'llm.complete' || ($req['params']['provider'] ?? null) !== 'minimax' || isset($req['params']['stream'])) {
    kill_proc($ctx);
    fail('llm_complete: wrong request ' . json_encode($req));
}
write_frame($ctx, ['jsonrpc' => '2.0', 'id' => $req['id'], 'result' => ['content' => 'one line.', 'finish_reason' => 'stop', 'usage' => ['prompt_tokens' => 3, 'completion_tokens' => 4]]]);
$out = read_frame($ctx);
if (($out['params']['event']['payload'] ?? null) !== ['content' => 'one line.', 'finish_reason' => 'stop', 'ct' => 4]) {
    kill_proc($ctx);
    fail('llm_complete: wrong publish ' . json_encode($out));
}
write_frame($ctx, shutdown_frame());
read_frame($ctx);
expect_exit_0($ctx);
$ok('llm_complete_non_streaming_happy_path');

// ── llm.complete streaming happy path ──────────────────────────
$ctx = spawn_fixture();
write_frame($ctx, event_frame('plugin.in', ['op' => 'stream']));
$req = read_frame($ctx);
if (($req['method'] ?? null) !== 'llm.complete' || ($req['params']['stream'] ?? null) !== true) {
    kill_proc($ctx);
    fail('llm_stream: wrong request ' . json_encode($req));
}
$rid = $req['id'];
foreach (['hel', 'lo ', 'world'] as $c) {
    write_frame($ctx, ['jsonrpc' => '2.0', 'method' => 'llm.complete.delta', 'params' => ['request_id' => $rid, 'chunk' => $c]]);
}
write_frame($ctx, ['jsonrpc' => '2.0', 'id' => $rid, 'result' => ['finish_reason' => 'stop', 'usage' => ['prompt_tokens' => 1, 'completion_tokens' => 3]]]);
$out = read_frame($ctx);
$p = $out['params']['event']['payload'] ?? [];
if (($p['chunks'] ?? null) !== ['hel', 'lo ', 'world'] || ($p['joined'] ?? null) !== 'hello world' || ($p['content_is_null'] ?? null) !== true || ($p['finish_reason'] ?? null) !== 'stop') {
    kill_proc($ctx);
    fail('llm_stream: wrong publish ' . json_encode($out));
}
write_frame($ctx, shutdown_frame());
read_frame($ctx);
expect_exit_0($ctx);
$ok('llm_complete_streaming_happy_path');

// ── two in-flight memory.recall calls resolve out of order ─────
$ctx = spawn_fixture();
write_frame($ctx, event_frame('plugin.in', ['q' => 'alpha']));
write_frame($ctx, event_frame('plugin.in', ['q' => 'beta']));
$req1 = read_frame($ctx);
$req2 = read_frame($ctx);
$ids = [$req1['params']['query'] => $req1['id'], $req2['params']['query'] => $req2['id']];
ksort($ids);
if (array_keys($ids) !== ['alpha', 'beta']) {
    kill_proc($ctx);
    fail('multiplex: unexpected queries ' . json_encode($ids));
}
// Reply to BETA first, then ALPHA.
write_frame($ctx, ['jsonrpc' => '2.0', 'id' => $ids['beta'], 'result' => ['entries' => [['id' => 'b', 'agent_id' => 'a', 'content' => 'B-content']]]]);
write_frame($ctx, ['jsonrpc' => '2.0', 'id' => $ids['alpha'], 'result' => ['entries' => [['id' => 'a', 'agent_id' => 'a', 'content' => 'A-content']]]]);
$o1 = read_frame($ctx);
$o2 = read_frame($ctx);
$byQ = [];
foreach ([$o1, $o2] as $o) {
    $byQ[$o['params']['event']['payload']['q']] = $o['params']['event']['payload']['got'];
}
ksort($byQ);
if ($byQ !== ['alpha' => ['A-content'], 'beta' => ['B-content']]) {
    kill_proc($ctx);
    fail('multiplex: cross-talk ' . json_encode($byQ));
}
write_frame($ctx, shutdown_frame());
read_frame($ctx);
expect_exit_0($ctx);
$ok('two_in_flight_calls_resolve_out_of_order');

// ── shutdown while a host call is in flight does not hang ──────
$ctx = spawn_fixture();
write_frame($ctx, event_frame('plugin.in', ['op' => 'inflight']));
$req = read_frame($ctx);
if (($req['method'] ?? null) !== 'memory.recall') {
    kill_proc($ctx);
    fail('inflight: expected memory.recall');
}
// Don't reply — shut down. The adapter must abandon the in-flight call
// (handler gets RpcTransportError) and reply {ok:true} promptly.
write_frame($ctx, shutdown_frame());
if ((read_frame($ctx, 5.0)['result']['ok'] ?? null) !== true) {
    kill_proc($ctx);
    fail('inflight: shutdown reply not ok / hung');
}
$err = expect_exit_0($ctx);
if (!str_contains($err, 'HANDLER_GOT_TRANSPORT')) {
    fail('inflight: handler should have caught RpcTransportError; stderr=' . $err);
}
$ok('shutdown_while_host_call_in_flight_does_not_hang');

exit(0);
