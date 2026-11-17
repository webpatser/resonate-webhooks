<?php

use Fledge\Async\Stream\ResourceServerSocket;
use Fledge\Async\Stream\ResourceSocket;
use Webpatser\ResonateWebhooks\HttpWebhookTransport;

use function Fledge\Async\async;
use function Fledge\Async\Stream\listen;

/**
 * Serve one HTTP/1.1 request per connection on a local socket.
 *
 * Every request is recorded as "METHOD /path" plus its body; $respond maps the
 * request line to a raw status line and extra headers.
 *
 * @param  Closure(string $method, string $path): array{0: int, 1: array<string, string>}  $respond
 * @param  list<array{line: string, body: string, headers: string}>  $requests
 */
function serveHttp(ResourceServerSocket $server, Closure $respond, array &$requests): void
{
    async(function () use ($server, $respond, &$requests) {
        while ($client = $server->accept()) {
            async(function () use ($client, $respond, &$requests) {
                handleHttpRequest($client, $respond, $requests);
            })->ignore();
        }
    })->ignore();
}

/**
 * @param  list<array{line: string, body: string, headers: string}>  $requests
 */
function handleHttpRequest(ResourceSocket $client, Closure $respond, array &$requests): void
{
    $buffer = '';

    while (! str_contains($buffer, "\r\n\r\n")) {
        $chunk = $client->read();

        if ($chunk === null) {
            return;
        }

        $buffer .= $chunk;
    }

    [$head, $body] = explode("\r\n\r\n", $buffer, 2);
    $length = preg_match('/^content-length:\s*(\d+)/im', $head, $m) ? (int) $m[1] : 0;

    while (strlen($body) < $length && ($chunk = $client->read()) !== null) {
        $body .= $chunk;
    }

    [$method, $path] = explode(' ', strtok($head, "\r\n"));
    $requests[] = ['line' => "{$method} {$path}", 'body' => $body, 'headers' => $head];

    [$status, $headers] = $respond($method, $path);

    $response = "HTTP/1.1 {$status} X\r\nContent-Length: 0\r\nConnection: close\r\n";

    foreach ($headers as $name => $value) {
        $response .= "{$name}: {$value}\r\n";
    }

    $client->write($response."\r\n");
    $client->end();
}

it('returns a redirect status instead of following it as a GET', function () {
    $server = listen('tcp://127.0.0.1:0');
    $base = 'http://'.$server->getAddress()->toString();
    $requests = [];
    $status = null;

    serveHttp($server, fn (string $method, string $path) => $path === '/hook'
        ? [302, ['Location' => $base.'/landing']]
        : [200, []], $requests);

    runLoop(function () use ($base, &$status, $server) {
        $status = (new HttpWebhookTransport)->deliver($base.'/hook', ['X-Pusher-Key' => 'k'], '{"events":[]}');
        $server->close();
    });

    // Following the redirect would report the 200 of a bodyless GET as delivered.
    expect($status)->toBe(302)
        ->and(array_column($requests, 'line'))->toBe(['POST /hook']);
});

it('still delivers the signed POST unchanged to a direct endpoint', function () {
    $server = listen('tcp://127.0.0.1:0');
    $base = 'http://'.$server->getAddress()->toString();
    $requests = [];
    $status = null;

    serveHttp($server, fn () => [200, []], $requests);

    runLoop(function () use ($base, &$status, $server) {
        $status = (new HttpWebhookTransport)->deliver(
            $base.'/hook',
            ['X-Pusher-Key' => 'app-key', 'X-Pusher-Signature' => 'sig'],
            '{"events":[]}',
        );
        $server->close();
    });

    expect($status)->toBe(200)
        ->and($requests)->toHaveCount(1)
        ->and($requests[0]['line'])->toBe('POST /hook')
        ->and($requests[0]['body'])->toBe('{"events":[]}')
        ->and(strtolower($requests[0]['headers']))->toContain('x-pusher-signature: sig');
});
