<?php

namespace Webpatser\ResonateWebhooks;

use Fledge\Async\Http\Client\BufferedContent;
use Fledge\Async\Http\Client\HttpClient;
use Fledge\Async\Http\Client\HttpClientBuilder;
use Fledge\Async\Http\Client\Request;

/**
 * Delivers webhooks over HTTP with the fledge-fiber async client.
 *
 * The request suspends the calling fiber, so a slow endpoint never blocks the
 * Resonate event loop.
 */
final class HttpWebhookTransport implements WebhookTransport
{
    /**
     * The async HTTP client.
     */
    private HttpClient $client;

    /**
     * Create a new transport, building a default client when none is given.
     *
     * The default client never follows redirects. Following one would turn a
     * 301/302 POST into a bodyless GET whose 2xx then counts as delivered,
     * while the endpoint never received the signed payload. Returning the 3xx
     * status instead makes the dispatcher treat it as a failed attempt, the
     * way Pusher's own webhook sender does.
     *
     * A client passed in keeps its own redirect setting: it is used as given,
     * so a client built with redirects on still follows them.
     */
    public function __construct(?HttpClient $client = null)
    {
        $this->client = $client ?? (new HttpClientBuilder)->followRedirects(0)->build();
    }

    /**
     * POST a webhook body and return the HTTP status code.
     *
     * @param  array<non-empty-string, string>  $headers
     */
    public function deliver(string $url, array $headers, string $body): int
    {
        $request = new Request($url, 'POST', BufferedContent::fromString($body, 'application/json'));

        foreach ($headers as $name => $value) {
            $request->setHeader($name, $value);
        }

        $request->setTcpConnectTimeout(5);
        $request->setTransferTimeout(10);

        return $this->client->request($request)->getStatus();
    }
}
