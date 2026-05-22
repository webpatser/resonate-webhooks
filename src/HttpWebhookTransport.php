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
     */
    public function __construct(?HttpClient $client = null)
    {
        $this->client = $client ?? HttpClientBuilder::buildDefault();
    }

    /**
     * POST a webhook body and return the HTTP status code.
     *
     * @param  array<string, string>  $headers
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
