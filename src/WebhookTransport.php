<?php

namespace Webpatser\ResonateWebhooks;

/**
 * Delivers a signed webhook body over HTTP.
 *
 * A port: {@see HttpWebhookTransport} is the production implementation; tests
 * substitute a recording one.
 */
interface WebhookTransport
{
    /**
     * POST a webhook body and return the HTTP status code.
     *
     * Implementations run inside the Resonate event loop, so the call must be
     * fiber-suspending rather than blocking.
     *
     * @param  array<string, string>  $headers
     */
    public function deliver(string $url, array $headers, string $body): int;
}
