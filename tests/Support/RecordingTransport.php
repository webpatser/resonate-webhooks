<?php

namespace Webpatser\ResonateWebhooks\Tests\Support;

use Webpatser\ResonateWebhooks\WebhookTransport;

/**
 * A {@see WebhookTransport} that records deliveries instead of sending them,
 * and returns a configurable status so retry behaviour can be exercised.
 */
class RecordingTransport implements WebhookTransport
{
    /**
     * Every delivery received, as ['url' => ..., 'headers' => ..., 'body' => ...].
     *
     * @var list<array{url: string, headers: array<string, string>, body: string}>
     */
    public array $deliveries = [];

    /**
     * @param  int  $status  The HTTP status every delivery returns.
     */
    public function __construct(public int $status = 200)
    {
        //
    }

    /**
     * Record a delivery and return the configured status.
     *
     * @param  array<string, string>  $headers
     */
    public function deliver(string $url, array $headers, string $body): int
    {
        $this->deliveries[] = ['url' => $url, 'headers' => $headers, 'body' => $body];

        return $this->status;
    }
}
