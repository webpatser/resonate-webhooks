<?php

namespace Webpatser\ResonateWebhooks;

/**
 * One built, signed webhook POST awaiting delivery.
 *
 * Mutable: {@see WebhookDispatcher} advances {@see $attempts} and {@see $dueAt}
 * across retries and flips {@see $inFlight} while a delivery fiber runs.
 */
final class PendingDelivery
{
    /**
     * Delivery attempts made so far.
     */
    public int $attempts = 0;

    /**
     * Unix timestamp (float) before which the next attempt must not run.
     */
    public float $dueAt = 0.0;

    /**
     * Whether a delivery fiber is currently in flight for this delivery.
     */
    public bool $inFlight = false;

    /**
     * @param  array<string, string>  $headers
     */
    public function __construct(
        public readonly string $url,
        public readonly array $headers,
        public readonly string $body,
    ) {
        //
    }
}
