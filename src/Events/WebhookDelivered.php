<?php

namespace Webpatser\ResonateWebhooks\Events;

use Illuminate\Foundation\Events\Dispatchable;

/**
 * A webhook POST was accepted by the receiving endpoint (HTTP 2xx).
 *
 * Carries enough information for a metrics consumer to bucket by application
 * and to see how many retries it took to land.
 */
class WebhookDelivered
{
    use Dispatchable;

    /**
     * Create a new event instance.
     *
     * @param  string  $url  The endpoint URL that accepted the delivery.
     * @param  int  $status  The HTTP response status code.
     * @param  string  $appId  The Resonate application id the events belonged to.
     * @param  int  $attempts  Total attempts including the successful one.
     */
    public function __construct(
        public string $url,
        public int $status,
        public string $appId,
        public int $attempts,
    ) {
        //
    }
}
