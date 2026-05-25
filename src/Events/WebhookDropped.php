<?php

namespace Webpatser\ResonateWebhooks\Events;

use Illuminate\Foundation\Events\Dispatchable;

/**
 * A webhook delivery was abandoned after exhausting its retry budget.
 *
 * Fired exactly once per dropped delivery, on the attempt that crossed
 * `max_attempts`. The payload is gone; only metadata remains.
 */
class WebhookDropped
{
    use Dispatchable;

    /**
     * Create a new event instance.
     *
     * @param  string  $url  The endpoint URL the delivery was for.
     * @param  string  $appId  The Resonate application id the events belonged to.
     * @param  int  $attempts  How many attempts were made before giving up.
     * @param  string  $reason  Last failure reason (HTTP status string or exception message).
     */
    public function __construct(
        public string $url,
        public string $appId,
        public int $attempts,
        public string $reason,
    ) {
        //
    }
}
