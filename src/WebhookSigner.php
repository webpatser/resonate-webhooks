<?php

namespace Webpatser\ResonateWebhooks;

/**
 * Signs a webhook body the Pusher way.
 *
 * The receiver verifies the `X-Pusher-Signature` header against the raw body
 * using the shared app secret, so a tampered or forged payload is rejected.
 */
final class WebhookSigner
{
    /**
     * The HMAC-SHA256 signature of a raw body under an app secret.
     */
    public function sign(string $body, string $secret): string
    {
        return hash_hmac('sha256', $body, $secret);
    }
}
