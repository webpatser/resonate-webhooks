<?php

use Webpatser\ResonateWebhooks\WebhookSigner;

it('signs a body with the Pusher HMAC-SHA256 scheme', function () {
    $body = '{"time_ms":1700000000000,"events":[]}';

    $signature = (new WebhookSigner)->sign($body, 'app-secret');

    expect($signature)->toBe(hash_hmac('sha256', $body, 'app-secret'));
});

it('produces a different signature for a different secret', function () {
    $signer = new WebhookSigner;
    $body = '{"a":1}';

    expect($signer->sign($body, 'secret-one'))
        ->not->toBe($signer->sign($body, 'secret-two'));
});
