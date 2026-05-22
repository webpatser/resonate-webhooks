<?php

use Webpatser\Resonate\Contracts\ApplicationProvider;
use Webpatser\ResonateWebhooks\Tests\Support\RecordingTransport;
use Webpatser\ResonateWebhooks\WebhookDispatcher;
use Webpatser\ResonateWebhooks\WebhookEndpoint;
use Webpatser\ResonateWebhooks\WebhookEvent;
use Webpatser\ResonateWebhooks\WebhookSigner;

use function Fledge\Async\delay;

/**
 * Build a dispatcher wired to a recording transport.
 *
 * @param  list<WebhookEndpoint>  $endpoints
 */
function makeDispatcher(RecordingTransport $transport, array $endpoints, int $maxAttempts = 5): WebhookDispatcher
{
    return new WebhookDispatcher(
        $transport,
        new WebhookSigner,
        app(ApplicationProvider::class),
        $endpoints,
        $maxAttempts,
    );
}

it('delivers one coalesced, signed POST per endpoint', function () {
    $dispatcher = makeDispatcher(
        $transport = new RecordingTransport,
        [WebhookEndpoint::fromConfig(['url' => 'http://hook.test', 'app_id' => '*'])],
    );

    $dispatcher->record(WebhookEvent::channelOccupied('app-id', 'presence-room'));
    $dispatcher->record(WebhookEvent::memberAdded('app-id', 'presence-room', '7'));

    runLoop(function () use ($dispatcher) {
        $dispatcher->drain();
        delay(0.1);
    });

    expect($transport->deliveries)->toHaveCount(1);

    $delivery = $transport->deliveries[0];

    expect($delivery['url'])->toBe('http://hook.test')
        ->and($delivery['headers']['X-Pusher-Key'])->toBe('app-key')
        ->and($delivery['headers']['X-Pusher-Signature'])
        ->toBe(hash_hmac('sha256', $delivery['body'], 'app-secret'));

    $body = json_decode($delivery['body'], associative: true);

    expect($body)->toHaveKey('time_ms')
        ->and(array_column($body['events'], 'name'))->toBe(['channel_occupied', 'member_added']);
});

it('routes events to endpoints by the event filter', function () {
    $dispatcher = makeDispatcher($transport = new RecordingTransport, [
        WebhookEndpoint::fromConfig(['url' => 'http://occupancy.test', 'events' => ['channel_occupied']]),
        WebhookEndpoint::fromConfig(['url' => 'http://activity.test', 'events' => ['client_event']]),
    ]);

    $dispatcher->record(WebhookEvent::channelOccupied('app-id', 'presence-room'));
    $dispatcher->record(WebhookEvent::clientEvent('app-id', 'presence-room', 'client-typing', null, 'sock-1', '7'));

    runLoop(function () use ($dispatcher) {
        $dispatcher->drain();
        delay(0.1);
    });

    $byUrl = collect($transport->deliveries)->keyBy('url');

    expect($byUrl)->toHaveCount(2)
        ->and(array_column(json_decode($byUrl['http://occupancy.test']['body'], true)['events'], 'name'))
        ->toBe(['channel_occupied'])
        ->and(array_column(json_decode($byUrl['http://activity.test']['body'], true)['events'], 'name'))
        ->toBe(['client_event']);
});

it('keeps a failed delivery pending for retry', function () {
    $dispatcher = makeDispatcher(
        new RecordingTransport(500),
        [WebhookEndpoint::fromConfig(['url' => 'http://hook.test'])],
    );

    $dispatcher->record(WebhookEvent::channelOccupied('app-id', 'presence-room'));

    runLoop(function () use ($dispatcher) {
        $dispatcher->drain();
        delay(0.1);
    });

    expect($dispatcher->pendingCount())->toBe(1);
});

it('drops a delivery after the attempt limit', function () {
    $dispatcher = makeDispatcher(
        new RecordingTransport(500),
        [WebhookEndpoint::fromConfig(['url' => 'http://hook.test'])],
        maxAttempts: 1,
    );

    $dispatcher->record(WebhookEvent::channelOccupied('app-id', 'presence-room'));

    runLoop(function () use ($dispatcher) {
        $dispatcher->drain();
        delay(0.1);
    });

    expect($dispatcher->pendingCount())->toBe(0);
});
