<?php

use Webpatser\ResonateWebhooks\WebhookEndpoint;
use Webpatser\ResonateWebhooks\WebhookEvent;

it('builds an endpoint from config with defaults', function () {
    $endpoint = WebhookEndpoint::fromConfig(['url' => 'https://example.test/hook']);

    expect($endpoint->url)->toBe('https://example.test/hook')
        ->and($endpoint->appId)->toBe('*')
        ->and($endpoint->events)->toBe(WebhookEndpoint::allEvents());
});

it('accepts an event matching the app and event filter', function () {
    $endpoint = WebhookEndpoint::fromConfig([
        'url' => 'https://example.test/hook',
        'app_id' => 'app-id',
        'events' => ['channel_occupied'],
    ]);

    expect($endpoint->accepts(WebhookEvent::channelOccupied('app-id', 'presence-room')))->toBeTrue()
        ->and($endpoint->accepts(WebhookEvent::channelVacated('app-id', 'presence-room')))->toBeFalse()
        ->and($endpoint->accepts(WebhookEvent::channelOccupied('other-app', 'presence-room')))->toBeFalse();
});

it('accepts any app when the filter is a wildcard', function () {
    $endpoint = WebhookEndpoint::fromConfig([
        'url' => 'https://example.test/hook',
        'app_id' => '*',
        'events' => ['client_event'],
    ]);

    $event = WebhookEvent::clientEvent('any-app', 'private-x', 'client-typing', null, 'sock-1', null);

    expect($endpoint->accepts($event))->toBeTrue();
});
