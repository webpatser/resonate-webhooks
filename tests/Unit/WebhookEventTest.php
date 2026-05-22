<?php

use Webpatser\ResonateWebhooks\WebhookEvent;

it('builds occupancy events without extra payload', function () {
    expect(WebhookEvent::channelOccupied('app-id', 'presence-room')->toArray())
        ->toBe(['name' => 'channel_occupied', 'channel' => 'presence-room']);
});

it('builds member events carrying the user id', function () {
    expect(WebhookEvent::memberAdded('app-id', 'presence-room', '7')->toArray())
        ->toBe(['name' => 'member_added', 'channel' => 'presence-room', 'user_id' => '7']);
});

it('builds a client event with data, socket id, and user id', function () {
    $event = WebhookEvent::clientEvent('app-id', 'presence-room', 'client-typing', '{"x":1}', 'sock-1', '7');

    expect($event->toArray())->toBe([
        'name' => 'client_event',
        'channel' => 'presence-room',
        'event' => 'client-typing',
        'socket_id' => 'sock-1',
        'data' => '{"x":1}',
        'user_id' => '7',
    ]);
});

it('omits absent client event fields', function () {
    $event = WebhookEvent::clientEvent('app-id', 'private-x', 'client-ping', null, 'sock-1', null);

    expect($event->toArray())->toBe([
        'name' => 'client_event',
        'channel' => 'private-x',
        'event' => 'client-ping',
        'socket_id' => 'sock-1',
    ]);
});

it('keeps the app id off the wire payload', function () {
    expect(WebhookEvent::channelOccupied('secret-app', 'presence-room')->toArray())
        ->not->toHaveKey('appId');
});
