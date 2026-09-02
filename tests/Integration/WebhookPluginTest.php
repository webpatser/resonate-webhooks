<?php

use Predis\Client;
use Webpatser\Resonate\Application;
use Webpatser\Resonate\Contracts\ApplicationProvider;
use Webpatser\Resonate\Plugins\PluginContext;
use Webpatser\Resonate\Protocols\Pusher\Contracts\ChannelManager;
use Webpatser\ResonateRoster\RedisRosterPlugin;
use Webpatser\ResonateWebhooks\Tests\Support\FakeConnection;
use Webpatser\ResonateWebhooks\Tests\Support\RecordingTransport;
use Webpatser\ResonateWebhooks\WebhookPlugin;
use Webpatser\ResonateWebhooks\WebhookTransport;

use function Fledge\Async\delay;

beforeEach(function () {
    if (! redisReachable()) {
        $this->markTestSkipped('Redis not reachable');
    }

    $this->redis = new Client(['host' => '127.0.0.1', 'port' => 6379, 'database' => 15]);

    foreach (['roster-test:*', 'wh-test:*'] as $pattern) {
        foreach ($this->redis->keys($pattern) as $key) {
            $this->redis->del($key);
        }
    }

    $this->transport = new RecordingTransport;
    $this->app->instance(WebhookTransport::class, $this->transport);
});

afterEach(function () {
    if (isset($this->redis)) {
        foreach (['roster-test:*', 'wh-test:*'] as $pattern) {
            foreach ($this->redis->keys($pattern) as $key) {
                $this->redis->del($key);
            }
        }
    }
});

/**
 * Subscribe a fake connection to a presence channel with a valid auth token.
 *
 * The channel is resolved for the connection's own application, so the same
 * channel name on two applications gives two distinct channels.
 */
function joinPresenceChannel(string $channelName, FakeConnection $connection, string $userId): object
{
    $app = $connection->app();
    $data = json_encode(['user_id' => $userId]);

    $channel = app(ChannelManager::class)->for($app)->findOrCreate($channelName);
    $channel->subscribe($connection, presenceAuth($connection->id(), $channelName, $data, $app->secret()), $data);

    return $channel;
}

/**
 * Subscribe a fake connection to a public channel.
 */
function joinPublicChannel(string $channelName, FakeConnection $connection): object
{
    $channel = app(ChannelManager::class)->for($connection->app())->findOrCreate($channelName);
    $channel->subscribe($connection);

    return $channel;
}

/**
 * The event names across every delivery the recording transport received.
 *
 * @return list<string>
 */
function deliveredEventNames(RecordingTransport $transport): array
{
    $names = [];

    foreach ($transport->deliveries as $delivery) {
        foreach (json_decode($delivery['body'], associative: true)['events'] as $event) {
            $names[] = $event['name'];
        }
    }

    return $names;
}

/**
 * The event names delivered for one application, keyed by its Pusher key.
 *
 * Deliveries are built and signed per application, so the X-Pusher-Key header
 * is what says which application an event batch belongs to.
 *
 * @return list<string>
 */
function deliveredEventNamesForKey(RecordingTransport $transport, string $pusherKey): array
{
    $names = [];

    foreach ($transport->deliveries as $delivery) {
        if (($delivery['headers']['X-Pusher-Key'] ?? null) !== $pusherKey) {
            continue;
        }

        foreach (json_decode($delivery['body'], associative: true)['events'] as $event) {
            $names[] = $event['name'];
        }
    }

    return $names;
}

it('emits channel_occupied and member_added on a first subscription', function () {
    $app = app(ApplicationProvider::class)->findById('app-id');
    $context = new PluginContext(app(ChannelManager::class));
    $channelName = 'presence-room-'.uniqid();

    $connection = new FakeConnection('sock-1', $app);
    $channel = joinPresenceChannel($channelName, $connection, 'u-1');

    $roster = new RedisRosterPlugin;
    $webhooks = new WebhookPlugin;

    runLoop(function () use ($roster, $webhooks, $context, $channel, $connection) {
        $roster->boot($context);
        $webhooks->boot($context);

        // The roster plugin runs first so its Redis writes land before the
        // webhooks plugin reads the cluster count.
        $roster->onSubscribe($connection, $channel);
        $webhooks->onSubscribe($connection, $channel);

        ($webhooks->ticks()[0]['callback'])();
        delay(0.1);
    });

    expect($this->transport->deliveries)->toHaveCount(1)
        ->and(deliveredEventNames($this->transport))
        ->toContain('channel_occupied')
        ->toContain('member_added');
});

it('emits member_removed and channel_vacated when the last connection closes', function () {
    $app = app(ApplicationProvider::class)->findById('app-id');
    $context = new PluginContext(app(ChannelManager::class));
    $channelName = 'presence-room-'.uniqid();

    $connection = new FakeConnection('sock-1', $app);
    $channel = joinPresenceChannel($channelName, $connection, 'u-1');

    $roster = new RedisRosterPlugin;
    $webhooks = new WebhookPlugin;

    runLoop(function () use ($roster, $webhooks, $context, $channel, $connection) {
        $roster->boot($context);
        $webhooks->boot($context);

        $roster->onSubscribe($connection, $channel);
        $webhooks->onSubscribe($connection, $channel);
        ($webhooks->ticks()[0]['callback'])();
        delay(0.05);

        // The connection drops: it leaves the channel, then the close hooks run.
        $channel->unsubscribe($connection);
        $roster->onClose($connection);
        $webhooks->onClose($connection);
        ($webhooks->ticks()[0]['callback'])();
        delay(0.05);
    });

    expect($this->transport->deliveries)->toHaveCount(2);

    $closing = json_decode($this->transport->deliveries[1]['body'], associative: true);

    expect(array_column($closing['events'], 'name'))
        ->toContain('member_removed')
        ->toContain('channel_vacated');
});

it('emits a client_event for a client whisper', function () {
    $app = app(ApplicationProvider::class)->findById('app-id');
    $context = new PluginContext(app(ChannelManager::class));
    $channelName = 'presence-room-'.uniqid();

    $connection = new FakeConnection('sock-1', $app);
    $channel = joinPresenceChannel($channelName, $connection, 'u-1');

    $roster = new RedisRosterPlugin;
    $webhooks = new WebhookPlugin;

    runLoop(function () use ($roster, $webhooks, $context, $channel, $connection, $channelName) {
        $roster->boot($context);
        $webhooks->boot($context);

        $roster->onSubscribe($connection, $channel);
        $webhooks->onSubscribe($connection, $channel);

        $webhooks->onMessage($connection, [
            'event' => 'client-typing',
            'channel' => $channelName,
            'data' => ['typing' => true],
        ]);

        ($webhooks->ticks()[0]['callback'])();
        delay(0.1);
    });

    $events = collect($this->transport->deliveries)
        ->flatMap(fn ($delivery) => json_decode($delivery['body'], associative: true)['events']);

    $clientEvent = $events->firstWhere('name', 'client_event');

    expect($clientEvent)->not->toBeNull()
        ->and($clientEvent['event'])->toBe('client-typing')
        ->and($clientEvent['socket_id'])->toBe('sock-1')
        ->and($clientEvent['user_id'])->toBe('u-1');
});

it('does not emit a client_event when the sender never joined the channel', function () {
    $app = app(ApplicationProvider::class)->findById('app-id');
    $context = new PluginContext(app(ChannelManager::class));
    $channelName = 'presence-room-'.uniqid();

    // One legitimate member holds the channel open, and an unrelated socket
    // whispers on it without ever having subscribed.
    joinPresenceChannel($channelName, new FakeConnection('sock-1', $app), 'u-1');

    $outsider = new FakeConnection('sock-2', $app);
    $webhooks = new WebhookPlugin;

    runLoop(function () use ($webhooks, $context, $outsider, $channelName) {
        $webhooks->boot($context);

        $webhooks->onMessage($outsider, [
            'event' => 'client-typing',
            'channel' => $channelName,
            'data' => ['typing' => true],
        ]);

        ($webhooks->ticks()[0]['callback'])();
        delay(0.1);
    });

    expect($this->transport->deliveries)->toBeEmpty();
});

it('does not emit a client_event on a public channel', function () {
    $app = app(ApplicationProvider::class)->findById('app-id');
    $context = new PluginContext(app(ChannelManager::class));
    $channelName = 'room-'.uniqid();

    $connection = new FakeConnection('sock-1', $app);
    joinPublicChannel($channelName, $connection);

    $webhooks = new WebhookPlugin;

    runLoop(function () use ($webhooks, $context, $connection, $channelName) {
        $webhooks->boot($context);

        $webhooks->onMessage($connection, [
            'event' => 'client-typing',
            'channel' => $channelName,
            'data' => ['typing' => true],
        ]);

        ($webhooks->ticks()[0]['callback'])();
        delay(0.1);
    });

    expect($this->transport->deliveries)->toBeEmpty();
});

it('does not emit a client_event when client messaging is disabled for the app', function () {
    $context = new PluginContext(app(ChannelManager::class));
    $channelName = 'presence-room-'.uniqid();

    $app = new Application(
        'app-id', 'app-key', 'app-secret', 60, 30, ['*'], 10_000, null, 'never',
    );

    $connection = new FakeConnection('sock-1', $app);
    joinPresenceChannel($channelName, $connection, 'u-1');

    $webhooks = new WebhookPlugin;

    runLoop(function () use ($webhooks, $context, $connection, $channelName) {
        $webhooks->boot($context);

        $webhooks->onMessage($connection, [
            'event' => 'client-typing',
            'channel' => $channelName,
            'data' => ['typing' => true],
        ]);

        ($webhooks->ticks()[0]['callback'])();
        delay(0.1);
    });

    expect($this->transport->deliveries)->toBeEmpty();
});

it('does not emit a client_event for a whisper the protocol validator rejects', function () {
    $app = app(ApplicationProvider::class)->findById('app-id');
    $context = new PluginContext(app(ChannelManager::class));
    $channelName = 'presence-room-'.uniqid();

    $connection = new FakeConnection('sock-1', $app);
    joinPresenceChannel($channelName, $connection, 'u-1');

    $webhooks = new WebhookPlugin;

    runLoop(function () use ($webhooks, $context, $connection, $channelName) {
        $webhooks->boot($context);

        // `data` must be an array or absent; a scalar payload never reaches
        // the whisper path.
        $webhooks->onMessage($connection, [
            'event' => 'client-typing',
            'channel' => $channelName,
            'data' => 'not-an-array',
        ]);

        ($webhooks->ticks()[0]['callback'])();
        delay(0.1);
    });

    expect($this->transport->deliveries)->toBeEmpty();
});

it('emits an occupancy edge per application on a same-named channel', function () {
    withSecondApplication();

    $provider = app(ApplicationProvider::class);
    $context = new PluginContext(app(ChannelManager::class));
    $channelName = 'presence-lobby-'.uniqid();

    $alice = new FakeConnection('sock-alice', $provider->findById('app-id'));
    $bob = new FakeConnection('sock-bob', $provider->findById('app-two'));

    $lobbyOne = joinPresenceChannel($channelName, $alice, 'u-alice');
    $lobbyTwo = joinPresenceChannel($channelName, $bob, 'u-bob');

    $roster = new RedisRosterPlugin;
    $webhooks = new WebhookPlugin;

    runLoop(function () use ($roster, $webhooks, $context, $lobbyOne, $lobbyTwo, $alice, $bob) {
        $roster->boot($context);
        $webhooks->boot($context);

        $roster->onSubscribe($alice, $lobbyOne);
        $webhooks->onSubscribe($alice, $lobbyOne);

        $roster->onSubscribe($bob, $lobbyTwo);
        $webhooks->onSubscribe($bob, $lobbyTwo);

        ($webhooks->ticks()[0]['callback'])();
        delay(0.1);
    });

    // The second application shares the channel name, which used to mean it
    // found the first application's occupied flag already claimed and never
    // announced its own channel as occupied.
    expect(deliveredEventNamesForKey($this->transport, 'app-key'))
        ->toContain('channel_occupied')
        ->toContain('member_added')
        ->and(deliveredEventNamesForKey($this->transport, 'app-two-key'))
        ->toContain('channel_occupied')
        ->toContain('member_added');
});

it('does not suppress the second application channel_occupied when the first joined first', function () {
    withSecondApplication();

    $provider = app(ApplicationProvider::class);
    $context = new PluginContext(app(ChannelManager::class));
    $channelName = 'presence-lobby-'.uniqid();

    $alice = new FakeConnection('sock-alice', $provider->findById('app-id'));
    $bob = new FakeConnection('sock-bob', $provider->findById('app-two'));

    $lobbyOne = joinPresenceChannel($channelName, $alice, 'u-alice');
    $lobbyTwo = joinPresenceChannel($channelName, $bob, 'u-bob');

    $roster = new RedisRosterPlugin;
    $webhooks = new WebhookPlugin;

    runLoop(function () use ($roster, $webhooks, $context, $lobbyOne, $lobbyTwo, $alice, $bob) {
        $roster->boot($context);
        $webhooks->boot($context);

        // The first application occupies the channel and its batch is
        // delivered before the second application joins at all.
        $roster->onSubscribe($alice, $lobbyOne);
        $webhooks->onSubscribe($alice, $lobbyOne);
        ($webhooks->ticks()[0]['callback'])();
        delay(0.05);

        $roster->onSubscribe($bob, $lobbyTwo);
        $webhooks->onSubscribe($bob, $lobbyTwo);
        ($webhooks->ticks()[0]['callback'])();
        delay(0.05);
    });

    expect(deliveredEventNamesForKey($this->transport, 'app-key'))->toBe(['channel_occupied', 'member_added'])
        ->and(deliveredEventNamesForKey($this->transport, 'app-two-key'))->toBe(['channel_occupied', 'member_added']);
});

it('vacates one application without vacating the other', function () {
    withSecondApplication();

    $provider = app(ApplicationProvider::class);
    $context = new PluginContext(app(ChannelManager::class));
    $channelName = 'presence-lobby-'.uniqid();

    $alice = new FakeConnection('sock-alice', $provider->findById('app-id'));
    $bob = new FakeConnection('sock-bob', $provider->findById('app-two'));

    $lobbyOne = joinPresenceChannel($channelName, $alice, 'u-alice');
    $lobbyTwo = joinPresenceChannel($channelName, $bob, 'u-bob');

    $roster = new RedisRosterPlugin;
    $webhooks = new WebhookPlugin;

    runLoop(function () use ($roster, $webhooks, $context, $lobbyOne, $lobbyTwo, $alice, $bob) {
        $roster->boot($context);
        $webhooks->boot($context);

        $roster->onSubscribe($alice, $lobbyOne);
        $webhooks->onSubscribe($alice, $lobbyOne);
        $roster->onSubscribe($bob, $lobbyTwo);
        $webhooks->onSubscribe($bob, $lobbyTwo);
        ($webhooks->ticks()[0]['callback'])();
        delay(0.05);

        // Only the first application's connection drops.
        $lobbyOne->unsubscribe($alice);
        $roster->onClose($alice);
        $webhooks->onClose($alice);
        ($webhooks->ticks()[0]['callback'])();
        delay(0.05);
    });

    expect(deliveredEventNamesForKey($this->transport, 'app-key'))
        ->toContain('channel_vacated')
        ->toContain('member_removed')
        ->and(deliveredEventNamesForKey($this->transport, 'app-two-key'))
        ->not->toContain('channel_vacated');
});

it('does not report on a reserved channel', function () {
    // The Pusher protocol reserves "#" for channels the server owns rather than
    // the application. webpatser/resonate-users puts a signed-in connection on
    // "#server-to-user-{id}", which is that person's session rather than a
    // room: reporting it would post a channel_occupied every time someone
    // opened a tab and a channel_vacated every time they closed one.
    $app = app(ApplicationProvider::class)->findById('app-id');
    $context = new PluginContext(app(ChannelManager::class));

    $connection = new FakeConnection('sock-1', $app);
    $channel = app(ChannelManager::class)->for($app)->findOrCreate('#server-to-user-42');
    $channel->subscribe($connection, null, json_encode(['user_id' => '42']));

    $roster = new RedisRosterPlugin;
    $webhooks = new WebhookPlugin;

    runLoop(function () use ($roster, $webhooks, $context, $channel, $connection) {
        $roster->boot($context);
        $webhooks->boot($context);

        $roster->onSubscribe($connection, $channel);
        $webhooks->onSubscribe($connection, $channel);
        ($webhooks->ticks()[0]['callback'])();
        delay(0.05);

        $channel->unsubscribe($connection);
        $roster->onClose($connection);
        $webhooks->onClose($connection);
        ($webhooks->ticks()[0]['callback'])();
        delay(0.05);
    });

    expect($this->transport->deliveries)->toBe([]);
});

it('still reports on an ordinary channel alongside a reserved one', function () {
    // The exclusion is by prefix, so it must not swallow real traffic from a
    // connection that happens to also hold a user channel.
    $app = app(ApplicationProvider::class)->findById('app-id');
    $context = new PluginContext(app(ChannelManager::class));
    $channelName = 'presence-room-'.uniqid();

    $connection = new FakeConnection('sock-1', $app);
    $userChannel = app(ChannelManager::class)->for($app)->findOrCreate('#server-to-user-42');
    $userChannel->subscribe($connection, null, json_encode(['user_id' => '42']));
    $room = joinPresenceChannel($channelName, $connection, '42');

    $roster = new RedisRosterPlugin;
    $webhooks = new WebhookPlugin;

    runLoop(function () use ($roster, $webhooks, $context, $userChannel, $room, $connection) {
        $roster->boot($context);
        $webhooks->boot($context);

        $roster->onSubscribe($connection, $userChannel);
        $webhooks->onSubscribe($connection, $userChannel);
        $roster->onSubscribe($connection, $room);
        $webhooks->onSubscribe($connection, $room);

        ($webhooks->ticks()[0]['callback'])();
        delay(0.1);
    });

    expect(deliveredEventNames($this->transport))
        ->toContain('channel_occupied')
        ->toContain('member_added');

    foreach ($this->transport->deliveries as $delivery) {
        foreach (json_decode($delivery['body'], associative: true)['events'] as $event) {
            expect($event['channel'])->not->toStartWith('#');
        }
    }
});
