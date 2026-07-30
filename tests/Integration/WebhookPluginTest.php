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
 */
function joinPresenceChannel(string $channelName, FakeConnection $connection, string $userId): object
{
    $app = app(ApplicationProvider::class)->findById('app-id');
    $data = json_encode(['user_id' => $userId]);

    $channel = app(ChannelManager::class)->for($app)->findOrCreate($channelName);
    $channel->subscribe($connection, presenceAuth($connection->id(), $channelName, $data), $data);

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
