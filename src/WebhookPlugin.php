<?php

namespace Webpatser\ResonateWebhooks;

use Fledge\Async\Redis\RedisConfig;
use Webpatser\Resonate\Contracts\ApplicationProvider;
use Webpatser\Resonate\Contracts\Connection;
use Webpatser\Resonate\Plugins\Contracts\ConnectionLifecycle;
use Webpatser\Resonate\Plugins\Contracts\MessageInterceptor;
use Webpatser\Resonate\Plugins\Contracts\ServerPlugin;
use Webpatser\Resonate\Plugins\Contracts\TickScheduler;
use Webpatser\Resonate\Plugins\MessageDisposition;
use Webpatser\Resonate\Plugins\PluginContext;
use Webpatser\Resonate\Protocols\Pusher\Channels\Channel;
use Webpatser\ResonateRoster\RosterKeys;

use function Fledge\Async\Redis\createRedisClient;

/**
 * Emits Pusher-style HTTP webhooks for channel occupancy and activity.
 *
 * The lifecycle hooks turn a subscribe/unsubscribe/close into an occupancy
 * edge by consulting the cluster-wide roster through an {@see OccupancyTracker}
 * (so each edge fires once per cluster, not once per node), and `onMessage`
 * turns a `client-*` whisper into a `client_event`. Emitted events go to a
 * {@see WebhookDispatcher}, which delivers them off the connection path.
 *
 * Register `Webpatser\ResonateRoster\RedisRosterPlugin` *before* this plugin
 * in `config/reverb.php`: hooks run in array order within one fiber, so the
 * roster's Redis writes must land before this plugin reads the cluster count.
 */
class WebhookPlugin implements ConnectionLifecycle, MessageInterceptor, ServerPlugin, TickScheduler
{
    /**
     * The server API surface handed in at boot.
     */
    protected PluginContext $context;

    /**
     * The cluster-wide occupancy edge detector.
     */
    protected ?OccupancyTracker $occupancy = null;

    /**
     * The webhook buffer and delivery engine.
     */
    protected ?WebhookDispatcher $dispatcher = null;

    /**
     * Seconds between delivery ticks.
     */
    protected float $flushInterval;

    /**
     * Seconds between occupancy reconcile ticks.
     */
    protected float $reconcileInterval;

    /**
     * Presence channels seen on this node: channel name => application id.
     *
     * @var array<string, string>
     */
    protected array $tracked = [];

    /**
     * Boot the plugin: build the occupancy tracker and the dispatcher.
     */
    public function boot(PluginContext $context): void
    {
        $this->context = $context;

        $config = config('resonate-webhooks', []);

        $this->flushInterval = (float) ($config['flush_interval'] ?? 1.0);
        $this->reconcileInterval = (float) ($config['reconcile_interval'] ?? 30.0);

        $this->occupancy = new OccupancyTracker(
            createRedisClient($this->makeConfig($config['connection'] ?? [])),
            new RosterKeys(config('resonate-roster.key_prefix', 'roster')),
            $config['key_prefix'] ?? 'wh',
            (int) ($config['ttl'] ?? 90),
        );

        $this->dispatcher = new WebhookDispatcher(
            app(WebhookTransport::class),
            new WebhookSigner,
            app(ApplicationProvider::class),
            array_map(
                fn (array $endpoint) => WebhookEndpoint::fromConfig($endpoint),
                $config['endpoints'] ?? [],
            ),
            (int) ($config['max_attempts'] ?? 5),
        );
    }

    /**
     * Handle a connection opening. Nothing to do until it subscribes.
     */
    public function onOpen(Connection $connection): void
    {
        //
    }

    /**
     * Emit `channel_occupied` and `member_added` edges for a subscription.
     */
    public function onSubscribe(Connection $connection, Channel $channel): void
    {
        if ($this->occupancy === null) {
            return;
        }

        $appId = $connection->app()->id();
        $name = $channel->name();
        $userId = $this->presenceUserId($connection, $channel);

        $this->tracked[$name] = $appId;

        // Record the channel (and its presence user) on the connection: onClose
        // fires after the connection has left every channel, so this is the
        // only place a close can recover what to emit departures for.
        $subscriptions = $connection->state('webhooks.channels', []);
        $subscriptions[$name] = $userId;
        $connection->setState('webhooks.channels', $subscriptions);

        if ($this->occupancy->claimOccupied($name)) {
            $this->dispatcher->record(WebhookEvent::channelOccupied($appId, $name));
        }

        if ($userId !== '' && $this->occupancy->claimMemberAdded($name, $userId)) {
            $this->dispatcher->record(WebhookEvent::memberAdded($appId, $name, $userId));
        }
    }

    /**
     * Emit departure edges when a connection leaves one channel.
     */
    public function onUnsubscribe(Connection $connection, Channel $channel): void
    {
        $name = $channel->name();

        $subscriptions = $connection->state('webhooks.channels', []);
        $userId = (string) ($subscriptions[$name] ?? '');
        unset($subscriptions[$name]);
        $connection->setState('webhooks.channels', $subscriptions);

        $this->emitDepartures($connection->app()->id(), $name, $userId);
    }

    /**
     * Emit departure edges for every channel a closing connection held.
     */
    public function onClose(Connection $connection): void
    {
        $appId = $connection->app()->id();
        $subscriptions = $connection->state('webhooks.channels', []);

        foreach ($subscriptions as $name => $userId) {
            $this->emitDepartures($appId, (string) $name, (string) $userId);
        }

        $connection->forgetState('webhooks.channels');
    }

    /**
     * Emit a `client_event` webhook for a `client-*` whisper; relay all else.
     *
     * @param  array{event?:mixed,channel?:mixed,data?:mixed}  $event
     */
    public function onMessage(Connection $from, array $event): MessageDisposition
    {
        $name = $event['event'] ?? null;
        $channel = $event['channel'] ?? null;

        if ($this->dispatcher !== null
            && is_string($name)
            && is_string($channel)
            && str_starts_with($name, 'client-')) {
            $data = $event['data'] ?? null;

            $this->dispatcher->record(WebhookEvent::clientEvent(
                $from->app()->id(),
                $channel,
                $name,
                $data === null ? null : (is_string($data) ? $data : json_encode($data)),
                $from->id(),
                $this->presenceUserIdOn($from, $channel),
            ));
        }

        return MessageDisposition::Relay;
    }

    /**
     * Register the delivery tick and the occupancy reconcile tick.
     *
     * @return array<int, array{interval: float, callback: callable():void}>
     */
    public function ticks(): array
    {
        return [
            [
                'interval' => $this->flushInterval,
                'callback' => fn () => $this->dispatcher?->drain(),
            ],
            [
                'interval' => $this->reconcileInterval,
                'callback' => fn () => $this->reconcile(),
            ],
        ];
    }

    /**
     * Emit `member_removed` then `channel_vacated` for a departure.
     */
    protected function emitDepartures(string $appId, string $channel, string $userId): void
    {
        if ($this->occupancy === null) {
            return;
        }

        if ($userId !== '' && $this->occupancy->claimMemberRemoved($channel, $userId)) {
            $this->dispatcher->record(WebhookEvent::memberRemoved($appId, $channel, $userId));
        }

        if ($this->occupancy->claimVacated($channel)) {
            $this->dispatcher->record(WebhookEvent::channelVacated($appId, $channel));
            unset($this->tracked[$channel]);
        }
    }

    /**
     * Reconcile each tracked channel's occupancy against the roster.
     *
     * Recovers an edge missed during a crash, and forgets channels this node
     * no longer serves so the tracked set stays bounded.
     */
    protected function reconcile(): void
    {
        if ($this->occupancy === null) {
            return;
        }

        foreach ($this->tracked as $name => $appId) {
            $edge = $this->occupancy->reconcileOccupancy($name);

            if ($edge === 'occupied') {
                $this->dispatcher->record(WebhookEvent::channelOccupied($appId, $name));
            } elseif ($edge === 'vacated') {
                $this->dispatcher->record(WebhookEvent::channelVacated($appId, $name));
            }

            if ($this->context->connectionsOn($appId, $name) === []) {
                unset($this->tracked[$name]);
            }
        }
    }

    /**
     * The presence user id for a connection on a channel it just joined.
     */
    protected function presenceUserId(Connection $connection, Channel $channel): string
    {
        $member = $channel->connections()[$connection->id()] ?? null;

        return (string) ($member?->data('user_id') ?? '');
    }

    /**
     * The presence user id for a connection on a named channel.
     */
    protected function presenceUserIdOn(Connection $connection, string $channel): ?string
    {
        $member = $this->context->connectionsOn($connection->app(), $channel)[$connection->id()] ?? null;

        $userId = $member?->data('user_id');

        return $userId === null ? null : (string) $userId;
    }

    /**
     * Build the fledge-fiber Redis configuration from the connection config.
     *
     * @param  array<string, mixed>  $server
     */
    protected function makeConfig(array $server): RedisConfig
    {
        $timeout = (float) ($server['timeout'] ?? RedisConfig::DEFAULT_TIMEOUT);

        if (! empty($server['url'])) {
            return RedisConfig::fromUri($server['url'], $timeout);
        }

        $host = $server['host'] ?? '127.0.0.1';
        $port = $server['port'] ?? 6379;
        $database = $server['database'] ?? 0;

        $userInfo = '';

        if (! empty($server['password'])) {
            $userInfo = rawurlencode((string) ($server['username'] ?? ''))
                .':'.rawurlencode((string) $server['password']).'@';
        }

        return RedisConfig::fromUri(
            sprintf('redis://%s%s:%s/%s', $userInfo, $host, $port, $database),
            $timeout,
        );
    }
}
