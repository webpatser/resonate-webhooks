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
 * Occupancy is tracked per application, so two applications that both serve a
 * "presence-lobby" each get their own `channel_occupied` and `channel_vacated`
 * edges rather than sharing one.
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
     * Channel name prefixes this plugin does not report on.
     *
     * The Pusher protocol reserves "#" for channels the server owns rather than
     * the application: `webpatser/resonate-users` subscribes a signed-in
     * connection to "#server-to-user-{id}" so a message can be addressed to a
     * person. Those are a user's session, not a room, and reporting them would
     * post a `channel_occupied` every time someone opened a tab and a
     * `channel_vacated` every time they closed one, to backends that expect
     * real channels.
     *
     * @var list<string>
     */
    protected array $ignoredPrefixes = ['#'];

    /**
     * Channels seen on this node: application id => channel name => true.
     *
     * Keyed by application first, so two applications serving a channel of the
     * same name are reconciled separately instead of one overwriting the
     * other's entry.
     *
     * @var array<string, array<string, true>>
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
        $this->ignoredPrefixes = array_values(array_filter(
            array_map(strval(...), (array) ($config['ignore_channel_prefixes'] ?? ['#'])),
            static fn (string $prefix): bool => $prefix !== '',
        ));

        $this->occupancy = new OccupancyTracker(
            createRedisClient($this->makeConfig($config['connection'] ?? [])),
            // Built from the roster's own config, so the key prefix and the
            // legacy fallback window are read from one place instead of being
            // re-typed here and silently drifting from the roster's defaults.
            RosterKeys::fromConfig(config('resonate-roster', [])),
            $config['key_prefix'] ?? 'wh',
            (int) ($config['ttl'] ?? 90),
        );

        $this->dispatcher = new WebhookDispatcher(
            app(WebhookTransport::class),
            new WebhookSigner,
            app(ApplicationProvider::class),
            array_values(array_map(
                fn (array $endpoint) => WebhookEndpoint::fromConfig($endpoint),
                $config['endpoints'] ?? [],
            )),
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
        if ($this->occupancy === null || ! $this->reports($channel->name())) {
            return;
        }

        $appId = $connection->app()->id();
        $name = $channel->name();
        $userId = $this->presenceUserId($connection, $channel);

        $this->tracked[$appId][$name] = true;

        // Record the channel (and its presence user) on the connection: onClose
        // fires after the connection has left every channel, so this is the
        // only place a close can recover what to emit departures for. The
        // application comes off the connection, so it needs no bookkeeping.
        $subscriptions = $connection->state('webhooks.channels', []);
        $subscriptions[$name] = $userId;
        $connection->setState('webhooks.channels', $subscriptions);

        // One read of the channel's cluster-wide occupancy serves both edges.
        // The claims only write flag keys, never roster keys, so the second
        // edge cannot observe anything the first one changed.
        $state = $this->occupancy->state($appId, $name);

        if ($this->occupancy->claimOccupied($appId, $name, $state)) {
            $this->dispatcher?->record(WebhookEvent::channelOccupied($appId, $name));
        }

        if ($userId !== '' && $this->occupancy->claimMemberAdded($appId, $name, $userId, $state)) {
            $this->dispatcher?->record(WebhookEvent::memberAdded($appId, $name, $userId));
        }
    }

    /**
     * Emit departure edges when a connection leaves one channel.
     */
    public function onUnsubscribe(Connection $connection, Channel $channel): void
    {
        $name = $channel->name();

        if (! $this->reports($name)) {
            return;
        }

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
     * Interceptors run before the Pusher protocol layer validates the frame,
     * so this hook re-applies every check `ClientEvent::handle()` makes before
     * it relays a whisper. Without them an unsubscribed socket could whisper
     * any channel name it liked, be rejected by the server with pusher error
     * 4009, and still have a correctly signed `client_event` posted to the
     * backend attesting to activity on a channel it never joined.
     *
     * @param  array{event?:mixed,channel?:mixed,data?:mixed}  $event
     */
    public function onMessage(Connection $from, array $event): MessageDisposition
    {
        $name = $event['event'] ?? null;
        $channel = $event['channel'] ?? null;
        $data = $event['data'] ?? null;

        if ($this->dispatcher === null
            || ! is_string($name)
            || ! is_string($channel)
            || ! str_starts_with($name, 'client-')) {
            return MessageDisposition::Relay;
        }

        // The frame shape the protocol validator accepts: a whisper payload is
        // an array or absent, never a scalar.
        if ($data !== null && ! is_array($data)) {
            return MessageDisposition::Relay;
        }

        // Client messaging has to be enabled for the application.
        if (! in_array($from->app()->acceptClientEventsFrom(), ['all', 'members'], strict: true)) {
            return MessageDisposition::Relay;
        }

        // Whispers are only permitted on private and presence channels.
        if (! str_starts_with($channel, 'private-') && ! str_starts_with($channel, 'presence-')) {
            return MessageDisposition::Relay;
        }

        // And the sender has to be a member of the channel it whispers on.
        $member = $this->context->connectionsOn($from->app(), $channel)[$from->id()] ?? null;

        if ($member === null) {
            return MessageDisposition::Relay;
        }

        $userId = $member->data('user_id');

        $this->dispatcher->record(WebhookEvent::clientEvent(
            $from->app()->id(),
            $channel,
            $name,
            $this->encodePayload($data),
            $from->id(),
            $userId === null ? null : (string) $userId,
        ));

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

        // As in onSubscribe: read the departed-from channel once, claim twice.
        $state = $this->occupancy->state($appId, $channel);

        if ($userId !== '' && $this->occupancy->claimMemberRemoved($appId, $channel, $userId, $state)) {
            $this->dispatcher?->record(WebhookEvent::memberRemoved($appId, $channel, $userId));
        }

        if ($this->occupancy->claimVacated($appId, $channel, $state)) {
            $this->dispatcher?->record(WebhookEvent::channelVacated($appId, $channel));
            $this->forget($appId, $channel);
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
        $occupancy = $this->occupancy;

        if ($occupancy === null) {
            return;
        }

        foreach ($this->tracked as $appId => $channels) {
            // PHP coerces numeric-string array keys to int, and application
            // ids are usually numeric, so both segments are cast back before
            // they are used.
            $appId = (string) $appId;

            // One keyspace sweep for the whole application, rather than one per
            // tracked channel. A tick used to get more expensive the busier the
            // node was, which is the opposite of what a reconcile pass should
            // do. A channel missing from the snapshot has no roster key at all,
            // so it reconciles against an empty state.
            $snapshot = $occupancy->snapshot($appId);

            foreach (array_keys($channels) as $channel) {
                $channel = (string) $channel;

                $this->reconcileChannel(
                    $appId,
                    $channel,
                    $snapshot[$channel] ?? ['connections' => 0, 'users' => []],
                );
            }
        }
    }

    /**
     * Reconcile one application's channel against a slice of the roster read.
     *
     * @param  array{connections: int, users: list<string>}  $state
     */
    protected function reconcileChannel(string $appId, string $name, array $state): void
    {
        $edge = $this->occupancy?->reconcileOccupancy($appId, $name, $state);

        if ($edge === 'occupied') {
            $this->dispatcher?->record(WebhookEvent::channelOccupied($appId, $name));
        } elseif ($edge === 'vacated') {
            $this->dispatcher?->record(WebhookEvent::channelVacated($appId, $name));
        }

        if ($this->context->connectionsOn($appId, $name) === []) {
            $this->forget($appId, $name);
        }
    }

    /**
     * Determine whether a channel is one this plugin reports on.
     *
     * An ignored channel is never tracked, so it produces no occupancy edges
     * and no reconcile work either.
     */
    protected function reports(string $channel): bool
    {
        foreach ($this->ignoredPrefixes as $prefix) {
            if (str_starts_with($channel, $prefix)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Drop a channel this node no longer serves, and its application with it.
     */
    protected function forget(string $appId, string $name): void
    {
        unset($this->tracked[$appId][$name]);

        if (($this->tracked[$appId] ?? []) === []) {
            unset($this->tracked[$appId]);
        }
    }

    /**
     * Render a whisper's data payload as the wire string, or null.
     *
     * A value that cannot be encoded is carried as null rather than as the
     * `false` json_encode() hands back on failure.
     */
    protected function encodePayload(mixed $data): ?string
    {
        if ($data === null || is_string($data)) {
            return $data;
        }

        $encoded = json_encode($data);

        return $encoded === false ? null : $encoded;
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
     * Build the fledge-fiber Redis configuration from the connection config.
     *
     * `RedisConfig::fromParameters()` reads the Laravel-shaped connection array
     * directly. This used to assemble a `redis://user:pass@host:port/db` string
     * by hand, and everything a URI cannot carry was dropped on the way: the
     * `tls` / `rediss` scheme, unix socket paths, `read_timeout`, the retry
     * settings, the client name and tcp keepalive. A configured `url` still
     * wins, since that form is a URI to begin with.
     *
     * @param  array<string, mixed>  $server
     */
    protected function makeConfig(array $server): RedisConfig
    {
        if (! empty($server['url'])) {
            return RedisConfig::fromUri(
                (string) $server['url'],
                (float) ($server['timeout'] ?? RedisConfig::DEFAULT_TIMEOUT),
            );
        }

        return RedisConfig::fromParameters($server);
    }
}
