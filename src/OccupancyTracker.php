<?php

namespace Webpatser\ResonateWebhooks;

use Fledge\Async\Redis\RedisClient;
use Webpatser\ResonateRoster\RosterKeys;

/**
 * Turns the roster's cluster-wide state into exactly-once occupancy edges.
 *
 * The roster ({@see https://github.com/webpatser/resonate-roster}) keeps a
 * self-healing, per-node count of who is on each channel of each application.
 * This tracker reads that count over the async Redis client and claims each
 * edge with a small atomic flag key, so `channel_occupied`, `channel_vacated`,
 * `member_added`, and `member_removed` each fire once per cluster, not once
 * per node.
 *
 * Every read and every flag is scoped to one application: two applications
 * that both serve a "presence-lobby" get their own occupancy edges, instead of
 * the second one's subscription being swallowed by the first one's flag.
 *
 * Roster keys older than roster 0.3.0 carry no application. While the roster's
 * `legacy_fallback` window is open this tracker reads those too (per node, so
 * an upgraded and a not-yet-upgraded node are both counted) and treats a
 * pre-upgrade occupied flag as a claim already made, so an upgrade does not
 * re-announce channels that were occupied all along.
 */
class OccupancyTracker
{
    /**
     * @param  RedisClient  $redis  Async client, on the same Redis as the roster.
     * @param  RosterKeys  $rosterKeys  The roster's key schema, built from the roster's own config.
     * @param  string  $prefix  Namespace for this plugin's flag keys.
     * @param  int  $ttl  Flag-key TTL in seconds, so a dead node's flag self-heals.
     */
    public function __construct(
        protected RedisClient $redis,
        protected RosterKeys $rosterKeys,
        protected string $prefix,
        protected int $ttl,
    ) {
        //
    }

    /**
     * A channel's cluster-wide occupancy, read from the roster in one pass.
     *
     * The connection count and the distinct user ids come out of the same read:
     * a roster key is socket id => presence user id, so the field count is the
     * connection count and the values are the users. Asking for them separately
     * cost two keyspace sweeps and two rounds of per-node reads, and a caller
     * that needs both (every occupancy hook does) paid it twice per hook.
     *
     * @return array{connections: int, users: list<string>}
     */
    public function state(string $appId, string $channel): array
    {
        return $this->readState($this->rosterKeysFor($appId, $channel));
    }

    /**
     * Every occupied channel of one application, in a single keyspace sweep.
     *
     * The bulk form of {@see state()}, for the reconcile pass. Reconciling one
     * channel at a time cost a full sweep per channel per tick, so the tick got
     * more expensive as the cluster got busier; this costs the same one sweep
     * (two while the roster's legacy fallback window is open) at any channel
     * count, plus one read per live node key. It mirrors what
     * `RoomRoster::snapshot()` does on the synchronous side, over the async
     * client the server actually runs on.
     *
     * A channel absent from the result has no roster key at all, which is the
     * same thing as being empty.
     *
     * @return array<string, array{connections: int, users: list<string>}>
     */
    public function snapshot(string $appId): array
    {
        $channels = [];

        foreach ($this->rosterKeysByChannel($appId) as $channel => $keys) {
            // PHP coerces numeric-string array keys to int, so the channel name
            // is restored to a string before it keys the result.
            $channels[(string) $channel] = $this->readState(array_values($keys));
        }

        return $channels;
    }

    /**
     * The cluster-wide connection count for a channel, read from the roster.
     */
    public function connectionCount(string $appId, string $channel): int
    {
        return $this->state($appId, $channel)['connections'];
    }

    /**
     * The distinct presence user ids in a channel across the cluster.
     *
     * @return list<string>
     */
    public function users(string $appId, string $channel): array
    {
        return $this->state($appId, $channel)['users'];
    }

    /**
     * Claim the `channel_occupied` edge after a subscribe.
     *
     * True for exactly one node: the one whose flag write wins. False while a
     * pre-upgrade flag still holds the channel, since that edge already fired.
     *
     * Pass `$state` when the caller has already read the channel's occupancy
     * for this hook, so the roster is read once rather than once per edge.
     *
     * @param  array{connections: int, users: list<string>}|null  $state
     */
    public function claimOccupied(string $appId, string $channel, ?array $state = null): bool
    {
        $state ??= $this->state($appId, $channel);

        if ($state['connections'] < 1) {
            return false;
        }

        $claimed = $this->claimFlag($this->flag('occ', $appId, $channel));

        return $claimed && ! $this->legacyClaimHolds('occ', $channel);
    }

    /**
     * Claim the `channel_vacated` edge after an unsubscribe or close.
     *
     * True for exactly one node: the one whose flag delete wins.
     *
     * @param  array{connections: int, users: list<string>}|null  $state
     */
    public function claimVacated(string $appId, string $channel, ?array $state = null): bool
    {
        $state ??= $this->state($appId, $channel);

        if ($state['connections'] > 0) {
            return false;
        }

        return $this->releaseFlag($this->flag('occ', $appId, $channel), $this->legacyFlag('occ', $channel));
    }

    /**
     * Claim the `member_added` edge for a presence user after a subscribe.
     *
     * @param  array{connections: int, users: list<string>}|null  $state
     */
    public function claimMemberAdded(string $appId, string $channel, string $userId, ?array $state = null): bool
    {
        $state ??= $this->state($appId, $channel);

        if (! in_array($userId, $state['users'], true)) {
            return false;
        }

        $claimed = $this->claimFlag($this->flag('mem', $appId, $channel.':'.$userId));

        return $claimed && ! $this->legacyClaimHolds('mem', $channel.':'.$userId);
    }

    /**
     * Claim the `member_removed` edge for a presence user after a departure.
     *
     * @param  array{connections: int, users: list<string>}|null  $state
     */
    public function claimMemberRemoved(string $appId, string $channel, string $userId, ?array $state = null): bool
    {
        $state ??= $this->state($appId, $channel);

        if (in_array($userId, $state['users'], true)) {
            return false;
        }

        return $this->releaseFlag(
            $this->flag('mem', $appId, $channel.':'.$userId),
            $this->legacyFlag('mem', $channel.':'.$userId),
        );
    }

    /**
     * Reconcile a channel's occupied flag against the roster.
     *
     * Recovers an edge missed during a crash and refreshes the flag TTL.
     * Returns 'occupied', 'vacated', or null when nothing changed.
     *
     * The reconcile pass reads every channel of an application at once through
     * {@see snapshot()} and hands each channel's slice in here, so this makes
     * no keyspace sweep of its own unless a caller omits `$state`.
     *
     * @param  array{connections: int, users: list<string>}|null  $state
     */
    public function reconcileOccupancy(string $appId, string $channel, ?array $state = null): ?string
    {
        $state ??= $this->state($appId, $channel);

        $occupied = $state['connections'] > 0;
        $key = $this->flag('occ', $appId, $channel);
        $flagged = $this->redis->has($key);

        if ($occupied) {
            if (! $flagged) {
                return $this->claimFlag($key) && ! $this->legacyClaimHolds('occ', $channel) ? 'occupied' : null;
            }

            $this->redis->expireIn($key, $this->ttl);

            return null;
        }

        return $this->releaseFlag($key, $this->legacyFlag('occ', $channel)) ? 'vacated' : null;
    }

    /**
     * Claim a flag key: set it if absent, and refresh its TTL either way.
     */
    protected function claimFlag(string $key): bool
    {
        $claimed = $this->redis->setWithoutOverwrite($key, '1');

        $this->redis->expireIn($key, $this->ttl);

        return $claimed;
    }

    /**
     * Release an edge: drop the app-scoped flag and any pre-upgrade one.
     *
     * True when either delete won, so the edge fires exactly once whichever
     * schema the flag was claimed under.
     */
    protected function releaseFlag(string $key, string $legacyKey): bool
    {
        $released = $this->redis->delete($key) > 0;

        if ($this->rosterKeys->legacyFallback()) {
            $released = $this->redis->delete($legacyKey) > 0 || $released;
        }

        return $released;
    }

    /**
     * Whether a pre-upgrade flag already holds this edge.
     *
     * The pre-upgrade flag carries no application, so during the window it is
     * read as "some node already announced this channel". It expires on its
     * own TTL once the last pre-upgrade node is gone.
     */
    protected function legacyClaimHolds(string $kind, string $suffix): bool
    {
        return $this->rosterKeys->legacyFallback() && $this->redis->has($this->legacyFlag($kind, $suffix));
    }

    /**
     * Read a set of roster node keys into one channel's occupancy.
     *
     * A roster key is a hash of socket id => presence user id, so one HGETALL
     * per node yields both figures: the field count is that node's share of the
     * connections, and the non-empty values are its distinct users. A blank
     * value is a non-presence member, counted as a connection but not as a user,
     * exactly as the roster's own reader treats it.
     *
     * @param  list<string>  $keys
     * @return array{connections: int, users: list<string>}
     */
    protected function readState(array $keys): array
    {
        $connections = 0;
        $users = [];

        foreach ($keys as $key) {
            $hash = $this->redis->getMap($key)->getAll();

            $connections += count($hash);

            foreach ($hash as $userId) {
                if ($userId !== '') {
                    $users[$userId] = true;
                }
            }
        }

        // PHP silently casts numeric-string array keys ("42") to ints, which
        // would break the strict in_array() comparisons in the claim methods;
        // cast back so this is a list<string> as documented.
        return [
            'connections' => $connections,
            'users' => array_map('strval', array_keys($users)),
        ];
    }

    /**
     * Every roster node key of one application, grouped by channel.
     *
     * The whole-application form of {@see rosterKeysFor()}, and it holds the
     * same dual-read window: a node is served from its app-scoped key, or from
     * its pre-0.3.0 key while the fallback is on and it has no app-scoped one.
     * Membership is per node, so grouping this way can neither double count a
     * socket nor drop a node.
     *
     * @return array<string, array<string, string>> Channel name => node id => key.
     */
    protected function rosterKeysByChannel(string $appId): array
    {
        $keys = [];

        foreach ($this->redis->scan($this->rosterKeys->appPattern($appId), 100) as $key) {
            $channel = $this->rosterKeys->channelFromKey($appId, $key);

            if ($channel !== null) {
                $keys[$channel][$this->rosterKeys->nodeFromKey($key)] = $key;
            }
        }

        if ($this->rosterKeys->legacyFallback()) {
            foreach ($this->redis->scan($this->rosterKeys->legacyAllPattern(), 100) as $key) {
                $channel = $this->rosterKeys->legacyChannelFromKey($key);

                if ($channel !== null) {
                    $keys[$channel][$this->rosterKeys->nodeFromKey($key)] ??= $key;
                }
            }
        }

        return $keys;
    }

    /**
     * The roster's per-node keys for a channel, app-scoped key first.
     *
     * One key per node: the app-scoped key when that node writes it, and the
     * node's pre-0.3.0 key while the fallback window is open and it does not.
     * That is what keeps a rolling deploy from reading a channel as empty and
     * firing a spurious `channel_vacated`.
     *
     * @return list<string>
     */
    protected function rosterKeysFor(string $appId, string $channel): array
    {
        $keys = [];

        foreach ($this->redis->scan($this->rosterKeys->scanPattern($appId, $channel), 100) as $key) {
            $keys[$this->rosterKeys->nodeFromKey($key)] = $key;
        }

        if ($this->rosterKeys->legacyFallback()) {
            foreach ($this->redis->scan($this->rosterKeys->legacyScanPattern($channel), 100) as $key) {
                if (! $this->rosterKeys->isLegacyKeyFor($channel, $key)) {
                    continue;
                }

                $keys[$this->rosterKeys->nodeFromKey($key)] ??= $key;
            }
        }

        return array_values($keys);
    }

    /**
     * Build an app-scoped flag key.
     */
    protected function flag(string $kind, string $appId, string $suffix): string
    {
        return $this->prefix.':'.$kind.':'.$appId.':'.$suffix;
    }

    /**
     * Build the pre-upgrade flag key, which carried no application.
     */
    protected function legacyFlag(string $kind, string $suffix): string
    {
        return $this->prefix.':'.$kind.':'.$suffix;
    }
}
