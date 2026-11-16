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
     * more expensive as the cluster got busier; this costs the same one sweep at
     * any channel count, plus one read per live node key. It mirrors what
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
     * True for exactly one node: the one whose flag write wins.
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

        return $this->claimFlag($this->flag('occ', $appId, $channel));
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

        return $this->releaseFlag($this->flag('occ', $appId, $channel));
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

        return $this->claimFlag($this->flag('mem', $appId, $channel.':'.$userId));
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

        return $this->releaseFlag($this->flag('mem', $appId, $channel.':'.$userId));
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
                return $this->claimFlag($key) ? 'occupied' : null;
            }

            $this->redis->expireIn($key, $this->ttl);

            return null;
        }

        return $this->releaseFlag($key) ? 'vacated' : null;
    }

    /**
     * Keep the member flags of every present user armed.
     *
     * A member flag carries the TTL so a dead node's flag self-heals, but a
     * member who stays longer than that TTL would otherwise lose the flag, and
     * the later `member_removed` claim would find nothing to delete. The
     * reconcile pass calls this with each channel's roster slice, so a flag
     * lives exactly as long as its member is present.
     *
     * A flag that already lapsed is re-claimed silently: the member is present,
     * so no `member_added` edge is owed; the flag only has to exist again for
     * the departure to be claimed.
     *
     * @param  list<string>  $users  Presence user ids currently in the roster.
     */
    public function refreshMembers(string $appId, string $channel, array $users): void
    {
        foreach ($users as $userId) {
            $this->claimFlag($this->flag('mem', $appId, $channel.':'.$userId));
        }
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
     * Release an edge: drop its flag key.
     *
     * True only for the node whose delete wins, so the edge fires exactly once.
     */
    protected function releaseFlag(string $key): bool
    {
        return $this->redis->delete($key) > 0;
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
     * The whole-application form of {@see rosterKeysFor()}.
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

        return $keys;
    }

    /**
     * The roster's per-node keys for a channel of one application.
     *
     * Read from the roster's node index set when there is one: one SMEMBERS
     * instead of a sweep of the whole keyspace on every subscribe, unsubscribe
     * and close. A node id in the index whose hash already expired (a node that
     * died without cleaning up) yields a key that reads as empty, so it counts
     * as zero members.
     *
     * Falls back to the SCAN when the index is empty or absent: a roster
     * release without {@see RosterKeys::nodeIndexKey()}, or older roster nodes
     * still running that never write the index.
     *
     * @return list<string>
     */
    protected function rosterKeysFor(string $appId, string $channel): array
    {
        return $this->indexedRosterKeysFor($appId, $channel)
            ?? $this->scannedRosterKeysFor($appId, $channel);
    }

    /**
     * The roster's per-node keys for a channel, read from the node index set.
     *
     * Null when the installed roster has no index key or the index is empty,
     * which tells the caller to sweep instead. Redis drops a set with no
     * members, so an empty index and a missing one look the same here.
     *
     * @return list<string>|null
     */
    protected function indexedRosterKeysFor(string $appId, string $channel): ?array
    {
        if (! method_exists($this->rosterKeys, 'nodeIndexKey')) {
            return null;
        }

        $nodes = $this->redis->getSet($this->rosterKeys->nodeIndexKey($appId, $channel))->getAll();

        if ($nodes === []) {
            return null;
        }

        $keys = [];

        foreach ($nodes as $node) {
            $keys[] = $this->rosterKeys->hashKey($appId, $channel, (string) $node);
        }

        return $keys;
    }

    /**
     * The roster's per-node keys for a channel, found by sweeping the keyspace.
     *
     * @return list<string>
     */
    protected function scannedRosterKeysFor(string $appId, string $channel): array
    {
        $keys = [];

        foreach ($this->redis->scan($this->rosterKeys->scanPattern($appId, $channel), 100) as $key) {
            // The scan pattern is a glob prefix: `presence-room.7:x` also matches
            // the pattern for `presence-room.7`, so confirm the channel exactly.
            if ($this->rosterKeys->channelFromKey($appId, $key) !== $channel) {
                continue;
            }

            $keys[$this->rosterKeys->nodeFromKey($key)] = $key;
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
}
