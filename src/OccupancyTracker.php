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
     * The cluster-wide connection count for a channel, read from the roster.
     */
    public function connectionCount(string $appId, string $channel): int
    {
        $total = 0;

        foreach ($this->rosterKeysFor($appId, $channel) as $key) {
            $total += $this->redis->getMap($key)->getSize();
        }

        return $total;
    }

    /**
     * The distinct presence user ids in a channel across the cluster.
     *
     * @return list<string>
     */
    public function users(string $appId, string $channel): array
    {
        $users = [];

        foreach ($this->rosterKeysFor($appId, $channel) as $key) {
            foreach ($this->redis->getMap($key)->getAll() as $userId) {
                if ($userId !== '') {
                    $users[$userId] = true;
                }
            }
        }

        // PHP silently casts numeric-string array keys ("42") to ints, which
        // would break the strict in_array() comparisons in the claim methods;
        // cast back so this is a list<string> as documented.
        return array_map('strval', array_keys($users));
    }

    /**
     * Claim the `channel_occupied` edge after a subscribe.
     *
     * True for exactly one node: the one whose flag write wins. False while a
     * pre-upgrade flag still holds the channel, since that edge already fired.
     */
    public function claimOccupied(string $appId, string $channel): bool
    {
        if ($this->connectionCount($appId, $channel) < 1) {
            return false;
        }

        $claimed = $this->claimFlag($this->flag('occ', $appId, $channel));

        return $claimed && ! $this->legacyClaimHolds('occ', $channel);
    }

    /**
     * Claim the `channel_vacated` edge after an unsubscribe or close.
     *
     * True for exactly one node: the one whose flag delete wins.
     */
    public function claimVacated(string $appId, string $channel): bool
    {
        if ($this->connectionCount($appId, $channel) > 0) {
            return false;
        }

        return $this->releaseFlag($this->flag('occ', $appId, $channel), $this->legacyFlag('occ', $channel));
    }

    /**
     * Claim the `member_added` edge for a presence user after a subscribe.
     */
    public function claimMemberAdded(string $appId, string $channel, string $userId): bool
    {
        if (! in_array($userId, $this->users($appId, $channel), true)) {
            return false;
        }

        $claimed = $this->claimFlag($this->flag('mem', $appId, $channel.':'.$userId));

        return $claimed && ! $this->legacyClaimHolds('mem', $channel.':'.$userId);
    }

    /**
     * Claim the `member_removed` edge for a presence user after a departure.
     */
    public function claimMemberRemoved(string $appId, string $channel, string $userId): bool
    {
        if (in_array($userId, $this->users($appId, $channel), true)) {
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
     */
    public function reconcileOccupancy(string $appId, string $channel): ?string
    {
        $occupied = $this->connectionCount($appId, $channel) > 0;
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
