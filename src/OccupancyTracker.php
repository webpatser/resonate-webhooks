<?php

namespace Webpatser\ResonateWebhooks;

use Fledge\Async\Redis\RedisClient;
use Webpatser\ResonateRoster\RosterKeys;

/**
 * Turns the roster's cluster-wide state into exactly-once occupancy edges.
 *
 * The roster ({@see https://github.com/webpatser/resonate-roster}) keeps a
 * self-healing, per-node count of who is on each channel. This tracker reads
 * that count over the async Redis client and claims each edge with a small
 * atomic flag key, so `channel_occupied`, `channel_vacated`, `member_added`,
 * and `member_removed` each fire once per cluster, not once per node.
 */
class OccupancyTracker
{
    /**
     * @param  RedisClient  $redis  Async client, on the same Redis as the roster.
     * @param  RosterKeys  $rosterKeys  The roster's key schema.
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
    public function connectionCount(string $channel): int
    {
        $total = 0;

        foreach ($this->rosterKeysFor($channel) as $key) {
            $total += $this->redis->getMap($key)->getSize();
        }

        return $total;
    }

    /**
     * The distinct presence user ids in a channel across the cluster.
     *
     * @return list<string>
     */
    public function users(string $channel): array
    {
        $users = [];

        foreach ($this->rosterKeysFor($channel) as $key) {
            foreach ($this->redis->getMap($key)->getAll() as $userId) {
                if ($userId !== '') {
                    $users[$userId] = true;
                }
            }
        }

        return array_keys($users);
    }

    /**
     * Claim the `channel_occupied` edge after a subscribe.
     *
     * True for exactly one node: the one whose flag write wins.
     */
    public function claimOccupied(string $channel): bool
    {
        if ($this->connectionCount($channel) < 1) {
            return false;
        }

        return $this->claimFlag($this->flag('occ', $channel));
    }

    /**
     * Claim the `channel_vacated` edge after an unsubscribe or close.
     *
     * True for exactly one node: the one whose flag delete wins.
     */
    public function claimVacated(string $channel): bool
    {
        if ($this->connectionCount($channel) > 0) {
            return false;
        }

        return $this->redis->delete($this->flag('occ', $channel)) > 0;
    }

    /**
     * Claim the `member_added` edge for a presence user after a subscribe.
     */
    public function claimMemberAdded(string $channel, string $userId): bool
    {
        if (! in_array($userId, $this->users($channel), true)) {
            return false;
        }

        return $this->claimFlag($this->flag('mem', $channel.':'.$userId));
    }

    /**
     * Claim the `member_removed` edge for a presence user after a departure.
     */
    public function claimMemberRemoved(string $channel, string $userId): bool
    {
        if (in_array($userId, $this->users($channel), true)) {
            return false;
        }

        return $this->redis->delete($this->flag('mem', $channel.':'.$userId)) > 0;
    }

    /**
     * Reconcile a channel's occupied flag against the roster.
     *
     * Recovers an edge missed during a crash and refreshes the flag TTL.
     * Returns 'occupied', 'vacated', or null when nothing changed.
     */
    public function reconcileOccupancy(string $channel): ?string
    {
        $occupied = $this->connectionCount($channel) > 0;
        $key = $this->flag('occ', $channel);
        $flagged = $this->redis->has($key);

        if ($occupied && ! $flagged) {
            return $this->claimFlag($key) ? 'occupied' : null;
        }

        if ($occupied && $flagged) {
            $this->redis->expireIn($key, $this->ttl);

            return null;
        }

        if (! $occupied && $flagged) {
            return $this->redis->delete($key) > 0 ? 'vacated' : null;
        }

        return null;
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
     * The roster's per-node keys for a channel.
     *
     * @return list<string>
     */
    protected function rosterKeysFor(string $channel): array
    {
        $keys = [];

        foreach ($this->redis->scan($this->rosterKeys->scanPattern($channel), 100) as $key) {
            $keys[] = $key;
        }

        return $keys;
    }

    /**
     * Build a flag key.
     */
    protected function flag(string $kind, string $suffix): string
    {
        return $this->prefix.':'.$kind.':'.$suffix;
    }
}
