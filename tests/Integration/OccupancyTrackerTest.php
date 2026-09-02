<?php

use Fledge\Async\Redis\RedisConfig;
use Predis\Client;
use Webpatser\ResonateRoster\RosterKeys;
use Webpatser\ResonateWebhooks\OccupancyTracker;

use function Fledge\Async\Redis\createRedisClient;

beforeEach(function () {
    if (! redisReachable()) {
        $this->markTestSkipped('Redis not reachable');
    }

    $this->redis = new Client(['host' => '127.0.0.1', 'port' => 6379, 'database' => 15]);

    flushTrackerKeys($this->redis);
});

afterEach(function () {
    if (isset($this->redis)) {
        flushTrackerKeys($this->redis);
    }
});

function flushTrackerKeys(Client $redis): void
{
    foreach (['roster-test:*', 'wh-test:*'] as $pattern) {
        foreach ($redis->keys($pattern) as $key) {
            $redis->del($key);
        }
    }
}

/**
 * The number of SCAN commands Redis has served since the last CONFIG RESETSTAT.
 *
 * Read from INFO commandstats, so it counts what actually reached the server
 * rather than what the tracker believes it issued.
 */
function scanCommandCount(Client $redis): int
{
    $info = (string) $redis->executeRaw(['INFO', 'commandstats']);

    if (! preg_match('/^cmdstat_scan:calls=(\\d+)/m', $info, $matches)) {
        return 0;
    }

    return (int) $matches[1];
}

function makeTracker(): OccupancyTracker
{
    return new OccupancyTracker(
        createRedisClient(RedisConfig::fromUri('redis://127.0.0.1:6379/15')),
        RosterKeys::fromConfig(config('resonate-roster')),
        'wh-test',
        90,
    );
}

it('counts connections across nodes from the roster', function () {
    $this->redis->hset('roster-test:app-id:presence-room:node-a', 'sock-1', 'u-1');
    $this->redis->hset('roster-test:app-id:presence-room:node-a', 'sock-2', 'u-2');
    $this->redis->hset('roster-test:app-id:presence-room:node-b', 'sock-3', 'u-1');

    $count = null;

    runLoop(function () use (&$count) {
        $count = makeTracker()->connectionCount('app-id', 'presence-room');
    });

    expect($count)->toBe(3);
});

it('counts only the application it is asked about', function () {
    $this->redis->hset('roster-test:app-id:presence-lobby:node-a', 'sock-1', 'u-1');
    $this->redis->hset('roster-test:app-two:presence-lobby:node-a', 'sock-2', 'u-2');
    $this->redis->hset('roster-test:app-two:presence-lobby:node-b', 'sock-3', 'u-3');

    $counts = [];

    runLoop(function () use (&$counts) {
        $counts['app-id'] = makeTracker()->connectionCount('app-id', 'presence-lobby');
        $counts['app-two'] = makeTracker()->connectionCount('app-two', 'presence-lobby');
    });

    expect($counts)->toBe(['app-id' => 1, 'app-two' => 2]);
});

it('claims the occupied edge exactly once across the cluster', function () {
    // The roster shows the channel has a connection; two nodes both react.
    $this->redis->hset('roster-test:app-id:presence-room:node-a', 'sock-1', 'u-1');

    $results = [];

    runLoop(function () use (&$results) {
        $results[] = makeTracker()->claimOccupied('app-id', 'presence-room');
        $results[] = makeTracker()->claimOccupied('app-id', 'presence-room');
    });

    expect($results)->toBe([true, false])
        ->and($this->redis->exists('wh-test:occ:app-id:presence-room'))->toBe(1);
});

it('claims the occupied edge per application on a shared channel name', function () {
    $this->redis->hset('roster-test:app-id:presence-lobby:node-a', 'sock-1', 'u-1');
    $this->redis->hset('roster-test:app-two:presence-lobby:node-a', 'sock-2', 'u-2');

    $results = [];

    runLoop(function () use (&$results) {
        $results[] = makeTracker()->claimOccupied('app-id', 'presence-lobby');
        $results[] = makeTracker()->claimOccupied('app-two', 'presence-lobby');
    });

    expect($results)->toBe([true, true]);
});

it('claims the vacated edge once when the channel has emptied', function () {
    // The occupied flag is set but the roster has no connections left.
    $this->redis->set('wh-test:occ:app-id:presence-room', '1');

    $results = [];

    runLoop(function () use (&$results) {
        $results[] = makeTracker()->claimVacated('app-id', 'presence-room');
        $results[] = makeTracker()->claimVacated('app-id', 'presence-room');
    });

    expect($results)->toBe([true, false]);
});

it('does not vacate one application because another emptied', function () {
    $this->redis->set('wh-test:occ:app-id:presence-lobby', '1');
    $this->redis->set('wh-test:occ:app-two:presence-lobby', '1');
    // Only the second application still has a connection.
    $this->redis->hset('roster-test:app-two:presence-lobby:node-a', 'sock-2', 'u-2');

    $results = [];

    runLoop(function () use (&$results) {
        $results[] = makeTracker()->claimVacated('app-id', 'presence-lobby');
        $results[] = makeTracker()->claimVacated('app-two', 'presence-lobby');
    });

    expect($results)->toBe([true, false])
        ->and($this->redis->exists('wh-test:occ:app-two:presence-lobby'))->toBe(1);
});

it('does not claim vacated while the channel still has connections', function () {
    $this->redis->set('wh-test:occ:app-id:presence-room', '1');
    $this->redis->hset('roster-test:app-id:presence-room:node-a', 'sock-1', 'u-1');

    $result = null;

    runLoop(function () use (&$result) {
        $result = makeTracker()->claimVacated('app-id', 'presence-room');
    });

    expect($result)->toBeFalse();
});

it('claims member edges once per distinct user', function () {
    $this->redis->hset('roster-test:app-id:presence-room:node-a', 'sock-1', 'u-7');

    $added = [];
    $removed = [];

    runLoop(function () use (&$added) {
        $added[] = makeTracker()->claimMemberAdded('app-id', 'presence-room', 'u-7');
        $added[] = makeTracker()->claimMemberAdded('app-id', 'presence-room', 'u-7');
    });

    // The user leaves: the roster no longer lists them.
    $this->redis->del('roster-test:app-id:presence-room:node-a');

    runLoop(function () use (&$removed) {
        $removed[] = makeTracker()->claimMemberRemoved('app-id', 'presence-room', 'u-7');
        $removed[] = makeTracker()->claimMemberRemoved('app-id', 'presence-room', 'u-7');
    });

    expect($added)->toBe([true, false])
        ->and($removed)->toBe([true, false]);
});

it('claims member edges per application for the same user id', function () {
    $this->redis->hset('roster-test:app-id:presence-lobby:node-a', 'sock-1', '42');
    $this->redis->hset('roster-test:app-two:presence-lobby:node-a', 'sock-2', '42');

    $added = [];

    runLoop(function () use (&$added) {
        $added[] = makeTracker()->claimMemberAdded('app-id', 'presence-lobby', '42');
        $added[] = makeTracker()->claimMemberAdded('app-two', 'presence-lobby', '42');
    });

    expect($added)->toBe([true, true]);
});

it('claims member edges for numeric user ids despite PHP key casting', function () {
    // Real user ids are numeric ("42"); PHP casts numeric-string array keys
    // to ints, which used to break the strict in_array() check in
    // claimMemberAdded() so the member_added edge never fired.
    $this->redis->hset('roster-test:app-id:presence-room:node-a', 'sock-1', '42');

    $users = [];
    $added = [];

    runLoop(function () use (&$users, &$added) {
        $users = makeTracker()->users('app-id', 'presence-room');
        $added[] = makeTracker()->claimMemberAdded('app-id', 'presence-room', '42');
        $added[] = makeTracker()->claimMemberAdded('app-id', 'presence-room', '42');
    });

    // The user leaves: the roster no longer lists them.
    $this->redis->del('roster-test:app-id:presence-room:node-a');

    $removed = [];

    runLoop(function () use (&$removed) {
        $removed[] = makeTracker()->claimMemberRemoved('app-id', 'presence-room', '42');
        $removed[] = makeTracker()->claimMemberRemoved('app-id', 'presence-room', '42');
    });

    expect($users)->toBe(['42'])
        ->and($added)->toBe([true, false])
        ->and($removed)->toBe([true, false]);
});

it('reconciles a missed occupied edge against the roster', function () {
    // The roster shows a connection but no occupied flag was ever set.
    $this->redis->hset('roster-test:app-id:presence-room:node-a', 'sock-1', 'u-1');

    $edge = null;

    runLoop(function () use (&$edge) {
        $edge = makeTracker()->reconcileOccupancy('app-id', 'presence-room');
    });

    expect($edge)->toBe('occupied')
        ->and($this->redis->exists('wh-test:occ:app-id:presence-room'))->toBe(1);
});

it('counts a pre-0.3.0 roster key while the fallback window is open', function () {
    // A node that has not been upgraded yet still writes the unscoped key.
    $this->redis->hset('roster-test:presence-room:node-old', 'sock-1', 'u-1');

    $count = null;
    $users = [];

    runLoop(function () use (&$count, &$users) {
        $count = makeTracker()->connectionCount('app-id', 'presence-room');
        $users = makeTracker()->users('app-id', 'presence-room');
    });

    expect($count)->toBe(1)
        ->and($users)->toBe(['u-1']);
});

it('reads an upgraded and a not-yet-upgraded node together mid-deploy', function () {
    $this->redis->hset('roster-test:app-id:presence-room:node-a', 'sock-1', 'u-1');
    $this->redis->hset('roster-test:presence-room:node-b', 'sock-2', 'u-2');

    $count = null;

    runLoop(function () use (&$count) {
        $count = makeTracker()->connectionCount('app-id', 'presence-room');
    });

    // Both nodes counted, and neither counted twice.
    expect($count)->toBe(2);
});

it('prefers the app-scoped key over the pre-0.3.0 key of the same node', function () {
    $this->redis->hset('roster-test:presence-room:node-a', 'sock-stale', 'u-stale');
    $this->redis->hset('roster-test:app-id:presence-room:node-a', 'sock-1', 'u-1');

    $users = [];

    runLoop(function () use (&$users) {
        $users = makeTracker()->users('app-id', 'presence-room');
    });

    expect($users)->toBe(['u-1']);
});

it('ignores pre-0.3.0 roster keys once the window is closed', function () {
    config()->set('resonate-roster.legacy_fallback', false);

    $this->redis->hset('roster-test:presence-room:node-old', 'sock-1', 'u-1');

    $count = null;

    runLoop(function () use (&$count) {
        $count = makeTracker()->connectionCount('app-id', 'presence-room');
    });

    expect($count)->toBe(0);
});

it('does not re-announce a channel a pre-upgrade flag already holds', function () {
    // The channel was occupied before the upgrade, so the unscoped flag holds
    // its edge. Claiming under the new schema must not post a second
    // channel_occupied for a channel that never vacated.
    $this->redis->set('wh-test:occ:presence-room', '1');
    $this->redis->hset('roster-test:app-id:presence-room:node-a', 'sock-1', 'u-1');

    $claimed = null;
    $edge = null;

    runLoop(function () use (&$claimed, &$edge) {
        $claimed = makeTracker()->claimOccupied('app-id', 'presence-room');
        $edge = makeTracker()->reconcileOccupancy('app-id', 'presence-room');
    });

    expect($claimed)->toBeFalse()
        ->and($edge)->toBeNull()
        // The edge is adopted, so the app-scoped flag now holds it.
        ->and($this->redis->exists('wh-test:occ:app-id:presence-room'))->toBe(1);
});

it('vacates a channel whose edge a pre-upgrade flag still holds', function () {
    $this->redis->set('wh-test:occ:presence-room', '1');

    $results = [];

    runLoop(function () use (&$results) {
        $results[] = makeTracker()->claimVacated('app-id', 'presence-room');
        $results[] = makeTracker()->claimVacated('app-id', 'presence-room');
    });

    expect($results)->toBe([true, false])
        ->and($this->redis->exists('wh-test:occ:presence-room'))->toBe(0);
});

it('reads a channel\'s connections and users in one pass', function () {
    $this->redis->hset('roster-test:app-id:presence-room:node-a', 'sock-1', 'u-1');
    $this->redis->hset('roster-test:app-id:presence-room:node-a', 'sock-2', 'u-2');
    // A non-presence member: a connection, but not a distinct user.
    $this->redis->hset('roster-test:app-id:presence-room:node-b', 'sock-3', '');

    $state = null;

    runLoop(function () use (&$state) {
        $state = makeTracker()->state('app-id', 'presence-room');
    });

    expect($state['connections'])->toBe(3)
        ->and($state['users'])->toEqualCanonicalizing(['u-1', 'u-2']);
});

it('snapshots every occupied channel of an application at once', function () {
    $this->redis->hset('roster-test:app-id:presence-one:node-a', 'sock-1', 'u-1');
    $this->redis->hset('roster-test:app-id:presence-two:node-a', 'sock-2', 'u-2');
    $this->redis->hset('roster-test:app-id:presence-two:node-b', 'sock-3', 'u-3');
    // Another application's channel must not appear in this snapshot.
    $this->redis->hset('roster-test:app-two:presence-one:node-a', 'sock-4', 'u-4');

    $snapshot = null;

    runLoop(function () use (&$snapshot) {
        $snapshot = makeTracker()->snapshot('app-id');
    });

    expect(array_keys($snapshot))->toEqualCanonicalizing(['presence-one', 'presence-two'])
        ->and($snapshot['presence-one']['connections'])->toBe(1)
        ->and($snapshot['presence-two']['connections'])->toBe(2)
        ->and($snapshot['presence-two']['users'])->toEqualCanonicalizing(['u-2', 'u-3']);
});

it('snapshots a channel served only by a pre-0.3.0 roster key', function () {
    $this->redis->hset('roster-test:presence-legacy:node-old', 'sock-1', 'u-1');

    $snapshot = null;

    runLoop(function () use (&$snapshot) {
        $snapshot = makeTracker()->snapshot('app-id');
    });

    expect($snapshot)->toHaveKey('presence-legacy')
        ->and($snapshot['presence-legacy']['connections'])->toBe(1);
});

it('sweeps the keyspace a fixed number of times however many channels there are', function () {
    // The reconcile pass used to sweep the keyspace once per tracked channel,
    // so its cost grew with the busyness of the node. A snapshot is one sweep
    // for the application, plus one more while the legacy fallback window is
    // open, whatever the channel count. Guard that here: without the bound a
    // later refactor could quietly reintroduce the per-channel sweep.
    foreach (range(1, 8) as $n) {
        $this->redis->hset("roster-test:app-id:presence-{$n}:node-a", 'sock-'.$n, 'u-'.$n);
    }

    $this->redis->executeRaw(['CONFIG', 'RESETSTAT']);

    runLoop(function () {
        makeTracker()->snapshot('app-id');
    });

    expect(scanCommandCount($this->redis))->toBeLessThanOrEqual(4);
});

it('reads a channel once when a hook claims both of its edges', function () {
    $this->redis->hset('roster-test:app-id:presence-room:node-a', 'sock-1', 'u-1');

    $this->redis->executeRaw(['CONFIG', 'RESETSTAT']);

    $claims = [];

    runLoop(function () use (&$claims) {
        $tracker = makeTracker();
        $state = $tracker->state('app-id', 'presence-room');

        $claims[] = $tracker->claimOccupied('app-id', 'presence-room', $state);
        $claims[] = $tracker->claimMemberAdded('app-id', 'presence-room', 'u-1', $state);
    });

    // Both edges fire off the one read the caller already made.
    expect($claims)->toBe([true, true])
        ->and(scanCommandCount($this->redis))->toBeLessThanOrEqual(2);
});
