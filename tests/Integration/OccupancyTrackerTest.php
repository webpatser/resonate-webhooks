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
