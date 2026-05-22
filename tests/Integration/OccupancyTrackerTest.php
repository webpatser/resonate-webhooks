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
        new RosterKeys('roster-test'),
        'wh-test',
        90,
    );
}

it('counts connections across nodes from the roster', function () {
    $this->redis->hset('roster-test:presence-room:node-a', 'sock-1', 'u-1');
    $this->redis->hset('roster-test:presence-room:node-a', 'sock-2', 'u-2');
    $this->redis->hset('roster-test:presence-room:node-b', 'sock-3', 'u-1');

    $count = null;

    runLoop(function () use (&$count) {
        $count = makeTracker()->connectionCount('presence-room');
    });

    expect($count)->toBe(3);
});

it('claims the occupied edge exactly once across the cluster', function () {
    // The roster shows the channel has a connection; two nodes both react.
    $this->redis->hset('roster-test:presence-room:node-a', 'sock-1', 'u-1');

    $results = [];

    runLoop(function () use (&$results) {
        $results[] = makeTracker()->claimOccupied('presence-room');
        $results[] = makeTracker()->claimOccupied('presence-room');
    });

    expect($results)->toBe([true, false]);
});

it('claims the vacated edge once when the channel has emptied', function () {
    // The occupied flag is set but the roster has no connections left.
    $this->redis->set('wh-test:occ:presence-room', '1');

    $results = [];

    runLoop(function () use (&$results) {
        $results[] = makeTracker()->claimVacated('presence-room');
        $results[] = makeTracker()->claimVacated('presence-room');
    });

    expect($results)->toBe([true, false]);
});

it('does not claim vacated while the channel still has connections', function () {
    $this->redis->set('wh-test:occ:presence-room', '1');
    $this->redis->hset('roster-test:presence-room:node-a', 'sock-1', 'u-1');

    $result = null;

    runLoop(function () use (&$result) {
        $result = makeTracker()->claimVacated('presence-room');
    });

    expect($result)->toBeFalse();
});

it('claims member edges once per distinct user', function () {
    $this->redis->hset('roster-test:presence-room:node-a', 'sock-1', 'u-7');

    $added = [];
    $removed = [];

    runLoop(function () use (&$added) {
        $added[] = makeTracker()->claimMemberAdded('presence-room', 'u-7');
        $added[] = makeTracker()->claimMemberAdded('presence-room', 'u-7');
    });

    // The user leaves: the roster no longer lists them.
    $this->redis->del('roster-test:presence-room:node-a');

    runLoop(function () use (&$removed) {
        $removed[] = makeTracker()->claimMemberRemoved('presence-room', 'u-7');
        $removed[] = makeTracker()->claimMemberRemoved('presence-room', 'u-7');
    });

    expect($added)->toBe([true, false])
        ->and($removed)->toBe([true, false]);
});

it('reconciles a missed occupied edge against the roster', function () {
    // The roster shows a connection but no occupied flag was ever set.
    $this->redis->hset('roster-test:presence-room:node-a', 'sock-1', 'u-1');

    $edge = null;

    runLoop(function () use (&$edge) {
        $edge = makeTracker()->reconcileOccupancy('presence-room');
    });

    expect($edge)->toBe('occupied')
        ->and($this->redis->exists('wh-test:occ:presence-room'))->toBe(1);
});
