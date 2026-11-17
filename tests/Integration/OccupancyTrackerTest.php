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
    foreach (['roster-test:*', 'roster-test-nodes:*', 'wh-test:*'] as $pattern) {
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

/**
 * The number of SCAN commands the server served while the closure ran in the
 * event loop.
 */
function scansDuring(Client $redis, Closure $body): int
{
    $redis->executeRaw(['CONFIG', 'RESETSTAT']);

    runLoop($body);

    return scanCommandCount($redis);
}

function makeTracker(int $ttl = 90, ?RosterKeys $rosterKeys = null, float $reconcileInterval = 0.0): OccupancyTracker
{
    return new OccupancyTracker(
        createRedisClient(RedisConfig::fromUri('redis://127.0.0.1:6379/15')),
        $rosterKeys ?? RosterKeys::fromConfig(config('resonate-roster')),
        'wh-test',
        $ttl,
        $reconcileInterval,
    );
}

/**
 * An indexed roster key schema that runs a callback on every index read.
 *
 * Each roster read through the node index asks for the index key exactly once,
 * so the callback sees the reads in order and can change the roster between
 * two of them, which is how a member leaving mid-heal is staged.
 */
function rosterKeysObservingIndexReads(Closure $onRead): RosterKeys
{
    $prefix = RosterKeys::fromConfig(config('resonate-roster'))->prefix();

    return new class($prefix, $onRead) extends RosterKeys
    {
        protected int $reads = 0;

        public function __construct(string $prefix, protected Closure $onRead)
        {
            parent::__construct($prefix);
        }

        public function nodeIndexKey(string $appId, string $channel): string
        {
            ($this->onRead)(++$this->reads);

            return $this->prefix.'-nodes:'.$appId.':'.$channel;
        }
    };
}

/**
 * A roster key schema that has the node index key.
 *
 * The real schema when the installed roster release ships nodeIndexKey();
 * otherwise a subclass adding exactly that method, with the roster's own
 * "{prefix}-nodes:{appId}:{channel}" layout, so the index path is covered
 * before the roster dependency is bumped.
 */
function indexedRosterKeys(): RosterKeys
{
    $prefix = RosterKeys::fromConfig(config('resonate-roster'))->prefix();

    if (method_exists(RosterKeys::class, 'nodeIndexKey')) {
        return new RosterKeys($prefix);
    }

    return new class($prefix) extends RosterKeys
    {
        public function nodeIndexKey(string $appId, string $channel): string
        {
            return $this->prefix.'-nodes:'.$appId.':'.$channel;
        }
    };
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

it('does not count a channel whose name extends another with a colon', function () {
    $this->redis->hset('roster-test:app-id:presence-room.7:node-a', 'sock-1', 'u-alice');
    $this->redis->hset('roster-test:app-id:presence-room.7:x:node-b', 'sock-2', 'u-mallory');

    $state = null;
    $claimed = null;

    runLoop(function () use (&$state, &$claimed) {
        $tracker = makeTracker();
        $state = $tracker->state('app-id', 'presence-room.7');
        $claimed = $tracker->claimMemberAdded('app-id', 'presence-room.7', 'u-mallory');
    });

    expect($state)->toBe(['connections' => 1, 'users' => ['u-alice']])
        ->and($claimed)->toBeFalse();
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

it('keeps a member flag armed past the ttl while the member stays', function () {
    $this->redis->hset('roster-test:app-id:presence-room:node-a', 'sock-1', 'u-7');

    $added = null;

    runLoop(function () use (&$added) {
        $added = makeTracker(2)->claimMemberAdded('app-id', 'presence-room', 'u-7');
    });

    // The member stays for longer than the ttl; the reconcile tick keeps
    // refreshing their flag in the meantime.
    for ($tick = 0; $tick < 3; $tick++) {
        usleep(1_000_000);

        runLoop(function () {
            $tracker = makeTracker(2);
            $state = $tracker->state('app-id', 'presence-room');
            $tracker->refreshMembers('app-id', 'presence-room', $state['users']);
        });
    }

    // The member leaves after 3 s, well past the 2 s ttl.
    $this->redis->del('roster-test:app-id:presence-room:node-a');

    $removed = null;

    runLoop(function () use (&$removed) {
        $removed = makeTracker(2)->claimMemberRemoved('app-id', 'presence-room', 'u-7');
    });

    expect($added)->toBeTrue()
        ->and($removed)->toBeTrue();
});

it('re-claims a lapsed member flag with exactly one member_added per heal', function () {
    // The member is present but their flag has already expired.
    $this->redis->hset('roster-test:app-id:presence-room:node-a', 'sock-1', 'u-7');

    $healed = [];
    $added = null;

    runLoop(function () use (&$healed, &$added) {
        $tracker = makeTracker(2);
        // The first refresh finds no flag and owes the member_added edge.
        $healed[] = $tracker->refreshMembers('app-id', 'presence-room', ['u-7']);
        // The flag is back, so later refreshes and claims owe nothing.
        $healed[] = $tracker->refreshMembers('app-id', 'presence-room', ['u-7']);
        $added = $tracker->claimMemberAdded('app-id', 'presence-room', 'u-7');
    });

    $ttl = $this->redis->ttl('wh-test:mem:app-id:presence-room:u-7');

    // The flag lapses again while the member stays: a second heal, a second edge.
    $this->redis->del('wh-test:mem:app-id:presence-room:u-7');

    runLoop(function () use (&$healed) {
        $healed[] = makeTracker(2)->refreshMembers('app-id', 'presence-room', ['u-7']);
    });

    $this->redis->del('roster-test:app-id:presence-room:node-a');

    $removed = null;

    runLoop(function () use (&$removed) {
        $removed = makeTracker(2)->claimMemberRemoved('app-id', 'presence-room', 'u-7');
    });

    expect($healed)->toBe([['u-7'], [], ['u-7']])
        ->and($added)->toBeFalse()
        ->and($ttl)->toBeGreaterThan(0)->toBeLessThanOrEqual(2)
        ->and($removed)->toBeTrue();
});

it('does not restore a member flag from a stale snapshot after member_removed', function () {
    $this->redis->hset('roster-test:app-id:presence-room:node-a', 'sock-1', 'u-7');

    $added = null;

    runLoop(function () use (&$added) {
        $added = makeTracker()->claimMemberAdded('app-id', 'presence-room', 'u-7');
    });

    // The reconcile pass reads its snapshot while the member is still there.
    $stale = ['u-7'];

    // Before the pass refreshes, the member leaves and member_removed fires.
    $this->redis->del('roster-test:app-id:presence-room:node-a');

    $removed = null;
    $healed = null;

    runLoop(function () use (&$removed, &$healed, $stale) {
        $tracker = makeTracker();
        $removed = $tracker->claimMemberRemoved('app-id', 'presence-room', 'u-7');
        $healed = $tracker->refreshMembers('app-id', 'presence-room', $stale);
    });

    expect($added)->toBeTrue()
        ->and($removed)->toBeTrue()
        ->and($healed)->toBe([])
        ->and($this->redis->exists('wh-test:mem:app-id:presence-room:u-7'))->toBe(0);
});

it('drops a healed member flag when the member left before the re-read', function () {
    // The member is present and their flag has lapsed.
    $this->redis->hset('roster-test:app-id:presence-room:node-a', 'sock-1', 'u-7');
    $this->redis->sadd('roster-test-nodes:app-id:presence-room', ['node-a']);

    $redis = $this->redis;
    $removedDuringHeal = null;
    $healed = null;

    // Read 1 is the claim's fresh roster read, which still sees the member.
    // Before read 2, after the claim wrote the flag, the member leaves: their
    // member_removed claim ran before the flag existed and found nothing.
    $rosterKeys = rosterKeysObservingIndexReads(function (int $read) use ($redis, &$removedDuringHeal) {
        if ($read === 2) {
            $removedDuringHeal = $redis->exists('wh-test:mem:app-id:presence-room:u-7') === 1;
            $redis->del('roster-test:app-id:presence-room:node-a');
            $redis->del('roster-test-nodes:app-id:presence-room');
        }
    });

    runLoop(function () use (&$healed, $rosterKeys) {
        $healed = makeTracker(rosterKeys: $rosterKeys)->refreshMembers('app-id', 'presence-room', ['u-7']);
    });

    expect($removedDuringHeal)->toBeTrue()
        ->and($healed)->toBe([])
        ->and($this->redis->exists('wh-test:mem:app-id:presence-room:u-7'))->toBe(0);
});

it('raises a flag ttl at or below the reconcile interval to two intervals', function () {
    $this->redis->hset('roster-test:app-id:presence-room:node-a', 'sock-1', 'u-7');

    $ttls = [];

    runLoop(function () use (&$ttls) {
        $short = makeTracker(10, reconcileInterval: 30.0);
        $long = makeTracker(90, reconcileInterval: 30.0);

        $ttls['short'] = $short->ttl();
        $ttls['long'] = $long->ttl();

        $short->claimMemberAdded('app-id', 'presence-room', 'u-7');
    });

    $flagTtl = $this->redis->ttl('wh-test:mem:app-id:presence-room:u-7');

    expect($ttls)->toBe(['short' => 60, 'long' => 90])
        ->and($flagTtl)->toBeGreaterThan(30)->toBeLessThanOrEqual(60);
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

it('ignores pre-0.3.0 unscoped roster keys', function () {
    $this->redis->hset('roster-test:presence-room:node-old', 'sock-1', 'u-1');

    $count = null;
    $snapshot = null;

    runLoop(function () use (&$count, &$snapshot) {
        $count = makeTracker()->connectionCount('app-id', 'presence-room');
        $snapshot = makeTracker()->snapshot('app-id');
    });

    expect($count)->toBe(0)
        ->and($snapshot)->toBe([]);
});

it('ignores a pre-0.3.0 unscoped flag when claiming an edge', function () {
    $this->redis->set('wh-test:occ:presence-room', '1');
    $this->redis->hset('roster-test:app-id:presence-room:node-a', 'sock-1', 'u-1');

    $claimed = null;

    runLoop(function () use (&$claimed) {
        $claimed = makeTracker()->claimOccupied('app-id', 'presence-room');
    });

    expect($claimed)->toBeTrue()
        ->and($this->redis->exists('wh-test:occ:app-id:presence-room'))->toBe(1);
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

it('sweeps the keyspace a fixed number of times however many channels there are', function () {
    // The reconcile pass used to sweep the keyspace once per tracked channel,
    // so its cost grew with the busyness of the node. A snapshot is one sweep
    // for the application, whatever the channel count. Guard that here:
    // without the bound a later refactor could quietly reintroduce the
    // per-channel sweep.
    //
    // A SCAN sweep takes one call per batch of the whole database, so the
    // absolute count depends on whatever else lives in db 15. Compare against
    // a one-channel snapshot of the same database instead.
    $this->redis->hset('roster-test:app-id:presence-1:node-a', 'sock-1', 'u-1');

    $one = scansDuring($this->redis, fn () => makeTracker()->snapshot('app-id'));

    foreach (range(2, 8) as $n) {
        $this->redis->hset("roster-test:app-id:presence-{$n}:node-a", 'sock-'.$n, 'u-'.$n);
    }

    $eight = scansDuring($this->redis, fn () => makeTracker()->snapshot('app-id'));

    expect($eight)->toBe($one);
});

it('reads a channel from the roster node index without sweeping the keyspace', function () {
    $this->redis->hset('roster-test:app-id:presence-room:node-a', 'sock-1', 'u-1');
    $this->redis->hset('roster-test:app-id:presence-room:node-a', 'sock-2', 'u-2');
    $this->redis->hset('roster-test:app-id:presence-room:node-b', 'sock-3', 'u-1');
    // node-dead died without cleaning up: its hash expired, its index entry did not.
    $this->redis->sadd('roster-test-nodes:app-id:presence-room', ['node-a', 'node-b', 'node-dead']);

    $state = null;

    $scans = scansDuring($this->redis, function () use (&$state) {
        $state = makeTracker(rosterKeys: indexedRosterKeys())->state('app-id', 'presence-room');
    });

    expect($scans)->toBe(0)
        ->and($state['connections'])->toBe(3)
        ->and($state['users'])->toEqualCanonicalizing(['u-1', 'u-2']);
});

it('falls back to sweeping the keyspace when the roster node index is absent', function () {
    // Older roster nodes write the hashes but never the index.
    $this->redis->hset('roster-test:app-id:presence-room:node-a', 'sock-1', 'u-1');
    $this->redis->hset('roster-test:app-id:presence-room:node-b', 'sock-2', 'u-2');

    $state = null;

    $scans = scansDuring($this->redis, function () use (&$state) {
        $state = makeTracker(rosterKeys: indexedRosterKeys())->state('app-id', 'presence-room');
    });

    expect($scans)->toBeGreaterThan(0)
        ->and($state['connections'])->toBe(2)
        ->and($state['users'])->toEqualCanonicalizing(['u-1', 'u-2']);
});

it('reads a channel once when a hook claims both of its edges', function () {
    $this->redis->hset('roster-test:app-id:presence-room:node-a', 'sock-1', 'u-1');

    // The cost of a single read of this channel in the current database.
    $oneRead = scansDuring($this->redis, fn () => makeTracker()->state('app-id', 'presence-room'));

    $claims = [];

    $scans = scansDuring($this->redis, function () use (&$claims) {
        $tracker = makeTracker();
        $state = $tracker->state('app-id', 'presence-room');

        $claims[] = $tracker->claimOccupied('app-id', 'presence-room', $state);
        $claims[] = $tracker->claimMemberAdded('app-id', 'presence-room', 'u-1', $state);
    });

    // Both edges fire off the one read the caller already made.
    expect($claims)->toBe([true, true])
        ->and($oneRead)->toBeGreaterThan(0)
        ->and($scans)->toBe($oneRead);
});
