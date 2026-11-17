<?php

use Fledge\Async\DeferredFuture;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Log;
use Webpatser\Resonate\Contracts\ApplicationProvider;
use Webpatser\ResonateWebhooks\Events\WebhookDelivered;
use Webpatser\ResonateWebhooks\Events\WebhookDropped;
use Webpatser\ResonateWebhooks\Tests\Support\RecordingTransport;
use Webpatser\ResonateWebhooks\WebhookDispatcher;
use Webpatser\ResonateWebhooks\WebhookEndpoint;
use Webpatser\ResonateWebhooks\WebhookEvent;
use Webpatser\ResonateWebhooks\WebhookSigner;
use Webpatser\ResonateWebhooks\WebhookTransport;

use function Fledge\Async\delay;

/**
 * Build a dispatcher wired to a recording transport.
 *
 * @param  list<WebhookEndpoint>  $endpoints
 */
function makeDispatcher(RecordingTransport $transport, array $endpoints, int $maxAttempts = 5): WebhookDispatcher
{
    return new WebhookDispatcher(
        $transport,
        new WebhookSigner,
        app(ApplicationProvider::class),
        $endpoints,
        $maxAttempts,
    );
}

it('delivers one coalesced, signed POST per endpoint', function () {
    $dispatcher = makeDispatcher(
        $transport = new RecordingTransport,
        [WebhookEndpoint::fromConfig(['url' => 'http://hook.test', 'app_id' => '*'])],
    );

    $dispatcher->record(WebhookEvent::channelOccupied('app-id', 'presence-room'));
    $dispatcher->record(WebhookEvent::memberAdded('app-id', 'presence-room', '7'));

    runLoop(function () use ($dispatcher) {
        $dispatcher->drain();
        delay(0.1);
    });

    expect($transport->deliveries)->toHaveCount(1);

    $delivery = $transport->deliveries[0];

    expect($delivery['url'])->toBe('http://hook.test')
        ->and($delivery['headers']['X-Pusher-Key'])->toBe('app-key')
        ->and($delivery['headers']['X-Pusher-Signature'])
        ->toBe(hash_hmac('sha256', $delivery['body'], 'app-secret'));

    $body = json_decode($delivery['body'], associative: true);

    expect($body)->toHaveKey('time_ms')
        ->and(array_column($body['events'], 'name'))->toBe(['channel_occupied', 'member_added']);
});

it('routes events to endpoints by the event filter', function () {
    $dispatcher = makeDispatcher($transport = new RecordingTransport, [
        WebhookEndpoint::fromConfig(['url' => 'http://occupancy.test', 'events' => ['channel_occupied']]),
        WebhookEndpoint::fromConfig(['url' => 'http://activity.test', 'events' => ['client_event']]),
    ]);

    $dispatcher->record(WebhookEvent::channelOccupied('app-id', 'presence-room'));
    $dispatcher->record(WebhookEvent::clientEvent('app-id', 'presence-room', 'client-typing', null, 'sock-1', '7'));

    runLoop(function () use ($dispatcher) {
        $dispatcher->drain();
        delay(0.1);
    });

    $byUrl = collect($transport->deliveries)->keyBy('url');

    expect($byUrl)->toHaveCount(2)
        ->and(array_column(json_decode($byUrl['http://occupancy.test']['body'], true)['events'], 'name'))
        ->toBe(['channel_occupied'])
        ->and(array_column(json_decode($byUrl['http://activity.test']['body'], true)['events'], 'name'))
        ->toBe(['client_event']);
});

it('keeps a failed delivery pending for retry', function () {
    $dispatcher = makeDispatcher(
        new RecordingTransport(500),
        [WebhookEndpoint::fromConfig(['url' => 'http://hook.test'])],
    );

    $dispatcher->record(WebhookEvent::channelOccupied('app-id', 'presence-room'));

    runLoop(function () use ($dispatcher) {
        $dispatcher->drain();
        delay(0.1);
    });

    expect($dispatcher->pendingCount())->toBe(1);
});

it('drops a delivery after the attempt limit', function () {
    $dispatcher = makeDispatcher(
        new RecordingTransport(500),
        [WebhookEndpoint::fromConfig(['url' => 'http://hook.test'])],
        maxAttempts: 1,
    );

    $dispatcher->record(WebhookEvent::channelOccupied('app-id', 'presence-room'));

    runLoop(function () use ($dispatcher) {
        $dispatcher->drain();
        delay(0.1);
    });

    expect($dispatcher->pendingCount())->toBe(0);
});

it('strips basic-auth credentials from a dropped delivery log line', function () {
    Log::spy();

    $dispatcher = makeDispatcher(
        new RecordingTransport(500),
        [WebhookEndpoint::fromConfig(['url' => 'https://user:secret-pass@hook.test/in?token=abc'])],
        maxAttempts: 1,
    );

    $dispatcher->record(WebhookEvent::channelOccupied('app-id', 'presence-room'));

    runLoop(function () use ($dispatcher) {
        $dispatcher->drain();
        delay(0.1);
    });

    Log::shouldHaveReceived('warning')->withArgs(function (string $message) {
        return str_contains($message, 'https://hook.test/in?token=abc')
            && ! str_contains($message, 'secret-pass')
            && ! str_contains($message, 'user:');
    })->once();
});

it('dispatches WebhookDelivered on a successful delivery', function () {
    Event::fake([WebhookDelivered::class, WebhookDropped::class]);

    $dispatcher = makeDispatcher(
        new RecordingTransport(202),
        [WebhookEndpoint::fromConfig(['url' => 'http://hook.test'])],
    );

    $dispatcher->record(WebhookEvent::channelOccupied('app-id', 'presence-room'));

    runLoop(function () use ($dispatcher) {
        $dispatcher->drain();
        delay(0.1);
    });

    Event::assertDispatched(WebhookDelivered::class, function (WebhookDelivered $event) {
        return $event->url === 'http://hook.test'
            && $event->status === 202
            && $event->appId === 'app-id'
            && $event->attempts === 1;
    });

    Event::assertNotDispatched(WebhookDropped::class);
});

it('dispatches WebhookDropped after the attempt limit', function () {
    Event::fake([WebhookDelivered::class, WebhookDropped::class]);

    $dispatcher = makeDispatcher(
        new RecordingTransport(500),
        [WebhookEndpoint::fromConfig(['url' => 'http://hook.test'])],
        maxAttempts: 1,
    );

    $dispatcher->record(WebhookEvent::channelOccupied('app-id', 'presence-room'));

    runLoop(function () use ($dispatcher) {
        $dispatcher->drain();
        delay(0.1);
    });

    Event::assertDispatched(WebhookDropped::class, function (WebhookDropped $event) {
        return $event->url === 'http://hook.test'
            && $event->appId === 'app-id'
            && $event->attempts === 1
            && $event->reason === 'HTTP 500';
    });

    Event::assertNotDispatched(WebhookDelivered::class);
});

it('contains a throwing WebhookDropped listener inside the delivery fiber', function () {
    Event::listen(WebhookDropped::class, function () {
        throw new RuntimeException('listener exploded');
    });

    $dispatcher = makeDispatcher(
        new RecordingTransport(500),
        [WebhookEndpoint::fromConfig(['url' => 'http://hook.test'])],
        maxAttempts: 1,
    );

    $dispatcher->record(WebhookEvent::channelOccupied('app-id', 'presence-room'));

    // The delivery fiber is deliberately discarded, so anything escaping it
    // would surface as an UnhandledFutureError out of EventLoop::run() and
    // take the whole server down. Reaching the assertion is the assertion.
    runLoop(function () use ($dispatcher) {
        $dispatcher->drain();
        delay(0.1);
    });

    expect($dispatcher->pendingCount())->toBe(0);
});

it('contains a throwing WebhookDelivered listener inside the delivery fiber', function () {
    Event::listen(WebhookDelivered::class, function () {
        throw new RuntimeException('listener exploded');
    });

    $dispatcher = makeDispatcher(
        new RecordingTransport(200),
        [WebhookEndpoint::fromConfig(['url' => 'http://hook.test'])],
    );

    $dispatcher->record(WebhookEvent::channelOccupied('app-id', 'presence-room'));

    runLoop(function () use ($dispatcher) {
        $dispatcher->drain();
        delay(0.1);
    });

    expect($dispatcher->pendingCount())->toBe(0);
});

it('bounds the buffer by evicting the oldest client event first', function () {
    $dispatcher = new WebhookDispatcher(
        $transport = new RecordingTransport,
        new WebhookSigner,
        app(ApplicationProvider::class),
        [WebhookEndpoint::fromConfig(['url' => 'http://hook.test'])],
        maxBufferedEvents: 3,
    );

    $dispatcher->record(WebhookEvent::channelOccupied('app-id', 'room-1'));
    $dispatcher->record(WebhookEvent::clientEvent('app-id', 'room-1', 'client-a', null, 'sock-1', null));
    $dispatcher->record(WebhookEvent::clientEvent('app-id', 'room-1', 'client-b', null, 'sock-1', null));
    $dispatcher->record(WebhookEvent::channelOccupied('app-id', 'room-2'));
    $dispatcher->record(WebhookEvent::clientEvent('app-id', 'room-1', 'client-c', null, 'sock-1', null));

    expect($dispatcher->bufferedCount())->toBe(3);

    runLoop(function () use ($dispatcher) {
        $dispatcher->drain();
        delay(0.1);
    });

    expect($transport->deliveries)->toHaveCount(1);

    $events = json_decode($transport->deliveries[0]['body'], true)['events'];

    expect(array_map(fn (array $e) => $e['event'] ?? $e['channel'], $events))
        ->toBe(['room-1', 'room-2', 'client-c']);
});

it('bounds the buffer by evicting the oldest event when no client event is buffered', function () {
    $dispatcher = new WebhookDispatcher(
        $transport = new RecordingTransport,
        new WebhookSigner,
        app(ApplicationProvider::class),
        [WebhookEndpoint::fromConfig(['url' => 'http://hook.test'])],
        maxBufferedEvents: 2,
    );

    foreach (['room-1', 'room-2', 'room-3'] as $room) {
        $dispatcher->record(WebhookEvent::channelOccupied('app-id', $room));
    }

    expect($dispatcher->bufferedCount())->toBe(2);

    runLoop(function () use ($dispatcher) {
        $dispatcher->drain();
        delay(0.1);
    });

    expect(array_column(json_decode($transport->deliveries[0]['body'], true)['events'], 'channel'))
        ->toBe(['room-2', 'room-3']);
});

it('bounds pending deliveries by dropping the oldest idle one', function () {
    Event::fake([WebhookDelivered::class, WebhookDropped::class]);

    $dispatcher = new WebhookDispatcher(
        $transport = new RecordingTransport(500),
        new WebhookSigner,
        app(ApplicationProvider::class),
        [
            WebhookEndpoint::fromConfig(['url' => 'http://a.test']),
            WebhookEndpoint::fromConfig(['url' => 'http://b.test']),
            WebhookEndpoint::fromConfig(['url' => 'http://c.test']),
        ],
        maxPendingDeliveries: 2,
    );

    $dispatcher->record(WebhookEvent::channelOccupied('app-id', 'room-1'));

    runLoop(function () use ($dispatcher) {
        $dispatcher->drain();
        delay(0.1);
    });

    expect($dispatcher->pendingCount())->toBe(2)
        ->and(array_column($transport->deliveries, 'url'))->toBe(['http://b.test', 'http://c.test']);

    Event::assertDispatchedTimes(WebhookDropped::class, 1);
    Event::assertDispatched(WebhookDropped::class, fn (WebhookDropped $event) => $event->url === 'http://a.test'
        && $event->reason === 'pending limit reached'
        && $event->attempts === 0);
});

it('cancels a buffered occupied/vacated pair before evicting an unpaired edge', function () {
    $dispatcher = new WebhookDispatcher(
        $transport = new RecordingTransport,
        new WebhookSigner,
        app(ApplicationProvider::class),
        [WebhookEndpoint::fromConfig(['url' => 'http://hook.test'])],
        maxBufferedEvents: 3,
    );

    $dispatcher->record(WebhookEvent::channelVacated('app-id', 'room-x'));
    $dispatcher->record(WebhookEvent::channelOccupied('app-id', 'room-y'));
    $dispatcher->record(WebhookEvent::channelVacated('app-id', 'room-y'));
    $dispatcher->record(WebhookEvent::channelOccupied('app-id', 'room-z'));

    expect($dispatcher->bufferedCount())->toBe(2);

    runLoop(function () use ($dispatcher) {
        $dispatcher->drain();
        delay(0.1);
    });

    $events = json_decode($transport->deliveries[0]['body'], true)['events'];

    expect(array_map(fn (array $e) => $e['name'].':'.$e['channel'], $events))
        ->toBe(['channel_vacated:room-x', 'channel_occupied:room-z']);
});

it('cancels a buffered member added/removed pair for the same member only', function () {
    $dispatcher = new WebhookDispatcher(
        $transport = new RecordingTransport,
        new WebhookSigner,
        app(ApplicationProvider::class),
        [WebhookEndpoint::fromConfig(['url' => 'http://hook.test'])],
        maxBufferedEvents: 4,
    );

    $dispatcher->record(WebhookEvent::memberRemoved('app-id', 'presence-a', '1'));
    $dispatcher->record(WebhookEvent::memberAdded('app-id', 'presence-a', '2'));
    $dispatcher->record(WebhookEvent::memberRemoved('app-id', 'presence-a', '3'));
    $dispatcher->record(WebhookEvent::memberRemoved('app-id', 'presence-a', '2'));
    $dispatcher->record(WebhookEvent::memberAdded('app-id', 'presence-a', '4'));

    expect($dispatcher->bufferedCount())->toBe(3);

    runLoop(function () use ($dispatcher) {
        $dispatcher->drain();
        delay(0.1);
    });

    $events = json_decode($transport->deliveries[0]['body'], true)['events'];

    expect(array_map(fn (array $e) => $e['name'].':'.$e['user_id'], $events))
        ->toBe(['member_removed:1', 'member_removed:3', 'member_added:4']);
});

it('warns that occupancy edges were lost when it must evict one by age', function () {
    Log::spy();

    $dispatcher = new WebhookDispatcher(
        new RecordingTransport,
        new WebhookSigner,
        app(ApplicationProvider::class),
        [WebhookEndpoint::fromConfig(['url' => 'http://hook.test'])],
        maxBufferedEvents: 1,
    );

    $dispatcher->record(WebhookEvent::channelVacated('app-id', 'room-x'));
    $dispatcher->record(WebhookEvent::channelOccupied('app-id', 'room-y'));

    runLoop(function () use ($dispatcher) {
        $dispatcher->drain();
        delay(0.1);
    });

    Log::shouldHaveReceived('warning')->withArgs(
        fn (string $message) => str_contains($message, 'dropped 1 event(s)')
            && str_contains($message, '1 occupancy or presence edge(s)')
            && str_contains($message, 'resync'),
    )->once();
});

it('splits a large batch by encoded size', function () {
    $eventBytes = strlen(json_encode(WebhookEvent::channelOccupied('app-id', 'room-1')->toArray())) + 1;

    $dispatcher = new WebhookDispatcher(
        $transport = new RecordingTransport,
        new WebhookSigner,
        app(ApplicationProvider::class),
        [WebhookEndpoint::fromConfig(['url' => 'http://hook.test'])],
        maxRequestBytes: 2 * $eventBytes,
        maxInFlightPerEndpoint: 0,
    );

    foreach (range(1, 3) as $i) {
        $dispatcher->record(WebhookEvent::channelOccupied('app-id', "room-{$i}"));
    }

    runLoop(function () use ($dispatcher) {
        $dispatcher->drain();
        delay(0.1);
        $dispatcher->drain();
        delay(0.1);
    });

    expect(array_map(fn (array $d) => count(json_decode($d['body'], true)['events']), $transport->deliveries))
        ->toBe([2, 1]);

    foreach ($transport->deliveries as $delivery) {
        expect($delivery['headers']['X-Pusher-Signature'])
            ->toBe(hash_hmac('sha256', $delivery['body'], 'app-secret'));
    }
});

it('sends the chunks of a split batch strictly in order', function () {
    $gate = new DeferredFuture;

    // The first chunk blocks on the gate; later chunks would return at once,
    // so concurrent sending completes them before the first.
    $transport = new class($gate) implements WebhookTransport
    {
        /** @var list<string> */
        public array $started = [];

        /** @var list<string> */
        public array $completed = [];

        public function __construct(private DeferredFuture $gate) {}

        public function deliver(string $url, array $headers, string $body): int
        {
            $first = json_decode($body, true)['events'][0]['channel'];
            $this->started[] = $first;

            if ($first === 'room-1') {
                $this->gate->getFuture()->await();
            }

            $this->completed[] = $first;

            return 200;
        }
    };

    $eventBytes = strlen(json_encode(WebhookEvent::channelOccupied('app-id', 'room-1')->toArray())) + 1;

    $dispatcher = new WebhookDispatcher(
        $transport,
        new WebhookSigner,
        app(ApplicationProvider::class),
        [WebhookEndpoint::fromConfig(['url' => 'http://hook.test'])],
        maxRequestBytes: $eventBytes,
        maxInFlightPerEndpoint: 0,
    );

    foreach (range(1, 3) as $i) {
        $dispatcher->record(WebhookEvent::channelOccupied('app-id', "room-{$i}"));
    }

    $startedWhileBlocked = null;

    runLoop(function () use ($dispatcher, $gate, $transport, &$startedWhileBlocked) {
        $dispatcher->drain();
        delay(0.05);
        $dispatcher->drain();
        delay(0.05);

        $startedWhileBlocked = count($transport->started);

        $gate->complete();

        for ($i = 0; $i < 50 && $dispatcher->pendingCount() > 0; $i++) {
            $dispatcher->drain();
            delay(0.01);
        }
    });

    expect($startedWhileBlocked)->toBe(1)
        ->and($transport->completed)->toBe(['room-1', 'room-2', 'room-3'])
        ->and($dispatcher->pendingCount())->toBe(0);
});

it('sends the next chunk once the previous one was dropped', function () {
    $dispatcher = new WebhookDispatcher(
        $transport = new RecordingTransport(500),
        new WebhookSigner,
        app(ApplicationProvider::class),
        [WebhookEndpoint::fromConfig(['url' => 'http://hook.test'])],
        maxAttempts: 1,
        maxRequestBytes: 1,
        maxInFlightPerEndpoint: 0,
    );

    foreach (range(1, 3) as $i) {
        $dispatcher->record(WebhookEvent::channelOccupied('app-id', "room-{$i}"));
    }

    runLoop(function () use ($dispatcher) {
        $dispatcher->drain();
        delay(0.01);

        for ($i = 0; $i < 50 && $dispatcher->pendingCount() > 0; $i++) {
            $dispatcher->drain();
            delay(0.01);
        }
    });

    expect(array_map(
        fn (array $d) => json_decode($d['body'], true)['events'][0]['channel'],
        $transport->deliveries,
    ))->toBe(['room-1', 'room-2', 'room-3'])
        ->and($dispatcher->pendingCount())->toBe(0);
});

it('caps concurrent deliveries per endpoint', function () {
    $gate = new DeferredFuture;

    $transport = new class($gate) implements WebhookTransport
    {
        public int $started = 0;

        public function __construct(private DeferredFuture $gate) {}

        public function deliver(string $url, array $headers, string $body): int
        {
            $this->started++;
            $this->gate->getFuture()->await();

            return 200;
        }
    };

    $dispatcher = new WebhookDispatcher(
        $transport,
        new WebhookSigner,
        app(ApplicationProvider::class),
        // Five endpoint entries sharing one URL give five independent
        // deliveries to the same endpoint from a single event.
        array_map(
            fn () => WebhookEndpoint::fromConfig(['url' => 'http://hook.test']),
            range(1, 5),
        ),
        maxInFlightPerEndpoint: 2,
    );

    $dispatcher->record(WebhookEvent::channelOccupied('app-id', 'room-1'));

    $startedWhileBlocked = null;
    $drains = 0;

    runLoop(function () use ($dispatcher, $gate, $transport, &$startedWhileBlocked, &$drains) {
        $dispatcher->drain();
        delay(0.05);
        $dispatcher->drain();
        delay(0.05);

        $startedWhileBlocked = $transport->started;

        $gate->complete();
        delay(0.05);

        while ($dispatcher->pendingCount() > 0 && $drains < 50) {
            $dispatcher->drain();
            $drains++;
            delay(0.01);
        }
    });

    expect($drains)->toBeLessThan(50, 'pending deliveries never drained')
        ->and($startedWhileBlocked)->toBe(2)
        ->and($transport->started)->toBe(5)
        ->and($dispatcher->pendingCount())->toBe(0);
});

it('does not retry a delivered webhook when its listener throws', function () {
    Event::listen(WebhookDelivered::class, function () {
        throw new RuntimeException('listener exploded');
    });

    $dispatcher = makeDispatcher(
        $transport = new RecordingTransport(200),
        [WebhookEndpoint::fromConfig(['url' => 'http://hook.test'])],
    );

    $dispatcher->record(WebhookEvent::channelOccupied('app-id', 'presence-room'));

    runLoop(function () use ($dispatcher) {
        $dispatcher->drain();
        delay(0.1);

        // A second drain would resend the identical signed payload if the
        // throwing listener had been read as a delivery failure.
        $dispatcher->drain();
        delay(0.1);
    });

    expect($dispatcher->pendingCount())->toBe(0)
        ->and($transport->deliveries)->toHaveCount(1);
});
