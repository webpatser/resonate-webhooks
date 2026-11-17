<?php

namespace Webpatser\ResonateWebhooks;

use Illuminate\Support\Facades\Log;
use SplQueue;
use Throwable;
use Webpatser\Resonate\Contracts\ApplicationProvider;
use Webpatser\ResonateWebhooks\Events\WebhookDelivered;
use Webpatser\ResonateWebhooks\Events\WebhookDropped;

use function Fledge\Async\async;

/**
 * Buffers webhook events and delivers them, off the connection path.
 *
 * Events recorded by the plugin accumulate in a buffer. On each drain tick the
 * buffer is coalesced into one signed POST per endpoint and application, and
 * every due delivery is sent in a detached fiber, so a slow endpoint never
 * stalls the event loop or another endpoint. A failed delivery is retried with
 * exponential backoff and dropped after the attempt limit.
 */
class WebhookDispatcher
{
    /**
     * Events recorded since the last drain, keyed by an increasing sequence
     * number so insertion order survives an eviction from the middle.
     *
     * @var array<int, WebhookEvent>
     */
    protected array $buffer = [];

    /**
     * The next buffer sequence number.
     */
    protected int $bufferSequence = 0;

    /**
     * Buffer keys of client events, oldest first, so the oldest one can be
     * evicted in constant time when the buffer is full.
     *
     * @var SplQueue<int>
     */
    protected SplQueue $clientEventKeys;

    /**
     * Buffer key of the latest occupancy edge per app and channel, and of the
     * latest presence edge per app, channel and member.
     *
     * @var array<string, int>
     */
    protected array $lastEdge = [];

    /**
     * Buffered opposite pairs (occupied then vacated, added then removed for
     * the same member) as [first key, second key], oldest first. Cancelling
     * one frees two slots and loses nothing a receiver needs.
     *
     * @var SplQueue<array{0: int, 1: int}>
     */
    protected SplQueue $cancellablePairs;

    /**
     * Events evicted from the buffer since the last drain.
     */
    protected int $droppedEvents = 0;

    /**
     * Occupancy or presence edges among the evicted events since the last drain.
     */
    protected int $lostEdges = 0;

    /**
     * For a chunk of a split batch: the earlier chunk it must wait for, keyed
     * by spl_object_id of the waiting chunk.
     *
     * @var array<int, PendingDelivery>
     */
    protected array $predecessor = [];

    /**
     * The reverse of {@see $predecessor}: the next chunk, keyed by
     * spl_object_id of the earlier chunk.
     *
     * @var array<int, PendingDelivery>
     */
    protected array $successor = [];

    /**
     * Deliveries built and awaiting (or retrying) delivery, keyed by
     * spl_object_id so forget() is constant time. Insertion order is kept, so
     * the first entry is always the oldest.
     *
     * @var array<int, PendingDelivery>
     */
    protected array $pending = [];

    /**
     * Maximum buffered events between drains (0 = unbounded).
     */
    protected int $maxBufferedEvents;

    /**
     * Maximum deliveries awaiting or retrying delivery (0 = unbounded).
     */
    protected int $maxPendingDeliveries;

    /**
     * Maximum encoded size of the events in one request body (0 = unbounded).
     */
    protected int $maxRequestBytes;

    /**
     * Maximum concurrent deliveries to one endpoint URL (0 = unbounded).
     */
    protected int $maxInFlightPerEndpoint;

    /**
     * Limits left null are read from the `resonate-webhooks` config, so the
     * plugin, which builds the dispatcher directly, picks them up unchanged.
     *
     * @param  list<WebhookEndpoint>  $endpoints
     */
    public function __construct(
        protected WebhookTransport $transport,
        protected WebhookSigner $signer,
        protected ApplicationProvider $apps,
        protected array $endpoints,
        protected int $maxAttempts = 5,
        ?int $maxBufferedEvents = null,
        ?int $maxPendingDeliveries = null,
        ?int $maxRequestBytes = null,
        ?int $maxInFlightPerEndpoint = null,
    ) {
        $this->clientEventKeys = new SplQueue;
        $this->cancellablePairs = new SplQueue;

        $this->maxBufferedEvents = max(0, $maxBufferedEvents ?? $this->limit('max_buffered_events', 10000));
        $this->maxPendingDeliveries = max(0, $maxPendingDeliveries ?? $this->limit('max_pending_deliveries', 1000));
        $this->maxRequestBytes = max(0, $maxRequestBytes ?? $this->limit('max_request_bytes', 1048576));
        $this->maxInFlightPerEndpoint = max(0, $maxInFlightPerEndpoint ?? $this->limit('max_in_flight_per_endpoint', 16));
    }

    /**
     * Read one limit from the package config.
     */
    protected function limit(string $key, int $default): int
    {
        return (int) config('resonate-webhooks.'.$key, $default);
    }

    /**
     * Buffer an event for delivery on the next drain.
     *
     * When the buffer is full, room is made in this order:
     *
     * 1. Cancel the oldest buffered opposite pair for one channel (occupied
     *    then vacated) or one member (added then removed). The receiver ends
     *    in the state it was in before the pair, so nothing is lost.
     * 2. Evict the oldest client event; client events are best-effort.
     * 3. Evict the oldest event. That loses an occupancy or presence edge, for
     *    example a vacated whose occupied was already delivered, so a receiver
     *    may keep a stale state until it resyncs. The next drain logs a
     *    warning naming how many edges were lost.
     *
     * The new event is always kept, so the buffer never exceeds its bound.
     */
    public function record(WebhookEvent $event): void
    {
        if ($this->maxBufferedEvents > 0 && count($this->buffer) >= $this->maxBufferedEvents) {
            $this->evictOneEvent();
        }

        $key = $this->bufferSequence++;

        $this->buffer[$key] = $event;

        if ($event->name === 'client_event') {
            $this->clientEventKeys->enqueue($key);

            return;
        }

        $this->trackEdge($event, $key);
    }

    /**
     * Remember an occupancy or presence edge and queue the pair it closes.
     */
    protected function trackEdge(WebhookEvent $event, int $key): void
    {
        [$edge, $opens] = match ($event->name) {
            'channel_occupied' => ['occ', true],
            'channel_vacated' => ['occ', false],
            'member_added' => ['mem', true],
            'member_removed' => ['mem', false],
            default => [null, false],
        };

        if ($edge === null) {
            return;
        }

        $id = $edge."\0".$event->appId."\0".$event->channel;

        if ($edge === 'mem') {
            $id .= "\0".($event->payload['user_id'] ?? '');
        }

        $previous = $this->lastEdge[$id] ?? null;

        if (! $opens && $previous !== null) {
            $opening = $this->buffer[$previous] ?? null;

            if ($opening !== null && in_array($opening->name, ['channel_occupied', 'member_added'], true)) {
                $this->cancellablePairs->enqueue([$previous, $key]);
            }
        }

        $this->lastEdge[$id] = $key;
    }

    /**
     * Make room for one event: cancel a pair, else evict a client event, else
     * evict the oldest event (losing an edge).
     */
    protected function evictOneEvent(): void
    {
        while (! $this->cancellablePairs->isEmpty()) {
            [$first, $second] = $this->cancellablePairs->dequeue();

            // A pair is stale once either side was evicted by age.
            if (isset($this->buffer[$first], $this->buffer[$second])) {
                unset($this->buffer[$first], $this->buffer[$second]);

                return;
            }
        }

        $this->droppedEvents++;

        if (! $this->clientEventKeys->isEmpty()) {
            unset($this->buffer[$this->clientEventKeys->dequeue()]);

            return;
        }

        $oldest = array_key_first($this->buffer);

        if ($oldest !== null) {
            unset($this->buffer[$oldest]);
            $this->lostEdges++;
        }
    }

    /**
     * The number of events buffered since the last drain.
     */
    public function bufferedCount(): int
    {
        return count($this->buffer);
    }

    /**
     * The number of deliveries currently buffered or retrying.
     */
    public function pendingCount(): int
    {
        return count($this->pending);
    }

    /**
     * Coalesce the buffer and send every due delivery.
     */
    public function drain(): void
    {
        $this->flushBuffer();

        $now = microtime(true);

        $inFlight = [];

        if ($this->maxInFlightPerEndpoint > 0) {
            foreach ($this->pending as $delivery) {
                if ($delivery->inFlight) {
                    $inFlight[$delivery->url] = ($inFlight[$delivery->url] ?? 0) + 1;
                }
            }
        }

        foreach ($this->pending as $delivery) {
            if ($delivery->inFlight || $delivery->dueAt > $now) {
                continue;
            }

            // A later chunk of a split batch waits until the earlier chunk
            // succeeded or was dropped, so chunks arrive in order.
            if (isset($this->predecessor[spl_object_id($delivery)])) {
                continue;
            }

            // A slow endpoint holds at most this many fibers; the rest wait
            // in pending for a later drain instead of piling up concurrently.
            if ($this->maxInFlightPerEndpoint > 0) {
                if (($inFlight[$delivery->url] ?? 0) >= $this->maxInFlightPerEndpoint) {
                    continue;
                }

                $inFlight[$delivery->url] = ($inFlight[$delivery->url] ?? 0) + 1;
            }

            $this->launch($delivery);
        }
    }

    /**
     * Start one delivery attempt in its own fiber.
     */
    protected function launch(PendingDelivery $delivery): void
    {
        $delivery->inFlight = true;

        // Fire-and-forget: the delivery runs in its own fiber so a slow
        // endpoint never stalls the loop. attempt() swallows every
        // Throwable itself, and ->ignore() marks the Future as
        // deliberately discarded (fledge's own convention, see
        // RedisSubscriber), so a rejection can never reach the event loop
        // as an UnhandledFutureError and stop the server.
        async(fn () => $this->attempt($delivery))->ignore();
    }

    /**
     * Start the next chunk of a split batch right after its predecessor
     * finished, taking over the slot the predecessor held.
     */
    protected function launchSuccessor(?PendingDelivery $next): void
    {
        if ($next !== null && ! $next->inFlight && $next->dueAt <= microtime(true)) {
            $this->launch($next);
        }
    }

    /**
     * Turn buffered events into one delivery per endpoint and application.
     */
    protected function flushBuffer(): void
    {
        if ($this->droppedEvents > 0) {
            $dropped = $this->droppedEvents;
            $lost = $this->lostEdges;
            $this->droppedEvents = 0;
            $this->lostEdges = 0;

            $message = "Webhook buffer full, dropped {$dropped} event(s) since the last drain";

            if ($lost > 0) {
                $message .= ", including {$lost} occupancy or presence edge(s): receivers may hold stale channel or member state and need a resync";
            }

            Log::warning($message." (max_buffered_events {$this->maxBufferedEvents})");
        }

        if ($this->buffer === []) {
            return;
        }

        $events = array_values($this->buffer);
        $this->buffer = [];
        $this->clientEventKeys = new SplQueue;
        $this->cancellablePairs = new SplQueue;
        $this->lastEdge = [];

        foreach ($this->endpoints as $endpoint) {
            $byApp = [];

            foreach ($events as $event) {
                if ($endpoint->accepts($event)) {
                    $byApp[$event->appId][] = $event;
                }
            }

            foreach ($byApp as $appId => $appEvents) {
                $previous = null;

                foreach ($this->chunk($appEvents) as $chunk) {
                    $delivery = $this->buildDelivery($endpoint, (string) $appId, $chunk);

                    if ($delivery === null || ! $this->addPending($delivery)) {
                        continue;
                    }

                    // Chain the chunks so each waits for the one before it.
                    if ($previous !== null && $this->isPending($previous)) {
                        $this->predecessor[spl_object_id($delivery)] = $previous;
                        $this->successor[spl_object_id($previous)] = $delivery;
                    }

                    $previous = $delivery;
                }
            }
        }
    }

    /**
     * Whether this exact delivery is still pending.
     */
    protected function isPending(PendingDelivery $delivery): bool
    {
        return ($this->pending[spl_object_id($delivery)] ?? null) === $delivery;
    }

    /**
     * Split events into request-sized chunks by encoded size.
     *
     * A single event larger than the byte limit still goes out, alone. The
     * chunks of one batch are sent strictly in order, each after the previous
     * one succeeded or was dropped; they share the batch's `time_ms`.
     *
     * @param  list<WebhookEvent>  $events
     * @return list<list<WebhookEvent>>
     */
    protected function chunk(array $events): array
    {
        if ($this->maxRequestBytes === 0) {
            return [$events];
        }

        $chunks = [];
        $current = [];
        $bytes = 0;

        foreach ($events as $event) {
            $size = strlen(json_encode($event->toArray(), JSON_THROW_ON_ERROR)) + 1;

            if ($current !== [] && $bytes + $size > $this->maxRequestBytes) {
                $chunks[] = $current;
                $current = [];
                $bytes = 0;
            }

            $current[] = $event;
            $bytes += $size;
        }

        if ($current !== []) {
            $chunks[] = $current;
        }

        return $chunks;
    }

    /**
     * Add a delivery to the pending list, dropping the oldest idle one when
     * full. Returns whether the given delivery was kept.
     *
     * A delivery already in flight is never dropped. If every pending delivery
     * is in flight, the new delivery itself is dropped instead. "Idle" includes
     * a delivery still waiting for an in-flight slot or for its previous chunk,
     * so such a delivery can be dropped with 0 attempts.
     *
     * No pair cancelling happens here: a pending body is already serialised
     * and signed, so its events cannot be removed one by one. A dropped
     * delivery therefore may lose occupancy or presence edges; WebhookDropped
     * fires and the log line says a resync may be needed.
     */
    protected function addPending(PendingDelivery $delivery): bool
    {
        if ($this->maxPendingDeliveries > 0 && count($this->pending) >= $this->maxPendingDeliveries) {
            $victim = $delivery;

            foreach ($this->pending as $pending) {
                if (! $pending->inFlight) {
                    $victim = $pending;

                    break;
                }
            }

            if ($victim !== $delivery) {
                // A successor of the victim becomes eligible on the next drain.
                $this->forget($victim);
                $this->pending[spl_object_id($delivery)] = $delivery;
            }

            $this->drop($victim, 'pending limit reached');

            return $victim !== $delivery;
        }

        $this->pending[spl_object_id($delivery)] = $delivery;

        return true;
    }

    /**
     * Report a delivery dropped by the pending bound, letting nothing escape.
     *
     * This runs on the drain tick rather than in a delivery fiber, so a
     * throwing host listener must still be contained here.
     */
    protected function drop(PendingDelivery $delivery, string $reason): void
    {
        try {
            Log::warning("Webhook dropped ({$reason}), any occupancy or presence edges in it are lost and receivers may need a resync: ".$this->sanitizeUrlForLog($delivery->url));

            WebhookDropped::dispatch($delivery->url, $delivery->appId, $delivery->attempts, $reason);
        } catch (Throwable) {
            // Host code threw; the delivery is already gone either way.
        }
    }

    /**
     * Build a signed, Pusher-format delivery for one endpoint and application.
     *
     * @param  list<WebhookEvent>  $events
     */
    protected function buildDelivery(WebhookEndpoint $endpoint, string $appId, array $events): ?PendingDelivery
    {
        try {
            $app = $this->apps->findById($appId);
        } catch (Throwable $e) {
            Log::warning('Webhook delivery skipped, unknown application '.$appId.': '.$e->getMessage());

            return null;
        }

        $body = json_encode([
            'time_ms' => (int) (microtime(true) * 1000),
            'events' => array_map(fn (WebhookEvent $event) => $event->toArray(), $events),
        ], JSON_THROW_ON_ERROR);

        return new PendingDelivery($endpoint->url, [
            'Content-Type' => 'application/json',
            'X-Pusher-Key' => $app->key(),
            'X-Pusher-Signature' => $this->signer->sign($body, $app->secret()),
        ], $body, $appId);
    }

    /**
     * Run one delivery attempt, letting nothing escape the fiber.
     *
     * This is the whole body of the discarded fiber started by drain(). Every
     * Throwable has to die here: the events dispatched below reach host
     * listeners this package does not control, and a listener that throws
     * would reject an unobserved Future, which surfaces as an
     * UnhandledFutureError out of EventLoop::run() and takes the entire
     * WebSocket server down over one bad webhook endpoint.
     */
    protected function attempt(PendingDelivery $delivery): void
    {
        try {
            $this->send($delivery);
        } catch (Throwable $e) {
            // The delivery is released rather than left in flight, so a later
            // drain can still pick it up if it was never resolved.
            $delivery->inFlight = false;

            try {
                Log::warning('Webhook delivery attempt aborted: '.$e->getMessage());
            } catch (Throwable) {
                // The logger is host code too. If it throws there is nowhere
                // left to report to, and the fiber must still end quietly.
            }
        }
    }

    /**
     * Send one delivery, retrying with backoff or dropping on exhaustion.
     */
    protected function send(PendingDelivery $delivery): void
    {
        try {
            $status = $this->transport->deliver($delivery->url, $delivery->headers, $delivery->body);
        } catch (Throwable $e) {
            $this->fail($delivery, $e->getMessage());

            return;
        }

        if ($status < 200 || $status >= 300) {
            $this->fail($delivery, 'HTTP '.$status);

            return;
        }

        // Success bookkeeping sits outside the transport try/catch, and the
        // delivery is forgotten before the event fires. A throwing
        // WebhookDelivered listener must never be read as a delivery failure:
        // that would retry a webhook the endpoint already accepted, posting
        // the identical signed payload a second time.
        $this->launchSuccessor($this->forget($delivery));

        WebhookDelivered::dispatch($delivery->url, $status, $delivery->appId, $delivery->attempts + 1);
    }

    /**
     * Record a failed attempt: schedule a retry, or drop once exhausted.
     */
    protected function fail(PendingDelivery $delivery, string $reason): void
    {
        $delivery->attempts++;
        $delivery->inFlight = false;

        if ($delivery->attempts >= $this->maxAttempts) {
            $url = $this->sanitizeUrlForLog($delivery->url);

            Log::warning("Webhook dropped after {$delivery->attempts} attempts ({$reason}): {$url}");

            // Forgotten before the event fires, for the same reason as above:
            // a throwing listener must not leave the delivery pending.
            $this->launchSuccessor($this->forget($delivery));

            WebhookDropped::dispatch($delivery->url, $delivery->appId, $delivery->attempts, $reason);

            return;
        }

        // Exponential backoff, capped at a minute.
        $delivery->dueAt = microtime(true) + min(60.0, 2 ** $delivery->attempts);
    }

    /**
     * Strip any basic-auth userinfo from a URL before it reaches the logs.
     *
     * An operator may configure an endpoint as https://user:pass@host/path;
     * logging it verbatim would leak those credentials. This rebuilds the URL
     * with only scheme, host, port, path and query, dropping the userinfo.
     */
    protected function sanitizeUrlForLog(string $url): string
    {
        $parts = parse_url($url);

        if ($parts === false || ! isset($parts['host'])) {
            return $url;
        }

        $scheme = isset($parts['scheme']) ? $parts['scheme'].'://' : '';
        $port = isset($parts['port']) ? ':'.$parts['port'] : '';
        $path = $parts['path'] ?? '';
        $query = isset($parts['query']) ? '?'.$parts['query'] : '';

        return $scheme.$parts['host'].$port.$path.$query;
    }

    /**
     * Remove a delivery from the pending list and unchain it from its split
     * batch. Returns the next chunk, now free to send, if there is one.
     */
    protected function forget(PendingDelivery $delivery): ?PendingDelivery
    {
        $id = spl_object_id($delivery);

        // The identity check guards against a recycled object id: only the
        // exact delivery stored under this id is removed.
        if (($this->pending[$id] ?? null) !== $delivery) {
            return null;
        }

        unset($this->pending[$id]);

        $previous = $this->predecessor[$id] ?? null;
        $next = $this->successor[$id] ?? null;

        unset($this->predecessor[$id], $this->successor[$id]);

        // A middle chunk dropped from the chain: its neighbours are linked
        // directly, so the next chunk still waits for the earlier one.
        if ($previous !== null && $next !== null) {
            $this->successor[spl_object_id($previous)] = $next;
            $this->predecessor[spl_object_id($next)] = $previous;

            return null;
        }

        if ($previous !== null) {
            unset($this->successor[spl_object_id($previous)]);
        }

        if ($next !== null) {
            unset($this->predecessor[spl_object_id($next)]);
        }

        return $next;
    }
}
