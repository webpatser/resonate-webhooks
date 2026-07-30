<?php

namespace Webpatser\ResonateWebhooks;

use Illuminate\Support\Facades\Log;
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
     * Events recorded since the last drain.
     *
     * @var list<WebhookEvent>
     */
    protected array $buffer = [];

    /**
     * Deliveries built and awaiting (or retrying) delivery.
     *
     * @var list<PendingDelivery>
     */
    protected array $pending = [];

    /**
     * @param  list<WebhookEndpoint>  $endpoints
     */
    public function __construct(
        protected WebhookTransport $transport,
        protected WebhookSigner $signer,
        protected ApplicationProvider $apps,
        protected array $endpoints,
        protected int $maxAttempts = 5,
    ) {
        //
    }

    /**
     * Buffer an event for delivery on the next drain.
     */
    public function record(WebhookEvent $event): void
    {
        $this->buffer[] = $event;
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

        foreach ($this->pending as $delivery) {
            if ($delivery->inFlight || $delivery->dueAt > $now) {
                continue;
            }

            $delivery->inFlight = true;

            // Fire-and-forget: the delivery runs in its own fiber so a slow
            // endpoint never stalls the loop. attempt() swallows every
            // Throwable itself, and ->ignore() marks the Future as
            // deliberately discarded (fledge's own convention, see
            // RedisSubscriber), so a rejection can never reach the event loop
            // as an UnhandledFutureError and stop the server.
            async(fn () => $this->attempt($delivery))->ignore();
        }
    }

    /**
     * Turn buffered events into one delivery per endpoint and application.
     */
    protected function flushBuffer(): void
    {
        if ($this->buffer === []) {
            return;
        }

        $events = $this->buffer;
        $this->buffer = [];

        foreach ($this->endpoints as $endpoint) {
            $byApp = [];

            foreach ($events as $event) {
                if ($endpoint->accepts($event)) {
                    $byApp[$event->appId][] = $event;
                }
            }

            foreach ($byApp as $appId => $appEvents) {
                $delivery = $this->buildDelivery($endpoint, (string) $appId, $appEvents);

                if ($delivery !== null) {
                    $this->pending[] = $delivery;
                }
            }
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
        $this->forget($delivery);

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
            $this->forget($delivery);

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
     * Remove a delivery from the pending list.
     */
    protected function forget(PendingDelivery $delivery): void
    {
        $this->pending = array_values(array_filter(
            $this->pending,
            fn (PendingDelivery $pending) => $pending !== $delivery,
        ));
    }
}
