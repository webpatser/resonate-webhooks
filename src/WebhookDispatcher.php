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
            // endpoint never stalls the loop. Failures are captured by
            // attempt() itself, so the Future is intentionally discarded.
            (void) async(fn () => $this->attempt($delivery));
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
     * Attempt one delivery, retrying with backoff or dropping on exhaustion.
     */
    protected function attempt(PendingDelivery $delivery): void
    {
        try {
            $status = $this->transport->deliver($delivery->url, $delivery->headers, $delivery->body);

            if ($status >= 200 && $status < 300) {
                WebhookDelivered::dispatch($delivery->url, $status, $delivery->appId, $delivery->attempts + 1);

                $this->forget($delivery);

                return;
            }

            $reason = 'HTTP '.$status;
        } catch (Throwable $e) {
            $reason = $e->getMessage();
        }

        $delivery->attempts++;
        $delivery->inFlight = false;

        if ($delivery->attempts >= $this->maxAttempts) {
            Log::warning("Webhook dropped after {$delivery->attempts} attempts ({$reason}): {$delivery->url}");

            WebhookDropped::dispatch($delivery->url, $delivery->appId, $delivery->attempts, $reason);

            $this->forget($delivery);

            return;
        }

        // Exponential backoff, capped at a minute.
        $delivery->dueAt = microtime(true) + min(60.0, 2 ** $delivery->attempts);
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
