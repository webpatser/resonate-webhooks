<?php

namespace Webpatser\ResonateWebhooks;

/**
 * One configured webhook endpoint: a URL, the app it serves, and the event
 * types it wants.
 */
final class WebhookEndpoint
{
    /**
     * @param  list<string>  $events  Event names this endpoint receives.
     */
    public function __construct(
        public readonly string $url,
        public readonly string $appId,
        public readonly array $events,
    ) {
        //
    }

    /**
     * Build an endpoint from a `resonate-webhooks.endpoints` entry.
     *
     * @param  array<string, mixed>  $config
     */
    public static function fromConfig(array $config): self
    {
        return new self(
            (string) ($config['url'] ?? ''),
            (string) ($config['app_id'] ?? '*'),
            array_values($config['events'] ?? self::allEvents()),
        );
    }

    /**
     * Every event type the plugin can emit.
     *
     * @return list<string>
     */
    public static function allEvents(): array
    {
        return ['channel_occupied', 'channel_vacated', 'member_added', 'member_removed', 'client_event'];
    }

    /**
     * Determine whether this endpoint should receive an event.
     */
    public function accepts(WebhookEvent $event): bool
    {
        return ($this->appId === '*' || $this->appId === $event->appId)
            && in_array($event->name, $this->events, true);
    }
}
