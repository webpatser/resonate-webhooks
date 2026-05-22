<?php

namespace Webpatser\ResonateWebhooks;

/**
 * One Pusher-style webhook event: an occupancy or activity edge on a channel.
 *
 * The {@see $appId} routes and signs the event; it is not part of the wire
 * payload. {@see toArray()} produces the event object exactly as Pusher
 * delivers it, so existing Pusher webhook consumers parse it unchanged.
 */
final class WebhookEvent
{
    /**
     * @param  array<string, string>  $payload  Extra wire fields (user_id, event, data, socket_id).
     */
    public function __construct(
        public readonly string $appId,
        public readonly string $name,
        public readonly string $channel,
        public readonly array $payload = [],
    ) {
        //
    }

    /**
     * The first connection joined a channel.
     */
    public static function channelOccupied(string $appId, string $channel): self
    {
        return new self($appId, 'channel_occupied', $channel);
    }

    /**
     * The last connection left a channel.
     */
    public static function channelVacated(string $appId, string $channel): self
    {
        return new self($appId, 'channel_vacated', $channel);
    }

    /**
     * A distinct presence user joined a channel.
     */
    public static function memberAdded(string $appId, string $channel, string $userId): self
    {
        return new self($appId, 'member_added', $channel, ['user_id' => $userId]);
    }

    /**
     * A distinct presence user left a channel.
     */
    public static function memberRemoved(string $appId, string $channel, string $userId): self
    {
        return new self($appId, 'member_removed', $channel, ['user_id' => $userId]);
    }

    /**
     * A client whispered a `client-*` event on a channel.
     */
    public static function clientEvent(
        string $appId,
        string $channel,
        string $event,
        ?string $data,
        string $socketId,
        ?string $userId,
    ): self {
        $payload = ['event' => $event, 'socket_id' => $socketId];

        if ($data !== null) {
            $payload['data'] = $data;
        }

        if ($userId !== null && $userId !== '') {
            $payload['user_id'] = $userId;
        }

        return new self($appId, 'client_event', $channel, $payload);
    }

    /**
     * The Pusher webhook event object.
     *
     * @return array<string, string>
     */
    public function toArray(): array
    {
        return ['name' => $this->name, 'channel' => $this->channel, ...$this->payload];
    }
}
