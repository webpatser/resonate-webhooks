<?php

return [

    /*
    |--------------------------------------------------------------------------
    | Redis connection
    |--------------------------------------------------------------------------
    |
    | The plugin reads cluster-wide occupancy from the roster's Redis keys and
    | stores its own edge-detection flags alongside them. This connection must
    | therefore point at the same Redis server and database as
    | webpatser/resonate-roster.
    |
    */

    'connection' => [
        'url' => env('RESONATE_WEBHOOKS_REDIS_URL', env('REDIS_URL')),
        'scheme' => env('RESONATE_WEBHOOKS_REDIS_SCHEME', env('REDIS_SCHEME', 'tcp')),
        'host' => env('RESONATE_WEBHOOKS_REDIS_HOST', env('REDIS_HOST', '127.0.0.1')),
        'port' => env('RESONATE_WEBHOOKS_REDIS_PORT', env('REDIS_PORT', '6379')),
        'username' => env('RESONATE_WEBHOOKS_REDIS_USERNAME', env('REDIS_USERNAME')),
        'password' => env('RESONATE_WEBHOOKS_REDIS_PASSWORD', env('REDIS_PASSWORD')),
        'database' => env('RESONATE_WEBHOOKS_REDIS_DB', env('REDIS_DB', '0')),
        'timeout' => env('RESONATE_WEBHOOKS_REDIS_TIMEOUT', 5),
    ],

    /*
    |--------------------------------------------------------------------------
    | Edge-flag key prefix and TTL
    |--------------------------------------------------------------------------
    |
    | The plugin claims each occupied/vacated/member edge exactly once per
    | cluster with a small Redis flag key under this prefix. The TTL lets a
    | flag self-heal if the node that owned it dies; keep it above the roster
    | heartbeat interval.
    |
    */

    'key_prefix' => env('RESONATE_WEBHOOKS_PREFIX', 'wh'),

    'ttl' => (int) env('RESONATE_WEBHOOKS_TTL', 90),

    /*
    |--------------------------------------------------------------------------
    | Delivery
    |--------------------------------------------------------------------------
    |
    | flush_interval: seconds between delivery ticks. Events accrued in that
    |   window are coalesced into one POST per endpoint.
    | reconcile_interval: seconds between occupancy reconcile ticks, which
    |   recover any edge missed during a crash.
    | max_attempts: delivery attempts before a webhook is dropped.
    |
    */

    'flush_interval' => (float) env('RESONATE_WEBHOOKS_FLUSH', 1.0),

    'reconcile_interval' => (float) env('RESONATE_WEBHOOKS_RECONCILE', 30.0),

    'max_attempts' => (int) env('RESONATE_WEBHOOKS_MAX_ATTEMPTS', 5),

    /*
    |--------------------------------------------------------------------------
    | Limits
    |--------------------------------------------------------------------------
    |
    | Bounds that keep a slow or unreachable endpoint from growing memory
    | without limit. Set any of them to 0 to make it unbounded.
    |
    | max_buffered_events: events held between drain ticks. When full, a
    |   buffered occupied/vacated or added/removed pair is cancelled first
    |   (nothing lost), then the oldest client_event is dropped, then the
    |   oldest event. That last step loses an occupancy or presence edge; the
    |   warning logged on the next drain says so, and receivers need a resync.
    | max_pending_deliveries: built deliveries awaiting or retrying. When full,
    |   the oldest delivery not in flight is dropped and WebhookDropped fires
    |   with reason "pending limit reached". A delivery still waiting for an
    |   in-flight slot or for its previous chunk counts as not in flight, so
    |   it can be dropped with 0 attempts.
    | max_request_bytes: a larger batch for one endpoint and app is split over
    |   several signed POSTs, sent strictly one after the other.
    | max_in_flight_per_endpoint: concurrent POSTs to one endpoint URL; the
    |   rest wait for a later drain tick.
    |
    */

    'max_buffered_events' => (int) env('RESONATE_WEBHOOKS_MAX_BUFFERED_EVENTS', 10000),

    'max_pending_deliveries' => (int) env('RESONATE_WEBHOOKS_MAX_PENDING', 1000),

    'max_request_bytes' => (int) env('RESONATE_WEBHOOKS_MAX_REQUEST_BYTES', 1048576),

    'max_in_flight_per_endpoint' => (int) env('RESONATE_WEBHOOKS_MAX_IN_FLIGHT', 16),

    /*
    |--------------------------------------------------------------------------
    | Endpoints
    |--------------------------------------------------------------------------
    |
    | Each endpoint receives a signed, Pusher-format POST. 'app_id' may be a
    | specific application id or '*' for every app. 'events' filters which of
    | the five event types the endpoint receives.
    |
    */

    /*
    |--------------------------------------------------------------------------
    | Ignored channel prefixes
    |--------------------------------------------------------------------------
    |
    | Channels whose name starts with one of these are never reported on: no
    | occupancy edges, no reconcile work, nothing delivered to an endpoint.
    |
    | The Pusher protocol reserves "#" for channels the server owns rather than
    | the application. webpatser/resonate-users subscribes a signed-in
    | connection to "#server-to-user-{id}" so a message can be addressed to a
    | person rather than a room, and that channel is a user's session: without
    | this, every tab opened would post a `channel_occupied` and every tab
    | closed a `channel_vacated` to backends expecting real channels.
    |
    | Set it to an empty array to report on everything.
    |
    */

    'ignore_channel_prefixes' => ['#'],

    'endpoints' => [
        // [
        //     'url' => env('RESONATE_WEBHOOK_URL'),
        //     'app_id' => '*',
        //     'events' => [
        //         'channel_occupied', 'channel_vacated',
        //         'member_added', 'member_removed', 'client_event',
        //     ],
        // ],
    ],

];
