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
    | Endpoints
    |--------------------------------------------------------------------------
    |
    | Each endpoint receives a signed, Pusher-format POST. 'app_id' may be a
    | specific application id or '*' for every app. 'events' filters which of
    | the five event types the endpoint receives.
    |
    */

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
