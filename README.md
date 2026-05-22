# Resonate Webhooks

Pusher-style HTTP webhooks for [Resonate](https://github.com/webpatser/resonate). It POSTs `channel_occupied`, `channel_vacated`, `member_added`, `member_removed`, and `client_event` to your backend, so your application reacts to channel activity without polling the socket server.

## The problem it solves

Reverb fires Pusher webhooks; Resonate fires none. A backend that needs to know "this room just emptied", "a user joined", or "someone whispered a client event" has no push signal, so it polls or stands up its own metrics round-trip.

Two things make naive webhooks wrong on a scaled cluster:

1. **Occupancy is per node.** Resonate's `ChannelCreated`/`ChannelRemoved` events fire on each node independently. Firing `channel_occupied` from them double-sends it when a channel has subscribers on two nodes, and fires `channel_vacated` from one node while another still holds subscribers.
2. **Presence is per node.** The same user can be present on two nodes; a node firing `member_removed` for its own last connection is wrong if the user is still on another node.

This package fixes both by reading occupancy from [webpatser/resonate-roster](https://github.com/webpatser/resonate-roster), which keeps a cluster-wide, self-healing membership count, and claiming each edge exactly once per cluster.

## How it works

### Edges, not state

The roster answers "how many connections does channel C have" and "which users are in C", cluster-wide. A webhook is an *edge*: the transition into or out of those states. The plugin derives edges like this:

- On a subscribe, it reads the cluster connection count from the roster. The transition into the first connection is claimed with an atomic Redis flag (`SET NX`); the one node that wins the flag emits `channel_occupied`.
- On an unsubscribe or close that leaves the cluster count at zero, one node wins the flag delete and emits `channel_vacated`.
- `member_added` and `member_removed` use the same claim, keyed per user, driven by whether the user still appears in the roster.
- A reconcile tick re-checks every tracked channel against the roster, so an edge missed during a crash is recovered and flag TTLs are refreshed.

Because the flag is the arbiter, each edge fires exactly once per cluster even when several nodes see the transition at the same instant.

### Delivery, off the connection path

```
connection fiber                    background tick (every flush_interval)
────────────────                    ─────────────────────────────────────
onSubscribe / onUnsubscribe          coalesce buffered events per endpoint
onClose / onMessage                  build one signed Pusher-format POST
  -> derive edge from the roster     deliver via fledge async HTTP client
  -> buffer a WebhookEvent           retry with exponential backoff
```

Connection hooks only buffer events; they never make HTTP calls. A delivery tick coalesces the buffer into one POST per endpoint and sends each in a detached fiber, so a slow endpoint never stalls the event loop. A failed delivery is retried with exponential backoff and dropped after `max_attempts`. Delivery is in-process: undelivered webhooks are lost if the server process crashes.

### Plugin ordering

`RedisRosterPlugin` **must** be listed before `WebhookPlugin` in `config/reverb.php`. Resonate runs plugin hooks sequentially in array order within one fiber, so the roster's Redis writes land before this plugin reads the cluster count.

## Installation

```bash
composer require webpatser/resonate-webhooks
```

This pulls in `webpatser/resonate-roster`. Publish both configs if you want to change defaults:

```bash
php artisan vendor:publish --tag=resonate-roster-config
php artisan vendor:publish --tag=resonate-webhooks-config
```

## Configuration

### 1. Let the roster track all channels

Webhooks need occupancy for every channel type, not just presence channels. Set the roster's `track` mode to `all`:

```dotenv
RESONATE_ROSTER_TRACK=all
```

### 2. Register both plugins, roster first

```php
// config/reverb.php
'servers' => [
    'reverb' => [
        // ...
        'plugins' => [
            \Webpatser\ResonateRoster\RedisRosterPlugin::class,   // first
            \Webpatser\ResonateWebhooks\WebhookPlugin::class,      // second
        ],
    ],
],
```

### 3. Declare your endpoints

```php
// config/resonate-webhooks.php
'endpoints' => [
    [
        'url'    => env('RESONATE_WEBHOOK_URL'),
        'app_id' => '*', // or a specific application id
        'events' => [
            'channel_occupied', 'channel_vacated',
            'member_added', 'member_removed', 'client_event',
        ],
    ],
],
```

Restart Resonate (`php artisan resonate:start`, or `resonate:reload` for a zero-downtime swap).

## The webhook request

Each delivery is a Pusher-format POST, signed so the receiver can verify it:

```
POST {endpoint.url}
Content-Type: application/json
X-Pusher-Key: {app key}
X-Pusher-Signature: hmac_sha256(rawBody, app secret)

{"time_ms": 1700000000000, "events": [
  {"name": "channel_occupied", "channel": "presence-chat.42"},
  {"name": "member_added",     "channel": "presence-chat.42", "user_id": "7"},
  {"name": "client_event",     "channel": "presence-chat.42", "event": "client-typing",
   "data": "{...}", "socket_id": "...", "user_id": "7"}
]}
```

The key and secret come from the matching app in `reverb.apps`, so a Pusher webhook consumer verifies it with no changes. Verify the signature before trusting the body:

```php
$expected = hash_hmac('sha256', $request->getContent(), config('reverb.apps.apps.0.secret'));

abort_unless(hash_equals($expected, $request->header('X-Pusher-Signature')), 403);
```

## Configuration reference

| Key | Default | Purpose |
|-----|---------|---------|
| `connection` | `REDIS_*` env | Redis server. Must be the same server and database as resonate-roster. |
| `key_prefix` | `wh` | Namespace for the edge-detection flag keys. |
| `ttl` | `90` | Flag-key TTL in seconds, so a dead node's flag self-heals. |
| `flush_interval` | `1.0` | Seconds between delivery ticks; events in that window are coalesced. |
| `reconcile_interval` | `30.0` | Seconds between occupancy reconcile ticks. |
| `max_attempts` | `5` | Delivery attempts before a webhook is dropped. |
| `endpoints` | `[]` | The endpoints that receive webhooks. |

## Notes and caveats

- **Same Redis as the roster.** The plugin reads roster keys and writes its flag keys on one connection; it must point at the same server and database as resonate-roster.
- **Roster `track` must be `all`.** In the default `presence` mode the roster does not mirror public or private channels, so `channel_occupied`/`channel_vacated` would not fire for them.
- **In-process delivery.** Retries are bounded; a webhook is lost if the server crashes before delivery. The roster remains queryable as the source of truth.
- **Exactly-once per cluster.** Edges are claimed atomically, so a scaled deployment does not double-send.

## Requirements

- PHP 8.5+
- Resonate 0.4+
- `webpatser/resonate-roster` 0.2+, configured with `track => all`
- A Redis server reachable from the Resonate process

## Testing

```bash
composer test
```

Tests that touch Redis expect a server on `127.0.0.1:6379` and use database 15; they skip cleanly when no Redis is reachable.

## License

MIT. See [LICENSE](LICENSE).
