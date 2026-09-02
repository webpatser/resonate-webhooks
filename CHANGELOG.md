# Changelog

All notable changes to `webpatser/resonate-webhooks` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `OccupancyTracker::state()`: a channel's cluster-wide connection count and distinct users from a single read. A roster key is socket id => presence user id, so the field count and the values answer both questions at once.
- `OccupancyTracker::snapshot()`: every occupied channel of one application in one keyspace sweep, the async counterpart of `RoomRoster::snapshot()`.

### Changed

- Every claim method (`claimOccupied`, `claimVacated`, `claimMemberAdded`, `claimMemberRemoved`, `reconcileOccupancy`) takes an optional pre-read `$state` as a trailing argument. Existing call sites are unaffected; omitting it reads the channel as before.
- `onSubscribe` and the departure path read the channel's occupancy once and claim both of their edges from it, instead of each edge sweeping the keyspace for itself. A presence subscribe cost two to four full sweeps of the shared Redis and now costs one (two while the roster's legacy fallback window is open).
- The reconcile tick takes one snapshot per application instead of one sweep per tracked channel, so its cost no longer grows with how busy the node is. Measured against a node tracking eight channels: 16 `SCAN` commands before, 2 after.
- Require `webpatser/fledge-fiber` `^13.29` (was `^13.4`), and build the plugin's connection with `RedisConfig::fromParameters()`. TLS, unix sockets, ACL usernames, `read_timeout`, retry settings, client name and tcp keepalive now reach the connection; the hand-built URI dropped them. A configured `url` still wins.
- `connection.scheme` (`RESONATE_WEBHOOKS_REDIS_SCHEME`, default `tcp`) selects the transport.

## [0.3.1] - 2026-08-02

### Fixed

- Allow `webpatser/resonate` v0.6 and require `webpatser/resonate-roster` 0.3.1, which carries the same widening. The old constraint excluded the current server release. Verified against v0.6.0.

## [0.3.0] - 2026-08-02

### Changed

- Raise the `webpatser/resonate-roster` constraint to `^0.3.0`, the release carrying the app-scoped key schema.
- Scope occupancy flag keys to the application: `{prefix}:{kind}:{appId}:{channel}`, replacing `{prefix}:{kind}:{channel}`.
- Add the application id as the first argument to every `OccupancyTracker` method (`connectionCount()`, `users()`, `claimOccupied()`, `claimVacated()`, `claimMemberAdded()`, `claimMemberRemoved()`, `reconcileOccupancy()`). It is an internal collaborator; only host code that built one directly is affected.
- Track channels in `WebhookPlugin` as application id => channel name, so two applications serving one channel name reconcile separately.
- Build the roster key schema with `RosterKeys::fromConfig(config('resonate-roster'))` instead of re-typing the prefix literal, so the prefix and the fallback window come from the roster's own config.

### Fixed

- Emit occupancy edges per application. Two applications serving a channel of the same name shared one flag, so the second to fill up never got its `channel_occupied` and the first to empty fired a `channel_vacated` while the channel was still occupied on the other.

### Upgrading

Requires `webpatser/resonate-roster` 0.3+. Deploy both together and follow the roster's upgrade procedure; it owns the key-schema migration.

- Occupancy flag keys are app-scoped, so `channel_occupied`, `channel_vacated`, `member_added`, and `member_removed` are now per application. A backend keying its own state on channel name alone needs the application id as well.
- While the roster's `legacy_fallback` window is open this plugin also reads pre-0.3.0 roster keys per node, so a rolling deploy never reads a busy channel as empty and never fires a spurious `channel_vacated`. A pre-upgrade occupied flag counts as an edge already claimed, so occupied channels are not re-announced; the old flag expires on its own TTL.
- Upgrading from 0.2.1 or earlier also crosses the 0.2.2 change that records a `client_event` only for whispers the server actually relays. A client that never subscribed, a public channel, or an application with client messaging disabled no longer produces one, so expect lower `client_event` volume than before 0.2.2.

## [0.2.3] - 2026-07-30

### Changed

- Widen the `webpatser/resonate` constraint to `^0.4|^0.5`. Composer treats a
  `^0.4` caret on a 0.x package as `>=0.4 <0.5`, so this package could not be
  installed next to a server running Resonate v0.5 even though the suite passes
  against it. Both major lines are now accepted.
- Raise the `webpatser/resonate-roster` constraint to `^0.2.3`, the release that
  carries the matching Resonate v0.5 support.

## [0.2.2] - 2026-07-30

### Security

- `WebhookPlugin::onMessage()` now applies the same checks the Pusher protocol
  applies before it relays a whisper: client messaging must be enabled for the
  application, the channel must be `private-` or `presence-`, and the sender
  must be a member of that channel. Message interceptors run before the
  protocol layer validates the frame, so any socket could previously whisper
  `client-anything` on any channel string, be rejected by the server with
  pusher error 4009, and still have a correctly HMAC-signed `client_event`
  posted to the backend attesting to activity on a channel it never joined.
  A whisper carrying a non-array `data` payload is also no longer recorded,
  since the protocol validator rejects it.

### Fixed

- `WebhookDispatcher` no longer lets a throwing host listener stop the server.
  The delivery attempt runs in a deliberately discarded fiber, and the
  `WebhookDropped` dispatch and backoff bookkeeping sat outside the `try`, so
  a listener that threw rejected an unobserved Future; with no error handler
  installed that surfaces as an `UnhandledFutureError` out of
  `EventLoop::run()` and terminates the whole WebSocket server because one
  webhook endpoint was down. The attempt body is now fully guarded and the
  Future is marked `->ignore()`.
- A successful webhook is no longer retried when a `WebhookDelivered` listener
  throws. The success bookkeeping (forget, then dispatch) has moved out of the
  failure `try`, so a throwing listener can no longer be read as a delivery
  failure and re-post the identical signed payload to an endpoint that already
  accepted it.

## [0.2.1] - 2026-07-02

### Security
- Strip basic-auth credentials from webhook URLs before they are written to logs.

## [0.2.0] - 2026-05-25

### Added

- `Events\WebhookDelivered` and `Events\WebhookDropped`: Laravel events
  dispatched from `WebhookDispatcher::attempt()` on successful delivery and on
  give-up after the configured attempt limit. Carries url, status (delivered),
  reason (dropped), appId, and attempts so a metrics consumer can bucket
  cleanly. Used by `webpatser/resonate-pulse v0.2` for dashboard cards.

### Changed

- `PendingDelivery` now carries the `appId` of the events it carries, so the
  events emitted from `attempt()` can label their metric by application.
  Internal change; no behaviour shift for existing consumers.

## [0.1.0] - 2026-05-22

Initial release.

### Added

- `WebhookPlugin`: a Resonate server plugin that emits Pusher-style HTTP
  webhooks: `channel_occupied`, `channel_vacated`, `member_added`,
  `member_removed`, and `client_event`.
- Cluster-correct edges: occupancy is read from `webpatser/resonate-roster`'s
  shared Redis state, and each edge is claimed once per cluster with an atomic
  flag key, so a scaled deployment does not double-send `occupied` or mis-time
  `vacated`.
- `OccupancyTracker`: turns the roster's self-healing cluster state into
  exactly-once occupancy edges; a reconcile pass recovers edges missed during
  a crash.
- `WebhookDispatcher`: coalesces events into one signed POST per endpoint and
  delivers them off the connection path via the fledge-fiber async HTTP client,
  with exponential-backoff retries.
- `WebhookSigner`: Pusher-compatible `X-Pusher-Signature` HMAC, so existing
  Pusher webhook consumers verify the payload unchanged.
- Per-endpoint configuration: URL, application filter, and event-type filter.
- `WebhooksServiceProvider`: merges config and binds the `WebhookTransport`
  port; publishes config via `vendor:publish --tag=resonate-webhooks-config`.

[0.3.0]: https://github.com/webpatser/resonate-webhooks/compare/v0.2.3...v0.3.0
[0.2.3]: https://github.com/webpatser/resonate-webhooks/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/webpatser/resonate-webhooks/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/webpatser/resonate-webhooks/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/webpatser/resonate-webhooks/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/webpatser/resonate-webhooks/releases/tag/v0.1.0
