# Changelog

All notable changes to `webpatser/resonate-webhooks` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Occupancy is now tracked per application. Roster keys carry the application
  id from `webpatser/resonate-roster` 0.3.0 on, and the edge flags follow:
  `{prefix}:{kind}:{appId}:{channel}` instead of `{prefix}:{kind}:{channel}`.
  Two applications configured in one Resonate process that both serve a channel
  of the same name shared one occupancy state, so the second application to
  fill up found the flag already claimed and never got its `channel_occupied`,
  while the first one emptying fired a `channel_vacated` for a channel that was
  still occupied on the other application.
- `WebhookPlugin` no longer re-types the roster's default key prefix as a
  literal (`config('resonate-roster.key_prefix', 'roster')`). It builds the
  schema with `RosterKeys::fromConfig(config('resonate-roster'))`, so the
  prefix and the legacy fallback window come from the roster's own config and
  cannot drift from its defaults.

### Changed

- `OccupancyTracker`'s methods take the application id as their first argument:
  `connectionCount($appId, $channel)`, `users()`, `claimOccupied()`,
  `claimVacated()`, `claimMemberAdded($appId, $channel, $userId)`,
  `claimMemberRemoved()`, and `reconcileOccupancy()`. It is an internal
  collaborator of the plugin; host code that built one directly needs updating.
- `WebhookPlugin` tracks channels as application id => channel name, so two
  applications serving the same channel name are reconciled separately.
- The `webpatser/resonate-roster` constraint is now `^0.3.0`, the release that
  carries the app-scoped key schema. Upgrade both together.

### Upgrading

Deploy this alongside `webpatser/resonate-roster` 0.3.x and follow that
package's upgrade procedure; it owns the key-schema migration. While the
roster's `legacy_fallback` window is open this plugin reads pre-0.3.0 roster
keys per node as well, so a rolling deploy never reads a busy channel as empty
and never fires a spurious `channel_vacated`. A pre-upgrade occupied flag is
treated as an edge already claimed, so channels that were occupied before the
upgrade are not re-announced as `channel_occupied`; the old flag then expires
on its own TTL.

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

[Unreleased]: https://github.com/webpatser/resonate-webhooks/compare/v0.2.3...HEAD
[0.2.1]: https://github.com/webpatser/resonate-webhooks/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/webpatser/resonate-webhooks/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/webpatser/resonate-webhooks/releases/tag/v0.1.0
