# Changelog

All notable changes to `webpatser/resonate-webhooks` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/webpatser/resonate-webhooks/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/webpatser/resonate-webhooks/releases/tag/v0.1.0
