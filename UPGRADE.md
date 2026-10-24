# Upgrade Guide

## From 0.7.x to 1.0

### Requirements

- `webpatser/resonate` `^1.0` (was `^0.7`).
- `webpatser/resonate-roster` `^1.0` (was `^0.7`). Upgrade both packages together.

### The pre-0.3.0 roster fallback is gone

0.3.x through 0.7.x read the roster's pre-0.3.0 unscoped keys while the roster's `legacy_fallback` window was open, and treated an unscoped `{prefix}:occ:{channel}` or `{prefix}:mem:{channel}:{user}` flag as an edge already claimed. 1.0 reads only app-scoped roster keys and only app-scoped flags.

**Coming from a roster or webhooks release older than 0.3.0? Upgrade through 0.7.x first.** Follow the roster's 0.7.x upgrade procedure (deploy, roll every node, run `resonate-roster:migrate-keys`, close the window) before going to 1.0; see the roster's UPGRADE.md. Leftover unscoped flags expire on their own `ttl` and are simply ignored by 1.0.

If you were already on 0.3.0 or later with the roster's fallback window closed, there is nothing to do beyond the requirements above.

### Known limitation: not Redis Cluster safe

Occupancy is read by discovering roster keys with `SCAN`, which on Redis Cluster only walks the node it is sent to. Point the plugin and the roster at a single Redis server.
