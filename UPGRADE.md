# Upgrade Guide

## Unreleased changes: rolling deploys with the roster node index

The subscribe, unsubscribe and close hooks now read channel state from the roster's per-channel node index (`{prefix}-nodes:{appId}:{channel}`), with the keyspace `SCAN` as fallback. The reconcile pass is unchanged: its snapshot always discovers roster keys with `SCAN`. The index only lists roster nodes that already run the new `resonate-roster`, and it is used as soon as it is non-empty. During a rolling deploy it can therefore be partial: nodes still on the old roster are missing from it, so the hooks can undercount occupancy and `channel_vacated` / `member_removed` can fire early. Because the reconcile snapshot still sees those nodes, the next reconcile pass then re-claims the occupied flag and emits `channel_occupied` again for a channel vacated early. A member whose flag is missing is re-claimed against a fresh read, which goes through the index; a member who is only on a not yet upgraded roster node is in the snapshot but not in that read, so their healed `member_added` is delayed until the deploy finishes, and every reconcile tick costs one extra roster read for them in the meantime.

Upgrade every roster node, plus one heartbeat (`heartbeat_interval`), before relying on occupancy webhooks.

If your Redis user is restricted by ACLs, the roster plugin now also needs `SADD`, `SREM` and `EVAL` / `EVALSHA` (the node index is maintained with a set and Lua scripts), and the webhooks plugin's Redis user now also needs `SMEMBERS` (it reads the node index).

## From 0.7.x to 1.0

### Requirements

- `webpatser/resonate` `^1.0` (was `^0.7`).
- `webpatser/resonate-roster` `^1.0` (was `^0.7`). Upgrade both packages together.

### The pre-0.3.0 roster fallback is gone

0.3.x through 0.7.x read the roster's pre-0.3.0 unscoped keys while the roster's `legacy_fallback` window was open, and treated an unscoped `{prefix}:occ:{channel}` or `{prefix}:mem:{channel}:{user}` flag as an edge already claimed. 1.0 reads only app-scoped roster keys and only app-scoped flags.

**Coming from a roster or webhooks release older than 0.3.0? Upgrade through 0.7.x first.** Follow the roster's 0.7.x upgrade procedure (deploy, roll every node, run `resonate-roster:migrate-keys`, close the window) before going to 1.0; see the roster's UPGRADE.md. Leftover unscoped flags expire on their own `ttl` and are simply ignored by 1.0.

If you were already on 0.3.0 or later with the roster's fallback window closed, there is nothing to do beyond the requirements above.

### Known limitation: not Redis Cluster safe

The subscribe, unsubscribe and close hooks read the roster's node index when present and fall back to discovering roster keys with `SCAN`; the reconcile pass always uses `SCAN`. On Redis Cluster a `SCAN` only walks the node it is sent to. Point the plugin and the roster at a single Redis server.
