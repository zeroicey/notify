# notify

> Internal-only MVP. Do not expose this service to the public internet.

`notify` is a Rust WebSocket notification server for trusted internal and development environments. Clients subscribe to topics, publish text-only messages, and request recent SQLite-backed history.

## Local startup

### Default local run

Start the server:

```bash
cargo run
```

Default runtime values:

- HTTP health endpoint: `http://127.0.0.1:3000/health`
- WebSocket endpoint: `ws://127.0.0.1:3000/ws`
- Default bind: `127.0.0.1:3000`
- Default history limit: `50`
- Maximum history limit: `200`

### Optional local overrides

Use environment variables when you need a different port or database file:

```bash
NOTIFY_BIND_ADDR=127.0.0.1:3100 \
NOTIFY_DATABASE_URL=sqlite:///tmp/notify.sqlite3 \
cargo run
```

## Protocol summary

### Client -> server

| Frame | Required fields | Optional fields | Notes |
| --- | --- | --- | --- |
| `subscribe` | `topic` | - | Subscribe the current socket to a topic. |
| `publish` | `topic`, `text` | - | Persist the message, then fan it out to active subscribers. |
| `history` | `topic` | `since_id`, `limit` | `limit` defaults to `50` and is capped at `200`. |

### Server -> client

| Frame | Fields | Notes |
| --- | --- | --- |
| `subscribed` | `topic` | Confirms that the socket is subscribed to the topic. |
| `message` | `id`, `topic`, `text`, `ts` | Real-time fan-out payload for a committed message. |
| `history` | `topic`, `items`, `oldest_first` | History is returned oldest -> newest. |
| `error` | `code`, `message` | `code` is one of `bad_request`, `queue_overflow`, `storage_failure`, or `invalid_topic`. |

### Validation rules

- Topic names must be 1-64 characters and use only `[A-Za-z0-9_.:-]`.
- Message text must be non-empty after trimming and no more than 2000 characters.
- History queries return messages with `id > since_id` when `since_id` is present.
- Slow subscribers can receive a best-effort `queue_overflow` error before the socket closes.

## Local verification checklist

Use the detailed flow in [`docs/frontend-integration.md`](docs/frontend-integration.md), then confirm:

1. `curl http://127.0.0.1:3000/health` returns `{"status":"ok"}`.
2. Two clients can connect to `ws://127.0.0.1:3000/ws` and subscribe to the same topic.
3. A published text message is persisted before subscribers receive the `message` frame.
4. After a restart, `history` still returns recent topic messages.
5. History responses are ordered oldest -> newest.
6. A `history` request without `limit` uses `50`.
7. A `history` request above `200` is capped at `200`.

## Frontend integration

See [`docs/frontend-integration.md`](docs/frontend-integration.md) for connect, subscribe, publish, history, message, and error examples.

## Internal-only warning

This MVP assumes a trusted internal network or local developer machine. It is not designed for direct public internet exposure.
