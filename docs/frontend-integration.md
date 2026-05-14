# Frontend integration

> Internal-only MVP. These examples match the current Rust implementation.

## Safety boundary

This server is for trusted internal and development environments only.

- Default bind is `127.0.0.1:3000`.
- WebSocket traffic goes through `ws://127.0.0.1:3000/ws`.
- Do not expose the MVP directly to the public internet.
- Do not assume auth, tenant isolation, or public-facing hardening.

## Connection

### Health check

```text
GET http://127.0.0.1:3000/health
```

Expected response:

```json
{
  "status": "ok"
}
```

### WebSocket endpoint

```text
ws://127.0.0.1:3000/ws
```

## Frame shape

Each frame is a JSON object with a `type` field plus the payload fields for that frame.

## Client -> server examples

### Subscribe

```json
{
  "type": "subscribe",
  "topic": "deployments"
}
```

### Publish

```json
{
  "type": "publish",
  "topic": "deployments",
  "text": "api rollout completed"
}
```

### History

Request the most recent history for a topic:

```json
{
  "type": "history",
  "topic": "deployments"
}
```

Request history after a known cursor:

```json
{
  "type": "history",
  "topic": "deployments",
  "since_id": 41,
  "limit": 20
}
```

History semantics:

- `since_id` is optional.
- When `since_id` is present, the server returns only items with `id > since_id`.
- `limit` defaults to `50`.
- `limit` is capped at `200`.

## Server -> client examples

### Subscribed

```json
{
  "type": "subscribed",
  "topic": "deployments"
}
```

### Message

```json
{
  "type": "message",
  "id": 42,
  "topic": "deployments",
  "text": "api rollout completed",
  "ts": "2026-05-14T06:45:00+00:00"
}
```

Notes:

- `id` is the canonical message cursor.
- The server persists the message before it broadcasts this frame.
- The MVP does not define a separate publish acknowledgment frame; use the delivered `message` frame as the success signal.

### History response

```json
{
  "type": "history",
  "topic": "deployments",
  "items": [
    {
      "id": 40,
      "topic": "deployments",
      "text": "api rollout queued",
      "ts": "2026-05-14T06:40:00+00:00"
    },
    {
      "id": 41,
      "topic": "deployments",
      "text": "api rollout started",
      "ts": "2026-05-14T06:42:00+00:00"
    },
    {
      "id": 42,
      "topic": "deployments",
      "text": "api rollout completed",
      "ts": "2026-05-14T06:45:00+00:00"
    }
  ],
  "oldest_first": true
}
```

### Error responses

Invalid topic example:

```json
{
  "type": "error",
  "code": "invalid_topic",
  "message": "topic must be 1-64 chars using [A-Za-z0-9_.:-]"
}
```

Invalid text example:

```json
{
  "type": "error",
  "code": "bad_request",
  "message": "text payload must be non-empty and at most 2000 characters"
}
```

Other expected error codes:

- `queue_overflow`
- `storage_failure`

## Browser-console flow

Open two browser tabs, then run the following in each tab's devtools console.

### Shared setup

```js
const ws = new WebSocket("ws://127.0.0.1:3000/ws");

ws.addEventListener("open", () => {
  console.log("connected");
});

ws.addEventListener("message", (event) => {
  console.log("server", JSON.parse(event.data));
});

const send = (frame) => ws.send(JSON.stringify(frame));
```

### Tab A: subscribe

```js
send({ type: "subscribe", topic: "deployments" });
```

Expected frame:

```json
{
  "type": "subscribed",
  "topic": "deployments"
}
```

### Tab B: subscribe, then publish

```js
send({ type: "subscribe", topic: "deployments" });
send({ type: "publish", topic: "deployments", text: "api rollout completed" });
```

Expected result:

- Tab A receives a `message` frame for the published event.
- If Tab B is also subscribed, it receives the same `message` frame.

### History after restart

After restarting the server, reconnect and request history:

```js
send({ type: "history", topic: "deployments" });
send({ type: "history", topic: "deployments", since_id: 42, limit: 20 });
```

Expected result:

- The server returns a `history` frame.
- `items` are ordered oldest -> newest.
- The second request returns only messages with `id > 42`.

## Local verification steps

1. Start the server with `cargo run`.
2. Confirm `http://127.0.0.1:3000/health` returns `{"status":"ok"}`.
3. Open two browser tabs to any page and open devtools in both tabs.
4. Run the shared setup snippet in both tabs.
5. Subscribe both tabs to the same topic.
6. Publish from one tab.
7. Confirm the other tab logs a `message` frame that includes `id`, `topic`, `text`, and `ts`.
8. Restart the server.
9. Reconnect and send a `history` frame.
10. Confirm persisted messages are still returned, ordered oldest -> newest.
11. Repeat with no `limit`, then with `limit: 500`, and confirm the runtime uses `50` by default and caps at `200`.
