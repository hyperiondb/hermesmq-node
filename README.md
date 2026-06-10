# HermesMQ Node Client

Native Node.js client for [hermesmq](https://github.com/hyperiondb/hermesmq), a Raft-replicated message queue. Built with [napi-rs](https://napi.rs/); ships prebuilt binaries for Linux (glibc/musl, x64/arm64), macOS (x64/arm64) and Windows (x64).

## Install

```sh
npm install hermesmq-node
```

Requires Node.js >= 20.

## Quick start

```js
import { connect } from "hermesmq-node";

const client = await connect([
  { id: 1, clientAddr: "127.0.0.1:7600", peerAddr: "127.0.0.1:7700" },
]);

await client.createTopic({ topic: "orders" });

const offset = await client.produce({
  topic: "orders",
  body: Buffer.from("hello"),
});

const msgs = await client.poll({ topic: "orders", group: "workers" });
for (const msg of msgs) {
  console.log(msg.payload.toString());
  await client.ack({ topic: "orders", group: "workers", leaseId: msg.leaseId });
}
```

## Connecting

`connect(nodes, options?)` takes the full cluster membership. Each node needs its Raft `id`, the `clientAddr` the daemon serves clients on, and the `peerAddr` it uses for Raft replication. The client routes requests to the current leader, follows `not_leader` redirects, retries with exponential backoff, and reuses pooled TCP connections.

```js
const client = await connect(
  [
    { id: 1, clientAddr: "10.0.0.1:7600", peerAddr: "10.0.0.1:7700" },
    { id: 2, clientAddr: "10.0.0.2:7600", peerAddr: "10.0.0.2:7700" },
    { id: 3, clientAddr: "10.0.0.3:7600", peerAddr: "10.0.0.3:7700" },
  ],
  { bootstrap: true },
);
```

`connect` verifies the cluster is reachable and fails fast otherwise. Pass `{ bootstrap: true }` to initialize a fresh cluster with the given membership — this is an admin operation, idempotent on an already-formed cluster, and off by default. `client.bootstrap()` does the same explicitly.

## Topics

```js
await client.createTopic({
  topic: "orders",
  rateLimit: { ratePerSec: 1000, burst: 100 },
  retention: { maxMessages: 1_000_000, maxAgeMs: 86_400_000 },
});
```

`rateLimit` and `retention` are optional.

## Producing

```js
const offset = await client.produce({
  topic: "orders",
  body: Buffer.from(JSON.stringify({ id: 42 })),
  priority: 0,
});
```

`priority` ranges 0–7 (higher delivers first). Payloads are capped at 1 MiB by the server. Delivery is at-least-once: if a response is lost mid-retry, a message can be produced twice.

## Consuming

### Pull

```js
const msgs = await client.poll({
  topic: "orders",
  group: "workers",
  max: 16,
  visibilityMs: 30_000,
  waitMs: 5_000,
});
```

`waitMs` long-polls until a message arrives or the timeout elapses. Each message holds a lease for `visibilityMs`; `ack` it when done or `nack` it to redeliver immediately. Unacked messages redeliver after the visibility timeout.

```js
await client.ack({ topic, group, leaseId: msg.leaseId });
await client.nack({ topic, group, leaseId: msg.leaseId });
```

### Push

```js
const sub = await client.subscribe(
  { topic: "orders", group: "workers", prefetch: 16, ackMode: "manual" },
  async (msg) => {
    await handle(msg.payload);
  },
  (err) => {
    console.error("subscription error:", err);
  },
);

sub.unsubscribe();
```

In `manual` mode (the default) the message is acked when the handler returns (or its returned promise resolves) and nacked if it throws. In `auto` mode the server acks on delivery and the handler outcome is ignored. The subscription reconnects across leader changes and node failures with exponential backoff; the optional `onError` callback reports each failed attempt.

## Stats

```js
const stats = await client.stats();
// { lastApplied, currentLeader, currentTerm, lastLogIndex, isLeader, topics, messages, inFlight }
```

## Development

Build the addon and run the smoke tests (spawns a local `hermesmqd`, resolved from `HERMESMQD_PATH`, `../hermesmq/target/{debug,release}`, or `PATH`):

```sh
npm install
npm run build:debug
npm test
```
