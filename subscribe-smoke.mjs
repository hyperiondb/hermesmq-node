import { connect } from "./index.js";
import { startDaemon } from "./daemon.mjs";

const clientAddr = "127.0.0.1:7610";
const peerAddr = "127.0.0.1:7710";

const daemon = await startDaemon({ clientAddr, peerAddr });

const received = [];
let ok = false;
try {
  const client = await connect([{ id: 1, clientAddr, peerAddr }], { bootstrap: true });
  await client.createTopic({ topic: "push" });

  const sub = await client.subscribe(
    { topic: "push", group: "workers", prefetch: 8 },
    (msg) => {
      received.push(msg.payload.toString());
    },
    (err) => {
      console.error("subscribe error:", err);
    },
  );

  for (let i = 0; i < 5; i++) {
    await client.produce({ topic: "push", body: Buffer.from("m" + i) });
  }

  const deadline = Date.now() + 5000;
  while (received.length < 5 && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 50));
  }
  await new Promise((r) => setTimeout(r, 300));
  sub.unsubscribe();

  received.sort();
  const expected = ["m0", "m1", "m2", "m3", "m4"];
  console.log("received", received);

  const leftover = await client.poll({
    topic: "push",
    group: "workers",
    max: 10,
    visibilityMs: 500,
  });
  console.log("leftover after manual-ack subscribe:", leftover.length);

  await client.createTopic({ topic: "retry" });
  let failedOnce = false;
  const processed = [];
  const sub2 = await client.subscribe(
    { topic: "retry", group: "workers", visibilityMs: 1000 },
    async (msg) => {
      if (!failedOnce) {
        failedOnce = true;
        throw new Error("simulated failure");
      }
      processed.push(msg.payload.toString());
    },
  );
  await client.produce({ topic: "retry", body: Buffer.from("fail-once") });
  const retryDeadline = Date.now() + 10_000;
  while (processed.length === 0 && Date.now() < retryDeadline) {
    await new Promise((r) => setTimeout(r, 50));
  }
  await new Promise((r) => setTimeout(r, 300));
  sub2.unsubscribe();
  console.log("redelivered after nack:", processed);

  ok =
    JSON.stringify(received) === JSON.stringify(expected) &&
    leftover.length === 0 &&
    failedOnce &&
    JSON.stringify(processed) === JSON.stringify(["fail-once"]);
} catch (e) {
  console.error("ERROR", e);
} finally {
  await daemon.stop();
}

console.log(ok ? "SUBSCRIBE SMOKE PASSED" : "SUBSCRIBE SMOKE FAILED");
process.exit(ok ? 0 : 1);
