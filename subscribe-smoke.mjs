import { connect } from "./index.js";

const node = { id: 1, clientAddr: "127.0.0.1:7610", peerAddr: "127.0.0.1:7710" };

const received = [];
let ok = false;
try {
  const client = await connect([node]);
  await client.createTopic({ topic: "push" });

  const sub = await client.subscribe(
    { topic: "push", group: "workers", prefetch: 8 },
    async (msg) => {
      received.push(msg.payload.toString());
    }
  );

  for (let i = 0; i < 5; i++) {
    await client.produce({ topic: "push", body: Buffer.from("m" + i) });
  }

  await new Promise((r) => setTimeout(r, 1500));
  sub.unsubscribe();
  await new Promise((r) => setTimeout(r, 200));

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

  ok =
    JSON.stringify(received) === JSON.stringify(expected) &&
    leftover.length === 0;
} catch (e) {
  console.error("ERROR", e);
}

console.log(ok ? "SUBSCRIBE SMOKE PASSED" : "SUBSCRIBE SMOKE FAILED");
process.exit(ok ? 0 : 1);
