import { connect } from "./index.js";

const nodes = [
  { id: 1, clientAddr: "127.0.0.1:7600", peerAddr: "hermesmq1:7700" },
  { id: 2, clientAddr: "127.0.0.1:7601", peerAddr: "hermesmq2:7700" },
  { id: 3, clientAddr: "127.0.0.1:7602", peerAddr: "hermesmq3:7700" },
];

let ok = false;
try {
  const client = await connect(nodes);
  await client.createTopic({ topic: "orders" });

  const offset = await client.produce({
    topic: "orders",
    body: Buffer.from("hello cluster"),
    priority: 0,
  });
  console.log("produced -> offset", offset);

  const msgs = await client.poll({ topic: "orders", group: "workers", max: 10, visibilityMs: 1000 });
  console.log("polled", msgs.length, "payload:", msgs[0]?.payload?.toString());

  await client.ack({ topic: "orders", group: "workers", leaseId: msgs[0].leaseId });

  const stats = await client.stats();
  console.log("stats", stats);

  ok =
    msgs.length === 1 &&
    msgs[0].payload.toString() === "hello cluster" &&
    stats.currentLeader >= 1;
} catch (e) {
  console.error("ERROR", e);
}

console.log(ok ? "COMPOSE SMOKE PASSED" : "COMPOSE SMOKE FAILED");
process.exit(ok ? 0 : 1);
