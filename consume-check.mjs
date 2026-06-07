import { connect } from "./index.js";

const nodes = [
  { id: 1, clientAddr: "127.0.0.1:7600", peerAddr: "hermesmq1:7700" },
  { id: 2, clientAddr: "127.0.0.1:7601", peerAddr: "hermesmq2:7700" },
  { id: 3, clientAddr: "127.0.0.1:7602", peerAddr: "hermesmq3:7700" },
];

const topic = "orders";
const group = "workers";

let consumed = 0;
try {
  const client = await connect(nodes);
  const offset = await client.produce({
    topic,
    body: Buffer.from("two-node-" + Date.now()),
  });
  const msgs = await client.poll({ topic, group, max: 10, visibilityMs: 2000 });
  for (const m of msgs) {
    await client.ack({ topic, group, leaseId: m.leaseId });
  }
  consumed = msgs.length;
  const stats = await client.stats();
  console.log(
    `produced offset=${offset}  consumed=${consumed}  leader=${stats.currentLeader}  lastApplied=${stats.lastApplied}`,
  );
} catch (e) {
  console.error("ERROR", e.message ?? e);
}

console.log(consumed > 0 ? "CONSUMER HEALTHY (with a node down)" : "CONSUMER UNHEALTHY");
process.exit(consumed > 0 ? 0 : 1);
