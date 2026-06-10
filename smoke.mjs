import { connect } from "./index.js";
import { startDaemon } from "./daemon.mjs";

const clientAddr = "127.0.0.1:7620";
const peerAddr = "127.0.0.1:7720";

const daemon = await startDaemon({ clientAddr, peerAddr });

let ok = false;
try {
  const client = await connect([{ id: 1, clientAddr, peerAddr }], { bootstrap: true });
  await client.createTopic({ topic: "orders" });

  const offset = await client.produce({
    topic: "orders",
    body: Buffer.from("hello from JS"),
    priority: 0,
  });
  console.log("produced -> offset", offset);

  const msgs = await client.poll({ topic: "orders", group: "workers", max: 10, visibilityMs: 1000 });
  console.log("polled", msgs.length, "payload:", msgs[0]?.payload?.toString());

  await client.ack({ topic: "orders", group: "workers", leaseId: msgs[0].leaseId });

  const again = await client.poll({ topic: "orders", group: "workers", max: 10, visibilityMs: 1000 });
  console.log("after ack -> polled", again.length);

  const stats = await client.stats();
  console.log("stats", stats);

  ok =
    msgs.length === 1 &&
    msgs[0].payload.toString() === "hello from JS" &&
    again.length === 0 &&
    stats.isLeader;
} catch (e) {
  console.error("ERROR", e);
} finally {
  await daemon.stop();
}

console.log(ok ? "SMOKE TEST PASSED" : "SMOKE TEST FAILED");
process.exit(ok ? 0 : 1);
