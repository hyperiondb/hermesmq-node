import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import os from "node:os";
import path from "node:path";
import fs from "node:fs";
import { connect } from "./index.js";

const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), "hermesmq-js-"));
const exe = path.resolve("..", "hermesmq", "target", "debug", "hermesmqd.exe");
const clientAddr = "127.0.0.1:7620";
const peerAddr = "127.0.0.1:7720";

const daemon = spawn(
  exe,
  ["--node-id", "1", "--data-dir", dataDir, "--client-addr", clientAddr, "--peer-addr", peerAddr],
  { stdio: "ignore" },
);

let ok = false;
try {
  await sleep(1500);

  const client = await connect([{ id: 1, clientAddr, peerAddr }]);
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

  const again = await client.poll("orders", "workers", 10, 1000);
  console.log("after ack -> polled", again.length);

  const stats = await client.stats();
  console.log("stats", stats);

  ok =
    msgs.length === 1 &&
    msgs[0].payload.toString() === "hello from JS" &&
    again.length === 0;
} catch (e) {
  console.error("ERROR", e);
} finally {
  daemon.kill();
  fs.rmSync(dataDir, { recursive: true, force: true });
}

console.log(ok ? "SMOKE TEST PASSED" : "SMOKE TEST FAILED");
process.exit(ok ? 0 : 1);
