import { connect } from "./index.js";
import { startDaemon } from "./daemon.mjs";

const clientAddr = "127.0.0.1:7621";
const peerAddr = "127.0.0.1:7721";

const daemon = await startDaemon({ clientAddr, peerAddr });

let ok = false;
try {
  const client = await connect([{ id: 1, clientAddr, peerAddr }], { bootstrap: true });
  await client.createTopic({ topic: "bulk" });

  const N = 2000;
  const producerId = "pipeline-smoke";

  const t0 = Date.now();
  const results = await client.produceMany(
    Array.from({ length: N }, (_, i) => ({
      topic: "bulk",
      body: Buffer.from(`msg-${i}`),
      producerId,
      seq: i + 1,
    })),
  );
  const dt = (Date.now() - t0) / 1000;
  const failed = results.filter((r) => !r.offset);
  const offsets = new Set(results.map((r) => r.offset));
  console.log(
    `produceMany: ${N} msgs in ${dt.toFixed(2)}s -> ${Math.round(N / dt)} msg/s; failed=${failed.length}`,
  );

  const dup = await client.produce({
    topic: "bulk",
    body: Buffer.from("msg-7-retry"),
    producerId,
    seq: 8,
  });
  const dedupOk = dup === results[7].offset;
  console.log(`idempotent retry: seq=8 -> offset ${dup} (original ${results[7].offset})`);

  const burst = await Promise.all(
    Array.from({ length: 200 }, (_, i) =>
      client.produce({
        topic: "bulk",
        body: Buffer.from(`burst-${i}`),
        producerId,
        seq: N + 1 + i,
      }),
    ),
  );

  let rejected = false;
  try {
    await client.produce({ topic: "bulk", body: Buffer.from("x"), producerId });
  } catch {
    rejected = true;
  }

  let drained = 0;
  for (;;) {
    const msgs = await client.poll({ topic: "bulk", group: "g", max: 1024, visibilityMs: 60_000 });
    if (msgs.length === 0) break;
    drained += msgs.length;
    await Promise.all(msgs.map((m) => client.ack({ topic: "bulk", group: "g", leaseId: m.leaseId })));
  }
  console.log(`drained ${drained} (expected ${N + 200})`);

  ok =
    failed.length === 0 &&
    offsets.size === N &&
    dedupOk &&
    new Set(burst).size === 200 &&
    rejected &&
    drained === N + 200 &&
    N / dt > 1000;
} catch (e) {
  console.error("ERROR", e);
} finally {
  await daemon.stop();
}

console.log(ok ? "PIPELINE SMOKE PASSED" : "PIPELINE SMOKE FAILED");
process.exit(ok ? 0 : 1);
