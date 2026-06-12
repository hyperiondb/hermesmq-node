import { connect } from "./index.js";
import { startDaemon } from "./daemon.mjs";

const clientAddr = "127.0.0.1:7621";
const peerAddr = "127.0.0.1:7721";

const daemon = await startDaemon({ clientAddr, peerAddr });

let ok = false;
try {
  const client = await connect([{ id: 1, clientAddr, peerAddr }], { bootstrap: true });
  await client.createTopic({ topic: "bulk" });

  const SERIAL_N = 100;
  const BULK_N = 2000;
  const BURST_N = 200;
  const producerId = "pipeline-smoke";
  let seq = 0;

  const t0s = Date.now();
  for (let i = 0; i < SERIAL_N; i++) {
    await client.produce({
      topic: "bulk",
      body: Buffer.from(`serial-${i}`),
      producerId,
      seq: ++seq,
    });
  }
  const serialRate = SERIAL_N / ((Date.now() - t0s) / 1000);

  const bulkSeqBase = seq;
  const t0b = Date.now();
  const results = await client.produceMany(
    Array.from({ length: BULK_N }, (_, i) => ({
      topic: "bulk",
      body: Buffer.from(`msg-${i}`),
      producerId,
      seq: bulkSeqBase + i + 1,
    })),
  );
  seq += BULK_N;
  const bulkRate = BULK_N / ((Date.now() - t0b) / 1000);
  const failed = results.filter((r) => !r.offset);
  const offsets = new Set(results.map((r) => r.offset));
  console.log(
    `serial: ${Math.round(serialRate)} msg/s; produceMany: ${Math.round(bulkRate)} msg/s ` +
      `(${(bulkRate / serialRate).toFixed(1)}x); failed=${failed.length}`,
  );

  const dup = await client.produce({
    topic: "bulk",
    body: Buffer.from("msg-7-retry"),
    producerId,
    seq: bulkSeqBase + 8,
  });
  const dedupOk = dup === results[7].offset;
  console.log(`idempotent retry: seq=${bulkSeqBase + 8} -> offset ${dup} (original ${results[7].offset})`);

  const burst = await Promise.all(
    Array.from({ length: BURST_N }, (_, i) =>
      client.produce({
        topic: "bulk",
        body: Buffer.from(`burst-${i}`),
        producerId,
        seq: ++seq,
      }),
    ),
  );

  let rejected = false;
  try {
    await client.produce({ topic: "bulk", body: Buffer.from("x"), producerId });
  } catch {
    rejected = true;
  }

  const expected = SERIAL_N + BULK_N + BURST_N;
  let drained = 0;
  for (;;) {
    const msgs = await client.poll({ topic: "bulk", group: "g", max: 1024, visibilityMs: 60_000 });
    if (msgs.length === 0) break;
    drained += msgs.length;
    await Promise.all(msgs.map((m) => client.ack({ topic: "bulk", group: "g", leaseId: m.leaseId })));
  }
  console.log(`drained ${drained} (expected ${expected})`);

  ok =
    failed.length === 0 &&
    offsets.size === BULK_N &&
    dedupOk &&
    new Set(burst).size === BURST_N &&
    rejected &&
    drained === expected &&
    bulkRate > 3 * serialRate;
  if (!ok && bulkRate <= 3 * serialRate) {
    console.error(
      `pipelining ineffective: bulk ${Math.round(bulkRate)} msg/s vs serial ${Math.round(serialRate)} msg/s`,
    );
  }
} catch (e) {
  console.error("ERROR", e);
} finally {
  await daemon.stop();
}

console.log(ok ? "PIPELINE SMOKE PASSED" : "PIPELINE SMOKE FAILED");
process.exit(ok ? 0 : 1);
