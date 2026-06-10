import { spawn } from "node:child_process";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import fs from "node:fs";

export function resolveDaemon() {
  if (process.env.HERMESMQD_PATH) return process.env.HERMESMQD_PATH;
  const exe = process.platform === "win32" ? "hermesmqd.exe" : "hermesmqd";
  for (const dir of ["debug", "release"]) {
    const candidate = path.resolve("..", "hermesmq", "target", dir, exe);
    if (fs.existsSync(candidate)) return candidate;
  }
  return "hermesmqd";
}

async function waitReachable(addr, timeoutMs) {
  const [host, port] = addr.split(":");
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const ok = await new Promise((resolve) => {
      const sock = net.connect({ host, port: Number(port) });
      sock.once("connect", () => {
        sock.destroy();
        resolve(true);
      });
      sock.once("error", () => resolve(false));
    });
    if (ok) return;
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`daemon not reachable at ${addr} within ${timeoutMs}ms`);
}

export async function startDaemon({ nodeId = 1, clientAddr, peerAddr }) {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), "hermesmq-js-"));
  const daemon = spawn(
    resolveDaemon(),
    [
      "--node-id",
      String(nodeId),
      "--data-dir",
      dataDir,
      "--client-addr",
      clientAddr,
      "--peer-addr",
      peerAddr,
    ],
    { stdio: "ignore" },
  );

  const cleanup = () =>
    fs.rmSync(dataDir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });

  const stop = () =>
    new Promise((resolve) => {
      if (daemon.exitCode !== null) {
        cleanup();
        resolve();
        return;
      }
      daemon.once("exit", () => {
        cleanup();
        resolve();
      });
      daemon.kill();
    });

  try {
    await waitReachable(clientAddr, 10_000);
  } catch (e) {
    await stop();
    throw e;
  }
  return { stop };
}
