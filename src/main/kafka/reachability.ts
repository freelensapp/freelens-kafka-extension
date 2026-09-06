import net from "node:net";
import { firstBrokerAddress } from "../../common/reachability";

/**
 * True if a TCP connection to `host:port` completes within `timeoutMs`. Read-only — the socket is
 * opened and immediately destroyed, no bytes are sent — so it is safe against any cluster.
 */
export function probeTcp(host: string, port: number, timeoutMs = 3000): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = new net.Socket();
    let settled = false;
    const finish = (ok: boolean): void => {
      if (settled) return;
      settled = true;
      socket.destroy();
      resolve(ok);
    };
    socket.setTimeout(timeoutMs);
    socket.once("connect", () => finish(true));
    socket.once("timeout", () => finish(false));
    socket.once("error", () => finish(false));
    socket.connect(port, host);
  });
}

/** Whether the machine running Freelens can open a TCP connection to a bootstrap's first broker. */
export async function probeBootstrapReachable(bootstrap: string, timeoutMs?: number): Promise<boolean> {
  const { host, port } = firstBrokerAddress(bootstrap);
  if (!host) return false;
  return probeTcp(host, port, timeoutMs);
}
