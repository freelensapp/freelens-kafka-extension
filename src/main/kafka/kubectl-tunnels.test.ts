// The kubectl tunnels on a fake `spawn`: the kubectl, environment and folder the host gives, the line of readiness and
// its port, the bounds of time, every way kubectl is killed, and what a kubectl that ends before its line says.

import { EventEmitter } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import { KubectlTunnels, TUNNEL_BOUNDS, TunnelError, type TunnelHost } from "./kubectl-tunnels";
import type { ChildProcess, SpawnOptions } from "node:child_process";

import type { PodPort } from "./types";

const CLUSTER = "cluster-a";
const POD: PodPort = { namespace: "kafka", pod: "my-cluster-broker-0", port: 9092 };
const KUBECTL = "/synthetic/freelens/binaries/kubectl";
const ENVIRONMENT = { KUBECONFIG: "/synthetic/freelens/kubeconfigs/cluster-a", PATH: "/synthetic/bin" };
const FOLDER = "/synthetic/extensions/kafka";
const SENTINEL = "PRIVATE-SENTINEL-OF-KUBECTL";

class FakeKubectl extends EventEmitter {
  readonly stdout = new EventEmitter();
  readonly stderr = new EventEmitter();
  pid: number | undefined = 4242;
  readonly signals: string[] = [];
  ends: "term" | "kill" | "never" = "term";
  exited = false;

  kill(signal: NodeJS.Signals = "SIGTERM"): boolean {
    this.signals.push(signal);
    if (this.ends === "term" || (this.ends === "kill" && signal === "SIGKILL")) this.exit(null, signal);
    return true;
  }

  exit(code: number | null, signal: string | null = null): void {
    if (this.exited) return;
    this.exited = true;
    this.emit("exit", code, signal);
    this.emit("close", code, signal);
  }

  say(text: string): void {
    this.stdout.emit("data", Buffer.from(text));
  }

  complain(text: string): void {
    this.stderr.emit("data", Buffer.from(text));
  }
}

function fixture(options: { hostRejects?: boolean } = {}) {
  const children: { command: string; args: string[]; options: SpawnOptions; child: FakeKubectl }[] = [];
  const spawn = vi.fn((command: string, args: string[], spawnOptions: SpawnOptions) => {
    const child = new FakeKubectl();
    children.push({ command, args, options: spawnOptions, child });
    return child as unknown as ChildProcess;
  });
  const watchers = new Set<(status: string | undefined) => void>();
  const state: { status: string | undefined } = { status: "connected" };
  const host: TunnelHost = {
    kubectl: vi.fn(async (_clusterId: string, onPhase?: (message: string) => void) => {
      onPhase?.("Downloading kubectl 1.35.0");
      if (options.hostRejects) throw new Error(`The cluster is not connected ${SENTINEL}`);
      return KUBECTL;
    }),
    environment: vi.fn(async () => {
      if (options.hostRejects) throw new Error(`The cluster is not connected ${SENTINEL}`);
      return ENVIRONMENT;
    }),
    watchStatus: vi.fn((_clusterId: string, listener: (status: string | undefined) => void) => {
      watchers.add(listener);
      listener(state.status);
      return () => watchers.delete(listener);
    }),
    cwd: () => FOLDER,
  };
  const exitListeners = new Set<() => void>();
  const onExit = (listener: () => void) => {
    exitListeners.add(listener);
    return () => exitListeners.delete(listener);
  };
  const tunnels = new KubectlTunnels(host, spawn, onExit);
  const setStatus = (status: string | undefined) => {
    state.status = status;
    for (const watcher of [...watchers]) watcher(status);
  };
  const started = async () => {
    await vi.waitFor(() => expect(children.length).toBeGreaterThan(0));
    return children[children.length - 1].child;
  };

  return { children, spawn, host, tunnels, watchers, setStatus, started, exitListeners };
}

afterEach(() => {
  vi.useRealTimers();
});

describe("KubectlTunnels", () => {
  it("runs the host's kubectl with its environment in the folder of the extension, on the loopback", async () => {
    const { tunnels, children, started, host } = fixture();
    const phases: string[] = [];
    const opening = tunnels.open(CLUSTER, POD, new AbortController().signal, (message) => phases.push(message));
    const child = await started();

    child.say("Forwarding from 127.0.0.1:43123 -> 9092\nForwarding from [::1]:43123 -> 9092\n");
    const tunnel = await opening;

    expect(tunnel).toMatchObject({ address: "127.0.0.1", port: 43123 });
    expect(children[0].command).toBe(KUBECTL);
    expect(children[0].args).toEqual([
      "port-forward",
      "--address",
      "127.0.0.1",
      "--namespace",
      "kafka",
      "pod/my-cluster-broker-0",
      ":9092",
    ]);
    expect(children[0].options).toMatchObject({ cwd: FOLDER, env: ENVIRONMENT, windowsHide: true });
    expect(host.kubectl).toHaveBeenCalledWith(CLUSTER, expect.any(Function));
    expect(phases).toEqual(["Downloading kubectl 1.35.0"]);
    expect(tunnels.count).toBe(1);

    await tunnel.close();
    expect(child.signals).toEqual(["SIGTERM"]);
    expect(tunnels.count).toBe(0);
  });

  it("reads a line of readiness cut across chunks only once its port is whole", async () => {
    const { tunnels, started } = fixture();
    const opening = tunnels.open(CLUSTER, POD, new AbortController().signal);
    const child = await started();

    child.say("Forwarding from 127.0.0.1:4");
    child.say("3123 -> 9092\n");
    await expect(opening).resolves.toMatchObject({ port: 43123 });
  });

  it("refuses what is not a broker pod without starting kubectl", async () => {
    const { tunnels, spawn } = fixture();
    const bad = [
      { ...POD, namespace: "Kafka;rm" },
      { ...POD, pod: "--kubeconfig=/etc/passwd" },
      { ...POD, port: 0 },
      { ...POD, port: 70000 },
    ];

    for (const pod of bad) {
      await expect(tunnels.open(CLUSTER, pod, new AbortController().signal)).rejects.toMatchObject({ reason: "ended" });
    }
    expect(spawn).not.toHaveBeenCalled();
  });

  it("says a cluster the host gives nothing for is not connected, with nothing of the host's error", async () => {
    const { tunnels, spawn } = fixture({ hostRejects: true });
    const error = await tunnels.open(CLUSTER, POD, new AbortController().signal).catch((e: unknown) => e);

    expect(error).toBeInstanceOf(TunnelError);
    expect(error).toMatchObject({ reason: "disconnected" });
    expect(String((error as Error).message)).not.toContain(SENTINEL);
    expect(spawn).not.toHaveBeenCalled();
  });

  it("names the refusal of pods/portforward and of the credential, with nothing kubectl wrote", async () => {
    const { tunnels, started } = fixture();
    const forbidden = tunnels.open(CLUSTER, POD, new AbortController().signal);
    const first = await started();

    first.complain(
      `error: pods "my-cluster-broker-0" is forbidden: User "${SENTINEL}" cannot create resource "pods/portforward" in API group ""\n`,
    );
    first.exit(1);
    const refused = await forbidden.then(
      () => undefined as unknown as TunnelError,
      (e: unknown) => e as TunnelError,
    );

    expect(refused.reason).toBe("forbidden");
    expect(refused.message).not.toContain(SENTINEL);

    const { tunnels: other, started: otherStarted } = fixture();
    const unauthorized = other.open(CLUSTER, POD, new AbortController().signal);
    const second = await otherStarted();

    second.complain(`error: You must be logged in to the server (Unauthorized) ${SENTINEL}\n`);
    second.exit(1);
    const credential = await unauthorized.then(
      () => undefined as unknown as TunnelError,
      (e: unknown) => e as TunnelError,
    );

    expect(credential.reason).toBe("credential");
    expect(credential.message).not.toContain(SENTINEL);
  });

  it("says a kubectl that ended for another reason ended before it was ready", async () => {
    const { tunnels, started } = fixture();
    const opening = tunnels.open(CLUSTER, POD, new AbortController().signal);
    const child = await started();

    child.complain(`error: unable to forward port because pod is not running. Current status=Pending ${SENTINEL}\n`);
    child.exit(1);
    const error = await opening.then(
      () => undefined as unknown as TunnelError,
      (e: unknown) => e as TunnelError,
    );

    expect(error.reason).toBe("ended");
    expect(error.message).not.toContain(SENTINEL);
  });

  it("kills a kubectl that is not ready within its bound", async () => {
    const { tunnels, started } = fixture();

    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const settled = tunnels.open(CLUSTER, POD, new AbortController().signal).then(
      () => undefined as unknown as TunnelError,
      (e: unknown) => e as TunnelError,
    );
    const child = await started();

    await vi.advanceTimersByTimeAsync(TUNNEL_BOUNDS.readyMs);

    expect((await settled).reason).toBe("deadline");
    expect(child.signals).toEqual(["SIGTERM"]);
  });

  it("kills for good a kubectl that has not exited within its bound", async () => {
    const { tunnels, started } = fixture();
    const opening = tunnels.open(CLUSTER, POD, new AbortController().signal);
    const child = await started();

    child.ends = "kill";
    child.say("Forwarding from 127.0.0.1:43123 -> 9092\n");
    const tunnel = await opening;

    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const closing = tunnel.close();
    await vi.advanceTimersByTimeAsync(TUNNEL_BOUNDS.exitMs);
    await closing;

    expect(child.signals).toEqual(["SIGTERM", "SIGKILL"]);
    expect(tunnels.count).toBe(0);
  });

  it("kills kubectl when the signal aborts, before and after it is ready", async () => {
    const { tunnels, started, children } = fixture();
    const before = new AbortController();
    const opening = tunnels.open(CLUSTER, POD, before.signal);
    const first = await started();

    before.abort();
    await expect(opening).rejects.toMatchObject({ reason: "cancelled" });
    expect(first.signals).toEqual(["SIGTERM"]);

    const after = new AbortController();
    const reopening = tunnels.open(CLUSTER, POD, after.signal);
    await vi.waitFor(() => expect(children).toHaveLength(2));
    const second = children[1].child;

    second.say("Forwarding from 127.0.0.1:43124 -> 9092\n");
    await reopening;
    after.abort();
    expect(second.signals).toEqual(["SIGTERM"]);
    await vi.waitFor(() => expect(tunnels.count).toBe(0));
  });

  it("kills kubectl when the cluster leaves the connected state or the catalog", async () => {
    for (const status of ["disconnected", undefined]) {
      const { tunnels, started, setStatus, watchers } = fixture();
      const opening = tunnels.open(CLUSTER, POD, new AbortController().signal);
      const child = await started();

      child.say("Forwarding from 127.0.0.1:43123 -> 9092\n");
      await opening;
      setStatus(status);

      expect(child.signals).toEqual(["SIGTERM"]);
      await vi.waitFor(() => expect(tunnels.count).toBe(0));
      expect(watchers.size).toBe(0);
    }
  });

  it("refuses to open for a cluster that is already not connected", async () => {
    const { tunnels, started, setStatus } = fixture();

    setStatus("disconnected");
    const settled = tunnels.open(CLUSTER, POD, new AbortController().signal).catch((e: unknown) => e);
    const child = await started();

    expect(await settled).toMatchObject({ reason: "disconnected" });
    expect(child.signals).toEqual(["SIGTERM"]);
  });

  it("kills every kubectl on closeAll and on process exit, and opens no other after closeAll", async () => {
    const { tunnels, children, exitListeners } = fixture();
    const signal = new AbortController().signal;
    const openings = [tunnels.open(CLUSTER, POD, signal), tunnels.open(CLUSTER, { ...POD, pod: "b-1" }, signal)];

    await vi.waitFor(() => expect(children).toHaveLength(2));
    children.forEach(({ child }, index) => child.say(`Forwarding from 127.0.0.1:4312${index} -> 9092\n`));
    await Promise.all(openings);

    for (const listener of exitListeners) listener();
    expect(children.map(({ child }) => child.signals)).toEqual([["SIGTERM"], ["SIGTERM"]]);

    const third = tunnels.open(CLUSTER, POD, signal);
    await vi.waitFor(() => expect(children).toHaveLength(3));
    children[2].child.say("Forwarding from 127.0.0.1:43129 -> 9092\n");
    await third;

    tunnels.closeAll();
    expect(children[2].child.signals).toEqual(["SIGTERM"]);
    expect(exitListeners.size).toBe(0);
    await expect(tunnels.open(CLUSTER, POD, signal)).rejects.toMatchObject({ reason: "cancelled" });
    expect(children).toHaveLength(3);
  });
});
