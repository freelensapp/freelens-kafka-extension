/**
 * The lab of the itests (SPEC-019, REQ-232): the kubeconfig file `scripts/lab.sh up` writes, `.lab/kubeconfig`, and
 * its context. Nothing else is read: an itest refuses a missing file, another context and a server that is not on
 * this machine, and never falls back to `~/.kube/config` or `$KUBECONFIG`.
 */
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const LAB_CONTEXT = "kind-freelens-kafka-lab";
export const LAB_KUBECONFIG = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../.lab/kubeconfig");
const LOCAL_SERVER = /^https:\/\/(127\.0\.0\.1|localhost|\[::1\]):\d+\/?$/;

export interface LabKube {
  kubeConfigPath: string;
  context: string;
}

/** The lab's kubeconfig and context, checked; throws with what to do when the lab is not there. */
export function labKube(): LabKube {
  let text: string;

  try {
    text = readFileSync(LAB_KUBECONFIG, "utf8");
  } catch {
    throw new Error(`no lab kubeconfig at ${LAB_KUBECONFIG}: run 'pnpm lab:up' first`);
  }
  const servers = [...text.matchAll(/^\s*server:\s*(\S+)\s*$/gm)].map((match) => match[1]);

  if (servers.length === 0 || servers.some((server) => !LOCAL_SERVER.test(server))) {
    throw new Error(`refusing ${LAB_KUBECONFIG}: it names a server that is not local`);
  }
  if (!new RegExp(`^\\s*name:\\s*${LAB_CONTEXT}\\s*$`, "m").test(text)) {
    throw new Error(`refusing ${LAB_KUBECONFIG}: it has no context ${LAB_CONTEXT}`);
  }
  return { kubeConfigPath: LAB_KUBECONFIG, context: LAB_CONTEXT };
}
