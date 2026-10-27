/**
 * Copyright (c) Freelens Authors. All rights reserved.
 * Licensed under MIT License. See LICENSE in root directory for more information.
 */

// What the kubectl tunnels take of Freelens: the kubectl and the environment the host gives for a cluster, its
// connection status from the catalog, and the folder of the extension (SPEC-019, REQ-224).

import { dirname } from "node:path";
import { Main } from "@freelensapp/extensions";
import { reaction } from "mobx";

import type { TunnelHost } from "./kafka/kubectl-tunnels";

function watchStatus(clusterId: string, listener: (status: string | undefined) => void): () => void {
  return reaction(
    () => {
      const status = Main.Catalog.getClusterById(clusterId)?.status;

      return status === undefined ? undefined : String(status);
    },
    (status) => listener(status),
    { fireImmediately: true },
  );
}

/** The host of the tunnels of the extension whose package is at `manifest`. */
export function createTunnelHost(manifest: () => string): TunnelHost {
  return {
    kubectl: (clusterId, onPhase) => Main.Shell.getKubectlPath(clusterId, { onPhase }),
    environment: (clusterId) => Main.Shell.getEnvironment(clusterId),
    watchStatus,
    cwd: () => dirname(manifest()),
  };
}
