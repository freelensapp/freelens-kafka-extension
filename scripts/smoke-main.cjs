/**
 * Fast smoke test for the built Main bundle.
 *
 * Loads `out/main/index.js` with the host globals stubbed, to catch bundling
 * failures (missing / ESM-only / optional-peer dependencies) that make the Main
 * process throw at load — the class of bug that yields a silent
 * "No handler registered" in Freelens. Unit tests cannot see this because they
 * never load the real `@freelensapp/extensions` runtime or the bundled deps.
 *
 * Runs automatically after `pnpm build` (see the `postbuild` script).
 */
const path = require("node:path");

class Stub {}

global.LensExtensions = {
  Common: {
    Store: {
      ExtensionStore: Stub,
    },
  },
  Main: {
    LensExtension: Stub,
    Ipc: Stub,
    Catalog: { getAllClusters: () => [] },
    K8s: {},
    K8sApi: {},
    Navigation: {},
    Power: {},
  },
};
global.Mobx = {};

const entry = path.resolve(__dirname, "..", "out", "main", "index.js");

try {
  const mod = require(entry);
  const extension = mod.default || mod;

  if (typeof extension !== "function") {
    throw new Error(`expected a default-exported extension class, got ${typeof extension}`);
  }

  console.log("smoke:main OK — Main bundle loads and exports an extension class");
} catch (error) {
  console.error("smoke:main FAILED — the Main bundle threw at load:\n", (error && error.stack) || error);
  process.exit(1);
}
