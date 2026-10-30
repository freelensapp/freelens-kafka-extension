// Minimal stub of `@freelensapp/extensions` for unit tests.
//
// The published package is a shim that reads `Common`, `Main` and `Renderer` off `globalThis.FreelensExtensionApi`,
// which the Freelens host sets before it loads an extension. A Vitest process has no host, so importing the real
// package throws. `vitest.config.ts` aliases the import to this file instead.
//
// Only the surface the tests reach is stubbed here, and only at runtime: the tests are type-checked against the real
// declaration of the package. A test that needs a behaviour of the host mocks it with `vi.mock`.

// The host's store loads the saved model with `fromStore()` and saves it whenever `toJSON()` changes. The stub keeps
// nothing.
class ExtensionStore<M extends object> {
  constructor(_params: { configName: string; defaults: M }) {}
}

// The host's IPC classes register their channels with the extension; the stub does nothing.
class Ipc {
  constructor(_extension: unknown) {}
}

// The host's logger writes to its log; the stub drops the message. A test that checks a message spies on it.
const logger = { error: (..._args: unknown[]) => {} };

export const Common = { Store: { ExtensionStore }, logger };
export const Main = { Ipc };
export const Renderer = { Ipc };
