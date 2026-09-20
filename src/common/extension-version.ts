declare const __KAFKA_EXTENSION_VERSION__: string | undefined;

/**
 * Version of the package this bundle was built from, baked in at build time (electron.vite.config.ts).
 * The manifest cannot be trusted for this: after an in-place update Freelens keeps the main module
 * of the previous version loaded until the app restarts, but hands it the manifest of the new one.
 */
export const EXTENSION_VERSION: string =
  typeof __KAFKA_EXTENSION_VERSION__ === "string" ? __KAFKA_EXTENSION_VERSION__ : "0.0.0-dev";
