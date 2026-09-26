import { readFileSync } from "node:fs";
import { builtinModules } from "node:module";
import { resolve } from "node:path";
import react from "@vitejs/plugin-react";
import sassDts from "vite-plugin-sass-dts";
import { globalExternals } from "./build/global-externals.js";

const runtimeExternals = [
  "electron",
  /^electron\//,
  "bufferutil",
  "utf-8-validate",
  ...builtinModules,
  ...builtinModules.map((module) => `node:${module}`),
];

// Both halves carry the version they were built from, so the renderer can tell when Freelens still
// runs the main side of a previous version after an in-place update (SPEC-016).
const extensionVersion = (JSON.parse(readFileSync(resolve(__dirname, "package.json"), "utf8")) as { version: string })
  .version;
const define = { __KAFKA_EXTENSION_VERSION__: JSON.stringify(extensionVersion) };

const preserveModules = (process.env.VITE_PRESERVE_MODULES ?? "true") === "true";

const mainBuild = {
  lib: {
    entry: resolve(__dirname, "src/main/index.ts"),
    formats: ["cjs" as const],
  },
  rolldownOptions: {
    external: runtimeExternals,
    output: {
      exports: "named" as const,
      preserveModules,
      preserveModulesRoot: "src/main",
      // The release build is one file: the lazy `import()` calls of the AWS SDK credential chain
      // (SSO, STS, process providers) would otherwise become chunks beside index.js.
      inlineDynamicImports: !preserveModules,
    },
  },
  sourcemap: true,
};

const rendererBuild = {
  lib: {
    entry: resolve(__dirname, "src/renderer/index.tsx"),
    formats: ["cjs" as const],
  },
  outDir: "out/renderer",
  rolldownOptions: {
    external: runtimeExternals,
    output: {
      exports: "named" as const,
      preserveModules,
      preserveModulesRoot: "src/renderer",
    },
  },
  sourcemap: true,
};

export default {
  // main process has full access to Node.js APIs
  main: {
    build: mainBuild,
    define,
    oxc: {
      decorator: {
        legacy: true,
        emitDecoratorMetadata: true,
      },
    },
    plugins: [
      globalExternals({
        "@freelensapp/extensions": "global.LensExtensions",
        mobx: "global.Mobx",
      }),
    ],
  },
  // renderer process in Freelens can use Node.js modules then it is configured
  // with settings for preload script
  preload: {
    build: rendererBuild,
    define,
    css: {
      modules: {
        localsConvention: "camelCaseOnly",
      },
    },
    oxc: {
      decorator: {
        legacy: true,
        emitDecoratorMetadata: true,
      },
    },
    plugins: [
      sassDts({
        enabledMode: ["development", "production"],
      }),
      react(),
      globalExternals({
        "@freelensapp/extensions": "global.LensExtensions",
        mobx: "global.Mobx",
        "mobx-react": "global.MobxReact",
        react: "global.React",
        "react-dom": "global.ReactDom",
        "react-router-dom": "global.ReactRouterDom",
        "react/jsx-runtime": "global.ReactJsxRuntime",
      }),
    ],
  },
};
