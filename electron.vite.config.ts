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

const mainBuild = {
  lib: {
    entry: resolve(__dirname, "src/main/index.ts"),
    formats: ["cjs" as const],
  },
  rolldownOptions: {
    external: runtimeExternals,
    output: {
      exports: "named" as const,
      preserveModules: (process.env.VITE_PRESERVE_MODULES ?? "true") === "true",
      preserveModulesRoot: "src/main",
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
      preserveModules: (process.env.VITE_PRESERVE_MODULES ?? "true") === "true",
      preserveModulesRoot: "src/renderer",
    },
  },
  sourcemap: true,
};

export default {
  // main process has full access to Node.js APIs
  main: {
    build: mainBuild,
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
