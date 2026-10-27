/**
 * Copyright (c) Freelens Authors. All rights reserved.
 * Licensed under MIT License. See LICENSE in root directory for more information.
 */

// Loads dist/main.js in Node, as Freelens does, with a stand-in for the host's
// globalThis.FreelensExtensionApi, and checks that its default export is the
// extension class. The build does not evaluate the bundle, so a bundled library
// that throws while it is imported (a CommonJS package reading `__dirname` in an
// ES module, a `require()` of a package the installed extension does not have)
// would otherwise show up only when Freelens activates the extension.
// `pnpm build` runs it after the main entry is built.

import * as mobx from "mobx";

class StandInLensExtension {}

// Any member of the host API is a class whose members are the same, enough for
// the bundle to be evaluated: nothing of the extension runs.
const standIn = () =>
  new Proxy(class {}, {
    get: (target, key) => (key in target ? target[key] : standIn()),
    construct: () => standIn(),
    apply: () => standIn(),
  });

globalThis.FreelensExtensionApi = {
  Common: standIn(),
  Main: new Proxy(
    { LensExtension: StandInLensExtension },
    { get: (target, key) => (key in target ? target[key] : standIn()) },
  ),
  Mobx: mobx,
};

const main = await import(new URL("../dist/main.js", import.meta.url).href);

if (typeof main.default !== "function" || !(main.default.prototype instanceof StandInLensExtension)) {
  console.error("dist/main.js: the default export is not a class extending Main.LensExtension");
  process.exit(1);
}

console.log(`dist/main.js loads in Node; its default export is ${main.default.name}`);
