import fs from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";

const require = createRequire(import.meta.url);

const ROOT = process.cwd();
const IDENTIFIER_RE = /^[A-Za-z_$][A-Za-z0-9_$]*$/;
const RESERVED = new Set([
  "default",
  "__esModule",
  "break",
  "case",
  "catch",
  "class",
  "const",
  "continue",
  "debugger",
  "delete",
  "do",
  "else",
  "enum",
  "export",
  "extends",
  "false",
  "finally",
  "for",
  "function",
  "if",
  "import",
  "in",
  "instanceof",
  "new",
  "null",
  "return",
  "super",
  "switch",
  "this",
  "throw",
  "true",
  "try",
  "typeof",
  "var",
  "void",
  "while",
  "with",
  "yield",
]);

function packageNameOf(id) {
  const parts = id.split("/");
  return id.startsWith("@") ? parts.slice(0, 2).join("/") : parts[0];
}

function resolveEntry(id) {
  try {
    return require.resolve(id, { paths: [ROOT] });
  } catch {}

  const pnpmDir = path.join(ROOT, "node_modules", ".pnpm");
  let entries = [];
  try {
    entries = fs.readdirSync(pnpmDir);
  } catch {
    return null;
  }

  const prefix = `${packageNameOf(id).replace(/\//g, "+")}@`;
  for (const dir of entries.filter((entry) => entry.startsWith(prefix)).sort()) {
    const base = path.join(pnpmDir, dir, "node_modules", packageNameOf(id));
    try {
      return require.resolve(id, { paths: [path.dirname(base)] });
    } catch {}
  }
  return null;
}

function namesFromSource(source) {
  const found = new Set();
  for (const block of source.matchAll(/__webpack_require__\.d\(\s*\w+\s*,\s*\{([\s\S]*?)\}\s*\)/g)) {
    for (const match of block[1].matchAll(/([A-Za-z_$][A-Za-z0-9_$]*)\s*:/g)) found.add(match[1]);
  }
  for (const match of source.matchAll(/exports\.([A-Za-z_$][A-Za-z0-9_$]*)\s*=/g)) found.add(match[1]);
  for (const match of source.matchAll(
    /Object\.defineProperty\(\s*exports\s*,\s*["']([A-Za-z_$][A-Za-z0-9_$]*)["']/g,
  )) {
    found.add(match[1]);
  }
  return [...found];
}

function resolveExportNames(id) {
  const entry = resolveEntry(id);
  try {
    const real = require(entry || id);
    const keys = Object.keys(real).filter((name) => IDENTIFIER_RE.test(name) && !RESERVED.has(name));
    if (keys.length > 0) return keys;
  } catch {}

  if (entry) {
    try {
      return namesFromSource(fs.readFileSync(entry, "utf8")).filter(
        (name) => IDENTIFIER_RE.test(name) && !RESERVED.has(name),
      );
    } catch {}
  }
  return [];
}

function globalExternals(globals) {
  const prefix = "\0global-external:";
  const codeCache = new Map();
  return {
    name: "global-externals",
    enforce: "pre",
    resolveId(id) {
      if (Object.prototype.hasOwnProperty.call(globals, id)) {
        return { id: prefix + id, moduleSideEffects: false };
      }
      return null;
    },
    load(id) {
      if (!id.startsWith(prefix)) return null;
      const moduleId = id.slice(prefix.length);
      let code = codeCache.get(moduleId);
      if (code == null) {
        const globalName = globals[moduleId];
        const names = resolveExportNames(moduleId);
        code = [
          `const __m = ${globalName};`,
          "export default __m;",
          ...names.map((name) => `export const ${name} = __m.${name};`),
        ].join("\n");
        codeCache.set(moduleId, code);
      }
      return { code, moduleSideEffects: false };
    },
  };
}

export { globalExternals };