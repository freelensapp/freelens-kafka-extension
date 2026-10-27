// The extension's styles apply to its own pages only (SPEC-019, REQ-230): every style rule of its CSS module, once
// Sass has compiled it, sits under one of its own classes, `Kafka*`, which the build makes local. The host's classes
// and the state modifiers appear only as `:global(...)` under them.

import { join } from "node:path";
import { compile } from "sass-embedded";
import { describe, expect, it } from "vitest";

// The style selectors of the compiled CSS, each with whether it sits in a @media block.
function styleRules(css: string): { selector: string; inMedia: boolean }[] {
  const text = css.replace(/\/\*[\s\S]*?\*\//g, "");
  const stack: string[] = [];
  const rules: { selector: string; inMedia: boolean }[] = [];
  let buffer = "";

  for (const character of text) {
    if (character === "{") {
      const prelude = buffer.trim();
      const inKeyframes = stack.some((outer) => /^@(-webkit-)?keyframes/.test(outer));
      const inMedia = stack.some((outer) => outer.startsWith("@media"));

      stack.push(prelude);
      buffer = "";
      if (!prelude.startsWith("@") && !inKeyframes) {
        for (const part of prelude.split(",")) rules.push({ selector: part.trim(), inMedia });
      }
    } else if (character === "}") {
      stack.pop();
      buffer = "";
    } else if (character === ";") {
      buffer = "";
    } else {
      buffer += character;
    }
  }
  return rules;
}

function styleSelectors(css: string): string[] {
  const text = css.replace(/\/\*[\s\S]*?\*\//g, "");
  const stack: string[] = [];
  const selectors: string[] = [];
  let buffer = "";

  for (const character of text) {
    if (character === "{") {
      const prelude = buffer.trim();
      const inKeyframes = stack.some((outer) => /^@(-webkit-)?keyframes/.test(outer));

      stack.push(prelude);
      buffer = "";
      if (!prelude.startsWith("@") && !inKeyframes) selectors.push(...prelude.split(",").map((part) => part.trim()));
    } else if (character === "}") {
      stack.pop();
      buffer = "";
    } else if (character === ";") {
      buffer = "";
    } else {
      buffer += character;
    }
  }
  return selectors;
}

describe("kafka.module.scss", () => {
  const { css } = compile(join(process.cwd(), "src", "renderer", "kafka.module.scss"));
  const selectors = styleSelectors(css);

  it("puts every style rule under a class of the extension", () => {
    expect(selectors.length).toBeGreaterThan(300);
    expect(selectors.filter((selector) => !/\.Kafka[A-Z][A-Za-z0-9]*/.test(selector))).toEqual([]);
  });

  it("styles in a @media block only what it also styles outside one", () => {
    const rules = styleRules(css);
    const classes = (selector: string) =>
      [...selector.replace(/:global\([^)]*\)/g, "").matchAll(/\.(Kafka[A-Za-z0-9]*)/g)].map((match) => match[1]);
    const outside = new Set(rules.filter((rule) => !rule.inMedia).flatMap((rule) => classes(rule.selector)));
    const onlyInMedia = rules
      .filter((rule) => rule.inMedia)
      .flatMap((rule) => classes(rule.selector))
      .filter((name) => !outside.has(name));

    expect([...new Set(onlyInMedia)]).toEqual([]);
  });

  it("names the host's classes only as :global", () => {
    const local = selectors.flatMap((selector) =>
      [...selector.replace(/:global\([^)]*\)/g, "").matchAll(/\.([A-Za-z_][\w-]*)/g)].map((match) => match[1]),
    );

    expect(local.filter((name) => !name.startsWith("Kafka"))).toEqual([]);
  });
});
