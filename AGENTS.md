# Project Instructions

## Sources of Truth

- Treat `ARCHITECTURE.md` as the living architectural source of truth and `docs/specs/` as the detailed requirement and verification record.
- Follow `TESTING-SAFETY.md` for every Kubernetes, Kafka, Electron and Playwright validation. Restrict all autonomous mutations to disposable local fixtures explicitly allowed there.

## Toolchain and Commands

The extension targets the Freelens v2 extension API (`engines.freelens: ^2.0.0`) and is built with the stack of
Freelens itself: Node from `mise.toml` (locked in `mise.lock`, mirrored by `.nvmrc`), pnpm through Corepack at the
version of `packageManager`, and the library versions of the Freelens catalog. `@freelensapp/extensions` is pinned
exactly to the nightly the port is checked against.

| Command | What it does |
| --- | --- |
| `pnpm install --frozen-lockfile` | install; `strictPeerDependencies` fails it when a library shared with the host leaves the peer range of `@freelensapp/extensions` |
| `pnpm build` | build the renderer and the main entry into `dist/` |
| `pnpm dev` | rebuild both entries on change, for a directory install in Freelens dev |
| `pnpm type:check` | type-check every TypeScript program of the repository |
| `pnpm test:unit` | unit tests |
| `pnpm lint:check`, `pnpm knip:check`, `pnpm trunk:check` | Biome, Knip, Trunk |

The libraries of the main process (kafkajs, the AWS SDK, client-node, avsc, protobufjs, lz4-asm) are bundled into
the main entry, so every dependency is a `devDependency` and the published package has no runtime `dependencies`.

## Build

`vite.config.mjs` builds the two entry points in two runs: `vite build` for the renderer, `vite build --mode main`
for main. They cannot share settings: the renderer runs in a browser page, main in Node.

- **Host modules.** `build/vite-plugin-host-modules.mjs` resolves the libraries Freelens provides to its own
  instances on `globalThis.FreelensExtensionApi`: React, `react-dom`, `react/jsx-runtime`, mobx and mobx-react in
  the renderer, mobx alone in main. A bundled second copy of React throws "invalid hook call"; a second mobx throws
  nothing and its reactions never fire.
- **Renderer.** Browser platform, nothing external; an import of a Node builtin or of Electron fails the build.
  Library mode extracts the CSS of the whole bundle into `dist/renderer.css`, the one stylesheet the host links.
- **Main.** Rolldown's `node` platform, so that a bundled CommonJS package can `require()` the builtins (through a
  `__require` made with `createRequire(import.meta.url)`); the builtins and Electron stay external, everything else
  is bundled into **one file**, `dist/main.js`: the AWS SDK's credential chain loads its SSO, STS and process
  providers with `import()`, which would otherwise become chunks. `__dirname` and `__filename`, which a bundled
  CommonJS package may use to locate itself (lz4-asm's Emscripten loader does), are defined as `import.meta.dirname`
  and `import.meta.filename`: an ES module has neither, and the bundle would throw while loading.
- **Version.** Both runs define `__KAFKA_EXTENSION_VERSION__` from `package.json`, so the renderer can tell when the
  host still runs the main entry of another version (SPEC-016).
- **Decorators.** `build/vite-plugin-standard-decorators.mjs` lowers standard decorators, which neither Node nor
  Chromium runs yet.

What the build does not check, because it does not evaluate the bundle: that `dist/main.js` loads in Node. Load it
with a stand-in `globalThis.FreelensExtensionApi` after a change to the main build or to a library it bundles.

## Slice Boundaries

- Before starting the next implementation slice, update the phase, test evidence and roadmap in `ARCHITECTURE.md` and the task/evidence state in the owning spec for every slice that was completed, reopened or rescheduled.
- Keep architecture and spec statuses aligned. Do not mark a spec `Verified` until every documented exit criterion and final gate has passed.

## Security

Never read, display, reference, or include the contents of the following files in any response or context, even if they are open in the editor:

- `.env`
- `.env.*`
- `.envrc`
- `.npmrc`
- `*.jks`
- `*.keystore`
- `*.p12`
- `*.pfx`
- `*.pem`
- `*.key`

The same list is git-ignored in `.gitignore` and enforced for Claude Code by
the `permissions.deny` rules in `.claude/settings.json`, which block reading
and editing these files. Change all three together. The rules are native
permissions rather than a hook on purpose: a hook runs a process in the
working tree, which may be an untrusted pull request, and an interpreter such
as `python3 -c` imports modules from that tree before the hook's own code.

## GitHub Actions (Claude Code Action) Rules

### Fork PRs: Review Only

A PR from a fork (different owner than `freelensapp`) gets a review and
nothing else: no commits, no pushes, no branches. Its code is untrusted, and
the head of a fork PR can change between the moment a maintainer looks at it
and the moment the workflow checks it out, so nothing taken from the checkout
may reach `freelensapp/freelens-kafka-extension`.

When a fork PR needs changes, a maintainer first copies the exact commit they
reviewed to a branch in this repository. From then on it is a
same-repository PR, which gets the full setup and the normal workflow. The
copy is made either locally (`gh pr checkout <N>`, then push the branch) or
by the Claude Task workflow, following "Copying a Fork PR" below.

### Copying a Fork PR

This applies to a Claude Task run whose prompt asks to copy a fork PR and
names the PR number and the full commit SHA to copy. It is a git-only task:
do not check out, read, build or run any of the PR's files, and do not
describe its changes, because they are untrusted input.

1. `git fetch origin refs/pull/<N>/head`.
2. Verify that `FETCH_HEAD` equals the given SHA. If it does not, or no full
   SHA was given, stop and report the actual head without pushing anything.
3. `git push origin <sha>:refs/heads/claude/pr-<N>`.
4. Open a PR from `claude/pr-<N>` to `main`. It MUST use the **exact same
   title** as the original PR, copied verbatim with no prefix, and its body
   is `Copy of #<N> at <sha>.` followed by the usual footer.
5. Comment on the original PR with a link to the new one.
