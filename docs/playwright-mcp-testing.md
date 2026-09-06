# Playwright MCP Testing Runbook

Playwright MCP is the fast, agent-driven **Tier 1** loop. It complements but never replaces the
focused packaged-app E2E and committed CI integration suite. See [ADR-001](./decisions/001-playwright-mcp-assisted-verification.md)
and [SPEC-002](./specs/002-playwright-mcp-assisted-verification.md).

## 1. Start a safe Freelens CDP session

Default (`kind-kind` only):

```sh
corepack pnpm mcp:app
```

The launcher creates a temporary `HOME`, writes a one-context kubeconfig, and pre-seeds Freelens's
`syncKubeconfigEntries` preference to that exact file before starting the packaged app with CDP on
`127.0.0.1:9223`. The preference override is required because Freelens 1.10.3 otherwise defaults to
`os.userInfo().homedir/.kube` even when `HOME`/`KUBECONFIG` are changed. The launcher prints the
runtime/artifact directory. Temporary files are user-private (`umask 077`); a detached watchdog
removes the kubeconfig and app data after normal exit or forced terminal termination.

A user-approved real context may be used **read-only**:

```sh
ALLOW_REAL_READ_ONLY=1 corepack pnpm mcp:app my-authorized-context
```

Aggregate cache timing can be reproduced without UI automation through the stricter fail-closed
probe (Kubernetes list, TCP connect without bytes, Kafka `describeCluster`/`listTopics` only):

```sh
ALLOW_REAL_READ_ONLY=1 KUBE_CONTEXT=my-authorized-context AUTHORIZED_KUBE_CONTEXT=my-authorized-context corepack pnpm itest:cache:real
```

The probe rejects every other invocation and prints no endpoint, Secret, credential or topic name.

This does not relax [`TESTING-SAFETY.md`](../TESTING-SAFETY.md). Never use a generic catalog selector;
select the exact approved context.

Environment overrides:

- `FREELENS_APP_BINARY` — packaged Freelens executable.
- `PLAYWRIGHT_MCP_CDP_PORT` — CDP port (default `9223`).
- `FREELENS_MCP_RUNTIME_DIR` — intentionally retain a chosen runtime/artifact directory instead of
   deleting a temporary one.

## 2. Configure MCP locally

Do **not** commit `.mcp.json`. Pin the reviewed package version in your local MCP client:

```sh
code --add-mcp '{"name":"playwright-freelens-kafka","command":"npx","args":["-y","@playwright/mcp@0.0.78","--cdp-endpoint","http://127.0.0.1:9223","--console-level","warning","--output-mode","file","--output-dir","/tmp/freelens-kafka-playwright-mcp","--test-id-attribute","data-testid","--timeout-action","10000","--timeout-navigation","60000"]}'
```

Equivalent client-local configuration is acceptable. Do not add:

- `--allow-unrestricted-file-access`;
- persistent storage export;
- network/storage/devtools/vision capabilities unless a spec explicitly requires and reviews them;
- a non-local CDP endpoint.

Playwright MCP is not a security boundary. Client permissions and the one-context launcher are the
actual guardrails.

## 3. Frame-aware workflow

Freelens has two surfaces:

- **Top page:** catalog, welcome, preferences and extensions (`https://renderer.freelens.app:*`).
- **Cluster frame:** sidebar, Kafka resource pages and secondary Connection Settings Drawer
  (`#cluster-frame-<entityId>` → `https://<entityId>.renderer.freelens.app:*`).

Always identify the frame explicitly. For cluster work, first select the **exact** allowlisted catalog
row, then operate in the iframe containing `[data-testid=cluster-sidebar]`.

## 4. Kafka exploratory scenario

Record pass/fail/blocked and evidence for the relevant steps:

1. **Extension install/reload (top page)**
   - Install the newest `freelensapp-kafka-extension-*.tgz`.
   - Confirm Enabled; collect extension-scoped console errors.
2. **Open exact local cluster (top → cluster frame)**
   - Select exactly `kind-kind` unless the user approved another read-only context.
   - Confirm cluster frame and sidebar.
3. **Discovery progress**
   - Open Kafka.
   - Observe Cluster/Resources/Workloads/Merge, monotonic percentage and workload counts.
4. **List semantics**
   - Confirm Kubernetes usage, security and reachability badges.
   - Exercise filtering and responsive width.
5. **Page and settings navigation**
   - Open Overview from a row cell and from Enter/Space.
   - Open the named `tune` action and confirm only Connection Settings appears in the Drawer.
   - Confirm semantic form submission, explicit reconnect, error/completion state and manual removal.
6. **Visual evidence**
   - Capture desktop/compact screenshots only where layout matters.
   - Review warning/error console messages; exclude known unrelated Freelens noise explicitly.
7. **Promotion**
   - Turn defects and stable contracts into unit or committed Playwright assertions.
   - Run focused packaged-app E2E, then normal CI-equivalent gates.

## 5. What MCP may replace

MCP can replace ad hoc manual clicking, repeated selector reconnaissance and temporary diagnostic edits.
It can provide evidence while a scenario is still evolving.

MCP cannot replace:

- type/lint/unit gates;
- protocol/kind integration tests;
- clean-state packaged-app Playwright assertions;
- committed integration CI;
- security review or read/write safety controls.

## 6. Evidence template

```md
### MCP run — <date> / <spec requirement>
- App/build: <version/tarball>
- Kube context: <exact allowlisted context>
- Result: PASS | FAIL | BLOCKED
- Top page / cluster frame: <URLs or selectors>
- Console: <new relevant errors only>
- Visual evidence: <artifact paths>
- Finding promoted to: <test/spec/blocked reason>
```

## 7. Efficiency review

After five UI iterations, compare MCP against Playwright CLI + Skills. The official MCP project notes
that CLI + Skills is often more token-efficient for high-throughput coding agents, while MCP is better
for persistent state and rich exploratory introspection. Keep MCP only where that persistence produces
measurable value.
