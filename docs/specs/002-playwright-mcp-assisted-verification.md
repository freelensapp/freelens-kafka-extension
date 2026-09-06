# SPEC-002 — Playwright MCP Assisted Verification

| Field | Value |
| --- | --- |
| Status | Verified |
| Date | 2026-07-22 |
| Source | Evaluation requested after Freelens PR #2272 |
| Decision | [ADR-001](../decisions/001-playwright-mcp-assisted-verification.md) |
| Safety | [`TESTING-SAFETY.md`](../../TESTING-SAFETY.md) |

## Problem

The deterministic packaged-app integration suite is essential but is not the fastest way to inspect a
UI during development. Freelens also uses cross-origin cluster frames, so generic top-page browser
tools cannot inspect the extension page. We need a fast, frame-aware exploratory layer without
weakening clean-state regression coverage or cluster-safety rules.

## Scope

- Local, persistent, frame-aware Playwright MCP sessions attached over CDP.
- Safe context isolation and evidence capture.
- A promotion rule from exploratory findings to deterministic tests.
- Clear division of responsibility between MCP and existing integration tests.

## Non-goals

- Running MCP in CI.
- Replacing Jest/Playwright integration tests.
- Committing developer MCP configuration.
- Testing writes against Kafka or Kubernetes.
- Providing a security boundary around the agent/browser.

## User Scenarios

### US-005 — Fast exploratory UI loop (P1)

**Given** an isolated running Freelens app and a rebuilt extension, **when** the developer attaches
Playwright MCP, **then** the agent can inspect the top page and cluster frame without relaunching the
app for each observation.

### US-006 — Promote a finding (P1)

**Given** an MCP run reveals a defect or selector contract, **when** the iteration is completed,
**then** the behavior is represented by a deterministic automated test or explicitly documented as
blocked/non-reproducible.

### US-007 — Protect real clusters (P1)

**Given** the workstation kubeconfig contains real/prod contexts, **when** the MCP app starts,
**then** only the allowlisted context is visible to that Freelens process and the agent cannot select
a different catalog target.

## Functional Requirements

- **REQ-012** — Playwright MCP MUST remain an optional local assisted-verification layer; all existing
  static, unit, packaged-app E2E and CI gates remain required.
- **REQ-013** — The workflow MUST attach to an already-running Electron CDP endpoint and support both
  the Freelens top page and cross-origin cluster frame.
- **REQ-014** — The app launcher MUST expose only one explicitly selected kube context; autonomous
  runs default to `kind-kind`.
- **REQ-015** — A non-kind target MUST require an explicit user-approved context plus a read-only opt-in.
- **REQ-016** — Cluster selection MUST use exact allowlisted selectors/IDs; broad or first-match
  selection MUST NOT be used when real contexts are present.
- **REQ-017** — MCP configuration MUST be local, version-pinned and uncommitted; unrestricted file,
  storage, network, devtools and vision capabilities MUST remain disabled by default.
- **REQ-018** — MCP artifacts MUST NOT contain credentials, Secret values, storage state or sensitive
  request bodies.
- **REQ-019** — Each accepted MCP finding MUST be promoted to a deterministic test/spec evidence or
  recorded as blocked/non-reproducible before the iteration closes.
- **REQ-020** — A Tier 2 focused packaged-app E2E MUST pass after MCP-assisted UI work; the committed
  Tier 3 integration test MUST remain the regression gate.
- **REQ-021** — The workflow SHOULD record structured evidence: scenario result, relevant console
  errors, frame/selector context and screenshots where visual behavior matters.
- **REQ-022** — Temporary kubeconfig and app-data files MUST be user-private and removed after normal
  exit or forced launcher termination unless the operator explicitly requests a retained runtime.

## Success Criteria

- **SC-007** — CDP attach completes in under one second on the local workstation.
- **SC-008** — A pilot reaches `kind-kind`'s cross-origin cluster frame and observes its sidebar.
- **SC-009** — The isolated Freelens catalog exposes only the configured kube context.
- **SC-010** — Existing focused and committed integration tests remain unchanged as required gates.
- **SC-011** — Every MCP-discovered defect referenced in a completed iteration links to automated
  regression evidence or a documented blocked result.
- **SC-012** — Forced terminal termination leaves no CDP listener, temporary runtime, app process or
  cleanup watchdog.

## Verification Evidence

- Pilot attach: **44 ms**.
- Top-page flow: `/welcome` → catalog succeeded.
- Frame flow: exact `kind-kind` row → `#cluster-frame-7652…` → cross-origin `/overview` frame; sidebar
  visible; Playwright reported two frames.
- CDP remained alive after a client disconnected.
- Safety finding: an unisolated pilot displayed 37 contexts. A first HOME/KUBECONFIG-only launcher
  attempt also exposed them because Freelens 1.10.3 defaults sync from
  `os.userInfo().homedir/.kube`. The launcher therefore pre-seeds `lens-user-store.json` with one
  `syncKubeconfigEntries` file; its catalog isolation is an explicit acceptance criterion.
- Corrected launcher pilot: catalog names were exactly `["kind-kind"]`; frame sidebar was visible.
- Forced-teardown pilot: CDP inactive, runtime absent, zero app processes and zero watchdog processes.
- Existing packaged-app/committed integration suites remain the authoritative regression gates.

## Traceability

| Requirements | Artifact/evidence |
| --- | --- |
| REQ-012, REQ-019–REQ-021 | ADR-001 testing tiers and promotion rule; existing integration workflow retained |
| REQ-013 | Local CDP pilot, frame URLs and sidebar evidence |
| REQ-014–REQ-016 | `scripts/start-playwright-mcp-app.sh`; exact `kind-kind` pilot selector |
| REQ-017–REQ-018 | Local-only pinned setup and security section in the runbook |
| REQ-020 | Existing `.github/workflows/integration-tests.yaml` + focused local E2E policy |
| REQ-022 | Private launcher runtime; detached, marker-validated cleanup watchdog; forced-teardown pilot |

## Decision Log

- **2026-07-22:** Adopt MCP as Tier 1 exploratory verification; reject replacement of integration/CI.
- **2026-07-22:** Require one-context app isolation after pilot exposed 37 catalog contexts.
- **2026-07-22:** Keep local MCP config uncommitted and pin the reviewed package version.
- **2026-07-22:** Require a user-preference sync override; environment-only isolation is invalid on
  Freelens 1.10.3.
- **2026-07-22:** Require cleanup to survive terminal process-tree termination via a detached,
  narrowly scoped watchdog.
