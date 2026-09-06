# SPEC-004 Requirements Quality Checklist

## Requirement ID Coverage

- [x] Requirements continue globally from `REQ-033` through `REQ-054`.
- [x] IDs are unique, sequential and not reused.
- [x] Success criteria continue from `SC-019` through `SC-032`.

## Testability

- [x] User scenarios use Given/When/Then.
- [x] Sidebar, route, history, selector and page outcomes are independently observable.
- [x] Success criteria cover one/multiple clusters, stale responses, loading, error and compact states.
- [x] Existing topic behavior is protected through explicit `REQ-023`–`REQ-032` parity.
- [x] Duplicate navigation, metric layout and warm-navigation request counts are observable.
- [x] Topics and Partitions header/row alignment, clipping and overflow are measured geometrically.

## Scope and UX Contract

- [x] Initial visible pages and hidden entity routes are explicit.
- [x] User-facing **Kafka cluster** and internal `targetId` terminology are separated.
- [x] Overview and Brokers make only metadata-supported claims.
- [x] Mockup controls and future/unimplemented entries are excluded from production.
- [x] Drawer retirement is gated on verified page parity.
- [x] Shared cache freshness, invalidation and non-secret storage constraints are explicit.
- [x] Cache age/state and exact cold/warm request counts are observable without sensitive payloads.

## Safety

- [x] Kafka and Kubernetes writes are explicit non-goals.
- [x] Credentials and Secret values are prohibited from URLs, logs and Renderer DTOs.
- [x] Real-cluster activity remains explicitly authorized and read-only.
- [x] Produce Message requires a separate future specification and approval.

## Implementation Plan

- [x] Every requirement maps to a planned code surface and verification tier.
- [x] Implementation slices are ordered around stable identity, navigation, page migration and parity.
- [x] Consumer Groups and message browsing remain separate roadmap increments.

## Verification Gates

- [x] Focused route, state and view-model unit tests pass.
- [x] Typecheck, Biome/Prettier, Knip, build and smoke gates pass.
- [x] Disposable Docker Kafka and KinD metadata/discovery regressions pass.
- [x] Isolated Playwright MCP desktop/compact exploration is promoted to deterministic evidence.
- [x] Focused packaged Freelens 1.10.3 navigation/page-parity E2E passes.
- [x] Committed integration workflow passes.
- [x] SPEC-004 traceability names delivered code and executable evidence; status is `Verified`.
