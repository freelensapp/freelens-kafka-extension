# Project Instructions

## Sources of Truth

- Treat `ARCHITECTURE.md` as the living architectural source of truth and `docs/specs/` as the detailed requirement and verification record.
- Follow `TESTING-SAFETY.md` for every Kubernetes, Kafka, Electron and Playwright validation. Restrict all autonomous mutations to disposable local fixtures explicitly allowed there.

## Slice Boundaries

- Before starting the next implementation slice, update the phase, test evidence and roadmap in `ARCHITECTURE.md` and the task/evidence state in the owning spec for every slice that was completed, reopened or rescheduled.
- Keep architecture and spec statuses aligned. Do not mark a spec `Verified` until every documented exit criterion and final gate has passed.
