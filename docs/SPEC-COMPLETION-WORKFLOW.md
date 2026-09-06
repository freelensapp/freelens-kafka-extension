# Specification Completion Workflow

**Authoritative procedure for marking a specification as Verified and transitioning its roadmap status.**

When a specification moves from `Implementing` → `Verified` (or `Verified` → `Superseded`), the following checklist **MUST** be completed before closing the work item. This ensures roadmap visibility is always current and all agents (human and automated) can see at a glance what is complete.

## Completion Checklist (Every Spec)

### 1. Implementation & Testing Complete
- [ ] All code changes committed and pushed
- [ ] All unit tests passing (`pnpm test:unit` green)
- [ ] All integration tests passing (protocol evidence, E2E)
- [ ] Build artifact clean (`pnpm build`)
- [ ] Packaged extension builds (`pnpm pack:dev` or `pnpm pack`)
- [ ] Biome/knip/type checks passing

### 2. Update Specification Document
- [ ] `docs/specs/SPEC-NNN.md` decision log includes completion date (ISO 8601)
- [ ] Traceability table names all passing test files / evidence
- [ ] Status field in YAML frontmatter: `Status: Verified`
- [ ] Non-goals and assumptions are still accurate; update if user feedback changed them

### 3. Update Specification Index
- [ ] `docs/specs/README.md` table: change Spec status from `Implementing` to `Verified`
- [ ] If this was the last spec in a phase, update the sentence about next available `REQ-NNN`
- [ ] Example: if SPEC-006 used REQ-074–083, the text must say "The next available requirement ID is `REQ-084`"

### 4. Update Architecture Document
- [ ] `ARCHITECTURE.md`: update `Last updated` date to today (ISO 8601)
- [ ] Update the `Phase` line to reflect all completed specs (e.g., "**UX v3 verified complete: SPEC-001–006 delivered and verified.**")
- [ ] Update the `Consequence` paragraph to list what is verified and what remains future work
- [ ] In the "Current status" table (section 2), add a row for each verified SPEC with link to docs and "Verified" state

### 5. Roadmap Visibility
- [ ] Ensure `README.md` Status line reflects the latest completed specs
- [ ] For transparency, add a **Roadmap section** in `README.md` if not already present (see example below)

### 6. Documentation Consistency
- [ ] Search `grep -r "SPEC-NNN" docs/` for any dangling references (e.g., "messages is next" when messages is now done); update them
- [ ] Verify no orphaned "future work" statements contradict the newly verified spec

---

## Example: SPEC-006 Completion (2026-08-06)

```diff
# docs/specs/README.md

- | [SPEC-006](./006-consumer-groups-lag.md) | Implementing | REQ-074–REQ-083 | Consumer group list, Offsets & Lag and Members workspace |
+ | [SPEC-006](./006-consumer-groups-lag.md) | Verified | REQ-074–REQ-083 | Consumer group list, Offsets & Lag and Members workspace |

- The next available requirement ID is `REQ-084`.
+ The next available requirement ID is `REQ-084`.  (Already correct; SPEC-007 will allocate if accepted.)
```

```diff
# ARCHITECTURE.md

- | **Last updated**          | 2026-07-30 |
+ | **Last updated**          | 2026-08-06 |

- | **Phase** | P3.1 (a,b,c), P3.2 (f,g,h,j core), P3.3 and **SPEC-001–004 verified**. ... |
+ | **Phase** | **UX v3 verified complete: SPEC-001–006 delivered and verified.** ... |

- | **SPEC-005 verified**: ... | ... | [`SPEC-005`](./docs/specs/005-read-only-message-browser.md) |
- | UI (resource navigation, topics, messages, consumer-group lag) | UX v3 pages ... |
+ | **SPEC-005 verified**: ... | **Verified** | [`SPEC-005`](./docs/specs/005-read-only-message-browser.md) |
+ | **SPEC-006 verified**: Consumer Groups list, Offsets & Lag, Members, tab workspace | **Verified** | [`SPEC-006`](./docs/specs/006-consumer-groups-lag.md) |
```

---

## Future Automated Agents

When implementing this in CI/CD or agent-based workflows:

1. **Detect Spec Completion**: Listen for a commit touching `docs/specs/SPEC-NNN.md` with status change `Implementing` → `Verified`.
2. **Run This Checklist**: Validate all items are done before allowing merge.
3. **Auto-Update Roadmap Files**: If human forgets, bot can auto-apply steps 3–4 on a new commit (pre-approval by human is recommended).
4. **Report Status**: Post a summary comment / GitHub workflow output naming which specs are now verified and what remains.

---

## Non-Example: What NOT to Do

❌ **Do NOT** leave SPEC status as `Implementing` after code ships  
❌ **Do NOT** update only ARCHITECTURE.md without updating specs/README.md  
❌ **Do NOT** silently change requirements after Spec is Verified (use Evolve step)  
❌ **Do NOT** allocate REQ-NNN IDs for future specs that are not yet Accepted  

---

## References

- [`docs/specs/README.md`](./specs/README.md) — specification lifecycle and index
- [`ARCHITECTURE.md`](../ARCHITECTURE.md) — living document of system state
- [`TESTING-SAFETY.md`](../TESTING-SAFETY.md) — safety constraints and cluster guards
