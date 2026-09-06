# Kafka UX v3 — Sidebar-First Resource Navigation

| Field | Value |
| --- | --- |
| Status | Accepted |
| Date | 2026-07-24 |
| Source | Manual-review feedback after UX v2; accepted by the user on 2026-07-24 |
| Delivery contract | [`SPEC-004`](../specs/004-resource-navigation-cluster-context.md) |
| Compare | [`kafka-ux-v2-proposal.md`](./kafka-ux-v2-proposal.md) |
| Mockup | [`../mockups/kafka-navigation-v3.html`](../mockups/kafka-navigation-v3.html) |

## Clarification About the v2 Mockup

The numbered strip at the very top of the v2 mockup (`Kafka targets`, `Cluster workspace`, `Topic &
messages`, `Consumer groups`) was only a design-review switcher. It was not intended to be rendered
inside Freelens. Its visual treatment was too similar to application navigation, so the v3 mockup
labels it explicitly as **MOCKUP CONTROLS — Not part of the extension UI**.

The v2 application proposal did still repeat cluster-level sections as tabs in the Cluster Workspace.
The v3 proposal removes that duplication.

## Decision

Use two distinct navigation levels, each with one responsibility:

1. **Freelens sidebar children** select stable Kafka resource areas for the current Kafka cluster.
2. **Page tabs** select views of the entity currently open.

A label MUST appear in only one of these levels at a time. For example, Consumer Groups appears in
the sidebar, while a Consumer Group detail page may contain `Offsets & Lag`, `Members` and `Topics`
tabs. It does not repeat a `Consumer Groups` tab.

## Sidebar Structure

One collapsible Kafka parent is added under Extensions:

```text
Apache Kafka
  Clusters
  Overview
  Topics
  Consumer Groups
  Brokers
  ----------------
  Schema Registry     future
  Kafka Connect       future
```

Freelens already supports this structure through `clusterPageMenus.parentId`; Sveltos uses the same
pattern for Overview, Clusters and Profiles.

### Clusters Versus Targets

The user-facing term is **Kafka cluster**. It is already familiar to Kafka users and matches the
current extension, which reports discovered clusters. The list page and sidebar entry are therefore
named **Clusters**, with a **Kafka clusters** page heading so it is not confused with the active
Kubernetes cluster in Freelens.

The implementation still uses **connection target** and `targetId` internally. Discovery may find a
Kafka connection through a Strimzi resource, a Kubernetes Service, a workload reference, an MSK
bootstrap address or a manually entered endpoint. Before Kafka metadata is read, that object is an
effective connection target rather than a cryptographically verified cluster identity. This
technical distinction does not need to leak into normal navigation.

Rules for scaling:

- Add only stable resource domains to the sidebar.
- Do not add individual Kafka clusters, topics, groups or brokers to the sidebar.
- Keep the primary set to roughly 5–7 entries; lower-frequency future capabilities can be grouped or
  moved to a `More` page if the list grows.
- Entity-specific operations remain in page headers or tabs.

## Cluster Overview

Overview is a real selected-cluster page, not a placeholder. It provides a concise operational
summary while the resource pages retain their detailed tables.

The first implementation includes:

- Kafka cluster health derived from unavailable and under-replicated partitions;
- broker count and active controller;
- topic and partition totals;
- Consumer Group count and aggregate lag when that capability is available;
- provider, security summary, connection strategy and reachability;
- bootstrap identity with secret values excluded;
- refresh status and actionable partial/error states.

Selecting a row on Clusters opens Overview for that Kafka cluster. Selecting Overview directly uses
the last explicit Kafka cluster selection or asks the user to choose one. Overview MUST NOT perform
continuous polling by default and MUST remain read-only.

## Brokers

Brokers is a real cluster-scoped inventory page. Its first implementation uses the broker metadata
already returned by `describeCluster()` and includes:

- broker node ID;
- controller role;
- advertised host;
- advertised port;
- selected Kafka cluster, provider and connection status in the page header.

The page does not claim per-broker health or reachability because listing a broker in cluster
metadata is not an individual connectivity probe. It remains read-only and refreshes only on an
explicit page load, cluster change or refresh action.

Broker rows are informational in the first iteration. A dedicated Broker Workspace remains a future
extension point if partition leadership or broker configuration data later justifies a detail page.

## Page Tabs

Tabs exist only after an entity is selected.

### Topic Workspace

- Overview
- Messages
- Partitions
- Consumers
- Configuration

### Consumer Group Workspace

- Offsets & Lag
- Members
- Topics

### Broker Workspace, if needed later

- Overview
- Partitions
- Configuration

This is consistent with common Kafka console patterns: list pages are dedicated routes, while the
selected Topic or Consumer Group owns its internal tabs.

## Kafka Cluster Selector

The v3 mockup adds a **Kafka cluster** selector to every cluster-scoped resource page:

- Overview
- Topics
- Consumer Groups
- Brokers
- Topic Workspace
- Consumer Group Workspace

The Clusters page has no selector because its purpose is to show every available Kafka cluster
connection.

### Selection Behavior

- The selected Kafka cluster is stored internally as a non-secret stable `targetId` page parameter.
- Credentials, passwords and resolved Secret data never enter the URL or persisted selection.
- Switching Kafka cluster on Topics keeps the user on Topics, preserves harmless list preferences where
  useful and clears the selected Topic.
- Switching Kafka cluster on Consumer Groups keeps the user on Consumer Groups.
- Switching Kafka cluster from a Topic or Group detail returns to that resource's list for the new cluster.
  It does not assume an entity with the same name exists in both clusters.
- In-flight requests for the previous cluster are cancelled or ignored by operation ID.
- If there is one available Kafka cluster, it may be selected automatically.
- With multiple Kafka clusters and no previous explicit selection, the UI asks for a cluster instead of
  silently choosing the first row.
- The last explicit Kafka cluster may be remembered per Kubernetes cluster using only its stable ID.

### Why This Is Useful

This model is better than a cluster-expanded sidebar for Freelens. The Kubernetes cluster is
already selected by Freelens, but one Kubernetes cluster can reference several Kafka clusters. The
selector lets a developer compare the same resource type across those clusters without returning to
Clusters each time.

## Future Message Production

Producing messages is technically feasible through KafkaJS and belongs in the Topic Workspace as an
explicit header action, not as a sidebar entry or passive tab.

Recommended flow:

1. Click **Produce message** from one Topic.
2. Open a dedicated page with the Kafka cluster and topic locked in its header.
3. Edit key, value, headers and optional partition using native controls and Monaco editors.
4. Review the exact destination and payload.
5. Confirm and send once.
6. Show the resulting partition/offset or a precise error.

### Safety Requirements

- Write functionality is disabled by default.
- It requires an explicit extension preference and a write-enabled Kafka cluster allowlist.
- The destination Kafka cluster and topic cannot change silently while composing.
- There is no automatic retry initiated by the UI after an ambiguous delivery result.
- No autonomous test writes to a real Kafka cluster; protocol and E2E tests use disposable local
  Kafka/KinD fixtures only.
- This capability receives its own future SDD specification and threat/safety review.

The v3 mockup includes a future Produce page to validate the information architecture, not to add it
to the current implementation scope.

## Kafka Icon Decision

Keep the existing Material `hub` icon used by the extension. It is familiar in the current UI,
requires no new asset and does not imply that this is an official Apache Kafka product. The v3
mockup uses the same symbol.

The official Apache Kafka logo remains out of scope unless explicit ASF permission and the required
trademark attribution are obtained later.

## Route and Menu Model

| Page ID | Sidebar child | Parameters |
| --- | --- | --- |
| `kafka-clusters` | Clusters | filters/sort |
| `kafka-overview` | Overview | `target` |
| `kafka-topics` | Topics and Topic Workspace | `target`, `query`, `topic`, `view` |
| `kafka-groups` | Consumer Groups | `target`, filters/sort |
| `kafka-brokers` | Brokers | `target`, filters/sort |
| `kafka-group` | No, Consumer Groups remains active | `target`, `group`, `view` |
| `kafka-produce` | No, Topics remains active | `target`, `topic` |

All pages use the extension's `navigate(pageId, params)` API and registered `PageParam`s. This gives
back/forward/reload semantics without introducing a second router.

## Comparison with v2

| Concern | v2 | v3 |
| --- | --- | --- |
| Cluster-level sections | Tabs inside Cluster Workspace | Stable child entries in Freelens sidebar |
| Topic subviews | Tabs | Tabs, unchanged |
| Kafka cluster switching | Planned in workspace params | Visible selector on every cluster-scoped page |
| Scaling to Schema/Connect | More cluster tabs | Additional resource sidebar children |
| Consumer Groups duplication | Could look duplicated | Appears only as sidebar resource; detail tabs are subordinate |
| Message production | Not visualized | Dedicated future write page |
| Extension icon | Existing Material `hub` icon | Existing Material `hub` icon retained |

## Delivery Roadmap

### UX3.1 — Navigation and Kafka Cluster Context — Verified

- Deliver [`SPEC-004`](../specs/004-resource-navigation-cluster-context.md), `REQ-033`–`REQ-049`.
- Add stable non-secret target IDs and a selected Kafka cluster registry per Kubernetes cluster.
- Register sidebar child pages and URL-backed entity workspace state.
- Add typed URL params and Kafka cluster selector behavior.
- Retain the existing Material `hub` extension icon.
- Expose only implemented entries; Consumer Groups and future domains remain absent until functional.

Stable target identity, per-Kubernetes-cluster selection persistence, Clusters, Overview, Topics,
Topic Workspace, Brokers, the shared page shell/selector and URL-backed filtering were delivered on
2026-07-27. Stabilization, shared caching and primary Drawer retirement were verified by 2026-07-31.
The workspace exposes only functional Overview and Partitions tabs; Connection Settings remains a
secondary panel and other menu entries remain hidden until functional.

### UX3.2 — Resource Page Migration — SPEC-004

- Keep Clusters as the discovery/connection entry point.
- Implement Overview as the selected cluster's health, capacity and connection summary.
- Move Topics and Brokers out of the Drawer into dedicated pages.
- Keep Connection Settings as a secondary panel.

### UX3.3 — Topic Workspace — SPEC-004

- Move partition topology to Topic Overview/Partitions.
- Add entity tabs and back navigation.
- Preserve topic-list search/sort when returning.
- Remove Topic workflows from the cluster Drawer.

### UX3.4 — Read-Only Message Browser — Planned SPEC-005

- Allocate globally unique requirements only when the specification is accepted.
- Add bounded Browse and explicit bounded Tail.
- Add selected-message split inspector.
- Keep the page inert until the user explicitly starts a read.
- Preserve no-offset-commit semantics and test only against disposable local Kafka fixtures.

### UX3.5 — Consumer Groups and Lag — Planned SPEC-006

- Allocate globally unique requirements only when the specification is accepted.
- Add cluster-level group list.
- Add Offsets & Lag, Members and Topics detail tabs.
- Cross-link Topic and Group workspaces.
- Register the Consumer Groups sidebar entry only when the page is functional.

### UX3.6 — Future Produce Workflow — Separate Approval Required

- Separate specification and explicit user approval.
- Add opt-in write policy and allowlisted Kafka clusters.
- Implement dedicated compose/review/send page.
- Test writes only on disposable local Kafka.

### UX3.7 — Future Resource Domains — Unscheduled

- Schema Registry.
- Kafka Connect.
- ACL/security views if supported and appropriately scoped.

## Accepted Decisions

The implementation basis is **v3**, specifically:

1. nested Kafka resource entries in the Freelens sidebar;
2. tabs only for views of a selected Topic, Group or Broker;
3. Kafka cluster selector on resource pages, backed internally by `targetId`;
4. Kafka cluster switch from detail returns to the corresponding list;
5. existing Material `hub` sidebar icon retained;
6. Produce Message retained as a future dedicated, opt-in write workflow.

Runtime implementation is underway in SPEC-004. Later roadmap increments require their own accepted
specification and executable evidence; Produce Message additionally requires explicit write approval.

## Decision Log

- **2026-07-24:** User accepted v3 as the implementation basis.
- **2026-07-24:** Replace user-facing Targets with Clusters; retain `targetId` internally.
- **2026-07-24:** Make Overview and Brokers functional resource pages and prohibit inert production
  navigation entries.
- **2026-07-24:** Keep the existing Material `hub` icon.
- **2026-07-24:** Begin delivery with SPEC-004; message browsing and Consumer Groups remain separate
  read-only specifications, while Produce Message requires a later write approval.
