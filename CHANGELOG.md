# Changelog

## Unreleased

### Added

- AWS IAM authentication for direct Amazon MSK connections: `AWS IAM (MSK)` in the Security override with the AWS region and an optional AWS profile, automatic detection from a workload that declares `AWS_MSK_IAM` with its region, and the token minted in the main process from the AWS SDK default credential chain, so no access key is ever typed or stored (#62, #72).

### Fixed

- The Security override stays usable while the automatic connection is loading or has failed, and its menus open above the Connection Settings drawer, so every authentication mode can be selected (#62, #72).

## v1.3.1 - 2026-09-20

### Added

- Restart notice after an in-place update: Freelens keeps the main side of the previous version loaded until the app restarts, so the pages now ask the main process which version it runs and, when it differs, every Kafka page says to restart Freelens to finish the update instead of failing later with a raw IPC error.

### Fixed

- Produce message no longer pushes the Messages table out of view: the compose form opens as a panel beside the Topic Workspace (stacked above it on narrow windows), so the table keeps its height and Browse, Tail and the inspector stay usable while a record is composed. The table also keeps a minimum height under the inline confirmations (#66).
- The Produce form accepts what it documents: Value and Headers are multi-line, a header value may contain `=` (it was cut at the second `=`), a line that is not `key=value` or a partition the topic does not have blocks the send with an explicit message, every send needs its own confirmation, and leaving the topic or the cluster cancels the compose with a notice.
- Batch topic deletion works right after an in-place update of the extension, when Freelens still runs the main side of the previous version until it restarts: the list falls back to one deletion per topic and keeps the per-topic outcome (#61).

## v1.3.0 - 2026-09-11

### Added

- Delete several topics at once from the Topics list in write mode: a checkbox per topic and one for the visible page (internal topics excluded), a "Delete N topics" action, one confirmation that lists every selected name and requires the typed count plus the switch; the outcome reports the deleted topics and every failure with the full broker error, and the failed topics stay selected (#56).

### Fixed

- After a topic deletion the Size column and the Size summary no longer fall to `n/a`: the shared read connection kept targeting the deleted topic and the next metadata refresh failed with `UNKNOWN_TOPIC_OR_PARTITION`; the size reader now drops the stale names and retries once.

## v1.2.0 - 2026-09-10

### Added

- Topic size on disk in the Topics list (sortable, with the sum in the summary), in the Topic Workspace and per partition, read through `DescribeLogDirs` on every broker; lower bounds are marked when a broker does not report and `n/a` appears on clusters without the API (#30).

## v1.1.1 - 2026-09-10

### Fixed

- The main process refuses write calls (produce, delete topic, reset offsets, Schema Registry, Kafka Connect and ACL writes) for targets whose write mode is not enabled in the session; the renderer mirrors the switch to main, so the write policy no longer relies on the renderer alone.
- Basic auth for Schema Registry and Kafka Connect works: the connection settings ask for a password that is kept for the session only and never stored, and the pages now send the configured username (it was never mapped to the request before, so no `Authorization` header was ever sent).

## v1.1.0 - 2026-09-10

### Added

- `demo:up`, `demo:status` and `demo:down` scripts that build and remove a disposable demo environment with only Docker, kind and Node.js: a dedicated kind cluster with a Strimzi-like Kafka broker, an external Docker broker referenced by a workload, seeded topics with JSON records, an active and a lagging consumer group, and a live producer. Under WSL2 the kubeconfig is also copied to the Windows side for Freelens.
- Copy buttons in the message inspector for the key, the value as displayed and all headers as one JSON object (#23).
- Delete topic from the Topic Workspace in write mode, behind the reinforced confirmation (typed topic name and switch); the list refreshes and reports the outcome (#24).

### Fixed

- The broker port-forward now loads the kubeconfig file the selected Freelens cluster was added from (the catalog `kubeConfigPath`) instead of only the default `~/.kube/config` / `$KUBECONFIG`, so clusters kept in separate kubeconfig files connect through Freelens. An unreadable file still falls back to the default resolution (#22).
- The Messages tab table scrolls inside the browser, with a sticky header, when the loaded window is taller than the page; in the desktop layout the rows beyond the visible height were clipped with no scrollbar. The Offset, Timestamp, Key and Value columns are sortable like the other tables (#25).
- The format badges of the message inspector showed a literal `\u202f` escape between the byte count and `B` (JSX text does not interpret escapes); they now show the narrow space.

## v1.0.0 - 2026-09-06

### Added

- Kubernetes-native discovery for Strimzi, generic Services and workload-referenced external Kafka targets, with explicit scanning and a persisted non-secret catalog.
- Dedicated Clusters, Overview, Topics, Brokers, Consumer Groups, Schema Registry, Kafka Connect and ACL views in the Freelens cluster workspace.
- Bounded read-only message Browse and Tail, topic and broker configuration inspection, consumer lag/detail views and Topic-to-Consumer cross-links.
- Session-only write mode with explicit confirmations for Produce, offset reset, Schema Registry, Kafka Connect and ACL operations. Autonomous write tests remain restricted to disposable local fixtures.
- URL-backed message filters, timestamp seeking, Avro/Protobuf decoding and renderer-session route restoration.

### Changed

- Ordinary navigation now reuses bounded target sessions and stale-while-revalidate resource snapshots instead of repeating Kubernetes discovery, credential resolution and connection setup.
- Cluster topology renders before complete global lag. Aggregate Health uses one persistent cancellable worker, protocol-batched group offsets and high-only watermark reads while preserving complete, lossless semantics.
- Progress is phase-local with confidence-gated ETA, compact background Health status, explicit exact/lower-bound coverage and independent metadata/topology/aggregate freshness labels.
- Lists and caches are bounded for production-scale targets; warm and persisted snapshots remain immediately usable during background refresh and failure.

### Verification

- SPEC-001 through SPEC-014 are Verified; the final SPEC-014 approval was recorded on 2026-09-01.
- Final evidence: 54 unit files / 303 tests, all Docker/KinD protocol and security integrations, clean production build and Main smoke, 12/12 base packaged Freelens 1.10.3 Playwright/Jest scenarios, and a passing three-run packaged browser comparison.
- Three authorized read-only production-scale runs completed exact aggregate lag in 10.369 seconds median, 90.8% faster than the reopened 112.2-second baseline, with zero public fallback or low-offset requests on the supported path.
- Authorized click-to-visible first useful/topology paint measured 3.879 seconds median / 4.762 seconds P95, versus fresh pinned open-source reference-console Topics paint at 7.081 seconds median / 7.673 seconds P95.
- Full sanitized backend, API and packaged browser measurements are recorded in `docs/performance/spec-014-slice-16.json`.
- The publication-ready post-approval TGZ has SHA-256 `da9b175c8b17293864825f031ae11f62b5e44f4605a8b700c750718a05e21c63`; its runtime bundles are byte-identical to the browser-attested release candidate.

### Released

- Published on 2026-09-06 from tag `v1.0.0` (commit `1a2eb58`) by the
  release workflow: `@freelensapp/kafka-extension@1.0.0` on npm (tag
  `latest`) and the GitHub release asset `freelensapp-kafka-extension-1.0.0.tgz`
  are the same CI production build, SHA-256
  `ee939118bd31dace018261cfdfa1bd27567ea31eebc8457b9576688fcf24751d`.
