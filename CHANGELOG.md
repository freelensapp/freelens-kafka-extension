# Changelog

## Unreleased

### Added

- `demo:up`, `demo:status` and `demo:down` scripts that build and remove a disposable demo environment with only Docker, kind and Node.js: a dedicated kind cluster with a Strimzi-like Kafka broker, an external Docker broker referenced by a workload, seeded topics with JSON records, an active and a lagging consumer group, and a live producer. Under WSL2 the kubeconfig is also copied to the Windows side for Freelens.
- Copy buttons in the message inspector for the key, the value as displayed and all headers as one JSON object (#23).

### Fixed

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
