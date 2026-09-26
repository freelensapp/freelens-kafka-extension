# SPEC-017 - AWS IAM Authentication for Amazon MSK

| Field | Value |
| --- | --- |
| Status | Implemented |
| Date | 2026-09-26 |
| Source | Issue #62 and pull request #72 by its reporter, whose MSK Express brokers accept IAM only |
| Safety | Read-only: the token only authenticates the connection, the extension calls no AWS API and writes nothing; governed by `TESTING-SAFETY.md` |

## Problem

Amazon MSK clusters can require IAM authentication, and MSK Express brokers accept nothing else.
The extension speaks SASL/PLAIN, SASL/SCRAM and mTLS: on an IAM listener the broker closes the
connection during the handshake and the user only sees `Closed connection` (#62). The Security
override of the Connection Settings drawer could not be used to work around a failed automatic
connection either: its controls were disabled while the connection was loading or had failed, and
the authentication menu opened under the drawer, so only its first entry was reachable.

## Scope

- An `AWS IAM (MSK)` mode in the Security override for direct connections, with the AWS region and
  an optional shared-config profile.
- Automatic detection of IAM from a workload environment that declares the `AWS_MSK_IAM`
  mechanism together with a region.
- The token minted in the main process from the credentials of the machine: the AWS SDK default
  chain (environment, shared config and credentials files, SSO, assumed roles, credential
  processes, container and instance metadata) or a named profile of the shared config.
- The Security override usable while the automatic connection is loading or has failed, with its
  menus rendered above the drawer.

## Non-goals

- Access keys typed, shown or stored anywhere in the extension.
- IAM for connections through a Strimzi port-forward, or for Kafka Connect.
- A region inferred from the broker host names: the user or the workload declares it.
- OAUTHBEARER against a token endpoint of the user (still deferred, see ARCHITECTURE.md).
- Regions of the AWS China partition (`kafka.<region>.amazonaws.com.cn`): the AWS signer for MSK
  does not handle them either; a request would be the moment to add that endpoint.

## Functional Requirements

- **REQ-212** — The Security override MUST offer `AWS IAM (MSK)`; the mode MUST require an AWS
  region, MAY take a shared-config profile, and MUST NOT ask for a username or a password.
- **REQ-213** — A direct connection in this mode MUST authenticate with SASL/OAUTHBEARER and an
  MSK IAM token: the presigned `kafka-cluster:Connect` request of the region endpoint, SigV4 in the
  query string, base64url encoded, valid for 15 minutes or until the credentials expire, minted
  anew at every authentication. TLS MUST be on even when the endpoint did not identify it, and a
  `disabled` TLS override MUST be refused with the reason.
- **REQ-214** — Credentials MUST be resolved in the main process only, by the AWS SDK default chain
  or by the selected profile; no access key MUST cross the IPC boundary or be persisted. A profile
  that resolves nothing MUST fail with a message that names the profile.
- **REQ-215** — A workload environment that declares `AWS_MSK_IAM` (or `awsiam`) as SASL mechanism
  together with an AWS region MUST be detected as IAM with that region; without a region the
  automatic connection MUST fail with a message that says what is missing.
- **REQ-216** — A loading or failed automatic connection MUST NOT disable the Security override
  controls: the user MUST be able to choose a mode and reconnect without closing the drawer.
- **REQ-217** — The menus of the Security override MUST render above the Connection Settings
  drawer, so that every mode is selectable.

## Success Criteria

- **SC-125** — Unit: the token decodes to the region endpoint with `Action=kafka-cluster:Connect`,
  the SigV4 query parameters, `X-Amz-Expires=900`, sorted keys and no padding; its life follows
  temporary credentials; an empty region and empty credentials are refused with the cause; the SASL
  settings are OAUTHBEARER and mint a token per authentication; the override and the automatic
  profile both produce them; TLS off and a missing region are refused.
- **SC-126** — Equivalence: at a frozen instant and with the same credentials the token is byte for
  byte the one produced by `aws-msk-iam-sasl-signer-js` 1.0.3, the `User-Agent` parameter apart.
- **SC-127** — Release build: `VITE_PRESERVE_MODULES=false pnpm build` produces a single
  `out/main/index.js` that passes the smoke test, with no chunk beside it and no external `require`
  beyond Node and Electron.
- **SC-128** — A real MSK Express cluster reachable only with IAM (the reporter's, through a VPN on
  port 9098) is browsed with a pre-release package built with the release configuration: metadata
  and topics are read, nothing is written.

## Decision Log

- **2026-09-20** — Credentials come from the machine, never from the UI: the default chain plus an
  optional profile is what every AWS CLI user already has (#62).
- **2026-09-21** — The first implementation (#72) used a community kafkajs mechanism (a custom
  `AWS_MSK_IAM` SASL exchange) with `@aws-sdk/credential-providers`, then
  `@aws-sdk/credential-provider-ini` alone after a first Rolldown panic.
- **2026-09-26** — No ready-made library survives the release build: the community mechanism and
  the AWS signer for MSK (`aws-msk-iam-sasl-signer-js`, the intended choice) both pull
  `@aws-sdk/credential-providers`, on which Rolldown 1.2.8 and 1.2.11 panic (`module_finalizers`,
  "no entry found for key") when the main side is bundled into one file; the separate-modules
  build of the CI does not see it. The token is therefore composed in `src/main/kafka/msk-iam.ts`
  from the official building blocks that bundle fine: `@aws-sdk/credential-provider-node` (the same
  default chain, profile included), `@smithy/signature-v4`, `@aws-crypto/sha256-js` and
  `@aws-sdk/util-format-url`. The composition mirrors `generateAuthTokenFromCredentialsProvider` of
  the AWS signer step by step and is checked against it (SC-126). When Rolldown bundles the umbrella
  package, the AWS signer can replace the module with a small change.
- **2026-09-26** — SASL/OAUTHBEARER with the token instead of a custom mechanism: it is the transport
  AWS documents for kafkajs, it is built into kafkajs, and the token is minted at every
  authentication, so a reconnection after 15 minutes and refreshed credentials need nothing else.
  The built-in `aws` mechanism of kafkajs was not used: it takes static keys and an
  `authorizationIdentity`.
- **2026-09-26** — The release build inlines the dynamic imports of the SDK credential chain
  (`inlineDynamicImports` when modules are not preserved): otherwise the lazy providers (SSO, STS,
  credential process) become 27 chunk files beside `out/main/index.js`. The main bundle grows from
  8.2 to 8.8 MB, the price of the whole chain.

## Verification Evidence

| Requirement range | Evidence |
| --- | --- |
| REQ-212, REQ-216, REQ-217 | `src/renderer/kafka-overview.tsx` (mode, region and profile fields, controls no longer disabled while loading, menus in a portal above the drawer); the reporter used them against the MSK Express cluster of #62 on 2026-09-21 with the first build of #72 |
| REQ-213, REQ-214, SC-125, SC-126 | `src/main/kafka/msk-iam.test.ts` and `src/main/kafka/external-credentials.test.ts`; equivalence run of 2026-09-26 against `aws-msk-iam-sasl-signer-js` 1.0.3 at a frozen instant, two regions, URLs identical apart from `User-Agent` |
| REQ-215 | `src/main/kafka/external-credentials.test.ts` (detection of `AWS_MSK_IAM` with the region, no credential read) |
| SC-127 | Release build of 2026-09-26: one `out/main/index.js` of 8.8 MB, smoke test green, no chunk, no external `require` beyond Node and Electron |
| SC-128 | Pending: pre-release package to be tried by the reporter on the MSK Express cluster of #62 |
