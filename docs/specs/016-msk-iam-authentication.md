# SPEC-016 - Amazon MSK IAM Authentication

| Field | Value |
| --- | --- |
| Status | Implementing |
| Date | 2026-09-21 |
| Source | Issue #62 and its 2026-09-21 reporter validation offer |
| Safety | Read-only connection and metadata verification; governed by `TESTING-SAFETY.md` |

## Problem

Amazon MSK Express brokers support IAM authentication. A direct TLS connection from Freelens
currently has no compatible KafkaJS SASL mechanism, so connecting to an IAM-enabled broker closes
the connection.

## Scope

- Offer AWS IAM (MSK) as a Connection Settings authentication mode for direct Kafka targets.
- Create the KafkaJS `AWS_MSK_IAM` mechanism with an explicitly configured AWS region.
- Resolve AWS credentials only in the Main process through the AWS SDK default provider chain.
- Detect workload-provided IAM mode and region when both are present.

## Non-goals

- Persisting, rendering, or accepting AWS access keys in the extension.
- Inferring an AWS region from broker hostnames.
- IAM support for Strimzi port-forward connections, OAuthBearer, or validation that writes to MSK.

## Functional Requirements

- **REQ-206** — The Connection Settings drawer MUST offer AWS IAM (MSK), require an AWS region,
  and MUST NOT request a username or password for this mode.
- **REQ-207** — A direct connection configured with AWS IAM (MSK) MUST pass KafkaJS an
  `AWS_MSK_IAM` mechanism and TLS settings through the existing connection lifecycle. TLS MUST
  be enabled for IAM even when the detected endpoint did not identify TLS.
- **REQ-208** — AWS credentials MUST remain in the Main process and be resolved by the AWS SDK
  default provider chain; access keys MUST NOT be persisted or included in IPC payloads.
- **REQ-209** — Workload configuration specifying `AWS_MSK_IAM`/`awsiam` with an AWS region MUST
  be detected automatically; an IAM configuration without a region MUST fail with an actionable error.

## Success Criteria

- **SC-117** — Unit tests cover IAM environment detection, region validation, and construction of
  the `AWS_MSK_IAM` KafkaJS mechanism without a username or password.
- **SC-118** — An authorized reporter can read Kafka metadata from the supplied private MSK
  Express endpoint through VPN using a pre-release package, without performing Kafka writes.

## Verification Evidence

| Requirement range | Evidence |
| --- | --- |
| REQ-206–REQ-209, SC-117 | `pnpm type:check`, `pnpm lint:check`, `pnpm build`, and focused Vitest coverage in `src/main/kafka/external-credentials.test.ts` passed on 2026-09-21 |
| SC-118 | Pending authorized reporter validation against the MSK Express cluster |
