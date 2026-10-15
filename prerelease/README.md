# Diagnostic pre-release build 1.4.2-2

Build of the branch `diag-issue-82` at commit `c23a071f9611d31906325aa6494a3f0b0e9ddfac`, to find out which request makes the brokers
close the connection in [issue #82](https://github.com/freelensapp/freelens-kafka-extension/issues/82). It is
the 1.4.2-1 build plus a diagnostic probe, and it is never released.

| File | SHA-256 |
| --- | --- |
| [`freelensapp-kafka-extension-1.4.2-2.tgz`](./freelensapp-kafka-extension-1.4.2-2.tgz) | `78ab666cc2984c18df3b3f4915e2c0ba023ec57d02383e60effebf31656c2afc` |

After a failed size read the extension asks the cluster each request its readers send, one at a time and each
on a new connection, and writes a report to `~/freelens-kafka-diagnostics/`. Read-only requests only. The
report names no host, topic or consumer group: brokers appear by id, the rest by counts.

It is built the way the release workflow builds a release: frozen install, version set to `1.4.2-2`,
`VITE_PRESERVE_MODULES=false pnpm build`, `pnpm pack`.

This branch is temporary. It goes away when the issue is solved, and it is never merged.
