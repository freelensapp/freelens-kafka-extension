# Pre-release build 1.4.2-1

Build of the branch [`fix-topic-sizes-closed-connection`](https://github.com/freelensapp/freelens-kafka-extension/compare/main...fix-topic-sizes-closed-connection)
at commit `a8071ab8904786f323a2e99a829eaff5cacd5e97`, to try the fix for
[issue #82](https://github.com/freelensapp/freelens-kafka-extension/issues/82) before it is released.

| File | SHA-256 |
| --- | --- |
| [`freelensapp-kafka-extension-1.4.2-1.tgz`](./freelensapp-kafka-extension-1.4.2-1.tgz) | `2698b271cb9fd21267c7c5e12852dd147d46862cc055e7de8e069ceb1341688d` |

It is built the way the release workflow builds a release: frozen install, version set to `1.4.2-1`,
`VITE_PRESERVE_MODULES=false pnpm build`, `pnpm pack`.

To try it: download the file, open the Extensions page of Freelens, pick the file, install it and restart
Freelens once.

This branch is temporary. It goes away when the fix is released, and it is never merged: its only commit on top
of `fix-topic-sizes-closed-connection` holds the version and this folder.
