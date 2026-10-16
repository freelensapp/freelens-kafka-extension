# Pre-release build 1.4.2-3

Build of the branch [`fix-saved-connection-settings`](https://github.com/freelensapp/freelens-kafka-extension/compare/main...fix-saved-connection-settings)
at commit `ef2f22f96c84028eb83b3f100d109334d7309355`, to try the fix for
[issue #82](https://github.com/freelensapp/freelens-kafka-extension/issues/82) before it is released.

| File | SHA-256 |
| --- | --- |
| [`freelensapp-kafka-extension-1.4.2-3.tgz`](./freelensapp-kafka-extension-1.4.2-3.tgz) | `8c34bc897ba8f1b3c02d229e2cd6030fb110166f3412d4753043f65a72033c24` |

It is built the way the release workflow builds a release: frozen install, version set to `1.4.2-3`,
`VITE_PRESERVE_MODULES=false pnpm build`, `pnpm pack`.

To try it: download the file, open the Extensions page of Freelens, pick the file, install it and restart
Freelens once.

This branch is temporary. It goes away when the fix is released, and it is never merged: its only commit on top
of `fix-saved-connection-settings` holds the version and this folder.
