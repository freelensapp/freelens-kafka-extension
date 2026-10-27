#!/usr/bin/env bash
#
# Freelens dev for the integration tests (SPEC-019, REQ-232): a fresh profile under the system's temporary directory
# whose preferences sync the lab's kubeconfig alone, so that the app sees the lab cluster and none of the developer's.
#
#   scripts/lab-freelens.sh                 write the profile and run `pnpm dev` of the Freelens checkout
#   scripts/lab-freelens.sh --profile-only  write the profile only (the CI workflow starts Freelens itself)
#
# Env: FREELENS_DIR, the freelensapp/freelens checkout (default: ../freelens); FREELENS_INTEGRATION_TESTING_DIR, the
# profile (default: /tmp/freelens-kafka-integration), which the tests read the extension store from; THEME=Dark|Light.
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
kubeconfig="${repo}/.lab/kubeconfig"
profile="${FREELENS_INTEGRATION_TESTING_DIR:-/tmp/freelens-kafka-integration}"
freelens="${FREELENS_DIR:-${repo}/../freelens}"

[[ -f ${kubeconfig} ]] || {
  echo "no lab kubeconfig at ${kubeconfig}: run 'pnpm lab:up' first" >&2
  exit 1
}
if grep -E '^\s*server:' "${kubeconfig}" | grep -vqE 'https://(127\.0\.0\.1|localhost|\[::1\]):'; then
  echo "refusing ${kubeconfig}: it names a server that is not local" >&2
  exit 1
fi
case "${profile}" in
  /tmp/?*) ;;
  *)
    echo "refusing profile ${profile}: Freelens accepts FREELENS_INTEGRATION_TESTING_DIR under /tmp only" >&2
    exit 1
    ;;
esac

rm -rf "${profile}"
for app in FreelensDev Freelens; do
  mkdir -p "${profile}/${app}"
  node - "${profile}/${app}/lens-user-store.json" "${kubeconfig}" <<'NODE'
const fs = require("node:fs");
const [storePath, kubeconfig] = process.argv.slice(2);
const preferences = { allowErrorReporting: false, syncKubeconfigEntries: [{ filePath: kubeconfig }] };
if (process.env.THEME) preferences.colorTheme = process.env.THEME;
fs.writeFileSync(storePath, `${JSON.stringify({ preferences }, null, 2)}\n`, { mode: 0o600 });
NODE
done
echo "[lab-freelens] profile ${profile} syncs ${kubeconfig} only"

[[ ${1:-} == "--profile-only" ]] && exit 0

[[ -d ${freelens} ]] || {
  echo "no Freelens checkout at ${freelens}: set FREELENS_DIR" >&2
  exit 1
}
cd "${freelens}"
export FREELENS_INTEGRATION_TESTING_DIR="${profile}"
exec pnpm dev
