#!/usr/bin/env bash
set -euo pipefail
umask 077

context=${1:-kind-kind}
port=${PLAYWRIGHT_MCP_CDP_PORT:-9223}
app=${FREELENS_APP_BINARY:-"$(cd "$(dirname "$0")/../../freelens/freelens" && pwd)/dist/linux-unpacked/freelens"}
app_name=Freelens
watchdog="$(cd "$(dirname "$0")" && pwd)/watch-playwright-mcp-app.sh"

if [[ -n ${FREELENS_MCP_RUNTIME_DIR:-} ]]; then
  runtime_root=$FREELENS_MCP_RUNTIME_DIR
  remove_runtime=0
else
  runtime_root=
  remove_runtime=1
fi

runtime_marker="freelens-kafka-mcp:$$:$RANDOM:$RANDOM"

if [[ "$context" != "kind-kind" && ${ALLOW_REAL_READ_ONLY:-0} != "1" ]]; then
  echo "Refusing non-kind context '$context'. Set ALLOW_REAL_READ_ONLY=1 only for a user-approved read-only target." >&2
  exit 2
fi

if [[ ! -x "$app" ]]; then
  echo "Freelens binary is not executable: $app" >&2
  exit 2
fi

if ! command -v node >/dev/null 2>&1 || ! command -v setsid >/dev/null 2>&1 || ! command -v tail >/dev/null 2>&1; then
  echo "node, setsid and tail are required to create and clean the isolated runtime" >&2
  exit 2
fi

if ! kubectl config get-contexts "$context" --no-headers >/dev/null 2>&1; then
  echo "Kubernetes context not found: $context" >&2
  exit 2
fi

if command -v ss >/dev/null 2>&1 && ss -ltn "sport = :$port" | grep -q LISTEN; then
  echo "CDP port $port is already in use" >&2
  exit 2
fi

if [[ "$remove_runtime" == "1" ]]; then
  runtime_root=$(mktemp -d /tmp/freelens-kafka-mcp.XXXXXX)
fi

app_pid=

cleanup() {
  trap - EXIT HUP INT TERM

  if [[ -n "$app_pid" ]]; then
    kill "$app_pid" 2>/dev/null || true
    wait "$app_pid" 2>/dev/null || true
  fi

  marker_file="$runtime_root/.freelens-kafka-mcp-runtime"

  if [[ "$remove_runtime" == "1" ]] &&
    { [[ ! -e "$marker_file" ]] || [[ -f "$marker_file" && $(<"$marker_file") == "$runtime_marker" ]]; }; then
    rm -rf -- "$runtime_root"
  fi
}

trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

mkdir -p "$runtime_root"
printf '%s\n' "$runtime_marker" >"$runtime_root/.freelens-kafka-mcp-runtime"

mkdir -p "$runtime_root/home/.kube" "$runtime_root/app-data/$app_name" "$runtime_root/artifacts"
kubectl config view --raw --minify --context "$context" >"$runtime_root/home/.kube/config"
chmod 600 "$runtime_root/home/.kube/config"

node - "$runtime_root/app-data/$app_name/lens-user-store.json" "$runtime_root/home/.kube/config" <<'NODE'
const fs = require("node:fs");

const [storePath, kubeconfigPath] = process.argv.slice(2);
const store = {
  __internal__: {
    migrations: {
      version: "0.1.0",
    },
  },
  preferences: {
    syncKubeconfigEntries: [{ filePath: kubeconfigPath }],
  },
};

fs.writeFileSync(storePath, `${JSON.stringify(store, null, 2)}\n`, { mode: 0o600 });
NODE

cat <<EOF
Playwright MCP Freelens session
  context:   $context
  CDP:       http://127.0.0.1:$port
  app:       $app
  artifacts: $runtime_root/artifacts

Only the selected kube context is visible to this process.
EOF

env \
  HOME="$runtime_root/home" \
  KUBECONFIG="$runtime_root/home/.kube/config" \
  FREELENS_INTEGRATION_TESTING_DIR="$runtime_root/app-data" \
  DISPLAY="${DISPLAY:-:0}" \
  ELECTRON_DISABLE_SANDBOX="${ELECTRON_DISABLE_SANDBOX:-1}" \
  LOG_LEVEL="${LOG_LEVEL:-warn}" \
  "$app" --integration-testing --remote-debugging-port="$port" &
app_pid=$!

setsid --fork "$watchdog" "$$" "$app_pid" "$port" "$runtime_root" "$runtime_marker" "$remove_runtime" \
  </dev/null >/dev/null 2>&1

wait "$app_pid"
