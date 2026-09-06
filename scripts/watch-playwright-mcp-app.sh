#!/usr/bin/env bash
set -euo pipefail

launcher_pid=$1
app_pid=$2
port=$3
runtime_root=$4
runtime_marker=$5
remove_runtime=$6

tail --pid="$launcher_pid" -f /dev/null >/dev/null 2>&1 || true

if [[ -r "/proc/$app_pid/cmdline" ]] &&
  tr '\0' '\n' <"/proc/$app_pid/cmdline" | grep -Fxq -- "--remote-debugging-port=$port"; then
  kill "$app_pid" 2>/dev/null || true
  tail --pid="$app_pid" -f /dev/null >/dev/null 2>&1 || true
fi

marker_file="$runtime_root/.freelens-kafka-mcp-runtime"

if [[ "$remove_runtime" == "1" && -f "$marker_file" && $(<"$marker_file") == "$runtime_marker" ]]; then
  rm -rf -- "$runtime_root"
fi