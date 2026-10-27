#!/usr/bin/env bash
#
# The labs of the tests and scripts of the repository (SPEC-019, REQ-232): a kind cluster of their own,
# `freelens-kafka-lab`, whose kubeconfig is a file of its own, `.lab/kubeconfig`, and the docker-compose brokers.
# Nothing here reads or writes the developer's kubeconfig, and every kubectl names the lab's file and context.
#
#   scripts/lab.sh up                 create the kind cluster and load the Kafka image into it
#   scripts/lab.sh apply <profile>    apply a fixture profile of test/e2e/fixtures
#   scripts/lab.sh delete <profile>   delete it again
#   scripts/lab.sh kubectl <args…>    kubectl on the lab, for the itests and by hand
#   scripts/lab.sh status             the lab cluster, its pods, and the compose brokers
#   scripts/lab.sh compose <name> up|down   a compose broker (poc|direct|acl|auth), project freelens-kafka-e2e-<name>
#   scripts/lab.sh down               delete the kind cluster and its kubeconfig
#
# Profiles: base (a plain broker), p25 (a broker for the two-phase connect), strimzi (Strimzi CRD and fixture objects),
# strimzi-real (Strimzi CRD and a real broker), external, direct, auth (workloads that reference brokers).
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fixtures="${repo}/test/e2e/fixtures"
cluster="${LAB_CLUSTER:-freelens-kafka-lab}"
context="kind-${cluster}"
kubeconfig="${repo}/.lab/kubeconfig"
node_image="${LAB_NODE_IMAGE:-}"
kafka_image="apache/kafka:3.9.0"

die() {
  echo "[lab] error: $*" >&2
  exit 1
}

# The lab's kubeconfig, refused when it names a server that is not on this machine.
checked_kubeconfig() {
  [[ -f ${kubeconfig} ]] || die "no lab kubeconfig at ${kubeconfig}: run 'pnpm lab:up' first"
  if grep -E '^\s*server:' "${kubeconfig}" | grep -vqE 'https://(127\.0\.0\.1|localhost|\[::1\]):'; then
    die "${kubeconfig} names a server that is not local"
  fi
}

k() {
  checked_kubeconfig
  kubectl --kubeconfig "${kubeconfig}" --context "${context}" "$@"
}

profile_files() {
  case "$1" in
    base) echo "${fixtures}/kafka-kind.yaml" ;;
    p25) echo "${fixtures}/kafka-p25-kind.yaml" ;;
    strimzi) echo "${fixtures}/strimzi-crd-kind.yaml ${fixtures}/strimzi-fixtures-kind.yaml" ;;
    strimzi-real) echo "${fixtures}/strimzi-crd-kind.yaml ${fixtures}/strimzi-real-kind.yaml" ;;
    external) echo "${fixtures}/workload-external-kind.yaml" ;;
    direct) echo "${fixtures}/workload-direct-kind.yaml" ;;
    auth) echo "${fixtures}/workload-auth-kind.yaml" ;;
    *) die "unknown profile: $1 (base|p25|strimzi|strimzi-real|external|direct|auth)" ;;
  esac
}

compose_file() {
  case "$1" in
    poc) echo "${repo}/test/e2e/docker-compose.yml" ;;
    direct) echo "${repo}/test/e2e/docker-compose.direct.yml" ;;
    acl) echo "${repo}/test/e2e/docker-compose.acl.yml" ;;
    auth) echo "${repo}/test/e2e/docker-compose.auth.yml" ;;
    *) die "unknown compose broker: $1 (poc|direct|acl|auth)" ;;
  esac
}

cmd="${1:-}"
case "${cmd}" in
  up)
    mkdir -p "$(dirname "${kubeconfig}")"
    if kind get clusters 2>/dev/null | grep -qx "${cluster}"; then
      echo "[lab] the kind cluster ${cluster} exists"
      [[ -f ${kubeconfig} ]] || kind export kubeconfig --name "${cluster}" --kubeconfig "${kubeconfig}"
    else
      kind create cluster --name "${cluster}" --kubeconfig "${kubeconfig}" ${node_image:+--image "${node_image}"} --wait 3m
    fi
    chmod 600 "${kubeconfig}"
    docker image inspect "${kafka_image}" >/dev/null 2>&1 || docker pull "${kafka_image}"
    kind load docker-image "${kafka_image}" --name "${cluster}"
    k get nodes
    ;;
  apply | delete)
    profile="${2:?a profile}"
    for file in $(profile_files "${profile}"); do
      if [[ ${cmd} == apply ]]; then
        k apply -f "${file}"
        # The Strimzi objects need their CRD served first.
        if [[ ${file} == */strimzi-crd-kind.yaml ]]; then
          k wait --for=condition=Established crd/kafkas.kafka.strimzi.io --timeout=60s
        fi
      else
        k delete -f "${file}" --ignore-not-found
      fi
    done
    if [[ ${cmd} == apply && ${profile} =~ ^(base|p25)$ ]]; then
      k wait --for=condition=Ready pod/kafka-0 --timeout=180s
    fi
    if [[ ${cmd} == apply && ${profile} == strimzi-real ]]; then
      k wait --for=condition=Ready pod/my-cluster-kafka-0 --timeout=180s
    fi
    ;;
  kubectl)
    shift
    k "$@"
    ;;
  status)
    kind get clusters 2>/dev/null | grep -x "${cluster}" || echo "[lab] no kind cluster ${cluster}"
    if [[ -f ${kubeconfig} ]]; then
      # A lab that is down is reported, not an error.
      # shellcheck disable=SC2310
      pods="$(k get pods -A 2>/dev/null || true)"
      grep -v kube-system <<<"${pods}" || true
    fi
    docker ps -a --format '{{.Names}}\t{{.Status}}' | grep -E "^(${cluster}-control-plane|freelens-kafka-(poc|direct|acl|auth))\b" || true
    ;;
  compose)
    name="${2:?poc|direct|acl|auth}"
    action="${3:?up|down}"
    file="$(compose_file "${name}")"
    if [[ ${action} == up ]]; then
      docker compose -p "freelens-kafka-e2e-${name}" -f "${file}" up -d --wait
      case "${name}" in
        direct) bash "${repo}/test/e2e/setup-direct.sh" ;;
        auth) bash "${repo}/test/e2e/setup-auth.sh" ;;
        *) ;;
      esac
    else
      docker compose -p "freelens-kafka-e2e-${name}" -f "${file}" down -v
    fi
    ;;
  down)
    if kind get clusters 2>/dev/null | grep -qx "${cluster}"; then
      kind delete cluster --name "${cluster}" --kubeconfig "${kubeconfig}"
    fi
    rm -f "${kubeconfig}"
    ;;
  *)
    sed -n '6,16p' "$0"
    exit 2
    ;;
esac
