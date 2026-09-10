#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PID_FILE="${ROOT_DIR}/.tail-producer.pid"
LOG_FILE="${ROOT_DIR}/.tail-producer.log"
TOPIC="${KAFKA_TAIL_TOPIC:-freelens-orders}"
BROKER="${KAFKA_TAIL_BROKER:-127.0.0.1:19092}"
CONTAINER="${KAFKA_TAIL_CONTAINER:-freelens-kafka-direct}"
INTERVAL_SECONDS="${KAFKA_TAIL_INTERVAL_SECONDS:-1}"

print_help() {
	cat <<'EOF'
Usage: scripts/kafka-tail-local.sh <command>

Commands:
  seed      Seed deterministic fixture messages into freelens-orders
  start     Start continuous local message producer in background
  stop      Stop background producer
  status    Show producer status
  logs      Show producer logs (tail -n 40)

Optional env vars:
  KAFKA_TAIL_TOPIC
  KAFKA_TAIL_BROKER
  KAFKA_TAIL_CONTAINER
  KAFKA_TAIL_INTERVAL_SECONDS
EOF
}

require_container() {
	if ! docker ps --format '{{.Names}}' | grep -qx "${CONTAINER}"; then
		echo "Kafka container '${CONTAINER}' is not running." >&2
		echo "Start it first with: pnpm kafka:direct:up" >&2
		exit 1
	fi
}

seed_messages() {
	require_container
	cd "${ROOT_DIR}"
	pnpm tsx test/e2e/setup-direct-messages.ts
	echo "Seed completed for topic '${TOPIC}'."
}

run_loop() {
	require_container
	local i=1

	while true; do
		local ts
		ts="$(date +%s)"
		local payload
		payload=$(printf '{"id":%d,"state":"live-tail","ts":%s}' "${i}" "${ts}")

		docker exec "${CONTAINER}" /bin/bash -lc \
			"echo '${payload}' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server ${BROKER} --topic ${TOPIC} >/dev/null 2>&1"

		i=$((i + 1))
		sleep "${INTERVAL_SECONDS}"
	done
}

start_loop() {
	if [[ -f ${PID_FILE} ]]; then
		local existing_pid
		existing_pid="$(cat "${PID_FILE}")"

		if kill -0 "${existing_pid}" 2>/dev/null; then
			echo "Tail producer already running (pid ${existing_pid})."
			echo "Stop it with: scripts/kafka-tail-local.sh stop"
			exit 0
		fi

		rm -f "${PID_FILE}"
	fi

	require_container
	nohup "$0" run-loop >"${LOG_FILE}" 2>&1 &
	local pid=$!
	echo "${pid}" >"${PID_FILE}"

	echo "Tail producer started (pid ${pid})."
	echo "Topic: ${TOPIC}"
	echo "Logs:  ${LOG_FILE}"
	echo "Stop with: scripts/kafka-tail-local.sh stop"
}

stop_loop() {
	if [[ ! -f ${PID_FILE} ]]; then
		echo "Tail producer is not running."
		exit 0
	fi

	local pid
	pid="$(cat "${PID_FILE}")"

	if kill -0 "${pid}" 2>/dev/null; then
		kill "${pid}"
		echo "Stopped tail producer (pid ${pid})."
	else
		echo "Tail producer pid file existed, but process was not running."
	fi

	rm -f "${PID_FILE}"
}

status_loop() {
	if [[ -f ${PID_FILE} ]]; then
		local pid
		pid="$(cat "${PID_FILE}")"

		if kill -0 "${pid}" 2>/dev/null; then
			echo "Tail producer is running (pid ${pid})."
			exit 0
		fi

		echo "Tail producer pid file exists but process is not running."
		exit 1
	fi

	echo "Tail producer is not running."
}

show_logs() {
	if [[ -f ${LOG_FILE} ]]; then
		tail -n 40 "${LOG_FILE}"
	else
		echo "No log file yet: ${LOG_FILE}"
	fi
}

case "${1:-}" in
seed)
	seed_messages
	;;
start)
	start_loop
	;;
stop)
	stop_loop
	;;
status)
	status_loop
	;;
logs)
	show_logs
	;;
run-loop)
	run_loop
	;;
-h | --help | help | "")
	print_help
	;;
*)
	echo "Unknown command: $1" >&2
	print_help
	exit 1
	;;
esac
