#!/usr/bin/env bash
set -euo pipefail

docker exec freelens-kafka-direct \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server 127.0.0.1:19092 \
	--create \
	--if-not-exists \
	--topic freelens-orders \
	--partitions 3 \
	--replication-factor 1

docker exec freelens-kafka-direct \
	/opt/kafka/bin/kafka-topics.sh \
	--bootstrap-server 127.0.0.1:19092 \
	--create \
	--if-not-exists \
	--topic freelens-orders-archive-with-a-very-long-topic-name-for-layout-validation \
	--partitions 1 \
	--replication-factor 1

pnpm tsx test/e2e/setup-direct-messages.ts
