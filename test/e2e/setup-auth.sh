#!/usr/bin/env bash
set -euo pipefail

container=freelens-kafka-auth

docker exec "${container}" /opt/kafka/bin/kafka-configs.sh \
	--bootstrap-server 127.0.0.1:19093 \
	--alter \
	--add-config 'SCRAM-SHA-256=[iterations=8192,password=alice-secret]' \
	--entity-type users \
	--entity-name alice >/dev/null

docker exec "${container}" /opt/kafka/bin/kafka-configs.sh \
	--bootstrap-server 127.0.0.1:19093 \
	--alter \
	--add-config 'SCRAM-SHA-512=[iterations=8192,password=alice-secret]' \
	--entity-type users \
	--entity-name alice >/dev/null

docker exec "${container}" /opt/kafka/bin/kafka-configs.sh \
	--bootstrap-server 127.0.0.1:19093 \
	--describe \
	--entity-type users \
	--entity-name alice
