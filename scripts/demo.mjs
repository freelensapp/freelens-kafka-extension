#!/usr/bin/env node
/**
 * Disposable demo environment for the Freelens Kafka extension.
 *
 * Requirements: Docker, kind and Node.js. kubectl is optional: when it is
 * missing, the kubectl bundled in the kind node is used through docker exec.
 *
 *   node scripts/demo.mjs up       create the kind cluster, the brokers, the demo data and the live producer
 *   node scripts/demo.mjs down     delete the kind cluster and the Docker broker
 *   node scripts/demo.mjs status   show what is running
 *
 * Only the dedicated kind cluster and the dedicated Docker container are ever
 * touched, never the user's own `kind` cluster (see TESTING-SAFETY.md).
 */

import { spawn } from "node:child_process";
import process from "node:process";

const CLUSTER = process.env.DEMO_CLUSTER || "freelens-kafka-demo";
const CONTEXT = `kind-${CLUSTER}`;
const NODE_CONTAINER = `${CLUSTER}-control-plane`;
const KAFKA_IMAGE = process.env.DEMO_KAFKA_IMAGE || "apache/kafka:3.9.0";
const KIND_NODE_IMAGE = process.env.DEMO_KIND_NODE_IMAGE || "";
const DIRECT_ENABLED = process.env.DEMO_DIRECT !== "0";
const DIRECT_PORT = Number.parseInt(process.env.DEMO_DIRECT_PORT || "19093", 10);
const DIRECT_CONTAINER = `${CLUSTER}-direct`;
const DIRECT_BOOTSTRAP = `127.0.0.1:${DIRECT_PORT}`;
const PRODUCE_INTERVAL = process.env.DEMO_PRODUCE_INTERVAL_SECONDS || "1";

const KAFKA_NAMESPACE = "kafka-demo";
const APP_NAMESPACE = "shop";
const STRIMZI_CLUSTER = "orders";
const BROKER_POD = `${STRIMZI_CLUSTER}-kafka-0`;
const BROKERS_SERVICE = `${STRIMZI_CLUSTER}-kafka-brokers`;
const BOOTSTRAP_SERVICE = `${STRIMZI_CLUSTER}-kafka-bootstrap`;
const IN_CLUSTER_BOOTSTRAP = `${BOOTSTRAP_SERVICE}.${KAFKA_NAMESPACE}.svc:9092`;
const MARKER_GROUP = "orders-dashboard";
const LIVE_GROUP = "billing-service";
const TOOL_HEAP = "-Xmx128m";
const BROKER_HEAP = "-Xmx512m -Xms256m";

const startedAt = Date.now();
let hostKubectl = false;

function log(message) {
  const elapsed = Math.round((Date.now() - startedAt) / 1000)
    .toString()
    .padStart(4);
  console.log(`[demo ${elapsed}s] ${message}`);
}

/** Bash scripts are shipped through stdin, args or ConfigMaps: keep them LF-only even on Windows checkouts. */
function lf(text) {
  return text.replace(/\r\n/g, "\n");
}

function run(command, args, options = {}) {
  const { input, capture = false, quiet = false, allowFailure = false } = options;
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      stdio: [
        input === undefined ? "ignore" : "pipe",
        capture || quiet ? "pipe" : "inherit",
        capture || quiet ? "pipe" : "inherit",
      ],
    });
    let stdout = "";
    let stderr = "";
    child.stdout?.on("data", (chunk) => {
      stdout += chunk;
    });
    child.stderr?.on("data", (chunk) => {
      stderr += chunk;
    });
    child.on("error", (error) => {
      if (error.code === "ENOENT" && allowFailure) {
        resolve({ code: 127, stdout: "", stderr: `${command}: command not found`, missing: true });
        return;
      }
      reject(new Error(`${command} could not be started: ${error.message}`));
    });
    child.on("close", (code) => {
      const result = { code: code ?? 1, stdout, stderr, missing: false };
      if (result.code !== 0 && !allowFailure) {
        const detail = (stderr || stdout).trim();
        reject(
          new Error(`${command} ${args.join(" ")} failed with exit code ${result.code}${detail ? `:\n${detail}` : ""}`),
        );
        return;
      }
      resolve(result);
    });
    if (input !== undefined) {
      child.stdin.end(lf(input));
    }
  });
}

function kubectl(args, options = {}) {
  if (hostKubectl) {
    return run("kubectl", ["--context", CONTEXT, ...args], options);
  }
  return run(
    "docker",
    ["exec", "-i", NODE_CONTAINER, "kubectl", "--kubeconfig", "/etc/kubernetes/admin.conf", ...args],
    options,
  );
}

/** Run `producer | consumer` with the producer's stdout streamed into the consumer's stdin. */
function pipeRun(producer, consumer) {
  return new Promise((resolve, reject) => {
    const source = spawn(producer[0], producer[1], { stdio: ["ignore", "pipe", "pipe"] });
    const sink = spawn(consumer[0], consumer[1], { stdio: ["pipe", "pipe", "pipe"] });
    let stderr = "";
    let pending = 2;
    let failed = false;
    const fail = (message) => {
      if (failed) return;
      failed = true;
      source.kill();
      sink.kill();
      reject(new Error(message));
    };
    const done = (name, code) => {
      if (code !== 0) {
        fail(`${name} failed with exit code ${code}${stderr.trim() ? `:\n${stderr.trim()}` : ""}`);
        return;
      }
      pending -= 1;
      if (pending === 0 && !failed) resolve();
    };
    source.stderr.on("data", (chunk) => {
      stderr += chunk;
    });
    sink.stderr.on("data", (chunk) => {
      stderr += chunk;
    });
    sink.stdout.on("data", () => {});
    source.on("error", (error) => fail(`${producer[0]} could not be started: ${error.message}`));
    sink.on("error", (error) => fail(`${consumer[0]} could not be started: ${error.message}`));
    source.on("close", (code) => done(producer[0], code ?? 1));
    sink.on("close", (code) => done(consumer[0], code ?? 1));
    source.stdout.pipe(sink.stdin);
  });
}

function sleep(seconds) {
  return new Promise((resolve) => setTimeout(resolve, seconds * 1000));
}

// ---------------------------------------------------------------------------
// Bash scripts executed inside the Kafka containers (String.raw: no `${` here).
// ---------------------------------------------------------------------------

const SEED_SCRIPT = String.raw`#!/usr/bin/env bash
# Seed topics, JSON records and consumer groups on a demo broker.
# Idempotent: it does nothing when the marker consumer group already exists.
set -euo pipefail
B="$1"
T=/opt/kafka/bin
MARKER_GROUP=orders-dashboard

echo "waiting for the broker at $B"
for n in $(seq 1 90); do
  if $T/kafka-topics.sh --bootstrap-server "$B" --list >/dev/null 2>&1; then break; fi
  sleep 2
done
$T/kafka-topics.sh --bootstrap-server "$B" --list >/dev/null

if $T/kafka-consumer-groups.sh --bootstrap-server "$B" --list 2>/dev/null | grep -qx "$MARKER_GROUP"; then
  echo "already seeded, nothing to do"
  exit 0
fi

create() {
  $T/kafka-topics.sh --bootstrap-server "$B" --create --if-not-exists --topic "$1" --partitions "$2" --replication-factor 1 >/dev/null
  echo "topic $1 ready with $2 partitions"
}
produce() {
  $T/kafka-console-producer.sh --bootstrap-server "$B" --topic "$1" --property parse.key=true --property parse.headers=true >/dev/null
}
pick() {
  shift $((RANDOM % $#))
  echo "$1"
}
stamp() {
  date -u +%Y-%m-%dT%H:%M:%SZ
}

create orders 6
create payments 3
create shipments 3
create notifications 1

{
  for i in $(seq 1 120); do
    printf 'source:demo-seed,region:eu-south-1,content-type:application/json\tord-%04d\t{"orderId":"ord-%04d","customer":"cust-%03d","items":%d,"total":%d.%02d,"currency":"EUR","status":"%s","createdAt":"%s"}\n' \
      "$i" "$i" $((RANDOM % 200 + 1)) $((RANDOM % 5 + 1)) $((RANDOM % 400 + 5)) $((RANDOM % 100)) "$(pick created paid shipped delivered cancelled)" "$(stamp)"
  done
} | produce orders
echo "120 orders seeded"

{
  for i in $(seq 1 60); do
    printf 'source:demo-seed,region:eu-south-1,content-type:application/json\tpay-%04d\t{"paymentId":"pay-%04d","orderId":"ord-%04d","amount":%d.%02d,"currency":"EUR","method":"%s","status":"%s","processedAt":"%s"}\n' \
      "$i" "$i" $((i * 2)) $((RANDOM % 400 + 5)) $((RANDOM % 100)) "$(pick card card paypal bank-transfer)" "$(pick authorized captured captured failed)" "$(stamp)"
  done
} | produce payments
echo "60 payments seeded"

{
  for i in $(seq 1 40); do
    printf 'source:demo-seed,region:eu-south-1,content-type:application/json\tshp-%04d\t{"shipmentId":"shp-%04d","orderId":"ord-%04d","carrier":"%s","status":"%s","updatedAt":"%s"}\n' \
      "$i" "$i" $((i * 3)) "$(pick DHL UPS FedEx GLS)" "$(pick label-created in-transit in-transit delivered)" "$(stamp)"
  done
} | produce shipments
echo "40 shipments seeded"

{
  for i in $(seq 1 20); do
    printf 'source:demo-seed,region:eu-south-1,content-type:application/json\tord-%04d\t{"type":"%s","orderId":"ord-%04d","template":"%s","sentAt":"%s"}\n' \
      $((i * 6)) "$(pick email email sms push)" $((i * 6)) "$(pick order-confirmed shipped delivered)" "$(stamp)"
  done
} | produce notifications
echo "20 notifications seeded"

# A consumer group that stopped early: its committed offsets stay behind, so it shows a lag that
# keeps growing while the live producer runs. It is also the marker for "seed completed".
$T/kafka-console-consumer.sh --bootstrap-server "$B" --topic orders --group "$MARKER_GROUP" \
  --from-beginning --max-messages 40 --timeout-ms 60000 >/dev/null
echo "consumer group $MARKER_GROUP left behind with a lag"
echo "seed completed"
`;

const PRODUCER_SCRIPT = String.raw`#!/usr/bin/env bash
# Continuous demo producer: one order every INTERVAL seconds on the orders topic.
set -euo pipefail
B="$1"
INTERVAL="$2"
T=/opt/kafka/bin

until $T/kafka-consumer-groups.sh --bootstrap-server "$B" --list 2>/dev/null | grep -qx orders-dashboard; do
  echo "waiting for the seed to complete"
  sleep 3
done
pick() {
  shift $((RANDOM % $#))
  echo "$1"
}
stamp() {
  date -u +%Y-%m-%dT%H:%M:%SZ
}
echo "producing one order every $INTERVAL second(s) to $B"
i=0
while true; do
  i=$((i + 1))
  printf 'source:demo-live,region:eu-south-1,content-type:application/json\tord-live-%06d\t{"orderId":"ord-live-%06d","customer":"cust-%03d","items":%d,"total":%d.%02d,"currency":"EUR","status":"%s","createdAt":"%s"}\n' \
    "$i" "$i" $((RANDOM % 200 + 1)) $((RANDOM % 5 + 1)) $((RANDOM % 400 + 5)) $((RANDOM % 100)) "$(pick created created paid)" "$(stamp)"
  sleep "$INTERVAL"
done | exec $T/kafka-console-producer.sh --bootstrap-server "$B" --topic orders --property parse.key=true --property parse.headers=true >/dev/null
`;

const CONSUMER_SCRIPT = String.raw`#!/usr/bin/env bash
# Continuous demo consumer: keeps a consumer group active on the orders topic.
set -euo pipefail
B="$1"
GROUP="$2"
T=/opt/kafka/bin

until $T/kafka-consumer-groups.sh --bootstrap-server "$B" --list 2>/dev/null | grep -qx orders-dashboard; do
  echo "waiting for the seed to complete"
  sleep 3
done
echo "consuming orders from $B as group $GROUP"
exec $T/kafka-console-consumer.sh --bootstrap-server "$B" --topic orders --group "$GROUP" --from-beginning >/dev/null
`;

// ---------------------------------------------------------------------------
// Kubernetes manifests for the kind cluster.
// ---------------------------------------------------------------------------

const STRIMZI_CRD = `# Minimal fake Strimzi CRD (not the operator): enough for discovery to find the Kafka CR.
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: kafkas.kafka.strimzi.io
spec:
  group: kafka.strimzi.io
  scope: Namespaced
  names:
    plural: kafkas
    singular: kafka
    kind: Kafka
    shortNames:
      - k
  versions:
    - name: v1beta2
      served: true
      storage: true
      schema:
        openAPIV3Schema:
          type: object
          x-kubernetes-preserve-unknown-fields: true
`;

function indent(text, spaces) {
  const pad = " ".repeat(spaces);
  return lf(text)
    .split("\n")
    .map((line) => (line.length > 0 ? pad + line : line))
    .join("\n");
}

function scriptsConfigMap(namespace) {
  return `apiVersion: v1
kind: ConfigMap
metadata:
  name: demo-scripts
  namespace: ${namespace}
data:
  seed.sh: |
${indent(SEED_SCRIPT, 4)}
  producer.sh: |
${indent(PRODUCER_SCRIPT, 4)}
  consumer.sh: |
${indent(CONSUMER_SCRIPT, 4)}
`;
}

function kafkaManifests() {
  return `apiVersion: v1
kind: Namespace
metadata:
  name: ${KAFKA_NAMESPACE}
---
apiVersion: v1
kind: Namespace
metadata:
  name: ${APP_NAMESPACE}
---
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: ${STRIMZI_CLUSTER}
  namespace: ${KAFKA_NAMESPACE}
  labels:
    app: ${STRIMZI_CLUSTER}-kafka
spec:
  kafka:
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
status:
  listeners:
    - name: plain
      bootstrapServers: ${IN_CLUSTER_BOOTSTRAP}
---
# Headless service: the broker pod DNS name resolves inside the cluster before the pod is Ready.
apiVersion: v1
kind: Service
metadata:
  name: ${BROKERS_SERVICE}
  namespace: ${KAFKA_NAMESPACE}
  labels:
    app: ${STRIMZI_CLUSTER}-kafka
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  selector:
    app: ${STRIMZI_CLUSTER}-kafka
  ports:
    - name: tcp-clients
      port: 9092
---
apiVersion: v1
kind: Service
metadata:
  name: ${BOOTSTRAP_SERVICE}
  namespace: ${KAFKA_NAMESPACE}
  labels:
    app: ${STRIMZI_CLUSTER}-kafka
spec:
  selector:
    app: ${STRIMZI_CLUSTER}-kafka
  ports:
    - name: tcp-clients
      port: 9092
---
# A real single-node KRaft broker advertising a pod DNS listener, like Strimzi does.
apiVersion: v1
kind: Pod
metadata:
  name: ${BROKER_POD}
  namespace: ${KAFKA_NAMESPACE}
  labels:
    app: ${STRIMZI_CLUSTER}-kafka
    strimzi.io/cluster: ${STRIMZI_CLUSTER}
    strimzi.io/broker-role: "true"
spec:
  hostname: ${BROKER_POD}
  subdomain: ${BROKERS_SERVICE}
  containers:
    - name: kafka
      image: ${KAFKA_IMAGE}
      imagePullPolicy: IfNotPresent
      ports:
        - containerPort: 9092
      env:
        - name: KAFKA_NODE_ID
          value: "0"
        - name: KAFKA_PROCESS_ROLES
          value: broker,controller
        - name: KAFKA_LISTENERS
          value: PLAINTEXT://:9092,CONTROLLER://:9093
        - name: KAFKA_ADVERTISED_LISTENERS
          value: PLAINTEXT://${BROKER_POD}.${BROKERS_SERVICE}.${KAFKA_NAMESPACE}.svc:9092
        - name: KAFKA_CONTROLLER_LISTENER_NAMES
          value: CONTROLLER
        - name: KAFKA_LISTENER_SECURITY_PROTOCOL_MAP
          value: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
        - name: KAFKA_CONTROLLER_QUORUM_VOTERS
          value: 0@localhost:9093
        - name: KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR
          value: "1"
        - name: KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR
          value: "1"
        - name: KAFKA_TRANSACTION_STATE_LOG_MIN_ISR
          value: "1"
        - name: KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS
          value: "0"
        - name: KAFKA_HEAP_OPTS
          value: ${BROKER_HEAP}
      readinessProbe:
        exec:
          command:
            - /bin/sh
            - -c
            - /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
        initialDelaySeconds: 10
        periodSeconds: 5
        timeoutSeconds: 10
        failureThreshold: 30
---
${scriptsConfigMap(KAFKA_NAMESPACE)}
---
${scriptsConfigMap(APP_NAMESPACE)}
---
apiVersion: batch/v1
kind: Job
metadata:
  name: demo-seed
  namespace: ${KAFKA_NAMESPACE}
spec:
  backoffLimit: 4
  template:
    spec:
      restartPolicy: OnFailure
      containers:
        - name: seed
          image: ${KAFKA_IMAGE}
          imagePullPolicy: IfNotPresent
          command: ["bash", "/demo/seed.sh", "${IN_CLUSTER_BOOTSTRAP}"]
          env:
            - name: KAFKA_HEAP_OPTS
              value: ${TOOL_HEAP}
          volumeMounts:
            - name: scripts
              mountPath: /demo
      volumes:
        - name: scripts
          configMap:
            name: demo-scripts
---
${appDeployment("order-producer", ["bash", "/demo/producer.sh", IN_CLUSTER_BOOTSTRAP, PRODUCE_INTERVAL], IN_CLUSTER_BOOTSTRAP)}
---
${appDeployment(LIVE_GROUP, ["bash", "/demo/consumer.sh", IN_CLUSTER_BOOTSTRAP, LIVE_GROUP], IN_CLUSTER_BOOTSTRAP)}
${
  DIRECT_ENABLED
    ? `---
# Workload that references the external Docker broker: discovery lists it as an external Kafka.
${appDeployment("checkout-service", ["sh", "-c", "while true; do sleep 3600; done"], DIRECT_BOOTSTRAP)}
`
    : ""
}`;
}

function appDeployment(name, command, bootstrap) {
  const commandYaml = command.map((part) => `            - ${JSON.stringify(part)}`).join("\n");
  return `apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${name}
  namespace: ${APP_NAMESPACE}
  labels:
    app: ${name}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ${name}
  template:
    metadata:
      labels:
        app: ${name}
    spec:
      containers:
        - name: app
          image: ${KAFKA_IMAGE}
          imagePullPolicy: IfNotPresent
          command:
${commandYaml}
          env:
            - name: KAFKA_BOOTSTRAP_SERVERS
              value: ${bootstrap}
            - name: KAFKA_HEAP_OPTS
              value: ${TOOL_HEAP}
          volumeMounts:
            - name: scripts
              mountPath: /demo
      volumes:
        - name: scripts
          configMap:
            name: demo-scripts
`;
}

// ---------------------------------------------------------------------------
// Steps
// ---------------------------------------------------------------------------

async function preflight() {
  const docker = await run("docker", ["info", "--format", "{{.ServerVersion}}"], { quiet: true, allowFailure: true });
  if (docker.missing) throw new Error("Docker is not installed or not in PATH: https://docs.docker.com/get-docker/");
  if (docker.code !== 0) throw new Error("Docker is installed but the daemon is not running. Start Docker and retry.");
  const kind = await run("kind", ["version"], { quiet: true, allowFailure: true });
  if (kind.missing)
    throw new Error("kind is not installed or not in PATH: https://kind.sigs.k8s.io/docs/user/quick-start/");
  const kubectlProbe = await run("kubectl", ["version", "--client"], { quiet: true, allowFailure: true });
  hostKubectl = !kubectlProbe.missing && kubectlProbe.code === 0;
  log(
    `Docker ${docker.stdout.trim()}, ${kind.stdout.trim()}, kubectl ${hostKubectl ? "from PATH" : "from the kind node (not installed locally)"}`,
  );
}

async function clusterExists() {
  const result = await run("kind", ["get", "clusters"], { quiet: true, allowFailure: true });
  return result.stdout
    .split(/\r?\n/)
    .map((line) => line.trim())
    .includes(CLUSTER);
}

async function ensureImage() {
  const present = await run("docker", ["image", "inspect", KAFKA_IMAGE], { quiet: true, allowFailure: true });
  if (present.code === 0) return;
  log(`Pulling ${KAFKA_IMAGE}`);
  await run("docker", ["pull", KAFKA_IMAGE]);
}

async function ensureCluster() {
  if (await clusterExists()) {
    log(`kind cluster ${CLUSTER} already exists, reusing it`);
    return;
  }
  log(`Creating kind cluster ${CLUSTER} (the first run also pulls the node image)`);
  const previousContext = hostKubectl
    ? (await run("kubectl", ["config", "current-context"], { quiet: true, allowFailure: true })).stdout.trim()
    : "";
  const args = ["create", "cluster", "--name", CLUSTER, "--wait", "120s"];
  if (KIND_NODE_IMAGE) args.push("--image", KIND_NODE_IMAGE);
  await run("kind", args);
  // kind switches the current context to the new cluster: give the user back the one they had.
  if (previousContext && previousContext !== CONTEXT) {
    await run("kubectl", ["config", "use-context", previousContext], { quiet: true, allowFailure: true });
    log(`Current kubectl context restored to ${previousContext} (the demo cluster is ${CONTEXT})`);
  }
}

async function imageLoaded() {
  const loaded = await run("docker", ["exec", NODE_CONTAINER, "crictl", "inspecti", KAFKA_IMAGE], {
    quiet: true,
    allowFailure: true,
  });
  return loaded.code === 0;
}

async function loadImage() {
  if (await imageLoaded()) return;
  log(`Loading ${KAFKA_IMAGE} into the kind node`);
  const viaKind = await run("kind", ["load", "docker-image", KAFKA_IMAGE, "--name", CLUSTER], {
    quiet: true,
    allowFailure: true,
  });
  if (viaKind.code === 0 && (await imageLoaded())) return;
  // `kind load` imports with --all-platforms, which fails on Docker Desktop's containerd image store because
  // `docker save` only exports the local platform's layers. Importing the archive without --all-platforms works.
  log("kind load did not work with this Docker image store, importing the image archive directly");
  await pipeRun(
    ["docker", ["save", KAFKA_IMAGE]],
    [
      "docker",
      [
        "exec",
        "-i",
        "--privileged",
        NODE_CONTAINER,
        "ctr",
        "--namespace=k8s.io",
        "images",
        "import",
        "--digests",
        "--snapshotter=overlayfs",
        "-",
      ],
    ],
  );
  if (!(await imageLoaded())) throw new Error(`${KAFKA_IMAGE} is still not available inside the kind node`);
}

async function applyManifests() {
  log("Applying the Strimzi-like Kafka cluster, the demo workloads and the seed job");
  await kubectl(["apply", "-f", "-"], { input: STRIMZI_CRD, quiet: true });
  await kubectl(["wait", "--for=condition=Established", "crd/kafkas.kafka.strimzi.io", "--timeout=60s"], {
    quiet: true,
  });
  await kubectl(["delete", "job", "demo-seed", "-n", KAFKA_NAMESPACE, "--ignore-not-found"], { quiet: true });
  await kubectl(["apply", "-f", "-"], { input: kafkaManifests(), quiet: true });
}

async function waitForSeed() {
  log("Waiting for the in-cluster broker and the seed job (about a minute)");
  const deadline = Date.now() + 300_000;
  while (Date.now() < deadline) {
    const status = await kubectl(
      ["get", "job", "demo-seed", "-n", KAFKA_NAMESPACE, "-o", "jsonpath={.status.succeeded}/{.status.failed}"],
      { capture: true, allowFailure: true },
    );
    const [succeeded, failed] = status.stdout.trim().split("/");
    if (succeeded === "1") return;
    if (Number.parseInt(failed || "0", 10) >= 4) break;
    await sleep(5);
  }
  const logs = await kubectl(["logs", "job/demo-seed", "-n", KAFKA_NAMESPACE, "--tail=40"], {
    capture: true,
    allowFailure: true,
  });
  throw new Error(`The seed job did not complete. Last log lines:\n${logs.stdout || logs.stderr}`);
}

async function waitForWorkloads() {
  const names = ["order-producer", LIVE_GROUP];
  if (DIRECT_ENABLED) names.push("checkout-service");
  for (const name of names) {
    await kubectl(["rollout", "status", `deployment/${name}`, "-n", APP_NAMESPACE, "--timeout=180s"], { quiet: true });
  }
}

async function directContainerState() {
  const inspect = await run("docker", ["inspect", "--format", "{{.State.Running}}", DIRECT_CONTAINER], {
    quiet: true,
    allowFailure: true,
  });
  if (inspect.code !== 0) return "absent";
  return inspect.stdout.trim() === "true" ? "running" : "stopped";
}

async function startDirectBroker() {
  const state = await directContainerState();
  if (state === "running") {
    log(`Docker broker ${DIRECT_CONTAINER} already running on ${DIRECT_BOOTSTRAP}, reusing it`);
    return;
  }
  if (state === "stopped") {
    await run("docker", ["rm", "-f", DIRECT_CONTAINER], { quiet: true });
  }
  log(`Starting the Docker broker ${DIRECT_CONTAINER} on ${DIRECT_BOOTSTRAP}`);
  const controllerPort = DIRECT_PORT + 1;
  const env = {
    KAFKA_NODE_ID: "1",
    KAFKA_PROCESS_ROLES: "broker,controller",
    KAFKA_LISTENERS: `PLAINTEXT://:${DIRECT_PORT},CONTROLLER://:${controllerPort}`,
    KAFKA_ADVERTISED_LISTENERS: `PLAINTEXT://${DIRECT_BOOTSTRAP}`,
    KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER",
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
    KAFKA_CONTROLLER_QUORUM_VOTERS: `1@localhost:${controllerPort}`,
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1",
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1",
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1",
    KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: "0",
    KAFKA_HEAP_OPTS: BROKER_HEAP,
  };
  const args = [
    "run",
    "-d",
    "--name",
    DIRECT_CONTAINER,
    "--label",
    `io.freelens.kafka-demo=${CLUSTER}`,
    "-p",
    `127.0.0.1:${DIRECT_PORT}:${DIRECT_PORT}`,
  ];
  for (const [key, value] of Object.entries(env)) args.push("-e", `${key}=${value}`);
  args.push(KAFKA_IMAGE);
  await run("docker", args, { quiet: true });
}

async function directProcessRunning(marker) {
  const probe = await run("docker", ["exec", DIRECT_CONTAINER, "pgrep", "-f", marker], {
    quiet: true,
    allowFailure: true,
  });
  return probe.code === 0;
}

async function seedDirectBroker() {
  log("Seeding the Docker broker and starting its live producer and consumer");
  await run(
    "docker",
    ["exec", "-i", "-e", `KAFKA_HEAP_OPTS=${TOOL_HEAP}`, DIRECT_CONTAINER, "bash", "-s", "--", DIRECT_BOOTSTRAP],
    {
      input: SEED_SCRIPT,
      quiet: true,
    },
  );
  // The producer keeps its bash wrapper alive (pipeline), the consumer execs into the JVM: probe them differently.
  if (!(await directProcessRunning("producer.sh"))) {
    await run(
      "docker",
      [
        "exec",
        "-d",
        "-e",
        `KAFKA_HEAP_OPTS=${TOOL_HEAP}`,
        DIRECT_CONTAINER,
        "bash",
        "-c",
        lf(PRODUCER_SCRIPT),
        "producer.sh",
        DIRECT_BOOTSTRAP,
        PRODUCE_INTERVAL,
      ],
      { quiet: true },
    );
  }
  if (!(await directProcessRunning("ConsoleConsumer"))) {
    await run(
      "docker",
      [
        "exec",
        "-d",
        "-e",
        `KAFKA_HEAP_OPTS=${TOOL_HEAP}`,
        DIRECT_CONTAINER,
        "bash",
        "-c",
        lf(CONSUMER_SCRIPT),
        "consumer.sh",
        DIRECT_BOOTSTRAP,
        LIVE_GROUP,
      ],
      { quiet: true },
    );
  }
}

function printSummary() {
  const lines = [
    "",
    "Demo environment ready.",
    "",
    `  Kubernetes context : ${CONTEXT} (added to your kubeconfig)`,
    `  In-cluster Kafka   : Strimzi cluster "${STRIMZI_CLUSTER}" in namespace ${KAFKA_NAMESPACE}, reached through a port-forward`,
  ];
  if (DIRECT_ENABLED) {
    lines.push(
      `  External Kafka     : ${DIRECT_BOOTSTRAP} in Docker, referenced by the checkout-service workload, reached directly`,
    );
  }
  lines.push(
    "  Topics             : orders (6 partitions, live producer), payments, shipments, notifications",
    `  Consumer groups    : ${LIVE_GROUP} (active), ${MARKER_GROUP} (stopped, lag keeps growing)`,
    "",
    `In Freelens open the ${CONTEXT} cluster, then Kafka in the sidebar: Clusters, Overview, Topics, Messages (Browse and Tail), Consumer Groups.`,
    "If the extension is not installed yet, install @freelensapp/kafka-extension from the Extensions page.",
    "",
    "Tear down with: npm run demo:down",
  );
  console.log(lines.join("\n"));
}

async function up() {
  await preflight();
  await ensureImage();
  if (DIRECT_ENABLED) await startDirectBroker();
  await ensureCluster();
  await loadImage();
  await applyManifests();
  if (DIRECT_ENABLED) await seedDirectBroker();
  await waitForSeed();
  await waitForWorkloads();
  log("Everything is up");
  printSummary();
}

async function down() {
  await preflight();
  const state = await directContainerState();
  if (state !== "absent") {
    log(`Removing the Docker broker ${DIRECT_CONTAINER}`);
    await run("docker", ["rm", "-f", DIRECT_CONTAINER], { quiet: true });
  }
  if (await clusterExists()) {
    log(`Deleting the kind cluster ${CLUSTER}`);
    await run("kind", ["delete", "cluster", "--name", CLUSTER]);
  } else {
    log(`kind cluster ${CLUSTER} is not there`);
  }
  log("Demo environment removed (the pulled images are kept)");
}

async function status() {
  await preflight();
  const cluster = await clusterExists();
  console.log(`kind cluster ${CLUSTER}: ${cluster ? "running" : "absent"}`);
  console.log(`Docker broker ${DIRECT_CONTAINER}: ${await directContainerState()}`);
  if (cluster) {
    for (const namespace of [KAFKA_NAMESPACE, APP_NAMESPACE]) {
      await kubectl(["get", "pods", "-n", namespace], { allowFailure: true });
    }
  }
}

const usage = `Usage: node scripts/demo.mjs <up|down|status>

Environment variables:
  DEMO_CLUSTER                   kind cluster name (default: ${CLUSTER})
  DEMO_DIRECT=0                  skip the external Docker broker
  DEMO_DIRECT_PORT               host port of the Docker broker (default: ${DIRECT_PORT})
  DEMO_PRODUCE_INTERVAL_SECONDS  live producer interval (default: ${PRODUCE_INTERVAL})
  DEMO_KAFKA_IMAGE               Kafka image (default: ${KAFKA_IMAGE})
  DEMO_KIND_NODE_IMAGE           kind node image, when a specific Kubernetes version is wanted`;

const commands = { up, down, status };
const command = commands[process.argv[2]];
if (!command) {
  console.error(usage);
  process.exit(2);
}
command().catch((error) => {
  console.error(`\n[demo] ${error.message}`);
  process.exit(1);
});
