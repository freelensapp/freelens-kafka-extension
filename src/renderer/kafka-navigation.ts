export const KAFKA_PAGE_IDS = {
  clusters: "kafka-clusters",
  overview: "kafka-overview",
  topics: "kafka-topics",
  groups: "kafka-groups",
  brokers: "kafka-brokers",
  schemas: "kafka-schema-registry",
  connect: "kafka-connect",
  acls: "kafka-acls",
} as const;

export const KAFKA_RELOAD_ROUTE_KEY = "freelens-kafka.reload-route.v1";
const KAFKA_RELOAD_ROUTE_MAX_AGE_MS = 5 * 60_000;
const kafkaRendererSessionId = `${Date.now()}-${Math.random().toString(36).slice(2)}`;

export interface KafkaReloadRoute {
  pageId: KafkaPageId;
  params: Record<string, string>;
}

interface KafkaReloadRouteSnapshot extends KafkaReloadRoute {
  rendererSessionId: string;
  savedAt: number;
}

interface KafkaReloadRouteStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

export function saveKafkaReloadRoute(
  route: KafkaReloadRoute,
  storage: KafkaReloadRouteStorage = window.localStorage,
  rendererSessionId = kafkaRendererSessionId,
  savedAt = Date.now(),
): void {
  try {
    storage.setItem(KAFKA_RELOAD_ROUTE_KEY, JSON.stringify({ ...route, rendererSessionId, savedAt }));
  } catch {
    // Route restoration is best effort when storage is unavailable.
  }
}

export function readKafkaReloadRoute(
  storage: KafkaReloadRouteStorage = window.localStorage,
  rendererSessionId = kafkaRendererSessionId,
  now = Date.now(),
): KafkaReloadRoute | undefined {
  try {
    const value = storage.getItem(KAFKA_RELOAD_ROUTE_KEY);
    if (!value) return undefined;
    const route = JSON.parse(value) as KafkaReloadRouteSnapshot;
    if (
      !route ||
      typeof route.pageId !== "string" ||
      !route.params ||
      typeof route.params !== "object" ||
      typeof route.rendererSessionId !== "string" ||
      typeof route.savedAt !== "number" ||
      route.rendererSessionId === rendererSessionId ||
      now - route.savedAt > KAFKA_RELOAD_ROUTE_MAX_AGE_MS
    ) {
      storage.removeItem(KAFKA_RELOAD_ROUTE_KEY);
      return undefined;
    }
    return { pageId: route.pageId, params: route.params };
  } catch {
    return undefined;
  }
}

export function clearKafkaReloadRoute(storage: KafkaReloadRouteStorage = window.localStorage): void {
  try {
    storage.removeItem(KAFKA_RELOAD_ROUTE_KEY);
  } catch {
    // Route restoration is best effort when storage is unavailable.
  }
}

export type KafkaPageId = (typeof KAFKA_PAGE_IDS)[keyof typeof KAFKA_PAGE_IDS];
export type KafkaTopicView = "overview" | "messages" | "partitions" | "consumers" | "configuration";
export type ImplementedKafkaTopicView = Extract<
  KafkaTopicView,
  "overview" | "messages" | "partitions" | "consumers" | "configuration"
>;

export interface KafkaNavigationTarget {
  pageId: KafkaPageId;
  params: Record<string, string>;
}

export interface KafkaTargetReference {
  targetId: string;
}

export type KafkaClusterSelection<T extends KafkaTargetReference> =
  | { state: "empty" }
  | { state: "required" }
  | { state: "selected"; target: T; source: "requested" | "remembered" | "single" };

export interface KafkaSelectionStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
}

export interface KafkaQueryParam {
  set(value: string, options: { replaceHistory: boolean }): void;
}

export const KAFKA_SELECTIONS_KEY = "freelens-kafka.selected-clusters.v1";

function readSelections(storage: KafkaSelectionStorage): Record<string, string> {
  try {
    const value = storage.getItem(KAFKA_SELECTIONS_KEY);
    if (!value) return {};
    const parsed = JSON.parse(value) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};
    return Object.fromEntries(
      Object.entries(parsed).filter(
        (entry): entry is [string, string] => typeof entry[1] === "string" && /^kafka-[a-f0-9]{16}$/.test(entry[1]),
      ),
    );
  } catch {
    return {};
  }
}

function writeSelections(storage: KafkaSelectionStorage, selections: Record<string, string>): void {
  try {
    storage.setItem(KAFKA_SELECTIONS_KEY, JSON.stringify(selections));
  } catch {
    // Selection persistence is optional; current-session navigation remains usable.
  }
}

export function rememberedKafkaTargetId(
  storage: KafkaSelectionStorage,
  kubernetesClusterId: string,
): string | undefined {
  return readSelections(storage)[kubernetesClusterId];
}

export function rememberKafkaClusterSelection(
  storage: KafkaSelectionStorage,
  kubernetesClusterId: string,
  target: KafkaTargetReference,
): void {
  if (!kubernetesClusterId || !/^kafka-[a-f0-9]{16}$/.test(target.targetId)) return;
  writeSelections(storage, { ...readSelections(storage), [kubernetesClusterId]: target.targetId });
}

export function forgetKafkaClusterSelection(
  storage: KafkaSelectionStorage,
  kubernetesClusterId: string,
  expectedTargetId?: string,
): void {
  const selections = readSelections(storage);
  if (!(kubernetesClusterId in selections)) return;
  if (expectedTargetId && selections[kubernetesClusterId] !== expectedTargetId) return;
  const { [kubernetesClusterId]: _removed, ...remaining } = selections;
  writeSelections(storage, remaining);
}

export function resolveKafkaClusterSelection<T extends KafkaTargetReference>(
  targets: T[],
  requestedTargetId?: string,
  rememberedTargetId?: string,
): KafkaClusterSelection<T> {
  if (targets.length === 0) return { state: "empty" };

  const requested = targets.find((target) => target.targetId === requestedTargetId);
  if (requested) return { state: "selected", target: requested, source: "requested" };

  const remembered = targets.find((target) => target.targetId === rememberedTargetId);
  if (remembered) return { state: "selected", target: remembered, source: "remembered" };

  const uniqueTargets = [...new Map(targets.map((target) => [target.targetId, target])).values()];
  if (uniqueTargets.length === 1) return { state: "selected", target: uniqueTargets[0], source: "single" };
  return { state: "required" };
}

export function openKafkaCluster(targetId: string): KafkaNavigationTarget {
  return { pageId: KAFKA_PAGE_IDS.overview, params: { target: targetId } };
}

export function openKafkaTopic(targetId: string, topic: string, view: KafkaTopicView): KafkaNavigationTarget {
  return { pageId: KAFKA_PAGE_IDS.topics, params: { target: targetId, topic, view } };
}

export function implementedKafkaTopicView(view?: string): ImplementedKafkaTopicView {
  return view === "messages" || view === "partitions" || view === "consumers" || view === "configuration"
    ? view
    : "overview";
}

export function switchKafkaCluster(pageId: KafkaPageId, targetId: string): KafkaNavigationTarget {
  if (pageId === KAFKA_PAGE_IDS.clusters) return openKafkaCluster(targetId);
  return { pageId, params: { target: targetId } };
}

export function updateKafkaClusterQuery(param: KafkaQueryParam | undefined, query: string): void {
  param?.set(query, { replaceHistory: true });
}
