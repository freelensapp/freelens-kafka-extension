import { Renderer } from "@freelensapp/extensions";
import { computed } from "mobx";
import { kafkaPersistentStateStore } from "../common/kafka-persistent-state-store";
import { KafkaIcon } from "./icon";
import { KafkaAclAvailabilityStore } from "./kafka-acl-availability";
import { KafkaAclPage, type KafkaAclPageProps } from "./kafka-acl-pages";
import { KAFKA_CLUSTER_CATALOG_KEY } from "./kafka-cluster-catalog";
import { KafkaConnectPage, type KafkaConnectPageProps } from "./kafka-connect-pages";
import { KafkaConnectSettingsStore } from "./kafka-connect-settings";
import { KafkaConnectionSettingsStore } from "./kafka-connection-settings";
import { KafkaGroupsPage, type KafkaGroupsPageProps } from "./kafka-group-pages";
import { KafkaIpcRenderer } from "./kafka-ipc";
import { MANUAL_ENDPOINTS_KEY } from "./kafka-manual-endpoints";
import { KAFKA_PAGE_IDS, KAFKA_SELECTIONS_KEY, type KafkaReloadRoute } from "./kafka-navigation";
import { KafkaOverviewSettingsStore } from "./kafka-overview-settings";
import {
  KAFKA_CLUSTER_MENU_MANIFEST,
  KAFKA_CLUSTER_PAGE_MANIFEST,
  KAFKA_ROUTE_OWNER_MENU_MANIFEST,
} from "./kafka-page-manifest";
import { KafkaClustersPage, type KafkaClustersPageProps } from "./kafka-pages";
import { KafkaReloadRouteRestorer } from "./kafka-reload-route-restorer";
import { KafkaResourceCache } from "./kafka-resource-cache";
import {
  KafkaBrokersPage,
  type KafkaBrokersPageProps,
  KafkaClusterOverviewPage,
  type KafkaTargetPageProps,
} from "./kafka-resource-pages";
import { KafkaSchemaRegistryPage, type KafkaSchemaRegistryPageProps } from "./kafka-schema-registry-pages";
import { KafkaSchemaRegistrySettingsStore } from "./kafka-schema-registry-settings";
import { KafkaTopicsPage, type KafkaTopicsPageProps } from "./kafka-topic-pages";
import { createOperationId } from "./kafka-view-model";
import { KafkaWriteSettingsStore } from "./kafka-write-settings";

import type {
  AclsRequest,
  BrokerConfigRequest,
  ClusterHealthRequest,
  DeleteTopicRequest,
  DiscoverRequest,
  GroupDetailRequest,
  GroupsRequest,
  KafkaConnectDetailRequest,
  KafkaConnectRequest,
  KafkaProgressEvent,
  MessageBrowseRequest,
  OverviewRequest,
  ProduceRequest,
  ResetOffsetsRequest,
  SchemaDeleteSubjectRequest,
  SchemaRegisterRequest,
  SchemaSubjectDetailRequest,
  SchemaSubjectNamesRequest,
  TopicConfigRequest,
  TopicConsumersRequest,
  TopicRequest,
} from "../common/ipc";

export default class KafkaExtensionRenderer extends Renderer.LensExtension {
  private readonly connectionSettings = new KafkaConnectionSettingsStore();
  private readonly writeSettings = new KafkaWriteSettingsStore();
  private readonly schemaRegistrySettings = new KafkaSchemaRegistrySettingsStore();
  private readonly connectSettings = new KafkaConnectSettingsStore();
  private readonly overviewSettings = new KafkaOverviewSettingsStore();
  private readonly aclAvailability = new KafkaAclAvailabilityStore();
  private readonly hiddenMenu = computed(() => false);
  private readonly alwaysVisible = computed(() => true);
  private readonly schemaRegistryVisible = computed(() => this.schemaRegistrySettings.hasAny());
  private readonly connectVisible = computed(() => this.connectSettings.hasAny());
  private readonly aclsVisible = computed(() => this.aclAvailability.hasAny());
  private readonly resourceCache = new KafkaResourceCache();

  async onActivate(): Promise<void> {
    const stateStore = kafkaPersistentStateStore();
    stateStore.loadExtension(this);
    stateStore.migrateLegacy(window.localStorage, [
      KAFKA_CLUSTER_CATALOG_KEY,
      MANUAL_ENDPOINTS_KEY,
      KAFKA_SELECTIONS_KEY,
    ]);
  }

  private get client(): KafkaIpcRenderer {
    return KafkaIpcRenderer.createInstance(this);
  }

  /** Id of the cluster this frame is showing, so Main queries the right connected cluster. */
  private get clusterId(): string | undefined {
    return Renderer.Catalog.getActiveCluster()?.id;
  }

  private readonly discover = (request: DiscoverRequest = {}) => {
    const cachedRequest = { clusterId: this.clusterId, ...request };
    const kubernetesClusterId = cachedRequest.clusterId ?? "active";
    const operationId = cachedRequest.operationId ?? createOperationId("discovery");
    return this.resourceCache.loadDiscovery(kubernetesClusterId, operationId, () =>
      this.client.discover({ ...cachedRequest, operationId }),
    );
  };
  private readonly overview = (request: OverviewRequest) => {
    const cachedRequest = { clusterId: this.clusterId, ...request };
    const kubernetesClusterId = cachedRequest.clusterId ?? "active";
    const targetId = cachedRequest.targetId ?? cachedRequest.bootstrap ?? cachedRequest.clusterName;
    const operationId = cachedRequest.operationId ?? createOperationId("overview");
    return this.resourceCache
      .loadOverview(kubernetesClusterId, targetId, operationId, () =>
        this.client.overview({ ...cachedRequest, operationId, targetId }),
      )
      .then((data) => {
        if (targetId) this.aclAvailability.set(targetId, data.aclsAvailable === true);
        return data;
      });
  };
  private readonly health = (request: ClusterHealthRequest) => {
    const cachedRequest = { clusterId: this.clusterId, ...request };
    const kubernetesClusterId = cachedRequest.clusterId ?? "active";
    const targetId = cachedRequest.targetId ?? cachedRequest.bootstrap ?? cachedRequest.clusterName;
    const operationId = cachedRequest.operationId ?? createOperationId("health");
    return this.resourceCache.loadHealth(kubernetesClusterId, targetId, operationId, () =>
      this.client.health({ ...cachedRequest, operationId, targetId }),
    );
  };
  private readonly invalidateHealth = (
    targetId: string,
    options: { closeSessions?: boolean; removePersisted?: boolean },
  ) => this.client.invalidateHealth({ clusterId: this.clusterId, targetId, ...options });
  private readonly topic = (request: TopicRequest) => this.client.topic({ clusterId: this.clusterId, ...request });
  private readonly topicConfig = (request: TopicConfigRequest) =>
    this.client.topicConfig({ clusterId: this.clusterId, ...request });
  private readonly topicConsumers = (request: TopicConsumersRequest) =>
    this.client.topicConsumers({ clusterId: this.clusterId, ...request });
  private readonly brokerConfig = (request: BrokerConfigRequest) =>
    this.client.brokerConfig({ clusterId: this.clusterId, ...request });
  private readonly messagesBrowse = (request: MessageBrowseRequest) =>
    this.client.messagesBrowse({ clusterId: this.clusterId, ...request });
  private readonly groups = (request: GroupsRequest) => this.client.groups({ clusterId: this.clusterId, ...request });
  private readonly groupDetail = (request: GroupDetailRequest) =>
    this.client.groupDetail({ clusterId: this.clusterId, ...request });
  private readonly produce = (request: ProduceRequest) =>
    this.client.produce({ clusterId: this.clusterId, ...request });
  private readonly deleteTopic = (request: DeleteTopicRequest) =>
    this.client.deleteTopic({ clusterId: this.clusterId, ...request });
  private readonly resetOffsets = (request: ResetOffsetsRequest) =>
    this.client.resetOffsets({ clusterId: this.clusterId, ...request });
  private readonly schemaSubjectNames = (request: SchemaSubjectNamesRequest) => this.client.schemaSubjectNames(request);
  private readonly schemaSubjectDetail = (request: SchemaSubjectDetailRequest) =>
    this.client.schemaSubjectDetail(request);
  private readonly schemaRegister = (request: SchemaRegisterRequest) => this.client.schemaRegister(request);
  private readonly schemaDeleteSubject = (request: SchemaDeleteSubjectRequest) =>
    this.client.schemaDeleteSubject(request);
  private readonly connectNames = (request: KafkaConnectRequest) => this.client.connectNames(request);
  private readonly connectDetail = (request: KafkaConnectDetailRequest) => this.client.connectDetail(request);
  private readonly acls = (request: AclsRequest) => this.client.acls({ clusterId: this.clusterId, ...request });
  private readonly reachability = (bootstraps: string[]) =>
    this.resourceCache.loadReachability(this.clusterId ?? "active", bootstraps, (missingBootstraps) =>
      this.client.reachability(missingBootstraps),
    );
  private readonly subscribeProgress = (listener: (progress: KafkaProgressEvent) => void) =>
    this.client.onProgress(listener);
  private readonly restoreReloadRoute = (route: KafkaReloadRoute) => {
    void this.navigate(route.pageId, route.params);
  };

  clusterFrameComponents = [
    {
      id: "kafka-reload-route-restorer",
      Component: () => <KafkaReloadRouteRestorer onRestoreRoute={this.restoreReloadRoute} />,
      shouldRender: this.alwaysVisible,
    },
  ];

  clusterPages = [
    {
      ...KAFKA_CLUSTER_PAGE_MANIFEST[0],
      components: {
        Page: ({ params }: Pick<KafkaClustersPageProps, "params">) => (
          <KafkaClustersPage
            params={params}
            connectionSettings={this.connectionSettings}
            writeSettings={this.writeSettings}
            schemaRegistrySettings={this.schemaRegistrySettings}
            connectSettings={this.connectSettings}
            kubernetesClusterId={this.clusterId}
            resourceCache={this.resourceCache}
            onOpenCluster={(cluster) => {
              void this.navigate(KAFKA_PAGE_IDS.overview, { target: cluster.targetId });
            }}
            discover={this.discover}
            overview={this.overview}
            invalidateHealth={this.invalidateHealth}
            reachability={this.reachability}
            subscribeProgress={this.subscribeProgress}
          />
        ),
      },
    },
    {
      ...KAFKA_CLUSTER_PAGE_MANIFEST[1],
      components: {
        Page: ({ params }: Pick<KafkaTargetPageProps, "params">) => (
          <KafkaClusterOverviewPage
            params={params}
            connectionSettings={this.connectionSettings}
            writeSettings={this.writeSettings}
            overviewSettings={this.overviewSettings}
            kubernetesClusterId={this.clusterId}
            resourceCache={this.resourceCache}
            discover={this.discover}
            overview={this.overview}
            health={this.health}
            reachability={this.reachability}
            subscribeProgress={this.subscribeProgress}
          />
        ),
      },
    },
    {
      ...KAFKA_CLUSTER_PAGE_MANIFEST[2],
      components: {
        Page: ({ params }: Pick<KafkaTopicsPageProps, "params">) => (
          <KafkaTopicsPage
            params={params}
            connectionSettings={this.connectionSettings}
            writeSettings={this.writeSettings}
            kubernetesClusterId={this.clusterId}
            resourceCache={this.resourceCache}
            discover={this.discover}
            overview={this.overview}
            topic={this.topic}
            topicConfig={this.topicConfig}
            topicConsumers={this.topicConsumers}
            onOpenGroup={(targetId, groupId) => {
              void this.navigate(KAFKA_PAGE_IDS.groups, { target: targetId, group: groupId, view: "offsets" });
            }}
            messagesBrowse={this.messagesBrowse}
            produce={this.produce}
            deleteTopic={this.deleteTopic}
            schemaRegistrySettings={this.schemaRegistrySettings}
            reachability={this.reachability}
            subscribeProgress={this.subscribeProgress}
          />
        ),
      },
    },
    {
      ...KAFKA_CLUSTER_PAGE_MANIFEST[3],
      components: {
        Page: ({ params }: Pick<KafkaGroupsPageProps, "params">) => (
          <KafkaGroupsPage
            params={params}
            connectionSettings={this.connectionSettings}
            writeSettings={this.writeSettings}
            kubernetesClusterId={this.clusterId}
            resourceCache={this.resourceCache}
            discover={this.discover}
            overview={this.overview}
            groups={this.groups}
            groupDetail={this.groupDetail}
            resetOffsets={this.resetOffsets}
            onOpenTopic={(targetId, topic) => {
              void this.navigate(KAFKA_PAGE_IDS.topics, { target: targetId, topic, view: "overview" });
            }}
            reachability={this.reachability}
            subscribeProgress={this.subscribeProgress}
          />
        ),
      },
    },
    {
      ...KAFKA_CLUSTER_PAGE_MANIFEST[4],
      components: {
        Page: ({ params }: Pick<KafkaBrokersPageProps, "params">) => (
          <KafkaBrokersPage
            params={params}
            connectionSettings={this.connectionSettings}
            writeSettings={this.writeSettings}
            kubernetesClusterId={this.clusterId}
            resourceCache={this.resourceCache}
            discover={this.discover}
            overview={this.overview}
            brokerConfig={this.brokerConfig}
            reachability={this.reachability}
            subscribeProgress={this.subscribeProgress}
          />
        ),
      },
    },
    {
      ...KAFKA_CLUSTER_PAGE_MANIFEST[5],
      components: {
        Page: ({ params }: Pick<KafkaSchemaRegistryPageProps, "params">) => (
          <KafkaSchemaRegistryPage
            params={params}
            connectionSettings={this.connectionSettings}
            writeSettings={this.writeSettings}
            kubernetesClusterId={this.clusterId}
            resourceCache={this.resourceCache}
            discover={this.discover}
            overview={this.overview}
            schemaRegistrySettings={this.schemaRegistrySettings}
            schemaSubjectNames={this.schemaSubjectNames}
            schemaSubjectDetail={this.schemaSubjectDetail}
            schemaRegister={this.schemaRegister}
            schemaDeleteSubject={this.schemaDeleteSubject}
            reachability={this.reachability}
            subscribeProgress={this.subscribeProgress}
          />
        ),
      },
    },
    {
      ...KAFKA_CLUSTER_PAGE_MANIFEST[6],
      components: {
        Page: ({ params }: Pick<KafkaConnectPageProps, "params">) => (
          <KafkaConnectPage
            params={params}
            connectionSettings={this.connectionSettings}
            writeSettings={this.writeSettings}
            kubernetesClusterId={this.clusterId}
            resourceCache={this.resourceCache}
            discover={this.discover}
            overview={this.overview}
            connectSettings={this.connectSettings}
            connectNames={this.connectNames}
            connectDetail={this.connectDetail}
            connectPause={(request) => this.client.connectPause(request)}
            connectResume={(request) => this.client.connectResume(request)}
            connectDelete={(request) => this.client.connectDelete(request)}
            connectRestart={(request) => this.client.connectRestart(request)}
            connectUpdate={(request) => this.client.connectUpdate(request)}
            connectCreate={(request) => this.client.connectCreate(request)}
            reachability={this.reachability}
            subscribeProgress={this.subscribeProgress}
          />
        ),
      },
    },
    {
      ...KAFKA_CLUSTER_PAGE_MANIFEST[7],
      components: {
        Page: ({ params }: Pick<KafkaAclPageProps, "params">) => (
          <KafkaAclPage
            params={params}
            connectionSettings={this.connectionSettings}
            writeSettings={this.writeSettings}
            kubernetesClusterId={this.clusterId}
            resourceCache={this.resourceCache}
            discover={this.discover}
            overview={this.overview}
            acls={this.acls}
            aclCreate={(request) => this.client.aclCreate({ clusterId: this.clusterId, ...request })}
            aclDelete={(request) => this.client.aclDelete({ clusterId: this.clusterId, ...request })}
            reachability={this.reachability}
            subscribeProgress={this.subscribeProgress}
          />
        ),
      },
    },
  ];

  clusterPageMenus = [
    ...KAFKA_ROUTE_OWNER_MENU_MANIFEST.map(({ pageId, ...menu }) => ({
      ...menu,
      title: "",
      target: { pageId },
      components: {},
      visible: this.hiddenMenu,
    })),
    ...KAFKA_CLUSTER_MENU_MANIFEST.map(({ pageId, ...menu }, index) => ({
      ...menu,
      target: { pageId },
      components: index === 0 ? { Icon: KafkaIcon } : {},
      visible:
        pageId === KAFKA_PAGE_IDS.connect
          ? this.connectVisible
          : pageId === KAFKA_PAGE_IDS.schemas
            ? this.schemaRegistryVisible
            : pageId === KAFKA_PAGE_IDS.acls
              ? this.aclsVisible
              : undefined,
    })),
  ];

  openClusters = () => this.navigate(KAFKA_PAGE_IDS.clusters);
}
