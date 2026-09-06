import { Renderer } from "@freelensapp/extensions";
import { useEffect, useMemo, useState } from "react";
import { KafkaPageShell } from "./kafka-page-shell";
import { type KafkaResourcePageDependencies, useKafkaPageParam, useKafkaResourcePage } from "./kafka-resource-pages";
import { canSubmitWriteAction } from "./kafka-write-policy";

import type { HTMLAttributes } from "react";

import type { ConnectorDetailDto, ConnectorSummaryDto, KafkaConnectCreateRequest } from "../common/ipc";
import type { KafkaConnectSettingsStore } from "./kafka-connect-settings";

export interface KafkaConnectPageProps extends KafkaResourcePageDependencies {
  params?: {
    target: Renderer.Navigation.PageParam<string>;
    query: Renderer.Navigation.PageParam<string>;
    connector: Renderer.Navigation.PageParam<string>;
  };
  connectSettings: KafkaConnectSettingsStore;
  connectNames: (request: { connectUrl: string; connectUsername?: string }) => Promise<string[]>;
  connectDetail: (request: {
    connectUrl: string;
    connectUsername?: string;
    connector: string;
  }) => Promise<ConnectorDetailDto>;
  connectPause: (request: { connectUrl: string; connectUsername?: string; connector: string }) => Promise<void>;
  connectResume: (request: { connectUrl: string; connectUsername?: string; connector: string }) => Promise<void>;
  connectDelete: (request: { connectUrl: string; connectUsername?: string; connector: string }) => Promise<void>;
  connectRestart: (request: { connectUrl: string; connectUsername?: string; connector: string }) => Promise<void>;
  connectUpdate: (request: KafkaConnectCreateRequest) => Promise<void>;
  connectCreate: (request: KafkaConnectCreateRequest) => Promise<{ name: string }>;
}

export function KafkaConnectPage({
  params,
  connectSettings,
  connectNames,
  connectDetail,
  connectPause,
  connectResume,
  connectDelete,
  connectRestart,
  connectUpdate,
  connectCreate,
  ...dependencies
}: KafkaConnectPageProps) {
  const state = useKafkaResourcePage({ ...dependencies, params });
  const [query, setQuery] = useKafkaPageParam(params?.query);
  const [connector, setConnector] = useKafkaPageParam(params?.connector);
  const selectedTargetId = state.selectedCluster?.targetId;
  const settings = useMemo(
    () => (selectedTargetId ? connectSettings.get(selectedTargetId) : undefined),
    [connectSettings, selectedTargetId],
  );
  const [items, setItems] = useState<ConnectorSummaryDto[]>([]);
  const [detail, setDetail] = useState<ConnectorDetailDto>();
  const [error, setError] = useState<string>();

  useEffect(() => {
    if (!settings) return void setItems([]);
    connectNames(settings)
      .then((names) => setItems(names.map((name) => ({ name, status: "Details on open", taskCount: 0 }))))
      .catch((reason: unknown) => setError(reason instanceof Error ? reason.message : String(reason)));
  }, [connectNames, settings]);
  useEffect(() => {
    if (!settings || !connector) return void setDetail(undefined);
    connectDetail({ ...settings, connector })
      .then(setDetail)
      .catch((reason: unknown) => setError(reason instanceof Error ? reason.message : String(reason)));
  }, [connectDetail, connector, settings]);

  const filtered = items.filter((item) => item.name.toLowerCase().includes(query.toLowerCase()));
  const canWrite = Boolean(state.selectedCluster && dependencies.writeSettings.get(state.selectedCluster.targetId));
  const [confirmed, setConfirmed] = useState(false);
  const [deleteText, setDeleteText] = useState("");
  const [status, setStatus] = useState<string>();
  const [configText, setConfigText] = useState(
    '{\n  "name": "new-connector",\n  "connector.class": "ExampleSource"\n}',
  );
  const [configError, setConfigError] = useState<string>();
  if (!settings)
    return (
      <KafkaPageShell title="Kafka Connect" subtitle="No Connect endpoint configured">
        <div className="KafkaPageState empty" data-testid="kafka-connect-unconfigured">
          Configure a Connect endpoint in Kafka settings.
        </div>
      </KafkaPageShell>
    );
  return (
    <KafkaPageShell title="Kafka Connect" subtitle={settings.connectUrl}>
      <main className="KafkaWorkspace" data-testid="kafka-connect-page">
        <section className="KafkaListPane">
          <Renderer.Component.Input
            value={query}
            onChange={(value) => setQuery(value, true)}
            placeholder="Filter connectors"
            aria-label="Filter connectors"
          />
          {error && <div role="alert">{error}</div>}
          <Renderer.Component.Table className="KafkaConnectTable">
            <Renderer.Component.TableHead>
              <Renderer.Component.TableCell>Name</Renderer.Component.TableCell>
              <Renderer.Component.TableCell>Type</Renderer.Component.TableCell>
              <Renderer.Component.TableCell>Status</Renderer.Component.TableCell>
              <Renderer.Component.TableCell>Tasks</Renderer.Component.TableCell>
            </Renderer.Component.TableHead>
            {filtered.map((item) => {
              const interaction: HTMLAttributes<HTMLDivElement> = {
                tabIndex: 0,
                onClick: () => setConnector(item.name),
                onKeyDown: (event) => {
                  if (event.key === "Enter" || event.key === " ") {
                    event.preventDefault();
                    setConnector(item.name);
                  }
                },
              };
              return (
                <Renderer.Component.TableRow {...interaction} key={item.name} data-connector={item.name}>
                  <Renderer.Component.TableCell>{item.name}</Renderer.Component.TableCell>
                  <Renderer.Component.TableCell>{item.type ?? "unknown"}</Renderer.Component.TableCell>
                  <Renderer.Component.TableCell>{item.status}</Renderer.Component.TableCell>
                  <Renderer.Component.TableCell>{item.taskCount}</Renderer.Component.TableCell>
                </Renderer.Component.TableRow>
              );
            })}
          </Renderer.Component.Table>
        </section>
        {detail && (
          <aside className="KafkaDetailPane" data-testid="kafka-connect-detail">
            <h2>{detail.name}</h2>
            <p>{detail.connector.state}</p>
            {detail.tasks.map((task) => (
              <article key={task.id}>
                <strong>
                  Task {task.id}: {task.state}
                </strong>
                {task.trace && <pre>{task.trace}</pre>}
              </article>
            ))}
            {canWrite && settings && (
              <section data-testid="kafka-connect-write-controls">
                <Renderer.Component.Switch
                  checked={confirmed}
                  onChange={setConfirmed}
                  aria-label="Confirm Connect action"
                />
                <Renderer.Component.Button
                  disabled={!canSubmitWriteAction({ confirmationAccepted: confirmed })}
                  onClick={() =>
                    void connectPause({ ...settings, connector }).then(() => setStatus("Paused connector"))
                  }
                >
                  Pause
                </Renderer.Component.Button>
                <Renderer.Component.Button
                  disabled={!canSubmitWriteAction({ confirmationAccepted: confirmed })}
                  onClick={() =>
                    void connectResume({ ...settings, connector }).then(() => setStatus("Resumed connector"))
                  }
                >
                  Resume
                </Renderer.Component.Button>
                <Renderer.Component.Button
                  disabled={!canSubmitWriteAction({ confirmationAccepted: confirmed })}
                  onClick={() =>
                    void connectRestart({ ...settings, connector }).then(() => setStatus("Restarted connector"))
                  }
                >
                  Restart
                </Renderer.Component.Button>
                <Renderer.Component.Input
                  value={deleteText}
                  onChange={setDeleteText}
                  aria-label="Type connector to confirm deletion"
                />
                <Renderer.Component.Button
                  disabled={
                    !canSubmitWriteAction({
                      confirmationAccepted: confirmed,
                      requiredResourceName: connector,
                      enteredResourceName: deleteText,
                    })
                  }
                  onClick={() =>
                    void connectDelete({ ...settings, connector }).then(() =>
                      setStatus(`Deleted connector ${connector}`),
                    )
                  }
                >
                  Delete
                </Renderer.Component.Button>
                <textarea
                  value={configText}
                  onChange={(event) => setConfigText(event.target.value)}
                  aria-label="Connector JSON configuration"
                />
                <Renderer.Component.Button
                  disabled={!canSubmitWriteAction({ confirmationAccepted: confirmed })}
                  onClick={() => {
                    try {
                      const config = JSON.parse(configText) as Record<string, string>;
                      void connectUpdate({ ...settings, config }).then(() => setStatus("Updated connector"));
                      setConfigError(undefined);
                    } catch {
                      setConfigError("Invalid connector JSON");
                    }
                  }}
                >
                  Update connector
                </Renderer.Component.Button>
                {configError && <div role="alert">{configError}</div>}
                {status && <div role="status">{status}</div>}
              </section>
            )}
          </aside>
        )}
        {canWrite && settings && (
          <section data-testid="kafka-connect-create-controls">
            <textarea
              value={configText}
              onChange={(event) => setConfigText(event.target.value)}
              aria-label="New connector JSON configuration"
            />
            <Renderer.Component.Button
              disabled={!canSubmitWriteAction({ confirmationAccepted: confirmed })}
              onClick={() => {
                try {
                  const config = JSON.parse(configText) as Record<string, string>;
                  void connectCreate({ ...settings, config }).then((created) => {
                    setConnector(created.name);
                    setStatus("Created connector");
                  });
                  setConfigError(undefined);
                } catch {
                  setConfigError("Invalid connector JSON");
                }
              }}
            >
              Create connector
            </Renderer.Component.Button>
          </section>
        )}
      </main>
    </KafkaPageShell>
  );
}
