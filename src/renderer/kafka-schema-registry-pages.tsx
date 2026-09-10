import { Renderer } from "@freelensapp/extensions";
import { useEffect, useMemo, useState } from "react";
import { KafkaPageShell } from "./kafka-page-shell";
import { type KafkaResourcePageDependencies, useKafkaPageParam, useKafkaResourcePage } from "./kafka-resource-pages";
import { canSubmitWriteAction } from "./kafka-write-policy";

import type { HTMLAttributes } from "react";

import type { SchemaSubjectDetail, SchemaSubjectSummary } from "../common/ipc";
import type { KafkaEndpointSecretsStore } from "./kafka-endpoint-secrets";
import type { KafkaSchemaRegistrySettingsStore } from "./kafka-schema-registry-settings";

export interface KafkaSchemaRegistryPageProps extends KafkaResourcePageDependencies {
  params?: {
    target: Renderer.Navigation.PageParam<string>;
    query: Renderer.Navigation.PageParam<string>;
    subject: Renderer.Navigation.PageParam<string>;
  };
  schemaRegistrySettings: KafkaSchemaRegistrySettingsStore;
  endpointSecrets: KafkaEndpointSecretsStore;
  schemaSubjectNames: (request: {
    registryUrl: string;
    registryUsername?: string;
    registryPassword?: string;
  }) => Promise<string[]>;
  schemaSubjectDetail: (request: {
    registryUrl: string;
    registryUsername?: string;
    registryPassword?: string;
    subject: string;
  }) => Promise<SchemaSubjectDetail>;
  schemaRegister: (request: {
    registryUrl: string;
    registryUsername?: string;
    registryPassword?: string;
    subject: string;
    schema: string;
    schemaType?: string;
  }) => Promise<{ id: number }>;
  schemaDeleteSubject: (request: {
    registryUrl: string;
    registryUsername?: string;
    registryPassword?: string;
    subject: string;
  }) => Promise<number[]>;
}

export function KafkaSchemaRegistryPage({
  params,
  schemaRegistrySettings,
  endpointSecrets,
  schemaSubjectNames,
  schemaSubjectDetail,
  schemaRegister,
  schemaDeleteSubject,
  ...dependencies
}: KafkaSchemaRegistryPageProps) {
  const state = useKafkaResourcePage({ ...dependencies, params });
  const [query, setQuery] = useKafkaPageParam(params?.query);
  const [subject, setSubject] = useKafkaPageParam(params?.subject);
  const selectedTargetId = state.selectedCluster?.targetId;
  // The IPC request shape (registryUsername/registryPassword), not the persisted settings shape.
  const registry = useMemo(() => {
    const configured = selectedTargetId ? schemaRegistrySettings.get(selectedTargetId) : undefined;
    if (!configured || !selectedTargetId) return undefined;
    return {
<<<<<<< HEAD
      targetId: selectedTargetId,
=======
>>>>>>> origin/main
      registryUrl: configured.registryUrl,
      registryUsername: configured.username,
      registryPassword: endpointSecrets.get(selectedTargetId)?.registryPassword,
    };
  }, [endpointSecrets, schemaRegistrySettings, selectedTargetId]);
  const [subjects, setSubjects] = useState<SchemaSubjectSummary[]>([]);
  const [detail, setDetail] = useState<SchemaSubjectDetail>();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>();
  const canWrite = Boolean(state.selectedCluster && dependencies.writeSettings.get(state.selectedCluster.targetId));
  const [newSchema, setNewSchema] = useState("");
  const [registerConfirmed, setRegisterConfirmed] = useState(false);
  const [deleteText, setDeleteText] = useState("");
  const [deleteConfirmed, setDeleteConfirmed] = useState(false);
  const [writeStatus, setWriteStatus] = useState<string>();
  const [writeError, setWriteError] = useState<string>();

  useEffect(() => {
    if (!registry) {
      setSubjects([]);
      setDetail(undefined);
      return;
    }
    let cancelled = false;
    setLoading(true);
    setError(undefined);
    schemaSubjectNames(registry)
      .then((names) => {
        if (!cancelled) {
          setSubjects(names.map((subject) => ({ subject, latestVersion: 0, schemaType: "Details on open" })));
        }
      })
      .catch((reason: unknown) => {
        if (!cancelled) setError(reason instanceof Error ? reason.message : String(reason));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [registry, schemaSubjectNames]);

  useEffect(() => {
    if (!registry || !subject) {
      setDetail(undefined);
      return;
    }
    let cancelled = false;
    setLoading(true);
    schemaSubjectDetail({ ...registry, subject })
      .then((value) => {
        if (!cancelled) setDetail(value);
      })
      .catch((reason: unknown) => {
        if (!cancelled) setError(reason instanceof Error ? reason.message : String(reason));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [registry, schemaSubjectDetail, subject]);

  const filtered = useMemo(
    () => subjects.filter((item) => item.subject.toLowerCase().includes(query.trim().toLowerCase())),
    [query, subjects],
  );

  if (!registry) {
    return (
      <KafkaPageShell title="Schema Registry" subtitle="No Schema Registry configured">
        <div className="KafkaPageState empty" data-testid="kafka-schema-registry-unconfigured">
          Configure a Registry endpoint in Kafka connection settings to browse subjects.
        </div>
      </KafkaPageShell>
    );
  }

  return (
    <KafkaPageShell title="Schema Registry" subtitle={registry.registryUrl}>
      <main className="KafkaWorkspace" data-testid="kafka-schema-registry-page">
        <section className="KafkaListPane">
          <div className="KafkaToolbar">
            <Renderer.Component.Input
              value={query}
              onChange={(value) => setQuery(value, true)}
              placeholder="Filter subjects"
              aria-label="Filter schema subjects"
            />
          </div>
          {loading && <div className="KafkaPageState loading">Loading schema subjects…</div>}
          {error && (
            <div className="KafkaPageState error" role="alert">
              {error}
            </div>
          )}
          {!loading && !error && filtered.length === 0 && (
            <div className="KafkaPageState empty">No schema subjects match.</div>
          )}
          <Renderer.Component.Table className="KafkaSchemaSubjectTable">
            <Renderer.Component.TableHead>
              <Renderer.Component.TableCell>Subject</Renderer.Component.TableCell>
              <Renderer.Component.TableCell>Latest version</Renderer.Component.TableCell>
              <Renderer.Component.TableCell>Type</Renderer.Component.TableCell>
              <Renderer.Component.TableCell>Compatibility</Renderer.Component.TableCell>
            </Renderer.Component.TableHead>
            {filtered.map((item) => {
              const interaction: HTMLAttributes<HTMLDivElement> = {
                tabIndex: 0,
                onClick: () => setSubject(item.subject),
                onKeyDown: (event) => {
                  if (event.key === "Enter" || event.key === " ") {
                    event.preventDefault();
                    setSubject(item.subject);
                  }
                },
              };
              return (
                <Renderer.Component.TableRow {...interaction} key={item.subject} data-subject={item.subject}>
                  <Renderer.Component.TableCell>{item.subject}</Renderer.Component.TableCell>
                  <Renderer.Component.TableCell>{item.latestVersion}</Renderer.Component.TableCell>
                  <Renderer.Component.TableCell>{item.schemaType}</Renderer.Component.TableCell>
                  <Renderer.Component.TableCell>{item.compatibility ?? "Default"}</Renderer.Component.TableCell>
                </Renderer.Component.TableRow>
              );
            })}
          </Renderer.Component.Table>
        </section>
        {detail && (
          <aside className="KafkaDetailPane" data-testid="kafka-schema-subject-detail">
            <h2>{detail.subject}</h2>
            <span>Compatibility: {detail.compatibility ?? "Default"}</span>
            {detail.versions.map((version) => (
              <article key={version.version} className="KafkaSchemaVersion">
                <h3>
                  Version {version.version} · {version.schemaType}
                </h3>
                <pre>{version.schema}</pre>
              </article>
            ))}
            {canWrite && registry && (
              <section className="KafkaWriteDrawer" data-testid="kafka-schema-write-controls">
                <h3>Register schema version</h3>
                <Renderer.Component.Input value={newSchema} onChange={setNewSchema} aria-label="Schema definition" />
                <Renderer.Component.Switch
                  checked={registerConfirmed}
                  onChange={setRegisterConfirmed}
                  aria-label="Confirm schema registration"
                />
                <Renderer.Component.Button
                  primary
                  disabled={!canSubmitWriteAction({ confirmationAccepted: registerConfirmed })}
                  onClick={() => {
                    setWriteError(undefined);
                    void schemaRegister({ ...registry, subject, schema: newSchema })
                      .then((result) => {
                        setNewSchema("");
                        setWriteStatus(`Registered schema version with id ${result.id}`);
                      })
                      .catch((reason: unknown) =>
                        setWriteError(reason instanceof Error ? reason.message : String(reason)),
                      );
                  }}
                >
                  Register schema
                </Renderer.Component.Button>
                <h3>Delete subject</h3>
                <Renderer.Component.Input
                  value={deleteText}
                  onChange={setDeleteText}
                  aria-label="Type subject to confirm deletion"
                />
                <Renderer.Component.Switch
                  checked={deleteConfirmed}
                  onChange={setDeleteConfirmed}
                  aria-label="Confirm subject deletion"
                />
                <Renderer.Component.Button
                  primary
                  disabled={
                    !canSubmitWriteAction({
                      confirmationAccepted: deleteConfirmed,
                      requiredResourceName: subject,
                      enteredResourceName: deleteText,
                    })
                  }
                  onClick={() => {
                    setWriteError(undefined);
                    void schemaDeleteSubject({ ...registry, subject })
                      .then(() => {
                        setWriteStatus(`Deleted subject ${subject}`);
                      })
                      .catch((reason: unknown) =>
                        setWriteError(reason instanceof Error ? reason.message : String(reason)),
                      );
                  }}
                >
                  Delete subject
                </Renderer.Component.Button>
                {writeStatus && (
                  <div role="status" data-testid="kafka-schema-write-status">
                    {writeStatus}
                  </div>
                )}
                {writeError && (
                  <div role="alert" data-testid="kafka-schema-write-error">
                    {writeError}
                  </div>
                )}
              </section>
            )}
          </aside>
        )}
      </main>
    </KafkaPageShell>
  );
}
