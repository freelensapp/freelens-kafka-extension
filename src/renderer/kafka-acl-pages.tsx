import { Renderer } from "@freelensapp/extensions";
import { useCallback, useEffect, useMemo, useState } from "react";
import { KafkaPageShell } from "./kafka-page-shell";
import { KafkaResourceActions, type KafkaResourcePageDependencies, useKafkaResourcePage } from "./kafka-resource-pages";
import { canSubmitWriteAction } from "./kafka-write-policy";

import type { HTMLAttributes } from "react";

import type { AclWriteRequest, KafkaAclDto, KafkaAclResultDto, KafkaWorkloadSourceLocator } from "../common/ipc";

const RESOURCE_TYPES = ["TOPIC", "GROUP", "CLUSTER", "TRANSACTIONAL_ID"];
const PATTERN_TYPES = ["LITERAL", "PREFIXED"];
const OPERATIONS = ["READ", "WRITE", "ALL", "CREATE", "DELETE", "ALTER", "DESCRIBE"];
const PERMISSION_TYPES = ["ALLOW", "DENY"];

interface AclTargetRequest {
  namespace: string;
  clusterName: string;
  source: string;
  bootstrap: string;
  tls: boolean;
  targetId: string;
  sourceLocator?: KafkaWorkloadSourceLocator;
}

export interface KafkaAclPageProps extends KafkaResourcePageDependencies {
  params?: { target: Renderer.Navigation.PageParam<string> };
  acls: (request: AclTargetRequest) => Promise<KafkaAclResultDto>;
  aclCreate: (request: AclWriteRequest) => Promise<void>;
  aclDelete: (request: AclWriteRequest) => Promise<void>;
}

function describeRule(acl: KafkaAclDto): string {
  return `${acl.permissionType} ${acl.principal} ${acl.operation} on ${acl.resourceType} ${acl.patternType} "${acl.resourceName}" from ${acl.host}`;
}

export function KafkaAclPage({ params, acls, aclCreate, aclDelete, ...dependencies }: KafkaAclPageProps) {
  const state = useKafkaResourcePage({ ...dependencies, params });
  const cluster = state.selectedCluster;
  const request = useMemo<AclTargetRequest | undefined>(
    () =>
      cluster
        ? {
            namespace: cluster.namespace,
            clusterName: cluster.name,
            source: cluster.source,
            bootstrap: cluster.bootstrap,
            tls: cluster.tls,
            sourceLocator: cluster.sourceLocator,
            targetId: cluster.targetId,
          }
        : undefined,
    [cluster],
  );

  const [result, setResult] = useState<KafkaAclResultDto>();
  const [principalFilter, setPrincipalFilter] = useState("");
  const [resourceTypeFilter, setResourceTypeFilter] = useState("ALL");
  const [operationFilter, setOperationFilter] = useState("ALL");

  const [resourceType, setResourceType] = useState("TOPIC");
  const [resourceName, setResourceName] = useState("");
  const [patternType, setPatternType] = useState("LITERAL");
  const [principal, setPrincipal] = useState("");
  const [host, setHost] = useState("*");
  const [operation, setOperation] = useState("READ");
  const [permissionType, setPermissionType] = useState("ALLOW");

  const [createConfirmed, setCreateConfirmed] = useState(false);
  const [selected, setSelected] = useState<KafkaAclDto>();
  const [deleteConfirmed, setDeleteConfirmed] = useState(false);
  const [deleteText, setDeleteText] = useState("");
  const [writeStatus, setWriteStatus] = useState<string>();
  const [writeError, setWriteError] = useState<string>();
  const [busy, setBusy] = useState(false);

  const reload = useCallback(async () => {
    if (!request) return;
    setResult(await acls(request));
  }, [acls, request]);

  useEffect(() => {
    reload().catch((error: unknown) =>
      setResult({
        available: false,
        acls: [],
        message: error instanceof Error ? error.message : String(error),
      }),
    );
  }, [reload]);

  useEffect(() => {
    setSelected(undefined);
    setWriteStatus(undefined);
    setWriteError(undefined);
  }, [cluster?.targetId]);

  const filtered = useMemo(
    () =>
      (result?.acls ?? []).filter(
        (acl) =>
          (resourceTypeFilter === "ALL" || acl.resourceType === resourceTypeFilter) &&
          (operationFilter === "ALL" || acl.operation === operationFilter) &&
          acl.principal.toLowerCase().includes(principalFilter.toLowerCase()),
      ),
    [operationFilter, principalFilter, resourceTypeFilter, result],
  );

  const canWrite = Boolean(cluster && dependencies.writeSettings.get(cluster.targetId));
  const draft: KafkaAclDto = { resourceType, resourceName, patternType, principal, host, operation, permissionType };
  const draftComplete = resourceName.trim().length > 0 && principal.trim().length > 0 && host.trim().length > 0;

  const runWrite = async (action: (write: AclWriteRequest) => Promise<void>, acl: KafkaAclDto, done: string) => {
    if (!request) return;
    setBusy(true);
    setWriteError(undefined);
    setWriteStatus(undefined);
    try {
      await action({ ...request, acl });
      await reload();
      setWriteStatus(done);
    } catch (error: unknown) {
      setWriteError(error instanceof Error ? error.message : String(error));
    } finally {
      setBusy(false);
    }
  };

  return (
    <KafkaPageShell
      title="ACLs"
      subtitle={cluster?.name ?? "Kafka security rules"}
      actions={<KafkaResourceActions state={state} />}
    >
      <main className="KafkaResourcePage" data-testid="kafka-acl-page">
        <div className="KafkaToolbar">
          <Renderer.Component.Input
            value={principalFilter}
            onChange={setPrincipalFilter}
            placeholder="Filter principal"
            aria-label="Filter ACL principal"
          />
          <select
            value={resourceTypeFilter}
            onChange={(event) => setResourceTypeFilter(event.target.value)}
            aria-label="Filter ACL resource type"
          >
            {["ALL", ...RESOURCE_TYPES].map((value) => (
              <option key={value}>{value}</option>
            ))}
          </select>
          <select
            value={operationFilter}
            onChange={(event) => setOperationFilter(event.target.value)}
            aria-label="Filter ACL operation"
          >
            {["ALL", ...OPERATIONS].map((value) => (
              <option key={value}>{value}</option>
            ))}
          </select>
        </div>

        {result && !result.available ? (
          <div className="KafkaPageState empty" data-testid="kafka-acl-unavailable">
            {result.message}
          </div>
        ) : (
          <>
            {result && filtered.length === 0 && <div className="KafkaPageState empty">No ACL rules match.</div>}
            <Renderer.Component.Table className="KafkaAclTable">
              <Renderer.Component.TableHead>
                <Renderer.Component.TableCell>Resource</Renderer.Component.TableCell>
                <Renderer.Component.TableCell>Pattern</Renderer.Component.TableCell>
                <Renderer.Component.TableCell>Principal</Renderer.Component.TableCell>
                <Renderer.Component.TableCell>Host</Renderer.Component.TableCell>
                <Renderer.Component.TableCell>Operation</Renderer.Component.TableCell>
                <Renderer.Component.TableCell>Permission</Renderer.Component.TableCell>
              </Renderer.Component.TableHead>
              {filtered.map((acl) => {
                const selectAcl = (): void => {
                  setSelected(acl);
                  setDeleteText("");
                  setDeleteConfirmed(false);
                };
                const interaction: HTMLAttributes<HTMLDivElement> = {
                  tabIndex: 0,
                  onClick: selectAcl,
                  onKeyDown: (event) => {
                    if (event.key === "Enter" || event.key === " ") {
                      event.preventDefault();
                      selectAcl();
                    }
                  },
                };
                return (
                  <Renderer.Component.TableRow
                    {...interaction}
                    key={describeRule(acl)}
                    data-resource-type={acl.resourceType}
                    data-principal={acl.principal}
                  >
                    <Renderer.Component.TableCell>
                      {acl.resourceType}:{acl.resourceName}
                    </Renderer.Component.TableCell>
                    <Renderer.Component.TableCell>{acl.patternType}</Renderer.Component.TableCell>
                    <Renderer.Component.TableCell>{acl.principal}</Renderer.Component.TableCell>
                    <Renderer.Component.TableCell>{acl.host}</Renderer.Component.TableCell>
                    <Renderer.Component.TableCell>{acl.operation}</Renderer.Component.TableCell>
                    <Renderer.Component.TableCell>{acl.permissionType}</Renderer.Component.TableCell>
                  </Renderer.Component.TableRow>
                );
              })}
            </Renderer.Component.Table>
          </>
        )}

        {canWrite && result?.available && (
          <section className="KafkaWriteDrawer" data-testid="kafka-acl-write-controls" aria-label="ACL write actions">
            <div className="KafkaWriteForm" style={{ display: "grid", gap: 12 }}>
              <select
                value={resourceType}
                onChange={(event) => setResourceType(event.target.value)}
                aria-label="ACL resource type"
              >
                {RESOURCE_TYPES.map((value) => (
                  <option key={value}>{value}</option>
                ))}
              </select>
              <Renderer.Component.Input
                value={resourceName}
                onChange={setResourceName}
                placeholder="Resource name"
                aria-label="ACL resource name"
              />
              <select
                value={patternType}
                onChange={(event) => setPatternType(event.target.value)}
                aria-label="ACL pattern type"
              >
                {PATTERN_TYPES.map((value) => (
                  <option key={value}>{value}</option>
                ))}
              </select>
              <Renderer.Component.Input
                value={principal}
                onChange={setPrincipal}
                placeholder="User:name"
                aria-label="ACL principal"
              />
              <Renderer.Component.Input value={host} onChange={setHost} placeholder="*" aria-label="ACL host" />
              <select
                value={operation}
                onChange={(event) => setOperation(event.target.value)}
                aria-label="ACL operation"
              >
                {OPERATIONS.map((value) => (
                  <option key={value}>{value}</option>
                ))}
              </select>
              <select
                value={permissionType}
                onChange={(event) => setPermissionType(event.target.value)}
                aria-label="ACL permission type"
              >
                {PERMISSION_TYPES.map((value) => (
                  <option key={value}>{value}</option>
                ))}
              </select>
            </div>

            <div data-testid="kafka-acl-rule-preview">{describeRule(draft)}</div>
            <Renderer.Component.Switch
              checked={createConfirmed}
              onChange={setCreateConfirmed}
              aria-label="Confirm ACL create"
            />
            <Renderer.Component.Button
              disabled={busy || !draftComplete || !canSubmitWriteAction({ confirmationAccepted: createConfirmed })}
              onClick={() => void runWrite(aclCreate, draft, `Created ACL for ${draft.principal}`)}
            >
              Create ACL
            </Renderer.Component.Button>

            {selected && (
              <div data-testid="kafka-acl-delete-controls">
                <div data-testid="kafka-acl-selected-rule">{describeRule(selected)}</div>
                <Renderer.Component.Input
                  value={deleteText}
                  onChange={setDeleteText}
                  placeholder={selected.resourceName}
                  aria-label="Type ACL resource to confirm deletion"
                />
                <Renderer.Component.Switch
                  checked={deleteConfirmed}
                  onChange={setDeleteConfirmed}
                  aria-label="Confirm ACL deletion"
                />
                <Renderer.Component.Button
                  disabled={
                    busy ||
                    !canSubmitWriteAction({
                      confirmationAccepted: deleteConfirmed,
                      requiredResourceName: selected.resourceName,
                      enteredResourceName: deleteText,
                    })
                  }
                  onClick={() =>
                    void runWrite(aclDelete, selected, `Deleted ACL for ${selected.principal}`).then(() =>
                      setSelected(undefined),
                    )
                  }
                >
                  Delete ACL
                </Renderer.Component.Button>
              </div>
            )}

            {writeStatus && (
              <div role="status" data-testid="kafka-acl-write-status">
                {writeStatus}
              </div>
            )}
            {writeError && (
              <div role="alert" data-testid="kafka-acl-write-error">
                {writeError}
              </div>
            )}
          </section>
        )}
      </main>
    </KafkaPageShell>
  );
}
