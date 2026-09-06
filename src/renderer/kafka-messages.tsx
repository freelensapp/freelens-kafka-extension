import { Renderer } from "@freelensapp/extensions";
import { useCallback, useEffect, useRef, useState } from "react";
import { OperationProgress } from "./kafka-overview";
import { createOperationId, matchesKafkaMessageFilters } from "./kafka-view-model";

import type { HTMLAttributes } from "react";

import type {
  DiscoveredKafkaInfo,
  KafkaMessageBytesDto,
  KafkaMessageStartMode,
  KafkaProgressEvent,
  KafkaRecordDto,
  KafkaSecurityOverride,
  MessageBrowseDto,
  MessageBrowseRequest,
  TopicPartitionDto,
} from "../common/ipc";

const START_MODES: Array<{ value: KafkaMessageStartMode; label: string }> = [
  { value: "latest", label: "Latest window" },
  { value: "earliest", label: "Earliest" },
  { value: "offset", label: "Offset" },
  { value: "timestamp", label: "Timestamp" },
];

const TAIL_POLL_INTERVAL_MS = 1500;

interface MessageBrowseState {
  loading: boolean;
  error?: string;
  data?: MessageBrowseDto;
  progress?: KafkaProgressEvent;
}

interface TailState {
  active: boolean;
  bufferLimit: number;
  droppedCount: number;
  nextOffset?: string;
}

interface KafkaMessagesBrowserProps {
  cluster: DiscoveredKafkaInfo;
  topic: string;
  partitions: TopicPartitionDto[];
  metadataLoading: boolean;
  metadataError?: string;
  metadataProgress?: KafkaProgressEvent;
  security?: KafkaSecurityOverride;
  browse: (request: MessageBrowseRequest) => Promise<MessageBrowseDto>;
  subscribeProgress: (listener: (progress: KafkaProgressEvent) => void) => () => void;
  onRetryMetadata: () => void;
  filters: KafkaMessageFilters;
  onFilterChange: (name: MessageFilterName, value: string) => void;
  timestamp: string;
  onTimestampChange: (value: string) => void;
  schemaRegistry?: { registryUrl: string; username?: string };
}

type MessageFilterName = "key" | "value" | "headerKey" | "headerValue";

interface KafkaMessageFilters {
  key: string;
  value: string;
  headerKey: string;
  headerValue: string;
}

function formatTimestamp(timestamp: string): string {
  const value = Number(timestamp);
  return Number.isFinite(value) ? new Date(value).toLocaleString() : timestamp;
}

function bytesContent(bytes: KafkaMessageBytesDto): string {
  if (bytes.format === "null") return "null";
  if (bytes.format === "json" && !bytes.truncated && bytes.text !== undefined) {
    try {
      return JSON.stringify(JSON.parse(bytes.text), null, 2);
    } catch {
      return bytes.text;
    }
  }
  return bytes.text ?? bytes.base64 ?? "";
}

function FormatBadge({ bytes }: { bytes: KafkaMessageBytesDto }) {
  if (bytes.format === "null") {
    return <span className="KafkaMsgFormatBadge format-null">null</span>;
  }
  const label = bytes.format === "json" ? "JSON" : bytes.format === "text" ? "Text" : "Binary";
  const suffix = bytes.truncated ? " · truncated" : "";
  return (
    <span className={`KafkaMsgFormatBadge format-${bytes.format}`}>
      {label} · {bytes.byteLength}\u202fB{suffix}
    </span>
  );
}

function BytesSection({ label, bytes }: { label: string; bytes: KafkaMessageBytesDto }) {
  return (
    <>
      <Renderer.Component.DrawerTitle size="sub-title">{label}</Renderer.Component.DrawerTitle>
      <div className="KafkaMsgBytesBody">
        <FormatBadge bytes={bytes} />
        {bytes.format !== "null" && (
          <pre className="KafkaMsgCodeBlock" data-format={bytes.format}>
            {bytesContent(bytes)}
          </pre>
        )}
        {(bytes.format === "binary" || bytes.truncated) && bytes.base64 !== undefined && (
          <details className="KafkaMsgBase64">
            <summary>Raw base64</summary>
            <code>{bytes.base64}</code>
          </details>
        )}
      </div>
    </>
  );
}

function MessageInspector({ message }: { message: KafkaRecordDto }) {
  const hasHeaders = message.headers.length > 0;
  return (
    <div className="KafkaMessageInspector" aria-label={`Message at offset ${message.offset}`}>
      <Renderer.Component.DrawerItem name="Partition">{message.partition}</Renderer.Component.DrawerItem>
      <Renderer.Component.DrawerItem name="Offset">
        <code className="KafkaMsgMono">{message.offset}</code>
      </Renderer.Component.DrawerItem>
      <Renderer.Component.DrawerItem name="Timestamp">
        <div className="KafkaMsgTimestamp">
          <span>{formatTimestamp(message.timestamp)}</span>
          <small>{message.timestamp}</small>
        </div>
      </Renderer.Component.DrawerItem>

      <BytesSection label="Key" bytes={message.key} />
      <BytesSection label="Value" bytes={message.value} />
      {message.decodedValue !== undefined && (
        <>
          <Renderer.Component.DrawerTitle size="sub-title">Decoded value</Renderer.Component.DrawerTitle>
          <pre className="KafkaMsgCodeBlock" data-testid="kafka-decoded-value">
            {JSON.stringify(message.decodedValue, null, 2)}
          </pre>
        </>
      )}
      {message.decodeWarning && (
        <div className="KafkaPageState warning" role="status" data-testid="kafka-decode-warning">
          <Renderer.Component.Icon material="warning" />
          {message.decodeWarning}
        </div>
      )}

      <Renderer.Component.DrawerTitle size="sub-title">
        Headers{hasHeaders ? ` (${message.headers.length})` : ""}
      </Renderer.Component.DrawerTitle>
      {hasHeaders ? (
        <div className="KafkaMsgHeaderList">
          {message.headers.map((header, index) => (
            <Renderer.Component.DrawerItem key={`${header.name}-${index}`} name={header.name}>
              {header.value.format === "null" ? (
                <FormatBadge bytes={header.value} />
              ) : (
                <div className="KafkaMsgHeaderValue">
                  <FormatBadge bytes={header.value} />
                  <code className="KafkaMsgMono">{bytesContent(header.value)}</code>
                </div>
              )}
            </Renderer.Component.DrawerItem>
          ))}
        </div>
      ) : (
        <div className="KafkaMsgEmpty">
          <Renderer.Component.Icon material="label_off" />
          No headers
        </div>
      )}
    </div>
  );
}

export function KafkaMessagesBrowser({
  cluster,
  topic,
  partitions,
  metadataLoading,
  metadataError,
  metadataProgress,
  security,
  browse,
  subscribeProgress,
  onRetryMetadata,
  filters,
  onFilterChange,
  timestamp,
  onTimestampChange,
  schemaRegistry,
}: KafkaMessagesBrowserProps) {
  const partitionOptions = partitions
    .map(({ partitionId }) => partitionId)
    .sort((left, right) => left - right)
    .map((partition) => ({ value: String(partition), label: `Partition ${partition}` }));
  const [partition, setPartition] = useState(partitionOptions[0]?.value ?? "0");
  const [startMode, setStartMode] = useState<KafkaMessageStartMode>("latest");
  const [offset, setOffset] = useState("0");
  const [limit, setLimit] = useState("50");
  const [state, setState] = useState<MessageBrowseState>({ loading: false });
  const [selectedOffset, setSelectedOffset] = useState<string>();
  const [tailState, setTailState] = useState<TailState>({ active: false, bufferLimit: 0, droppedCount: 0 });
  const [browseRequests, setBrowseRequests] = useState(0);
  const operationId = useRef<string>();
  const tailSession = useRef(0);
  const tailTimer = useRef<number>();

  useEffect(
    () =>
      subscribeProgress((progress) => {
        if (progress.operation === "messagesBrowse" && progress.operationId === operationId.current) {
          setState((current) => ({ ...current, progress }));
        }
      }),
    [subscribeProgress],
  );

  useEffect(() => {
    operationId.current = undefined;
    setState({ loading: false });
    setSelectedOffset(undefined);
    tailSession.current += 1;
    if (tailTimer.current !== undefined) window.clearTimeout(tailTimer.current);
    setTailState({ active: false, bufferLimit: 0, droppedCount: 0 });
    setBrowseRequests(0);
    setPartition(partitionOptions[0]?.value ?? "0");
  }, [cluster.targetId, topic]);

  useEffect(
    () => () => {
      operationId.current = undefined;
      tailSession.current += 1;
      if (tailTimer.current !== undefined) window.clearTimeout(tailTimer.current);
    },
    [],
  );

  const stopTail = useCallback(() => {
    tailSession.current += 1;
    if (tailTimer.current !== undefined) {
      window.clearTimeout(tailTimer.current);
      tailTimer.current = undefined;
    }
    setTailState((current) => (current.active ? { ...current, active: false } : current));
  }, []);

  const parseBrowseControls = useCallback(
    (next?: { startMode?: KafkaMessageStartMode; offset?: string; timestamp?: string; limit?: string }) => {
      const parsedPartition = Number(partition);
      const parsedLimit = Number(next?.limit ?? limit);
      const requestedMode = next?.startMode ?? startMode;
      const requestedOffset = next?.offset ?? offset;
      const requestedTimestampText = next?.timestamp ?? timestamp;
      const requestedTimestamp = requestedTimestampText ? Date.parse(requestedTimestampText) : Number.NaN;

      if (!Number.isInteger(parsedPartition) || !partitions.some((item) => item.partitionId === parsedPartition)) {
        return { error: "Select a valid partition." };
      }
      if (!Number.isInteger(parsedLimit) || parsedLimit < 1 || parsedLimit > 100) {
        return { error: "Limit must be between 1 and 100." };
      }
      if (requestedMode === "offset" && !/^(0|[1-9]\d*)$/.test(requestedOffset)) {
        return { error: "Offset must be a non-negative decimal value." };
      }
      if (requestedMode === "timestamp" && !Number.isSafeInteger(requestedTimestamp)) {
        return { error: "Select a valid timestamp." };
      }

      return {
        parsedPartition,
        parsedLimit,
        requestedMode,
        requestedOffset,
        requestedTimestamp,
      };
    },
    [limit, offset, partition, partitions, startMode, timestamp],
  );

  const runBrowse = useCallback(
    (next?: { startMode: KafkaMessageStartMode; offset?: string }) => {
      const resolved = parseBrowseControls(next);

      if ("error" in resolved) {
        setState({ loading: false, error: resolved.error });
        return;
      }

      const { parsedPartition, parsedLimit, requestedMode, requestedOffset, requestedTimestamp } = resolved;

      stopTail();
      const nextOperationId = createOperationId("messagesBrowse");
      operationId.current = nextOperationId;
      const progress: KafkaProgressEvent = {
        operationId: nextOperationId,
        operation: "messagesBrowse",
        value: 1,
        phase: "strategy",
        label: "Preparing message Browse",
        detail: "Selecting the existing read-only connection path.",
      };
      setState((current) => ({ loading: true, data: current.data, progress }));
      setBrowseRequests((count) => count + 1);
      setSelectedOffset(undefined);
      if (next) {
        setStartMode(next.startMode);
        if (next.offset !== undefined) setOffset(next.offset);
      }

      browse({
        operationId: nextOperationId,
        targetId: cluster.targetId,
        topic,
        partition: parsedPartition,
        startMode: requestedMode,
        limit: parsedLimit,
        ...(requestedMode === "offset" ? { offset: requestedOffset } : {}),
        ...(requestedMode === "timestamp" ? { timestamp: requestedTimestamp } : {}),
        namespace: cluster.namespace,
        clusterName: cluster.name,
        source: cluster.source,
        bootstrap: cluster.bootstrap,
        tls: cluster.tls,
        sourceLocator: cluster.sourceLocator,
        security,
        ...(schemaRegistry
          ? { registryUrl: schemaRegistry.registryUrl, registryUsername: schemaRegistry.username }
          : {}),
        ...(schemaRegistry
          ? { registryUrl: schemaRegistry.registryUrl, registryUsername: schemaRegistry.username }
          : {}),
      })
        .then((data) => {
          if (operationId.current !== nextOperationId) return;
          setState({ loading: false, data });
        })
        .catch((error: unknown) => {
          if (operationId.current !== nextOperationId) return;
          setState((current) => ({
            loading: false,
            data: current.data,
            error: error instanceof Error ? error.message : String(error),
            progress,
          }));
        });
    },
    [browse, cluster, parseBrowseControls, schemaRegistry, security, startMode, stopTail, topic],
  );

  const startTail = useCallback(() => {
    const resolved = parseBrowseControls({ startMode: "latest", limit });

    if ("error" in resolved) {
      setState({ loading: false, error: resolved.error });
      return;
    }

    stopTail();
    setSelectedOffset(undefined);

    const { parsedPartition, parsedLimit } = resolved;
    const sessionId = tailSession.current + 1;

    tailSession.current = sessionId;
    setTailState({ active: true, bufferLimit: parsedLimit, droppedCount: 0 });

    const poll = (cursor: string) => {
      const currentSession = sessionId;
      const nextOperationId = createOperationId("messagesBrowse");

      operationId.current = nextOperationId;

      browse({
        operationId: nextOperationId,
        targetId: cluster.targetId,
        topic,
        partition: parsedPartition,
        startMode: "offset",
        offset: cursor,
        limit: parsedLimit,
        namespace: cluster.namespace,
        clusterName: cluster.name,
        source: cluster.source,
        bootstrap: cluster.bootstrap,
        tls: cluster.tls,
        security,
      })
        .then((data) => {
          if (tailSession.current !== currentSession) return;

          setState((current) => {
            const existing = current.data?.messages ?? [];
            const seen = new Set(existing.map((message) => `${message.partition}:${message.offset}`));
            const appended = data.messages.filter((message) => !seen.has(`${message.partition}:${message.offset}`));
            const merged = [...existing, ...appended];
            const dropped = Math.max(0, merged.length - parsedLimit);
            const buffered = dropped > 0 ? merged.slice(dropped) : merged;

            setTailState((tail) => ({
              ...tail,
              active: true,
              bufferLimit: parsedLimit,
              droppedCount: tail.droppedCount + dropped,
              nextOffset: data.nextOffset,
            }));

            return {
              loading: false,
              data: {
                ...data,
                startMode: "latest",
                startOffset: buffered[0]?.offset ?? data.nextOffset,
                returnedCount: buffered.length,
                hasMore: false,
                messages: buffered,
              },
            };
          });

          tailTimer.current = window.setTimeout(() => poll(data.nextOffset), TAIL_POLL_INTERVAL_MS);
        })
        .catch((error: unknown) => {
          if (tailSession.current !== currentSession) return;
          stopTail();
          setState((current) => ({
            loading: false,
            data: current.data,
            error: error instanceof Error ? error.message : String(error),
          }));
        });
    };

    const nextOperationId = createOperationId("messagesBrowse");

    operationId.current = nextOperationId;
    setState((current) => ({ loading: true, data: current.data }));
    setBrowseRequests((count) => count + 1);

    browse({
      operationId: nextOperationId,
      targetId: cluster.targetId,
      topic,
      partition: parsedPartition,
      startMode: "latest",
      limit: 1,
      namespace: cluster.namespace,
      clusterName: cluster.name,
      source: cluster.source,
      bootstrap: cluster.bootstrap,
      tls: cluster.tls,
      sourceLocator: cluster.sourceLocator,
      security,
      ...(schemaRegistry ? { registryUrl: schemaRegistry.registryUrl, registryUsername: schemaRegistry.username } : {}),
    })
      .then((data) => {
        if (tailSession.current !== sessionId) return;

        setState({
          loading: false,
          data: {
            ...data,
            startMode: "latest",
            startOffset: data.nextOffset,
            returnedCount: 0,
            hasMore: false,
            messages: [],
          },
        });
        setTailState({ active: true, bufferLimit: parsedLimit, droppedCount: 0, nextOffset: data.nextOffset });
        tailTimer.current = window.setTimeout(() => poll(data.nextOffset), TAIL_POLL_INTERVAL_MS);
      })
      .catch((error: unknown) => {
        if (tailSession.current !== sessionId) return;
        stopTail();
        setState((current) => ({
          loading: false,
          data: current.data,
          error: error instanceof Error ? error.message : String(error),
        }));
      });
  }, [browse, cluster, limit, parseBrowseControls, schemaRegistry, security, stopTail, topic]);

  if (metadataLoading && metadataProgress) return <OperationProgress progress={metadataProgress} />;
  if (metadataError) {
    return (
      <div className="KafkaPageState error" role="alert">
        <Renderer.Component.Icon material="error_outline" />
        <div>
          <strong>Topic metadata failed</strong>
          <span>{metadataError}</span>
        </div>
        <Renderer.Component.Button outlined onClick={onRetryMetadata}>
          Retry
        </Renderer.Component.Button>
      </div>
    );
  }

  const allMessages = state.data?.messages ?? [];
  const filteredMessages = allMessages.filter((message) => matchesKafkaMessageFilters(message, filters));
  const filtersActive = Object.values(filters).some((value) => value.trim().length > 0);
  const selectedMessage = allMessages.find((message) => message.offset === selectedOffset);

  return (
    <section
      className="KafkaMessagesBrowser"
      data-testid="kafka-messages-browser"
      data-browse-state={state.loading ? "loading" : state.data ? "complete" : state.error ? "error" : "idle"}
      data-browse-requests={browseRequests}
    >
      <form
        className="KafkaMessageControls"
        aria-label="Message Browse controls"
        onSubmit={(event) => {
          event.preventDefault();
          runBrowse();
        }}
      >
        <label className="KafkaMessagePartitionField">
          <span>Partition</span>
          <Renderer.Component.Select
            id="kafka-message-partition"
            className="KafkaMessagePartitionSelect"
            options={partitionOptions}
            value={partition}
            aria-label="Partition"
            isSearchable={false}
            onChange={(option: Renderer.Component.SelectOption<string> | null) => {
              setPartition(option?.value ?? "0");
              setState({ loading: false });
            }}
            isDisabled={state.loading || partitionOptions.length === 0}
            themeName="lens"
            menuPosition="fixed"
          />
        </label>
        <fieldset className="KafkaMessageStartField">
          <legend>Start position</legend>
          <div className="KafkaMessageStartModes" role="group" aria-label="Start position">
            {START_MODES.map((mode) => (
              <Renderer.Component.Button
                key={mode.value}
                plain
                type="button"
                aria-pressed={startMode === mode.value}
                disabled={state.loading}
                onClick={() => {
                  setStartMode(mode.value);
                  setState({ loading: false });
                }}
              >
                {mode.label}
              </Renderer.Component.Button>
            ))}
          </div>
        </fieldset>
        {startMode === "offset" && (
          <label>
            <span>Offset</span>
            <Renderer.Component.Input
              value={offset}
              onChange={setOffset}
              disabled={state.loading}
              aria-label="Starting offset"
            />
          </label>
        )}
        {startMode === "timestamp" && (
          <label>
            <span>Timestamp</span>
            <Renderer.Component.Input
              type="datetime-local"
              value={timestamp}
              onChange={onTimestampChange}
              disabled={state.loading}
              aria-label="Starting timestamp"
            />
          </label>
        )}
        <label>
          <span>Limit</span>
          <Renderer.Component.Input
            type="number"
            min={1}
            max={100}
            value={limit}
            onChange={setLimit}
            disabled={state.loading}
            aria-label="Record limit"
          />
        </label>
        <Renderer.Component.Button
          primary
          type="button"
          waiting={state.loading}
          disabled={partitionOptions.length === 0}
          aria-label="Browse messages"
          onClick={() => runBrowse()}
        >
          <Renderer.Component.Icon material="search" />
          Browse
        </Renderer.Component.Button>
        <Renderer.Component.Button
          primary={!tailState.active}
          outlined={tailState.active}
          type="button"
          disabled={partitionOptions.length === 0 || state.loading}
          onClick={tailState.active ? stopTail : startTail}
        >
          <Renderer.Component.Icon material={tailState.active ? "stop_circle" : "sync"} />
          {tailState.active ? "Stop tail" : "Start tail"}
        </Renderer.Component.Button>
      </form>

      <fieldset className="KafkaMessageFilters" aria-label="Message filters">
        <legend>Filter loaded records</legend>
        <label>
          <span>Key</span>
          <Renderer.Component.Input
            value={filters.key}
            onChange={(value) => onFilterChange("key", value)}
            placeholder="Substring or /regex/"
            aria-label="Filter message key"
          />
        </label>
        <label>
          <span>Value</span>
          <Renderer.Component.Input
            value={filters.value}
            onChange={(value) => onFilterChange("value", value)}
            placeholder="Substring or /regex/"
            aria-label="Filter message value"
          />
        </label>
        <label>
          <span>Header key</span>
          <Renderer.Component.Input
            value={filters.headerKey}
            onChange={(value) => onFilterChange("headerKey", value)}
            placeholder="x-trace-id"
            aria-label="Filter message header key"
          />
        </label>
        <label>
          <span>Header value</span>
          <Renderer.Component.Input
            value={filters.headerValue}
            onChange={(value) => onFilterChange("headerValue", value)}
            placeholder="Substring or /regex/"
            aria-label="Filter message header value"
          />
        </label>
      </fieldset>

      {tailState.active && (
        <div className="KafkaMessageTailStatus" role="status" aria-live="polite">
          <Renderer.Component.Icon material="sync" />
          <div>
            <strong>Tailing partition {partition}</strong>
            <span>
              Buffer {state.data?.messages.length ?? 0}/{tailState.bufferLimit}
              {tailState.droppedCount > 0 ? ` · dropped ${tailState.droppedCount}` : ""}
              {tailState.nextOffset ? ` · next offset ${tailState.nextOffset}` : ""}
            </span>
          </div>
        </div>
      )}

      {state.error && (
        <div className="KafkaMessageError" role="alert">
          <Renderer.Component.Icon material="error_outline" />
          <span>{state.error}</span>
        </div>
      )}
      {state.loading && state.progress && <OperationProgress progress={state.progress} />}

      {!state.loading && !state.data && !state.error && (
        <div className="KafkaMessageIdle">
          <Renderer.Component.Icon material="pause_circle_outline" />
          <div>
            <strong>No message window loaded</strong>
            <span>Browse is idle.</span>
          </div>
        </div>
      )}

      {state.data && (
        <>
          <header className="KafkaMessageRange">
            <div>
              <strong>
                {tailState.active
                  ? `${filteredMessages.length} buffered records`
                  : filtersActive
                    ? `${filteredMessages.length} of ${state.data.returnedCount} records`
                    : `${state.data.returnedCount} records`}
              </strong>
              <span>
                {tailState.active
                  ? `Tailing from offset ${state.data.startOffset} · high watermark ${state.data.highWatermark}`
                  : `Offsets ${state.data.startOffset}–${state.data.nextOffset} · high watermark ${state.data.highWatermark}`}
              </span>
            </div>
            {!tailState.active && (
              <Renderer.Component.Button
                outlined
                disabled={!state.data.hasMore || state.loading}
                onClick={() => runBrowse({ startMode: "offset", offset: state.data?.nextOffset })}
              >
                Next window
                <Renderer.Component.Icon material="arrow_forward" />
              </Renderer.Component.Button>
            )}
          </header>
          {filteredMessages.length === 0 ? (
            <div className="KafkaMessageIdle">
              <Renderer.Component.Icon material="inbox" />
              <div>
                <strong>
                  {filtersActive ? "No messages match the current filter" : "No committed records in this window"}
                </strong>
                <span>
                  {filtersActive
                    ? "Adjust the filter to inspect other loaded records."
                    : `Next offset ${state.data.nextOffset}`}
                </span>
              </div>
            </div>
          ) : (
            <Renderer.Component.Table<KafkaRecordDto>
              className="KafkaMessageTable"
              tableId="kafka-message-browser"
              autoSize={false}
              scrollable
              selectable
              sortSyncWithUrl={false}
            >
              <Renderer.Component.TableHead sticky={false} nowrap>
                <Renderer.Component.TableCell className="messageOffsetCell">Offset</Renderer.Component.TableCell>
                <Renderer.Component.TableCell className="messageTimeCell">Timestamp</Renderer.Component.TableCell>
                <Renderer.Component.TableCell className="messageKeyCell">Key</Renderer.Component.TableCell>
                <Renderer.Component.TableCell className="messageValueCell">Value</Renderer.Component.TableCell>
                <Renderer.Component.TableCell className="messageActionCell" />
              </Renderer.Component.TableHead>
              {filteredMessages.map((message) => {
                const select = (event: { stopPropagation: () => void }): void => {
                  event.stopPropagation();
                  setSelectedOffset(message.offset);
                };
                const interaction: HTMLAttributes<HTMLDivElement> = {
                  role: "button",
                  tabIndex: 0,
                  "aria-label": `Inspect message at offset ${message.offset}`,
                  "aria-selected": selectedOffset === message.offset,
                  onClick: select,
                  onKeyDown: (event) => {
                    if (event.key === "Enter" || event.key === " ") {
                      event.preventDefault();
                      select(event);
                    }
                  },
                };
                return (
                  <Renderer.Component.TableRow
                    {...interaction}
                    className="KafkaInteractiveRow"
                    key={`${message.partition}-${message.offset}`}
                    sortItem={message}
                    selected={selectedOffset === message.offset}
                    data-offset={message.offset}
                    nowrap
                  >
                    <Renderer.Component.TableCell className="messageOffsetCell">
                      {message.offset}
                    </Renderer.Component.TableCell>
                    <Renderer.Component.TableCell className="messageTimeCell" title={message.timestamp}>
                      {formatTimestamp(message.timestamp)}
                    </Renderer.Component.TableCell>
                    <Renderer.Component.TableCell className="messageKeyCell" title={message.key.text}>
                      <span className="KafkaEllipsis">{bytesContent(message.key)}</span>
                    </Renderer.Component.TableCell>
                    <Renderer.Component.TableCell className="messageValueCell" title={message.value.text}>
                      <span className="KafkaEllipsis">{bytesContent(message.value)}</span>
                    </Renderer.Component.TableCell>
                    <Renderer.Component.TableCell className="messageActionCell">
                      <Renderer.Component.Button
                        plain
                        round
                        className="KafkaIconButton"
                        title={`View details for offset ${message.offset}`}
                        aria-label={`View details for offset ${message.offset}`}
                        onClick={select}
                      >
                        <Renderer.Component.Icon material="visibility" />
                      </Renderer.Component.Button>
                    </Renderer.Component.TableCell>
                  </Renderer.Component.TableRow>
                );
              })}
            </Renderer.Component.Table>
          )}
        </>
      )}

      {selectedMessage && (
        <Renderer.Component.Drawer
          className="KafkaMessageDetailDrawer"
          contentClass="KafkaMessageDetailBody"
          open
          usePortal
          size="min(760px, calc(100vw - 24px))"
          title={`Message detail: offset ${selectedMessage.offset}`}
          data-testid="kafka-message-detail"
          onClose={() => setSelectedOffset(undefined)}
        >
          <MessageInspector message={selectedMessage} />
        </Renderer.Component.Drawer>
      )}
    </section>
  );
}
