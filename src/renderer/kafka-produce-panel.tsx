import { Renderer } from "@freelensapp/extensions";
import { useState } from "react";
import { describeInvalidHeaderLines, parseProduceHeaders, parseProducePartition } from "./kafka-produce-draft";
import { canSubmitWriteAction, getWriteConfirmationLabel } from "./kafka-write-policy";

import type { ProduceResultDto } from "../common/ipc";

export interface KafkaProduceDraft {
  key?: string;
  value: string;
  headers: Record<string, string>;
  partition?: number;
}

interface KafkaProducePanelProps {
  clusterName: string;
  topicName: string;
  /** Known partition count of the locked topic, used to validate the optional partition. */
  partitionCount?: number;
  onSend: (draft: KafkaProduceDraft) => Promise<ProduceResultDto>;
  onClose: () => void;
}

/**
 * Compose panel of the Topic Workspace (SPEC-009 REQ-108–REQ-111, REQ-206–REQ-208). It sits beside
 * the workspace instead of above it, so the Messages browser keeps its height and stays usable
 * while a record is composed (#66). The native Drawer is not used on purpose: it asks to close on
 * every click outside of it, which would make the table unusable while composing.
 */
export function KafkaProducePanel({ clusterName, topicName, partitionCount, onSend, onClose }: KafkaProducePanelProps) {
  const [key, setKey] = useState("");
  const [value, setValue] = useState("");
  const [headersText, setHeadersText] = useState("");
  const [partitionText, setPartitionText] = useState("");
  const [confirmed, setConfirmed] = useState(false);
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<ProduceResultDto>();
  const [error, setError] = useState<string>();

  const parsedHeaders = parseProduceHeaders(headersText);
  const headersError = describeInvalidHeaderLines(parsedHeaders.invalidLines);
  const parsedPartition = parseProducePartition(partitionText, partitionCount);
  const canSend =
    !busy &&
    !headersError &&
    !parsedPartition.error &&
    canSubmitWriteAction({ confirmationAccepted: confirmed, requiredResourceName: undefined });

  const send = () => {
    if (!canSend) return;
    setBusy(true);
    setError(undefined);
    setResult(undefined);
    void onSend({ key: key || undefined, value, headers: parsedHeaders.headers, partition: parsedPartition.partition })
      .then((sent) => {
        setResult(sent);
        // Every send is confirmed on its own (REQ-104): the draft stays, the confirmation does not.
        setConfirmed(false);
      })
      .catch((cause: unknown) => setError(cause instanceof Error ? cause.message : String(cause)))
      .finally(() => setBusy(false));
  };

  return (
    <aside className="KafkaComposePanel" data-testid="kafka-produce-message-drawer" aria-label="Produce message">
      <header className="KafkaComposeHeader">
        <Renderer.Component.Icon material="edit" />
        <div>
          <strong>Produce message</strong>
          <span>Write operations must be confirmed before any Kafka call is made.</span>
        </div>
        <Renderer.Component.Button
          plain
          round
          className="KafkaIconButton"
          title="Close the produce panel"
          aria-label="Close the produce panel"
          data-testid="kafka-produce-close"
          onClick={onClose}
        >
          <Renderer.Component.Icon material="close" />
        </Renderer.Component.Button>
      </header>
      <form
        className="KafkaComposeBody"
        onSubmit={(event) => {
          event.preventDefault();
          send();
        }}
      >
        <div className="KafkaWriteSummary">
          <strong>Target</strong>
          <span>{clusterName}</span>
          <strong>Topic</strong>
          <span>{topicName}</span>
        </div>
        <label className="KafkaComposeField">
          <span>Key (optional)</span>
          <Renderer.Component.Input value={key} onChange={setKey} aria-label="Message key" disabled={busy} />
        </label>
        <label className="KafkaComposeField KafkaComposeValue">
          <span>Value</span>
          <Renderer.Component.Input
            multiLine
            maxRows={14}
            value={value}
            onChange={setValue}
            aria-label="Message value"
            disabled={busy}
          />
        </label>
        <label className="KafkaComposeField">
          <span>Headers (key=value, one per line)</span>
          <Renderer.Component.Input
            multiLine
            maxRows={6}
            value={headersText}
            onChange={setHeadersText}
            aria-label="Message headers"
            aria-invalid={headersError ? "true" : undefined}
            disabled={busy}
          />
          {headersError && (
            <em className="KafkaComposeFieldError" role="alert" data-testid="kafka-produce-headers-error">
              {headersError}
            </em>
          )}
        </label>
        <label className="KafkaComposeField">
          <span>
            {partitionCount && partitionCount > 0
              ? `Partition (optional, 0 to ${partitionCount - 1})`
              : "Partition (optional)"}
          </span>
          <Renderer.Component.Input
            value={partitionText}
            onChange={setPartitionText}
            aria-label="Message partition"
            aria-invalid={parsedPartition.error ? "true" : undefined}
            disabled={busy}
          />
          {parsedPartition.error && (
            <em className="KafkaComposeFieldError" role="alert" data-testid="kafka-produce-partition-error">
              {parsedPartition.error}
            </em>
          )}
        </label>
        <label className="KafkaComposeConfirm">
          <span>{getWriteConfirmationLabel({ destructive: false, resourceName: topicName })}</span>
          <Renderer.Component.Switch
            aria-label="Confirm produce message"
            data-testid="kafka-produce-confirmation-switch"
            checked={confirmed}
            onChange={setConfirmed}
            disabled={busy}
          />
        </label>
        {result && (
          <div className="KafkaWriteStatus" role="status" data-testid="kafka-produce-result">
            <Renderer.Component.Icon material="check_circle" />
            <span>
              Sent to partition {result.partition} at offset {result.offset}
            </span>
          </div>
        )}
        {error && (
          <div className="KafkaComposeError" role="alert" data-testid="kafka-produce-error">
            {error}
          </div>
        )}
        <footer className="KafkaComposeActions">
          <Renderer.Component.Button outlined onClick={onClose}>
            {result ? "Close" : "Cancel"}
          </Renderer.Component.Button>
          <Renderer.Component.Button primary type="submit" disabled={!canSend} waiting={busy}>
            Send message
          </Renderer.Component.Button>
        </footer>
      </form>
    </aside>
  );
}
