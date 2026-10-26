import { Renderer } from "@freelensapp/extensions";
import { describeVersionSkew, useKafkaVersionSkew } from "./kafka-version-skew";
// The extension's stylesheet: the build extracts it into dist/renderer.css, which the host links (C10).
import "./kafka-overview.scss";

import type { ReactNode } from "react";

import type { DiscoveredKafkaInfo } from "../common/ipc";

interface KafkaPageShellProps {
  title: string;
  subtitle: string;
  actions?: ReactNode;
  children: ReactNode;
}

interface KafkaClusterSelectorProps {
  clusters: DiscoveredKafkaInfo[];
  selectedTargetId?: string;
  onChange: (targetId: string) => void;
  disabled?: boolean;
}

export interface KafkaMetric {
  label: string;
  value: ReactNode;
  tone?: "warning" | "error";
}

interface KafkaMetricStripProps {
  ariaLabel: string;
  className?: string;
  metrics: KafkaMetric[];
}

/** Restart notice shown on every Kafka page while the two halves of the extension differ (SPEC-016). */
function KafkaVersionSkewNotice() {
  const skew = useKafkaVersionSkew();
  if (!skew) return null;
  const { title, detail } = describeVersionSkew(skew);

  return (
    <div className="KafkaVersionSkewNotice" role="alert" data-testid="kafka-version-skew-notice">
      <Renderer.Component.Icon material="restart_alt" />
      <div>
        <strong>{title}</strong>
        <span>{detail}</span>
      </div>
    </div>
  );
}

export function KafkaPageShell({ title, subtitle, actions, children }: KafkaPageShellProps) {
  return (
    <div className="KafkaOverviewPage KafkaPageShell">
      <KafkaVersionSkewNotice />
      <header className="KafkaPageHeader">
        <div>
          <h1>{title}</h1>
          <span>{subtitle}</span>
        </div>
        {actions && <div className="KafkaHeaderActions">{actions}</div>}
      </header>
      {children}
    </div>
  );
}

export function KafkaMetricStrip({ ariaLabel, className = "", metrics }: KafkaMetricStripProps) {
  return (
    <section className={`KafkaMetrics ${className}`.trim()} aria-label={ariaLabel} data-metric-count={metrics.length}>
      {metrics.map((metric) => (
        <div key={metric.label} className={`KafkaMetric ${metric.tone ?? ""}`.trim()}>
          <strong className="KafkaMetricValue">{metric.value}</strong>
          <span className="KafkaMetricLabel">{metric.label}</span>
        </div>
      ))}
    </section>
  );
}

export function KafkaClusterSelector({ clusters, selectedTargetId, onChange, disabled }: KafkaClusterSelectorProps) {
  const options: Renderer.Component.SelectOption<string>[] = clusters.map((cluster) => ({
    value: cluster.targetId,
    label: cluster.name,
  }));

  return (
    <label className="KafkaClusterSelector">
      <span>Kafka cluster</span>
      <Renderer.Component.Select
        options={options}
        value={selectedTargetId}
        onChange={(option: Renderer.Component.SelectOption<string> | null) => {
          if (option) onChange(option.value);
        }}
        isDisabled={disabled || options.length === 0}
        placeholder={options.length === 0 ? "No Kafka clusters" : "Select a Kafka cluster"}
        themeName="lens"
        menuPosition="fixed"
      />
    </label>
  );
}
