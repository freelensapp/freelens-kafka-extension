import { Renderer } from "@freelensapp/extensions";
import { useEffect } from "react";
import kafkaStyles from "./kafka-overview.scss?inline";

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

const KAFKA_STYLE_ID = "freelens-kafka-extension-styles";

function ensureKafkaStyles(): void {
  let style = document.getElementById(KAFKA_STYLE_ID) as HTMLStyleElement | null;
  if (!style) {
    style = document.createElement("style");
    style.id = KAFKA_STYLE_ID;
    document.head.append(style);
  }
  if (style.textContent !== kafkaStyles) style.textContent = kafkaStyles;
}

export function KafkaPageShell({ title, subtitle, actions, children }: KafkaPageShellProps) {
  useEffect(() => ensureKafkaStyles(), []);

  return (
    <div className="KafkaOverviewPage KafkaPageShell">
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
