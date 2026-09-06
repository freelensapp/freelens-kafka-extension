import { Renderer } from "@freelensapp/extensions";

export function KafkaIcon(props: Renderer.Component.IconProps) {
  return <Renderer.Component.Icon {...props} material="hub" tooltip="Kafka" />;
}
