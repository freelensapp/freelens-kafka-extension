import { useEffect, useState } from "react";

/**
 * Answer of the main process to the version probe (SPEC-016): its baked version, or the fact that
 * it has no such handler because it predates the handshake.
 */
export type MainVersionProbe = { version: string } | { missing: true };

/** The two halves of the extension run different builds until Freelens restarts. */
export interface KafkaVersionSkew {
  rendererVersion: string;
  /** Undefined when the main side is too old to say which version it is. */
  mainVersion?: string;
}

/** Compare the version of this renderer bundle with what the main process runs. Pure. */
export function detectVersionSkew(rendererVersion: string, probe: MainVersionProbe): KafkaVersionSkew | undefined {
  if ("missing" in probe) return { rendererVersion };
  if (probe.version === rendererVersion) return undefined;

  return { rendererVersion, mainVersion: probe.version };
}

/** Wording of the restart notice. Pure. */
export function describeVersionSkew(skew: KafkaVersionSkew): { title: string; detail: string } {
  const running = skew.mainVersion ? `version ${skew.mainVersion}` : "a previous version";

  return {
    title: "Restart Freelens to finish the Kafka extension update",
    detail: `These pages are version ${skew.rendererVersion}, but Freelens still runs the background part of ${running}: it stays loaded until the app restarts. Until then some actions can fail.`,
  };
}

type Listener = () => void;

/**
 * One check per frame: every Kafka page reads the same outcome. A probe that fails for any other
 * reason than a missing handler proves nothing, so it shows no notice.
 */
export class KafkaVersionSkewStore {
  private skew?: KafkaVersionSkew;
  private started = false;
  private readonly listeners = new Set<Listener>();

  get(): KafkaVersionSkew | undefined {
    return this.skew;
  }

  subscribe(listener: Listener): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  check(rendererVersion: string, probe: () => Promise<MainVersionProbe>): Promise<void> {
    if (this.started) return Promise.resolve();
    this.started = true;

    return probe()
      .then((answer) => {
        this.skew = detectVersionSkew(rendererVersion, answer);
        if (this.skew) for (const listener of this.listeners) listener();
      })
      .catch(() => undefined);
  }
}

export const kafkaVersionSkewStore = new KafkaVersionSkewStore();

export function useKafkaVersionSkew(
  store: KafkaVersionSkewStore = kafkaVersionSkewStore,
): KafkaVersionSkew | undefined {
  const [skew, setSkew] = useState(() => store.get());

  useEffect(() => {
    const update = () => setSkew(store.get());
    update();
    return store.subscribe(update);
  }, [store]);

  return skew;
}
