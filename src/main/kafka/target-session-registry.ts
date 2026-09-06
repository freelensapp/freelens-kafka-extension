interface TargetSessionEntry {
  generation: number;
  sessionKeys: Set<string>;
}

export class KafkaTargetSessionRegistry {
  private readonly entries = new Map<string, TargetSessionEntry>();

  begin(targetKey: string): number {
    return this.entry(targetKey).generation;
  }

  register(targetKey: string, generation: number, sessionKey: string): boolean {
    const entry = this.entry(targetKey);
    if (entry.generation !== generation) return false;
    entry.sessionKeys.add(sessionKey);
    return true;
  }

  isCurrent(targetKey: string, generation: number): boolean {
    return this.entry(targetKey).generation === generation;
  }

  invalidate(targetKey: string): string[] {
    const entry = this.entry(targetKey);
    const sessionKeys = [...entry.sessionKeys];
    entry.generation++;
    entry.sessionKeys.clear();
    return sessionKeys;
  }

  clear(): string[] {
    const sessionKeys = new Set<string>();
    for (const targetKey of this.entries.keys()) {
      for (const sessionKey of this.invalidate(targetKey)) sessionKeys.add(sessionKey);
    }
    return [...sessionKeys];
  }

  private entry(targetKey: string): TargetSessionEntry {
    let entry = this.entries.get(targetKey);
    if (!entry) {
      entry = { generation: 0, sessionKeys: new Set() };
      this.entries.set(targetKey, entry);
    }
    return entry;
  }
}
