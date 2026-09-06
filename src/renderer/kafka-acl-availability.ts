import { observable, runInAction } from "mobx";

/**
 * Records which Kafka targets answered the DescribeAcls probe, so the ACL page is
 * offered only where it can actually work instead of as a permanent placeholder.
 */
export class KafkaAclAvailabilityStore {
  private readonly available = observable.map<string, boolean>();

  get(targetId: string): boolean {
    return this.available.get(targetId) ?? false;
  }

  hasAny(): boolean {
    return [...this.available.values()].some(Boolean);
  }

  set(targetId: string, value: boolean): void {
    if (!targetId || this.available.get(targetId) === value) return;
    runInAction(() => {
      this.available.set(targetId, value);
    });
  }
}
