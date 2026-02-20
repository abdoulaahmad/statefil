import type { Mergeable } from "./types";

export class CRDTMap<TValue extends Mergeable<TValue>> implements Mergeable<CRDTMap<TValue>> {
  private readonly entries: Map<string, TValue>;

  constructor(initial?: Map<string, TValue>) {
    this.entries = initial ? new Map(initial) : new Map();
  }

  get(key: string): TValue | undefined {
    return this.entries.get(key);
  }

  set(key: string, value: TValue): void {
    this.entries.set(key, value);
  }

  delete(key: string): void {
    this.entries.delete(key);
  }

  merge(other: CRDTMap<TValue>): CRDTMap<TValue> {
    const merged = new CRDTMap<TValue>(this.entries);

    for (const [key, otherValue] of other.entries) {
      const currentValue = merged.entries.get(key);
      if (!currentValue) {
        merged.entries.set(key, otherValue);
        continue;
      }

      // Core requirement: same-key CRDT values must be merged, never overwritten.
      merged.entries.set(key, currentValue.merge(otherValue));
    }

    return merged;
  }
}
