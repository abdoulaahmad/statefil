import { describe, expect, it } from "vitest";
import { CRDTMap, type Mergeable } from "../../src/crdt";

class MergeableCounter implements Mergeable<MergeableCounter> {
  constructor(private readonly n: number) {}

  value(): number {
    return this.n;
  }

  merge(other: MergeableCounter): MergeableCounter {
    return new MergeableCounter(this.n + other.n);
  }
}

describe("CRDTMap merge semantics", () => {
  it("merges child CRDT values for same key instead of overwriting", () => {
    const left = new CRDTMap<MergeableCounter>();
    const right = new CRDTMap<MergeableCounter>();

    left.set("user:1", new MergeableCounter(1));
    right.set("user:1", new MergeableCounter(2));

    const merged = left.merge(right);
    const value = merged.get("user:1");

    expect(value).toBeDefined();
    expect(value?.value()).toBe(3);
  });
});
