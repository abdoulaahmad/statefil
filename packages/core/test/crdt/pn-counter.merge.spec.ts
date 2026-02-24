import { describe, expect, it } from "vitest";
import { PNCounter, SafePNCounter } from "../../src/crdt";

describe("PNCounter merge semantics", () => {
  it("keeps per-node maxima during merge", () => {
    const newer = new PNCounter("node-a");
    newer.increment(5);

    const stale = new PNCounter("node-b", {
      positive: { "node-a": 2 },
      negative: {}
    });

    const merged = newer.merge(stale);

    expect(merged.snapshot().positive["node-a"]).toBe(5);
    expect(merged.value()).toBe(5);
  });

  it("can go negative under concurrent decrements across replicas", () => {
    const seed = new PNCounter("seed");
    seed.increment(1);
    const baseline = seed.snapshot();

    const left = new PNCounter("node-a", baseline);
    const right = new PNCounter("node-b", baseline);

    left.decrement(1);
    right.decrement(1);

    const merged = left.merge(right);

    expect(merged.value()).toBe(-1);
  });
});

describe("SafePNCounter floor behavior", () => {
  it("rejects local decrements that would cross floor", () => {
    const safe = new SafePNCounter("node-a", 0);
    safe.increment(1);

    expect(safe.tryDecrement(1)).toBe(true);
    expect(safe.tryDecrement(1)).toBe(false);
    expect(() => safe.decrement(1)).toThrow("SafePNCounter floor would be violated");
    expect(safe.value()).toBe(0);
  });

  it("clamps merged value to floor under concurrent decrements", () => {
    const seed = new SafePNCounter("seed", 0);
    seed.increment(1);
    const baseline = seed.snapshot();

    const left = new SafePNCounter("node-a", 0, baseline);
    const right = new SafePNCounter("node-b", 0, baseline);

    expect(left.tryDecrement(1)).toBe(true);
    expect(right.tryDecrement(1)).toBe(true);

    const merged = left.merge(right);

    expect(merged.rawValue()).toBe(-1);
    expect(merged.value()).toBe(0);
    expect(merged.isFloorApplied()).toBe(true);
  });
});
