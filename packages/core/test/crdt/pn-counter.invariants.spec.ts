import { describe, expect, it } from "vitest";
import { PNCounter } from "../../src/crdt";

describe("PNCounter merge invariants", () => {
  it("is commutative for representative operation sets", () => {
    const left = new PNCounter("node-a");
    left.increment(5);
    left.decrement(2);

    const right = new PNCounter("node-b");
    right.increment(3);
    right.decrement(1);

    const lr = left.merge(right);
    const rl = right.merge(left);

    expect(lr.snapshot()).toEqual(rl.snapshot());
    expect(lr.value()).toBe(rl.value());
  });

  it("is associative for representative operation sets", () => {
    const a = new PNCounter("node-a");
    a.increment(7);
    a.decrement(1);

    const b = new PNCounter("node-b");
    b.increment(2);

    const c = new PNCounter("node-c");
    c.decrement(4);

    const leftAssoc = a.merge(b).merge(c);
    const rightAssoc = a.merge(b.merge(c));

    expect(leftAssoc.snapshot()).toEqual(rightAssoc.snapshot());
    expect(leftAssoc.value()).toBe(rightAssoc.value());
  });

  it("is idempotent", () => {
    const state = new PNCounter("node-a");
    state.increment(4);
    state.decrement(2);

    const merged = state.merge(state);

    expect(merged.snapshot()).toEqual(state.snapshot());
    expect(merged.value()).toBe(state.value());
  });
});
