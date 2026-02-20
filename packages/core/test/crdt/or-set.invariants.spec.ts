import { describe, expect, it } from "vitest";
import { ORSet } from "../../src/crdt";

function sortedValues(set: ORSet<string>): string[] {
  return set.values().slice().sort();
}

describe("ORSet merge invariants", () => {
  it("is commutative for representative operations", () => {
    const left = new ORSet<string>();
    left.add("a", "a1");
    left.add("b", "b1");
    left.remove("b");

    const right = new ORSet<string>();
    right.add("b", "b2");
    right.add("c", "c1");

    const lr = left.merge(right);
    const rl = right.merge(left);

    expect(sortedValues(lr)).toEqual(sortedValues(rl));
  });

  it("is associative for representative operations", () => {
    const a = new ORSet<string>();
    a.add("x", "x1");

    const b = new ORSet<string>();
    b.add("x", "x2");
    b.add("y", "y1");

    const c = new ORSet<string>();
    c.add("y", "y2");
    c.remove("y");

    const leftAssoc = a.merge(b).merge(c);
    const rightAssoc = a.merge(b.merge(c));

    expect(sortedValues(leftAssoc)).toEqual(sortedValues(rightAssoc));
  });

  it("is idempotent", () => {
    const state = new ORSet<string>();
    state.add("session", "s1");
    state.add("memory", "m1");
    state.remove("session");

    const merged = state.merge(state);

    expect(sortedValues(merged)).toEqual(sortedValues(state));
  });
});
