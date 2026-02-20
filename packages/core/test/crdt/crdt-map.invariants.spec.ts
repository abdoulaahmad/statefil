import { describe, expect, it } from "vitest";
import { CRDTMap, type Mergeable } from "../../src/crdt";

class UnionValue implements Mergeable<UnionValue> {
  private readonly valuesSet: Set<string>;

  constructor(values: string[] = []) {
    this.valuesSet = new Set(values);
  }

  values(): string[] {
    return [...this.valuesSet].sort();
  }

  merge(other: UnionValue): UnionValue {
    return new UnionValue([...this.valuesSet, ...other.valuesSet]);
  }
}

function project(map: CRDTMap<UnionValue>, key: string): string[] {
  return map.get(key)?.values() ?? [];
}

describe("CRDTMap merge invariants", () => {
  it("is commutative when child CRDT merge is commutative", () => {
    const left = new CRDTMap<UnionValue>();
    const right = new CRDTMap<UnionValue>();

    left.set("k", new UnionValue(["a"]));
    right.set("k", new UnionValue(["b"]));

    const lr = left.merge(right);
    const rl = right.merge(left);

    expect(project(lr, "k")).toEqual(project(rl, "k"));
  });

  it("is associative when child CRDT merge is associative", () => {
    const a = new CRDTMap<UnionValue>();
    const b = new CRDTMap<UnionValue>();
    const c = new CRDTMap<UnionValue>();

    a.set("k", new UnionValue(["a"]));
    b.set("k", new UnionValue(["b"]));
    c.set("k", new UnionValue(["c"]));

    const leftAssoc = a.merge(b).merge(c);
    const rightAssoc = a.merge(b.merge(c));

    expect(project(leftAssoc, "k")).toEqual(project(rightAssoc, "k"));
  });

  it("is idempotent when child CRDT merge is idempotent", () => {
    const state = new CRDTMap<UnionValue>();
    state.set("k", new UnionValue(["v"]));

    const merged = state.merge(state);

    expect(project(merged, "k")).toEqual(project(state, "k"));
  });
});
